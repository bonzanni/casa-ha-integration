"""Casa integration for Home Assistant."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass

import aiohttp

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed, ConfigEntryNotReady
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .api import AuthenticationError, CasaApiClient
from .const import (
    CONF_AGENT_ROLE, CONF_HOST, CONF_IDLE_STABILITY_MS, CONF_PORT,
    CONF_SATELLITE_ENTITY_OVERRIDES, CONF_TRANSPORT, CONF_WEBHOOK_SECRET,
    DEFAULT_AGENT_ROLE, DEFAULT_IDLE_STABILITY_MS,
    DEFAULT_SATELLITE_ENTITY_OVERRIDES, DEFAULT_TRANSPORT, TRANSPORT_WS,
)
from .delivery import BackgroundDeliveryManager, SatelliteDirectory
from .registration import SessionRegistrationListener

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [Platform.CONVERSATION]


@dataclass
class CasaRuntimeData:
    client: CasaApiClient
    listener: SessionRegistrationListener
    directory: SatelliteDirectory
    manager: BackgroundDeliveryManager


def _configured_overrides(entry: ConfigEntry) -> dict[str, str]:
    raw = entry.options.get(
        CONF_SATELLITE_ENTITY_OVERRIDES,
        DEFAULT_SATELLITE_ENTITY_OVERRIDES,
    )
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    if not isinstance(parsed, dict) or not all(
        isinstance(device_id, str) and isinstance(entity_id, str)
        for device_id, entity_id in parsed.items()
    ):
        return {}
    return parsed


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    client = CasaApiClient(
        session=async_get_clientsession(hass),
        host=entry.data[CONF_HOST],
        port=entry.data[CONF_PORT],
        webhook_secret=entry.data[CONF_WEBHOOK_SECRET],
    )
    try:
        async with asyncio.timeout(5):
            alive = await client.health_check()
    except (aiohttp.ClientError, asyncio.TimeoutError) as err:
        raise ConfigEntryNotReady("Casa add-on not reachable") from err
    except AuthenticationError as err:
        raise ConfigEntryAuthFailed("Invalid Casa webhook secret") from err
    if not alive:
        raise ConfigEntryNotReady("Casa health check failed")

    transport = entry.options.get(CONF_TRANSPORT, DEFAULT_TRANSPORT)
    agent_role = entry.options.get(CONF_AGENT_ROLE, DEFAULT_AGENT_ROLE)
    directory = SatelliteDirectory(overrides=_configured_overrides(entry))
    manager = BackgroundDeliveryManager(
        hass,
        client,
        route_id=entry.entry_id,
        directory=directory,
        idle_stability_ms=entry.options.get(
            CONF_IDLE_STABILITY_MS,
            DEFAULT_IDLE_STABILITY_MS,
        ),
    )
    listener = SessionRegistrationListener(
        hass,
        client,
        transport,
        agent_role=agent_role,
        directory=directory,
    )
    try:
        listener.attach()
        if transport == TRANSPORT_WS:
            await client.start_background(
                route_id=entry.entry_id,
                agent_role=agent_role,
                job_handler=manager.handle_frame,
            )

        entry.runtime_data = CasaRuntimeData(
            client=client,
            listener=listener,
            directory=directory,
            manager=manager,
        )
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    except BaseException:
        entry.runtime_data = None
        try:
            listener.detach()
        finally:
            try:
                await manager.close()
            finally:
                await client.close()
        raise
    entry.async_on_unload(entry.add_update_listener(_async_options_updated))
    return True


async def _async_options_updated(hass: HomeAssistant, entry: ConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if not ok:
        return False
    if entry.runtime_data is not None:
        try:
            entry.runtime_data.listener.detach()
        finally:
            try:
                await entry.runtime_data.manager.close()
            finally:
                await entry.runtime_data.client.close()
    return True
