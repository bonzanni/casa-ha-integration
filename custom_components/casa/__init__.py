"""Casa integration for Home Assistant."""

from __future__ import annotations

import asyncio
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
    CONF_HOST, CONF_PORT, CONF_TRANSPORT, CONF_WEBHOOK_SECRET,
    DEFAULT_TRANSPORT,
)
from .registration import SessionRegistrationListener

_LOGGER = logging.getLogger(__name__)

PLATFORMS = [Platform.CONVERSATION]


@dataclass
class CasaRuntimeData:
    client: CasaApiClient
    listener: SessionRegistrationListener


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
    listener = SessionRegistrationListener(hass, client, transport)
    listener.attach()

    entry.runtime_data = CasaRuntimeData(client=client, listener=listener)
    entry.async_on_unload(entry.add_update_listener(_async_options_updated))
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def _async_options_updated(hass: HomeAssistant, entry: ConfigEntry) -> None:
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if entry.runtime_data is not None:
        entry.runtime_data.listener.detach()
        await entry.runtime_data.client.close()
    return ok
