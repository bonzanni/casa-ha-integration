"""Config flow for Casa."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import aiohttp
import voluptuous as vol

from homeassistant.config_entries import (
    ConfigEntry, ConfigFlow, ConfigFlowResult, OptionsFlow,
)
from homeassistant.core import callback
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.service_info.hassio import HassioServiceInfo

from .api import AuthenticationError, CasaApiClient
from .const import (
    CONF_AGENT_ROLE, CONF_HOST, CONF_IDLE_STABILITY_MS, CONF_PORT,
    CONF_SATELLITE_ENTITY_OVERRIDES, CONF_SESSION_MODE,
    CONF_TRANSPORT, CONF_WEBHOOK_SECRET, DEFAULT_AGENT_ROLE,
    DEFAULT_IDLE_STABILITY_MS, DEFAULT_PORT, DEFAULT_SATELLITE_ENTITY_OVERRIDES,
    DEFAULT_SESSION_MODE, DEFAULT_TRANSPORT, DOMAIN,
    SESSION_MODE_CONVERSATION, SESSION_MODE_DEVICE, SESSION_MODE_USER,
    TRANSPORT_SSE, TRANSPORT_WS,
)

_LOGGER = logging.getLogger(__name__)
_sleep = asyncio.sleep

USER_SCHEMA = vol.Schema({
    vol.Required(CONF_HOST, default="localhost"): str,
    vol.Required(CONF_PORT, default=DEFAULT_PORT): int,
    vol.Required(CONF_WEBHOOK_SECRET): str,
})

REAUTH_SCHEMA = vol.Schema({
    vol.Required(CONF_WEBHOOK_SECRET): str,
})


def _canonical_satellite_overrides(hass: Any, raw: Any) -> str:
    """Validate exact current registry bindings and return compact JSON."""
    if not isinstance(raw, str):
        raise ValueError
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as err:
        raise ValueError from err
    if not isinstance(parsed, dict):
        raise ValueError
    if not parsed:
        return DEFAULT_SATELLITE_ENTITY_OVERRIDES
    registry = er.async_get(hass)
    for device_id, entity_id in parsed.items():
        if (
            not isinstance(device_id, str)
            or not device_id
            or not isinstance(entity_id, str)
            or not entity_id.startswith("assist_satellite.")
        ):
            raise ValueError
        entry = registry.async_get(entity_id)
        if entry is None or entry.device_id != device_id:
            raise ValueError
    return json.dumps(parsed, sort_keys=True, separators=(",", ":"))


async def _validate_connection(hass, data: dict) -> None:
    client = CasaApiClient(
        session=async_get_clientsession(hass),
        host=data[CONF_HOST],
        port=data[CONF_PORT],
        webhook_secret=data[CONF_WEBHOOK_SECRET],
    )
    if not await client.health_check():
        raise aiohttp.ClientError("health failed")
    await client.probe_auth()


class CasaConfigFlow(ConfigFlow, domain=DOMAIN):
    VERSION = 1

    _host: str = ""
    _port: int = DEFAULT_PORT
    _secret: str = ""
    _discovery_name: str = ""

    async def async_step_user(self, user_input: dict | None = None) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                await _validate_connection(self.hass, user_input)
            except AuthenticationError:
                errors["base"] = "invalid_auth"
            except (aiohttp.ClientError, asyncio.TimeoutError):
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.error("Casa setup state=failed reason=unexpected")
                errors["base"] = "unknown"
            else:
                return self.async_create_entry(title="Casa", data=user_input)

        return self.async_show_form(
            step_id="user", data_schema=USER_SCHEMA, errors=errors,
        )

    async def async_step_hassio(
        self, discovery_info: HassioServiceInfo,
    ) -> ConfigFlowResult:
        self._host = discovery_info.config["host"]
        self._port = discovery_info.config["port"]
        self._secret = discovery_info.config.get("webhook_secret") or discovery_info.config.get("token", "")
        self._discovery_name = discovery_info.name

        await self.async_set_unique_id(discovery_info.uuid)
        self._abort_if_unique_id_configured()

        self.context.update({
            "title_placeholders": {"name": discovery_info.name},
            "configuration_url": (
                f"homeassistant://hassio/addon/{discovery_info.slug}/info"
            ),
        })
        return await self.async_step_hassio_confirm()

    async def async_step_hassio_confirm(
        self, user_input: dict | None = None,
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            last_error: Exception | None = None
            for attempt in range(3):
                try:
                    await _validate_connection(self.hass, {
                        CONF_HOST: self._host,
                        CONF_PORT: self._port,
                        CONF_WEBHOOK_SECRET: self._secret,
                    })
                    last_error = None
                    break
                except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                    last_error = err
                    if attempt < 2:
                        await _sleep(2)
            if last_error is not None:
                errors["base"] = "cannot_connect"
            else:
                return self.async_create_entry(
                    title=self._discovery_name,
                    data={
                        CONF_HOST: self._host,
                        CONF_PORT: self._port,
                        CONF_WEBHOOK_SECRET: self._secret,
                    },
                )

        return self.async_show_form(
            step_id="hassio_confirm",
            description_placeholders={"name": self._discovery_name},
            errors=errors,
        )

    async def async_step_reauth(self, entry_data: dict) -> ConfigFlowResult:
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict | None = None,
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            reauth_entry = self._get_reauth_entry()
            try:
                await _validate_connection(self.hass, {
                    CONF_HOST: reauth_entry.data[CONF_HOST],
                    CONF_PORT: reauth_entry.data[CONF_PORT],
                    CONF_WEBHOOK_SECRET: user_input[CONF_WEBHOOK_SECRET],
                })
            except AuthenticationError:
                errors["base"] = "invalid_auth"
            except (aiohttp.ClientError, asyncio.TimeoutError):
                errors["base"] = "cannot_connect"
            else:
                return self.async_update_reload_and_abort(
                    reauth_entry,
                    data_updates={CONF_WEBHOOK_SECRET: user_input[CONF_WEBHOOK_SECRET]},
                )

        return self.async_show_form(
            step_id="reauth_confirm", data_schema=REAUTH_SCHEMA, errors=errors,
        )

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> CasaOptionsFlow:
        return CasaOptionsFlow()


class CasaOptionsFlow(OptionsFlow):
    async def async_step_init(self, user_input: dict | None = None) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            data = dict(user_input)
            stability_ms = data.get(
                CONF_IDLE_STABILITY_MS,
                DEFAULT_IDLE_STABILITY_MS,
            )
            if (
                type(stability_ms) is not int
                or not 0 <= stability_ms <= 5000
            ):
                errors[CONF_IDLE_STABILITY_MS] = "invalid_idle_stability"
            else:
                data[CONF_IDLE_STABILITY_MS] = stability_ms
            try:
                data[CONF_SATELLITE_ENTITY_OVERRIDES] = (
                    _canonical_satellite_overrides(
                        self.hass,
                        data.get(
                            CONF_SATELLITE_ENTITY_OVERRIDES,
                            DEFAULT_SATELLITE_ENTITY_OVERRIDES,
                        ),
                    )
                )
            except ValueError:
                errors[CONF_SATELLITE_ENTITY_OVERRIDES] = (
                    "invalid_satellite_entity_overrides"
                )
            if not errors:
                return self.async_create_entry(data=data)

        return self.async_show_form(
            step_id="init",
            data_schema=self.add_suggested_values_to_schema(
                vol.Schema({
                    vol.Optional(CONF_AGENT_ROLE, default=DEFAULT_AGENT_ROLE): str,
                    vol.Optional(
                        CONF_IDLE_STABILITY_MS,
                        default=DEFAULT_IDLE_STABILITY_MS,
                    ): vol.All(int, vol.Range(min=0, max=5000)),
                    vol.Optional(
                        CONF_SATELLITE_ENTITY_OVERRIDES,
                        default=DEFAULT_SATELLITE_ENTITY_OVERRIDES,
                    ): str,
                    vol.Optional(CONF_SESSION_MODE, default=DEFAULT_SESSION_MODE):
                        vol.In({
                            SESSION_MODE_DEVICE: "Per device",
                            SESSION_MODE_USER: "Per user",
                            SESSION_MODE_CONVERSATION: "Per conversation",
                        }),
                    vol.Optional(CONF_TRANSPORT, default=DEFAULT_TRANSPORT):
                        vol.In({TRANSPORT_WS: "WebSocket", TRANSPORT_SSE: "SSE"}),
                }),
                user_input if user_input is not None else self.config_entry.options,
            ),
            errors=errors,
        )
