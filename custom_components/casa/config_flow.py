"""Config flow for Casa."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Mapping
from typing import Any

import aiohttp
import voluptuous as vol

from homeassistant.config_entries import (
    ConfigEntry,
    ConfigFlow,
    ConfigFlowResult,
    ConfigSubentryFlow,
    OptionsFlow,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.service_info.hassio import HassioServiceInfo

from .api import AuthenticationError, CasaApiClient
from .catalog import (
    CatalogValidationError,
    VoiceAgentCatalog,
    initial_subentry_data,
)
from .const import (
    CONF_HOST,
    CONF_IDLE_STABILITY_MS,
    CONF_PORT,
    CONF_SATELLITE_ENTITY_OVERRIDES,
    CONF_SESSION_MODE,
    CONF_TRANSPORT,
    CONF_WEBHOOK_SECRET,
    DEFAULT_IDLE_STABILITY_MS,
    DEFAULT_PORT,
    DEFAULT_SATELLITE_ENTITY_OVERRIDES,
    DEFAULT_SESSION_MODE,
    DEFAULT_TRANSPORT,
    DOMAIN,
    SESSION_MODE_CONVERSATION,
    SESSION_MODE_DEVICE,
    SESSION_MODE_USER,
    SUBENTRY_TYPE_AGENT,
    TRANSPORT_SSE,
    TRANSPORT_WS,
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

SESSION_MODE_OPTIONS = {
    SESSION_MODE_DEVICE: "Per device",
    SESSION_MODE_USER: "Per user",
    SESSION_MODE_CONVERSATION: "Per conversation",
}
TRANSPORT_OPTIONS = {
    TRANSPORT_WS: "WebSocket",
    TRANSPORT_SSE: "SSE",
}


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


def _agent_reconfigure_schema(values: Mapping[str, Any]) -> vol.Schema:
    """Build the schema for mutable per-agent settings only."""
    return vol.Schema({
        vol.Required(
            CONF_SESSION_MODE,
            default=values.get(CONF_SESSION_MODE, DEFAULT_SESSION_MODE),
        ): vol.In(SESSION_MODE_OPTIONS),
        vol.Required(
            CONF_TRANSPORT,
            default=values.get(CONF_TRANSPORT, DEFAULT_TRANSPORT),
        ): vol.In(TRANSPORT_OPTIONS),
        vol.Required(
            CONF_IDLE_STABILITY_MS,
            default=values.get(
                CONF_IDLE_STABILITY_MS,
                DEFAULT_IDLE_STABILITY_MS,
            ),
        ): vol.All(int, vol.Range(min=0, max=5000)),
    })


async def _fetch_catalog(
    hass: HomeAssistant,
    data: Mapping[str, Any],
) -> VoiceAgentCatalog:
    """Fetch Casa's authenticated catalog and release transient resources."""
    client = CasaApiClient(
        session=async_get_clientsession(hass),
        host=data[CONF_HOST],
        port=data[CONF_PORT],
        webhook_secret=data[CONF_WEBHOOK_SECRET],
    )
    try:
        return await client.fetch_voice_agents()
    finally:
        await client.close()


class CasaConfigFlow(ConfigFlow, domain=DOMAIN):
    """Create one Casa parent and catalog-owned voice-agent children."""

    VERSION = 2

    _host: str = ""
    _port: int = DEFAULT_PORT
    _secret: str = ""
    _discovery_name: str = ""

    async def async_step_user(
        self,
        user_input: dict | None = None,
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            self._async_abort_entries_match({
                CONF_HOST: user_input[CONF_HOST],
                CONF_PORT: user_input[CONF_PORT],
            })
            try:
                catalog = await _fetch_catalog(self.hass, user_input)
            except AuthenticationError:
                errors["base"] = "invalid_auth"
            except CatalogValidationError:
                errors["base"] = "invalid_catalog"
            except (aiohttp.ClientError, asyncio.TimeoutError):
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.error("Casa setup state=failed reason=unexpected")
                errors["base"] = "unknown"
            else:
                return self.async_create_entry(
                    title="Casa",
                    data=user_input,
                    subentries=initial_subentry_data(catalog),
                )

        return self.async_show_form(
            step_id="user",
            data_schema=USER_SCHEMA,
            errors=errors,
        )

    async def async_step_hassio(
        self,
        discovery_info: HassioServiceInfo,
    ) -> ConfigFlowResult:
        self._host = discovery_info.config["host"]
        self._port = discovery_info.config["port"]
        self._secret = (
            discovery_info.config.get("webhook_secret")
            or discovery_info.config.get("token", "")
        )
        self._discovery_name = discovery_info.name

        self._async_abort_entries_match({
            CONF_HOST: self._host,
            CONF_PORT: self._port,
        })
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
        self,
        user_input: dict | None = None,
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            data = {
                CONF_HOST: self._host,
                CONF_PORT: self._port,
                CONF_WEBHOOK_SECRET: self._secret,
            }
            for attempt in range(3):
                try:
                    catalog = await _fetch_catalog(self.hass, data)
                except AuthenticationError:
                    errors["base"] = "invalid_auth"
                    break
                except CatalogValidationError:
                    errors["base"] = "invalid_catalog"
                    break
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    if attempt < 2:
                        await _sleep(2)
                        continue
                    errors["base"] = "cannot_connect"
                    break
                except Exception:
                    _LOGGER.error(
                        "Casa discovery state=failed reason=unexpected",
                    )
                    errors["base"] = "unknown"
                    break
                else:
                    return self.async_create_entry(
                        title=self._discovery_name,
                        data=data,
                        subentries=initial_subentry_data(catalog),
                    )

        return self.async_show_form(
            step_id="hassio_confirm",
            description_placeholders={"name": self._discovery_name},
            errors=errors,
        )

    async def async_step_reauth(self, entry_data: dict) -> ConfigFlowResult:
        """Start reauthentication after a catalog request rejects the secret."""
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self,
        user_input: dict | None = None,
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            reauth_entry = self._get_reauth_entry()
            replacement_data = {
                CONF_HOST: reauth_entry.data[CONF_HOST],
                CONF_PORT: reauth_entry.data[CONF_PORT],
                CONF_WEBHOOK_SECRET: user_input[CONF_WEBHOOK_SECRET],
            }
            try:
                await _fetch_catalog(self.hass, replacement_data)
            except AuthenticationError:
                errors["base"] = "invalid_auth"
            except CatalogValidationError:
                errors["base"] = "invalid_catalog"
            except (aiohttp.ClientError, asyncio.TimeoutError):
                errors["base"] = "cannot_connect"
            except Exception:
                _LOGGER.error("Casa reauth state=failed reason=unexpected")
                errors["base"] = "unknown"
            else:
                secret = user_input[CONF_WEBHOOK_SECRET]
                updates = {CONF_WEBHOOK_SECRET: secret}
                if not reauth_entry.update_listeners:
                    return self.async_update_reload_and_abort(
                        reauth_entry,
                        data_updates=updates,
                    )
                unchanged = (
                    reauth_entry.data.get(CONF_WEBHOOK_SECRET) == secret
                )
                result = self.async_update_and_abort(
                    reauth_entry,
                    data_updates=updates,
                )
                if unchanged:
                    self.hass.config_entries.async_schedule_reload(
                        reauth_entry.entry_id,
                    )
                return result

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=REAUTH_SCHEMA,
            errors=errors,
        )

    @classmethod
    @callback
    def async_get_supported_subentry_types(
        cls,
        config_entry: ConfigEntry,
    ) -> dict[str, type[ConfigSubentryFlow]]:
        """Register the catalog-owned agent reconfigure flow."""
        return {SUBENTRY_TYPE_AGENT: CasaAgentSubentryFlow}

    @staticmethod
    @callback
    def async_get_options_flow(config_entry: ConfigEntry) -> CasaOptionsFlow:
        return CasaOptionsFlow()


class CasaOptionsFlow(OptionsFlow):
    """Configure parent-wide satellite disambiguation only."""

    async def async_step_init(
        self,
        user_input: dict | None = None,
    ) -> ConfigFlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                overrides = _canonical_satellite_overrides(
                    self.hass,
                    user_input.get(
                        CONF_SATELLITE_ENTITY_OVERRIDES,
                        DEFAULT_SATELLITE_ENTITY_OVERRIDES,
                    ),
                )
            except ValueError:
                errors[CONF_SATELLITE_ENTITY_OVERRIDES] = (
                    "invalid_satellite_entity_overrides"
                )
            else:
                return self.async_create_entry(data={
                    CONF_SATELLITE_ENTITY_OVERRIDES: overrides,
                })

        suggested_values = (
            user_input
            if user_input is not None
            else self.config_entry.options
        )
        return self.async_show_form(
            step_id="init",
            data_schema=self.add_suggested_values_to_schema(
                vol.Schema({
                    vol.Optional(
                        CONF_SATELLITE_ENTITY_OVERRIDES,
                        default=DEFAULT_SATELLITE_ENTITY_OVERRIDES,
                    ): str,
                }),
                suggested_values,
            ),
            errors=errors,
        )


class CasaAgentSubentryFlow(ConfigSubentryFlow):
    """Reconfigure mutable settings for one catalog-owned voice agent."""

    async def async_step_user(
        self,
        user_input: dict | None = None,
    ) -> ConfigFlowResult:
        """Prevent users from creating roles Casa did not advertise."""
        return self.async_abort(reason="catalog_managed")

    async def async_step_reconfigure(
        self,
        user_input: dict | None = None,
    ) -> ConfigFlowResult:
        """Update only the mutable settings and preserve catalog identity."""
        subentry = self._get_reconfigure_subentry()
        errors: dict[str, str] = {}
        if user_input is not None:
            stability_ms = user_input[CONF_IDLE_STABILITY_MS]
            if type(stability_ms) is not int or not 0 <= stability_ms <= 5000:
                errors[CONF_IDLE_STABILITY_MS] = "invalid_idle_stability"
            else:
                updated_data = {
                    **subentry.data,
                    CONF_SESSION_MODE: user_input[CONF_SESSION_MODE],
                    CONF_TRANSPORT: user_input[CONF_TRANSPORT],
                    CONF_IDLE_STABILITY_MS: stability_ms,
                }
                return self.async_update_and_abort(
                    self._get_entry(),
                    subentry,
                    data=updated_data,
                )

        return self.async_show_form(
            step_id="reconfigure",
            data_schema=_agent_reconfigure_schema(
                user_input if user_input is not None else subentry.data,
            ),
            errors=errors,
        )
