"""Tests for Casa config flow."""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from custom_components.casa.const import (
    CONF_AGENT_ROLE, CONF_HOST, CONF_PORT, CONF_SESSION_MODE,
    CONF_IDLE_STABILITY_MS, CONF_SATELLITE_ENTITY_OVERRIDES,
    CONF_TRANSPORT, CONF_WEBHOOK_SECRET, DEFAULT_IDLE_STABILITY_MS,
    DEFAULT_SATELLITE_ENTITY_OVERRIDES,
)
from custom_components.casa.config_flow import (
    CasaConfigFlow, CasaOptionsFlow,
)


class TestUserStep:
    @pytest.mark.asyncio
    async def test_form_shown_when_no_input(self):
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        result = await flow.async_step_user(None)
        assert result["type"] == "form"
        assert result["step_id"] == "user"

    @pytest.mark.asyncio
    async def test_creates_entry_on_valid_input(self):
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        with patch("custom_components.casa.config_flow.CasaApiClient") as mock_cls:
            client = MagicMock()
            client.health_check = AsyncMock(return_value=True)
            client.probe_auth = AsyncMock()
            mock_cls.return_value = client
            result = await flow.async_step_user({
                CONF_HOST: "1.1.1.1", CONF_PORT: 18065, CONF_WEBHOOK_SECRET: "s",
            })
        assert result["type"] == "create_entry"
        assert result["title"] == "Casa"
        assert result["data"][CONF_HOST] == "1.1.1.1"

    @pytest.mark.asyncio
    async def test_cannot_connect_shows_error(self):
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        with patch("custom_components.casa.config_flow.CasaApiClient") as mock_cls:
            client = MagicMock()
            client.health_check = AsyncMock(side_effect=aiohttp.ClientError("x"))
            mock_cls.return_value = client
            result = await flow.async_step_user({
                CONF_HOST: "1.1.1.1", CONF_PORT: 18065, CONF_WEBHOOK_SECRET: "s",
            })
        assert result["type"] == "form"
        assert result["errors"]["base"] == "cannot_connect"

    @pytest.mark.asyncio
    async def test_invalid_auth_shows_error(self):
        from custom_components.casa.api import AuthenticationError
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        with patch("custom_components.casa.config_flow.CasaApiClient") as mock_cls:
            client = MagicMock()
            client.health_check = AsyncMock(return_value=True)
            client.probe_auth = AsyncMock(side_effect=AuthenticationError("bad"))
            mock_cls.return_value = client
            result = await flow.async_step_user({
                CONF_HOST: "1.1.1.1", CONF_PORT: 18065, CONF_WEBHOOK_SECRET: "s",
            })
        assert result["type"] == "form"
        assert result["errors"]["base"] == "invalid_auth"

    @pytest.mark.asyncio
    async def test_unexpected_setup_error_log_omits_exception_details(self, caplog):
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        canary = "PRIVATE_SETUP_EXCEPTION_CANARY"
        with patch("custom_components.casa.config_flow.CasaApiClient") as mock_cls:
            client = MagicMock()
            client.health_check = AsyncMock(side_effect=RuntimeError(canary))
            mock_cls.return_value = client
            with caplog.at_level(logging.ERROR, logger="custom_components.casa.config_flow"):
                result = await flow.async_step_user({
                    CONF_HOST: "1.1.1.1",
                    CONF_PORT: 18065,
                    CONF_WEBHOOK_SECRET: "s",
                })

        assert result["errors"]["base"] == "unknown"
        assert canary not in caplog.text
        assert all(record.exc_info is None for record in caplog.records)


class TestReauth:
    @pytest.mark.asyncio
    async def test_reauth_confirm_form(self):
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        result = await flow.async_step_reauth_confirm(None)
        assert result["type"] == "form"

    @pytest.mark.asyncio
    async def test_reauth_success(self):
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        reauth_entry = MagicMock()
        reauth_entry.data = {CONF_HOST: "h", CONF_PORT: 1, CONF_WEBHOOK_SECRET: "old"}
        flow._get_reauth_entry = MagicMock(return_value=reauth_entry)
        with patch("custom_components.casa.config_flow.CasaApiClient") as mock_cls:
            client = MagicMock()
            client.health_check = AsyncMock(return_value=True)
            client.probe_auth = AsyncMock()
            mock_cls.return_value = client
            result = await flow.async_step_reauth_confirm({CONF_WEBHOOK_SECRET: "new"})
        assert result["type"] == "abort"
        assert result["reason"] == "reauth_successful"


class TestOptions:
    @staticmethod
    def _input(**changes):
        return {
            CONF_AGENT_ROLE: "butler",
            CONF_SESSION_MODE: "user",
            CONF_TRANSPORT: "ws",
            CONF_IDLE_STABILITY_MS: DEFAULT_IDLE_STABILITY_MS,
            CONF_SATELLITE_ENTITY_OVERRIDES: DEFAULT_SATELLITE_ENTITY_OVERRIDES,
            **changes,
        }

    @pytest.mark.asyncio
    async def test_options_form_shown(self):
        flow = CasaOptionsFlow()
        result = await flow.async_step_init(None)
        assert result["type"] == "form"

    @pytest.mark.asyncio
    async def test_options_saved(self):
        flow = CasaOptionsFlow()
        result = await flow.async_step_init(self._input(**{CONF_TRANSPORT: "sse"}))
        assert result["type"] == "create_entry"
        assert result["data"][CONF_TRANSPORT] == "sse"

    @pytest.mark.asyncio
    async def test_valid_override_is_registry_checked_and_canonicalized(self):
        from homeassistant.helpers import entity_registry as er

        registry = MagicMock()
        registry.async_get.return_value = MagicMock(device_id="dev-k")
        er.async_get = MagicMock(return_value=registry)
        flow = CasaOptionsFlow()
        flow.hass = MagicMock()

        result = await flow.async_step_init(self._input(**{
            CONF_SATELLITE_ENTITY_OVERRIDES: (
                '{ "dev-k" : "assist_satellite.kitchen" }'
            ),
        }))

        assert result["type"] == "create_entry"
        assert result["data"][CONF_SATELLITE_ENTITY_OVERRIDES] == (
            '{"dev-k":"assist_satellite.kitchen"}'
        )
        registry.async_get.assert_called_once_with("assist_satellite.kitchen")

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "raw",
        [
            "not-json",
            "[]",
            '{"dev-k":7}',
        ],
    )
    async def test_malformed_non_object_and_non_string_override_json_is_rejected(self, raw):
        flow = CasaOptionsFlow()
        flow.hass = MagicMock()

        result = await flow.async_step_init(self._input(**{
            CONF_SATELLITE_ENTITY_OVERRIDES: raw,
        }))

        assert result["type"] == "form"
        assert result["errors"][CONF_SATELLITE_ENTITY_OVERRIDES] == (
            "invalid_satellite_entity_overrides"
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("entity_id", "registry_device"),
        [
            ("light.kitchen", "dev-k"),
            ("assist_satellite.stale", None),
            ("assist_satellite.kitchen", "dev-other"),
        ],
    )
    async def test_wrong_domain_stale_and_wrong_device_override_are_rejected(
        self, entity_id, registry_device,
    ):
        from homeassistant.helpers import entity_registry as er

        registry = MagicMock()
        registry.async_get.return_value = (
            None if registry_device is None else MagicMock(device_id=registry_device)
        )
        er.async_get = MagicMock(return_value=registry)
        flow = CasaOptionsFlow()
        flow.hass = MagicMock()

        result = await flow.async_step_init(self._input(**{
            CONF_SATELLITE_ENTITY_OVERRIDES: f'{{"dev-k":"{entity_id}"}}',
        }))

        assert result["type"] == "form"
        assert result["errors"][CONF_SATELLITE_ENTITY_OVERRIDES] == (
            "invalid_satellite_entity_overrides"
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("stability_ms", [-1, 5001, True])
    async def test_idle_stability_outside_integer_range_is_rejected(self, stability_ms):
        flow = CasaOptionsFlow()
        flow.hass = MagicMock()

        result = await flow.async_step_init(self._input(**{
            CONF_IDLE_STABILITY_MS: stability_ms,
        }))

        assert result["type"] == "form"
        assert result["errors"][CONF_IDLE_STABILITY_MS] == "invalid_idle_stability"


class TestHassio:
    @pytest.mark.asyncio
    async def test_hassio_discovery_happy(self):
        from homeassistant.helpers.service_info.hassio import HassioServiceInfo
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        info = HassioServiceInfo(
            config={"host": "h", "port": 18065, "webhook_secret": "s"},
            name="Casa Butler",
            slug="casa_butler",
            uuid="uuid-1",
        )
        result = await flow.async_step_hassio(info)
        assert result["type"] == "form"
        assert result["step_id"] == "hassio_confirm"

    @pytest.mark.asyncio
    async def test_hassio_confirm_creates_entry(self):
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        flow._host = "h"
        flow._port = 18065
        flow._secret = "s"
        flow._discovery_name = "Casa Butler"
        with patch("custom_components.casa.config_flow._validate_connection", AsyncMock()):
            result = await flow.async_step_hassio_confirm({"confirm": True})
        assert result["type"] == "create_entry"
        assert result["title"] == "Casa Butler"

    @pytest.mark.asyncio
    async def test_hassio_confirm_retries_on_connection_error(self, monkeypatch):
        flow = CasaConfigFlow()
        flow.hass = MagicMock()
        flow._host = "h"
        flow._port = 18065
        flow._secret = "s"
        flow._discovery_name = "Casa Butler"

        calls = {"n": 0}

        async def flaky(hass, data):
            calls["n"] += 1
            raise aiohttp.ClientError("nope")

        async def _no_sleep(*a, **kw):
            return None

        monkeypatch.setattr("custom_components.casa.config_flow._validate_connection", flaky)
        monkeypatch.setattr("custom_components.casa.config_flow._sleep", _no_sleep)
        result = await flow.async_step_hassio_confirm({"confirm": True})
        assert calls["n"] == 3
        assert result["type"] == "form"
        assert result["errors"]["base"] == "cannot_connect"
