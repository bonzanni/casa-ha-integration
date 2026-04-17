"""Tests for Casa config flow."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from custom_components.casa.const import (
    CONF_AGENT_ROLE, CONF_HOST, CONF_PORT, CONF_SESSION_MODE,
    CONF_TRANSPORT, CONF_WEBHOOK_SECRET, DEFAULT_AGENT_ROLE,
    DEFAULT_PORT, DEFAULT_SESSION_MODE, DEFAULT_TRANSPORT,
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
    @pytest.mark.asyncio
    async def test_options_form_shown(self):
        flow = CasaOptionsFlow()
        result = await flow.async_step_init(None)
        assert result["type"] == "form"

    @pytest.mark.asyncio
    async def test_options_saved(self):
        flow = CasaOptionsFlow()
        result = await flow.async_step_init({
            CONF_AGENT_ROLE: "butler",
            CONF_SESSION_MODE: "user",
            CONF_TRANSPORT: "sse",
        })
        assert result["type"] == "create_entry"
        assert result["data"][CONF_TRANSPORT] == "sse"
