"""Tests for Casa __init__ setup wiring."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from custom_components.casa import async_setup_entry, async_unload_entry
from custom_components.casa.const import (
    CONF_HOST, CONF_PORT, CONF_WEBHOOK_SECRET,
    CONF_TRANSPORT, DEFAULT_TRANSPORT,
)


def _entry():
    entry = MagicMock()
    entry.entry_id = "entry-1"
    entry.data = {CONF_HOST: "127.0.0.1", CONF_PORT: 18065, CONF_WEBHOOK_SECRET: "s"}
    entry.options = {CONF_TRANSPORT: DEFAULT_TRANSPORT}
    entry.runtime_data = None
    entry.async_on_unload = MagicMock()
    entry.add_update_listener = MagicMock(return_value=lambda: None)
    return entry


class TestSetup:
    @pytest.mark.asyncio
    async def test_setup_happy(self):
        hass = MagicMock()
        hass.config_entries.async_forward_entry_setups = AsyncMock()
        entry = _entry()
        with patch("custom_components.casa.CasaApiClient") as mock_cls, \
             patch("custom_components.casa.PrewarmListener") as mock_listener:
            client_inst = MagicMock()
            client_inst.health_check = AsyncMock(return_value=True)
            mock_cls.return_value = client_inst
            ok = await async_setup_entry(hass, entry)
            assert ok is True
            mock_cls.assert_called_once()
            assert entry.runtime_data.client is client_inst
            mock_listener.return_value.attach.assert_called_once()
            hass.config_entries.async_forward_entry_setups.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_setup_connection_error_raises_not_ready(self):
        from homeassistant.exceptions import ConfigEntryNotReady
        hass = MagicMock()
        entry = _entry()
        with patch("custom_components.casa.CasaApiClient") as mock_cls:
            client_inst = MagicMock()
            client_inst.health_check = AsyncMock(side_effect=aiohttp.ClientError("down"))
            mock_cls.return_value = client_inst
            with pytest.raises(ConfigEntryNotReady):
                await async_setup_entry(hass, entry)

    @pytest.mark.asyncio
    async def test_unload_detaches_listener_and_closes_client(self):
        hass = MagicMock()
        hass.config_entries.async_forward_entry_setups = AsyncMock()
        hass.config_entries.async_unload_platforms = AsyncMock(return_value=True)
        entry = _entry()
        with patch("custom_components.casa.CasaApiClient") as mock_cls, \
             patch("custom_components.casa.PrewarmListener") as mock_listener:
            client_inst = MagicMock()
            client_inst.health_check = AsyncMock(return_value=True)
            client_inst.close = AsyncMock()
            mock_cls.return_value = client_inst
            await async_setup_entry(hass, entry)
            ok = await async_unload_entry(hass, entry)
            assert ok is True
            mock_listener.return_value.detach.assert_called_once()
            client_inst.close.assert_awaited_once()
