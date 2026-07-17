"""Tests for Casa __init__ setup wiring."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest

from custom_components.casa import async_setup_entry, async_unload_entry
from custom_components.casa.const import (
    CONF_AGENT_ROLE, CONF_HOST, CONF_IDLE_STABILITY_MS, CONF_PORT,
    CONF_SATELLITE_ENTITY_OVERRIDES, CONF_WEBHOOK_SECRET,
    CONF_TRANSPORT, DEFAULT_TRANSPORT,
)


def _entry():
    entry = MagicMock()
    entry.entry_id = "entry-1"
    entry.data = {CONF_HOST: "127.0.0.1", CONF_PORT: 18065, CONF_WEBHOOK_SECRET: "s"}
    entry.options = {
        CONF_AGENT_ROLE: "concierge",
        CONF_IDLE_STABILITY_MS: 1250,
        CONF_SATELLITE_ENTITY_OVERRIDES: (
            '{"dev-k":"assist_satellite.kitchen"}'
        ),
        CONF_TRANSPORT: DEFAULT_TRANSPORT,
    }
    entry.runtime_data = None
    entry.async_on_unload = MagicMock()
    entry.add_update_listener = MagicMock(return_value=lambda: None)
    return entry


class _SupervisedClient:
    """Small lifecycle-real client whose supervisor leaks unless close is awaited."""

    def __init__(self) -> None:
        self.reconnect_attempts_for_test = 0
        self._closed = False
        self._pulse = asyncio.Event()
        self._supervisor: asyncio.Task | None = None

    async def health_check(self) -> bool:
        return True

    async def start_background(self, **_kwargs) -> None:
        self._supervisor = asyncio.create_task(self._run_supervisor())

    async def _run_supervisor(self) -> None:
        while True:
            await self._pulse.wait()
            self._pulse.clear()
            if self._closed:
                return
            self.reconnect_attempts_for_test += 1

    async def pulse_reconnect(self) -> None:
        self._pulse.set()
        for _ in range(4):
            await asyncio.sleep(0)

    async def close(self) -> None:
        self._closed = True
        task = self._supervisor
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self._supervisor = None


class TestSetup:
    @pytest.mark.asyncio
    async def test_setup_happy(self):
        hass = MagicMock()
        hass.config_entries.async_forward_entry_setups = AsyncMock()
        entry = _entry()
        with patch("custom_components.casa.CasaApiClient") as mock_cls, \
             patch("custom_components.casa.SessionRegistrationListener") as mock_listener, \
             patch("custom_components.casa.SatelliteDirectory") as mock_directory, \
             patch("custom_components.casa.BackgroundDeliveryManager") as mock_manager:
            client_inst = MagicMock()
            client_inst.health_check = AsyncMock(return_value=True)
            client_inst.start_background = AsyncMock()
            mock_cls.return_value = client_inst
            ok = await async_setup_entry(hass, entry)
            assert ok is True
            mock_cls.assert_called_once()
            assert entry.runtime_data.client is client_inst
            assert entry.runtime_data.directory is mock_directory.return_value
            assert entry.runtime_data.manager is mock_manager.return_value
            mock_directory.assert_called_once_with(overrides={
                "dev-k": "assist_satellite.kitchen",
            })
            mock_manager.assert_called_once_with(
                hass,
                client_inst,
                route_id="entry-1",
                directory=mock_directory.return_value,
                idle_stability_ms=1250,
            )
            mock_listener.assert_called_once_with(
                hass,
                client_inst,
                DEFAULT_TRANSPORT,
                agent_role="concierge",
                directory=mock_directory.return_value,
            )
            mock_listener.return_value.attach.assert_called_once()
            client_inst.start_background.assert_awaited_once_with(
                route_id="entry-1",
                agent_role="concierge",
                job_handler=mock_manager.return_value.handle_frame,
            )
            hass.config_entries.async_forward_entry_setups.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_sse_setup_never_starts_websocket_for_background_jobs(self):
        hass = MagicMock()
        hass.config_entries.async_forward_entry_setups = AsyncMock()
        entry = _entry()
        entry.options[CONF_TRANSPORT] = "sse"
        with patch("custom_components.casa.CasaApiClient") as mock_cls, \
             patch("custom_components.casa.SessionRegistrationListener"), \
             patch("custom_components.casa.SatelliteDirectory"), \
             patch("custom_components.casa.BackgroundDeliveryManager"):
            client = MagicMock()
            client.health_check = AsyncMock(return_value=True)
            client.start_background = AsyncMock()
            mock_cls.return_value = client

            assert await async_setup_entry(hass, entry) is True

            client.start_background.assert_not_awaited()

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
    @pytest.mark.parametrize(
        "failure_seam",
        ["listener_attach", "start_background", "forward_platforms"],
    )
    async def test_setup_abort_after_attach_cleans_listener_manager_and_client(
        self, failure_seam,
    ):
        hass = MagicMock()
        hass.config_entries.async_forward_entry_setups = AsyncMock()
        entry = _entry()
        order: list[str] = []
        with patch("custom_components.casa.CasaApiClient") as mock_cls, \
             patch("custom_components.casa.SessionRegistrationListener") as mock_listener, \
             patch("custom_components.casa.SatelliteDirectory"), \
             patch("custom_components.casa.BackgroundDeliveryManager") as mock_manager:
            client = MagicMock()
            client.health_check = AsyncMock(return_value=True)
            client.start_background = AsyncMock()

            async def close_client():
                order.append("client")

            async def close_manager():
                order.append("manager")

            client.close = AsyncMock(side_effect=close_client)
            mock_listener.return_value.detach.side_effect = lambda: order.append("listener")
            mock_manager.return_value.close = AsyncMock(side_effect=close_manager)
            mock_cls.return_value = client
            if failure_seam == "listener_attach":
                mock_listener.return_value.attach.side_effect = RuntimeError(
                    "listener failed",
                )
            elif failure_seam == "start_background":
                client.start_background.side_effect = ConnectionError("offline")
            else:
                hass.config_entries.async_forward_entry_setups.side_effect = RuntimeError(
                    "platform failed",
                )

            with pytest.raises((ConnectionError, RuntimeError)):
                await async_setup_entry(hass, entry)

            assert order == ["listener", "manager", "client"]
            assert entry.runtime_data is None

    @pytest.mark.asyncio
    async def test_failed_platform_unload_keeps_runtime_resources_active(self):
        hass = MagicMock()
        hass.config_entries.async_forward_entry_setups = AsyncMock()
        hass.config_entries.async_unload_platforms = AsyncMock(return_value=False)
        entry = _entry()
        with patch("custom_components.casa.CasaApiClient") as mock_cls, \
             patch("custom_components.casa.SessionRegistrationListener") as mock_listener, \
             patch("custom_components.casa.SatelliteDirectory"), \
             patch("custom_components.casa.BackgroundDeliveryManager") as mock_manager:
            client = MagicMock()
            client.health_check = AsyncMock(return_value=True)
            client.start_background = AsyncMock()
            client.close = AsyncMock()
            mock_manager.return_value.close = AsyncMock()
            mock_cls.return_value = client
            await async_setup_entry(hass, entry)
            runtime = entry.runtime_data

            assert await async_unload_entry(hass, entry) is False

            assert entry.runtime_data is runtime
            mock_listener.return_value.detach.assert_not_called()
            mock_manager.return_value.close.assert_not_awaited()
            client.close.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_unload_order_is_listener_then_manager_then_client(self):
        hass = MagicMock()
        hass.config_entries.async_forward_entry_setups = AsyncMock()
        hass.config_entries.async_unload_platforms = AsyncMock(return_value=True)
        entry = _entry()
        order: list[str] = []
        with patch("custom_components.casa.CasaApiClient") as mock_cls, \
             patch("custom_components.casa.SessionRegistrationListener") as mock_listener, \
             patch("custom_components.casa.SatelliteDirectory"), \
             patch("custom_components.casa.BackgroundDeliveryManager") as mock_manager:
            client_inst = MagicMock()
            client_inst.health_check = AsyncMock(return_value=True)
            client_inst.start_background = AsyncMock()

            async def close_client():
                order.append("client")

            async def close_manager():
                order.append("manager")

            client_inst.close = AsyncMock(side_effect=close_client)
            mock_listener.return_value.detach.side_effect = lambda: order.append("listener")
            mock_manager.return_value.close = AsyncMock(side_effect=close_manager)
            mock_cls.return_value = client_inst
            await async_setup_entry(hass, entry)
            ok = await async_unload_entry(hass, entry)
            assert ok is True
            mock_listener.return_value.detach.assert_called_once()
            mock_manager.return_value.close.assert_awaited_once()
            client_inst.close.assert_awaited_once()
            assert order == ["listener", "manager", "client"]

    @pytest.mark.asyncio
    async def test_unload_cancels_supervisor_so_no_reconnect_occurs_afterward(self):
        hass = MagicMock()
        hass.config_entries.async_forward_entry_setups = AsyncMock()
        hass.config_entries.async_unload_platforms = AsyncMock(return_value=True)
        entry = _entry()
        client = _SupervisedClient()
        with patch("custom_components.casa.CasaApiClient", return_value=client), \
             patch("custom_components.casa.SessionRegistrationListener"), \
             patch("custom_components.casa.SatelliteDirectory"), \
             patch("custom_components.casa.BackgroundDeliveryManager") as mock_manager:
            mock_manager.return_value.close = AsyncMock()
            await async_setup_entry(hass, entry)
            await client.pulse_reconnect()
            assert client.reconnect_attempts_for_test == 1

            await async_unload_entry(hass, entry)
            await client.pulse_reconnect()

            assert client.reconnect_attempts_for_test == 1
            assert client._supervisor is None
