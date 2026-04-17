"""Tests for the assist_satellite state-listener prewarm."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from custom_components.casa.prewarm import PrewarmListener
from custom_components.casa.const import TRANSPORT_WS, TRANSPORT_SSE


def _state_change_event(entity_id: str, new_state_state: str, domain: str = "assist_satellite"):
    ev = MagicMock()
    new = MagicMock()
    new.entity_id = entity_id
    new.state = new_state_state
    new.domain = domain
    ev.data = {"new_state": new, "old_state": MagicMock()}
    return ev


class TestPrewarmListener:
    @pytest.mark.asyncio
    async def test_listening_fires_prewarm_with_device_id(self):
        hass = MagicMock()
        hass.async_create_task = MagicMock()

        client = MagicMock()
        client.prewarm = AsyncMock()

        from homeassistant.helpers import entity_registry as er
        entity_entry = MagicMock()
        entity_entry.device_id = "dev-kitchen"
        er.async_get = MagicMock(return_value=MagicMock(async_get=lambda _id: entity_entry))

        listener = PrewarmListener(hass, client, TRANSPORT_WS)
        listener.handle(_state_change_event("assist_satellite.kitchen", "listening"))

        # The coroutine should be submitted via async_create_task.
        hass.async_create_task.assert_called_once()
        coro = hass.async_create_task.call_args[0][0]
        await coro
        client.prewarm.assert_awaited_once_with(scope_id="dev-kitchen", transport=TRANSPORT_WS)

    @pytest.mark.asyncio
    async def test_ignores_non_listening_states(self):
        hass = MagicMock()
        hass.async_create_task = MagicMock()
        client = MagicMock()

        listener = PrewarmListener(hass, client, TRANSPORT_WS)
        listener.handle(_state_change_event("assist_satellite.kitchen", "idle"))
        listener.handle(_state_change_event("assist_satellite.kitchen", "processing"))
        hass.async_create_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_ignores_non_satellite_domain(self):
        hass = MagicMock()
        hass.async_create_task = MagicMock()
        client = MagicMock()

        listener = PrewarmListener(hass, client, TRANSPORT_WS)
        listener.handle(_state_change_event("light.kitchen", "listening", domain="light"))
        hass.async_create_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_sse_transport_noops(self):
        hass = MagicMock()
        hass.async_create_task = MagicMock()
        client = MagicMock()
        client.prewarm = AsyncMock()

        from homeassistant.helpers import entity_registry as er
        entity_entry = MagicMock()
        entity_entry.device_id = "dev-kitchen"
        er.async_get = MagicMock(return_value=MagicMock(async_get=lambda _id: entity_entry))

        listener = PrewarmListener(hass, client, TRANSPORT_SSE)
        listener.handle(_state_change_event("assist_satellite.kitchen", "listening"))
        hass.async_create_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_device_id_skips(self):
        hass = MagicMock()
        hass.async_create_task = MagicMock()
        client = MagicMock()

        from homeassistant.helpers import entity_registry as er
        entity_entry = MagicMock()
        entity_entry.device_id = None
        er.async_get = MagicMock(return_value=MagicMock(async_get=lambda _id: entity_entry))

        listener = PrewarmListener(hass, client, TRANSPORT_WS)
        listener.handle(_state_change_event("assist_satellite.kitchen", "listening"))
        hass.async_create_task.assert_not_called()
