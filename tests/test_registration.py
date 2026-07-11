"""Tests for the assist_satellite state-listener session registration."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from custom_components.casa.registration import SessionRegistrationListener
from custom_components.casa.const import TRANSPORT_WS, TRANSPORT_SSE


def _state_change_event(entity_id: str, new_state_state: str, domain: str = "assist_satellite"):
    ev = MagicMock()
    new = MagicMock()
    new.entity_id = entity_id
    new.state = new_state_state
    new.domain = domain
    ev.data = {"new_state": new, "old_state": MagicMock()}
    return ev


class TestSessionRegistrationListener:
    def test_attach_tracks_assist_satellite_domain(self):
        from homeassistant.helpers import event as ha_event

        ha_event.async_track_state_change_filtered.reset_mock()
        listener = SessionRegistrationListener(MagicMock(), MagicMock(), TRANSPORT_WS)
        listener.attach()

        args = ha_event.async_track_state_change_filtered.call_args[0]
        track_states = args[1]
        assert track_states.all_states is False
        assert track_states.entities == set()
        assert track_states.domains == {"assist_satellite"}
        assert args[2] == listener.handle

        tracker = ha_event.async_track_state_change_filtered.return_value
        listener.detach()
        tracker.async_remove.assert_called_once()

    @pytest.mark.asyncio
    async def test_listening_registers_session_with_device_id(self):
        hass = MagicMock()
        hass.async_create_task = MagicMock()

        client = MagicMock()
        client.register_session = AsyncMock()

        from homeassistant.helpers import entity_registry as er
        entity_entry = MagicMock()
        entity_entry.device_id = "dev-kitchen"
        er.async_get = MagicMock(return_value=MagicMock(async_get=lambda _id: entity_entry))

        listener = SessionRegistrationListener(hass, client, TRANSPORT_WS)
        listener.handle(_state_change_event("assist_satellite.kitchen", "listening"))

        # The coroutine should be submitted via async_create_task.
        hass.async_create_task.assert_called_once()
        coro = hass.async_create_task.call_args[0][0]
        await coro
        client.register_session.assert_awaited_once_with(scope_id="dev-kitchen", transport=TRANSPORT_WS)

    @pytest.mark.asyncio
    async def test_ignores_non_listening_states(self):
        hass = MagicMock()
        hass.async_create_task = MagicMock()
        client = MagicMock()

        listener = SessionRegistrationListener(hass, client, TRANSPORT_WS)
        listener.handle(_state_change_event("assist_satellite.kitchen", "idle"))
        listener.handle(_state_change_event("assist_satellite.kitchen", "processing"))
        hass.async_create_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_ignores_non_satellite_domain(self):
        hass = MagicMock()
        hass.async_create_task = MagicMock()
        client = MagicMock()

        listener = SessionRegistrationListener(hass, client, TRANSPORT_WS)
        listener.handle(_state_change_event("light.kitchen", "listening", domain="light"))
        hass.async_create_task.assert_not_called()

    @pytest.mark.asyncio
    async def test_sse_transport_noops(self):
        hass = MagicMock()
        hass.async_create_task = MagicMock()
        client = MagicMock()
        client.register_session = AsyncMock()

        from homeassistant.helpers import entity_registry as er
        entity_entry = MagicMock()
        entity_entry.device_id = "dev-kitchen"
        er.async_get = MagicMock(return_value=MagicMock(async_get=lambda _id: entity_entry))

        listener = SessionRegistrationListener(hass, client, TRANSPORT_SSE)
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

        listener = SessionRegistrationListener(hass, client, TRANSPORT_WS)
        listener.handle(_state_change_event("assist_satellite.kitchen", "listening"))
        hass.async_create_task.assert_not_called()
