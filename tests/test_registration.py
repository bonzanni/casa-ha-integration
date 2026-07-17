"""Tests for the assist_satellite state-listener session registration."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from custom_components.casa.delivery import SatelliteDirectory, SatelliteNotFound
from custom_components.casa.registration import SessionRegistrationListener


def _state(
    entity_id: str,
    state: str,
    *,
    domain: str = "assist_satellite",
    changed_at: float = 100.0,
):
    return SimpleNamespace(
        entity_id=entity_id,
        state=state,
        domain=domain,
        last_changed=datetime.fromtimestamp(changed_at, timezone.utc),
    )


def _state_change_event(
    entity_id: str,
    new_state_state: str | None,
    domain: str = "assist_satellite",
    *,
    old_state_state: str | None = "idle",
    changed_at: float = 100.0,
):
    ev = MagicMock()
    new = (
        _state(entity_id, new_state_state, domain=domain, changed_at=changed_at)
        if new_state_state is not None
        else None
    )
    old = (
        _state(entity_id, old_state_state, domain=domain, changed_at=changed_at - 1)
        if old_state_state is not None
        else None
    )
    ev.data = {"new_state": new, "old_state": old}
    return ev


def _registry_event(
    entity_id: str,
    *,
    action: str = "update",
    old_entity_id: str | None = None,
):
    data = {"action": action, "entity_id": entity_id}
    if old_entity_id is not None:
        data["old_entity_id"] = old_entity_id
    return SimpleNamespace(data=data)


def _listener(hass, on_listening=None, directory=None):
    return SessionRegistrationListener(
        hass,
        directory=directory or SatelliteDirectory(),
        on_listening=on_listening or MagicMock(),
    )


class TestSessionRegistrationListener:
    def test_attach_tracks_assist_satellite_domain(self):
        from homeassistant.helpers import entity_registry as er

        er.EVENT_ENTITY_REGISTRY_UPDATED = "entity_registry_updated"
        hass = MagicMock()
        hass.states.async_all.return_value = []
        listener = _listener(hass, MagicMock())
        tracker = MagicMock()
        with patch(
            "custom_components.casa.registration.async_track_state_change_filtered",
            return_value=tracker,
        ) as track_state_changes:
            listener.attach()

        args = track_state_changes.call_args[0]
        track_states = args[1]
        assert track_states.all_states is False
        assert track_states.entities == set()
        assert track_states.domains == {"assist_satellite"}
        assert args[2] == listener.handle
        hass.bus.async_listen.assert_called_once_with(
            er.EVENT_ENTITY_REGISTRY_UPDATED,
            listener.handle_registry_update,
        )

        registry_unsubscribe = hass.bus.async_listen.return_value
        listener.detach()
        tracker.async_remove.assert_called_once()
        registry_unsubscribe.assert_called_once_with()

    def test_attach_discovers_existing_idle_with_last_changed(self):
        hass = MagicMock()
        hass.states.async_all.return_value = [
            _state("assist_satellite.kitchen", "idle", changed_at=42.5),
        ]
        directory = SatelliteDirectory()
        from homeassistant.helpers import entity_registry as er

        entry = MagicMock(device_id="dev-kitchen")
        er.async_get = MagicMock(return_value=MagicMock(async_get=lambda _id: entry))

        listener = _listener(hass, MagicMock(), directory=directory)
        listener.attach()

        assert directory.resolve("dev-kitchen") == "assist_satellite.kitchen"
        assert directory.state("dev-kitchen") == "idle"
        assert directory.idle_since("dev-kitchen") == 42.5

    def test_detach_still_removes_registry_listener_if_state_detach_fails(self):
        listener = _listener(MagicMock(), MagicMock())
        tracker = MagicMock()
        tracker.async_remove.side_effect = RuntimeError("state detach failed")
        registry_unsubscribe = MagicMock()
        listener._tracker = tracker
        listener._registry_unsubscribe = registry_unsubscribe

        with pytest.raises(RuntimeError, match="state detach failed"):
            listener.detach()

        registry_unsubscribe.assert_called_once_with()
        assert listener._tracker is None
        assert listener._registry_unsubscribe is None

    def test_listening_calls_role_neutral_callback_once_with_device_id(self):
        hass = MagicMock()
        on_listening = MagicMock()

        from homeassistant.helpers import entity_registry as er
        entity_entry = MagicMock()
        entity_entry.device_id = "dev-kitchen"
        er.async_get = MagicMock(return_value=MagicMock(async_get=lambda _id: entity_entry))

        directory = SatelliteDirectory()
        listener = _listener(hass, on_listening, directory=directory)
        listener.handle(_state_change_event("assist_satellite.kitchen", "listening"))

        on_listening.assert_called_once_with("dev-kitchen")
        assert directory.state("dev-kitchen") == "listening"
        assert not hasattr(listener, "_client")
        assert not hasattr(listener, "_transport")
        assert not hasattr(listener, "_agent_role")

    def test_repeated_listening_state_event_does_not_register_twice(self):
        hass = MagicMock()
        on_listening = MagicMock()

        from homeassistant.helpers import entity_registry as er
        entity_entry = MagicMock(device_id="dev-kitchen")
        registry = MagicMock()
        registry.async_get = MagicMock(return_value=entity_entry)
        er.async_get = MagicMock(return_value=registry)

        directory = SatelliteDirectory()
        listener = _listener(hass, on_listening, directory=directory)
        listener.handle(_state_change_event(
            "assist_satellite.kitchen",
            "listening",
            old_state_state="idle",
            changed_at=100.0,
        ))
        listener.handle(_state_change_event(
            "assist_satellite.kitchen",
            "listening",
            old_state_state="listening",
            changed_at=101.0,
        ))

        on_listening.assert_called_once_with("dev-kitchen")
        assert directory.state("dev-kitchen") == "listening"
        assert registry.async_get.call_count == 2

    def test_initial_listening_state_without_old_state_registers(self):
        hass = MagicMock()
        on_listening = MagicMock()

        from homeassistant.helpers import entity_registry as er
        entity_entry = MagicMock(device_id="dev-kitchen")
        er.async_get = MagicMock(return_value=MagicMock(async_get=lambda _id: entity_entry))

        listener = _listener(hass, on_listening)
        listener.handle(_state_change_event(
            "assist_satellite.kitchen",
            "listening",
            old_state_state=None,
        ))

        on_listening.assert_called_once_with("dev-kitchen")

    def test_tracks_non_listening_states_without_callback(self):
        hass = MagicMock()
        on_listening = MagicMock()
        directory = SatelliteDirectory()

        from homeassistant.helpers import entity_registry as er
        entity_entry = MagicMock(device_id="dev-kitchen")
        registry = MagicMock()
        registry.async_get = MagicMock(return_value=entity_entry)
        er.async_get = MagicMock(return_value=registry)

        listener = _listener(hass, on_listening, directory=directory)
        listener.handle(_state_change_event("assist_satellite.kitchen", "idle"))
        listener.handle(_state_change_event("assist_satellite.kitchen", "processing"))
        on_listening.assert_not_called()
        assert directory.state("dev-kitchen") == "processing"
        assert registry.async_get.call_count == 2

    def test_ignores_non_satellite_domain(self):
        hass = MagicMock()
        on_listening = MagicMock()

        listener = _listener(hass, on_listening)
        listener.handle(_state_change_event("light.kitchen", "listening", domain="light"))
        on_listening.assert_not_called()

    def test_missing_device_id_skips_callback(self):
        hass = MagicMock()
        on_listening = MagicMock()

        from homeassistant.helpers import entity_registry as er
        entity_entry = MagicMock()
        entity_entry.device_id = None
        er.async_get = MagicMock(return_value=MagicMock(async_get=lambda _id: entity_entry))

        directory = SatelliteDirectory()
        directory.add("dev-old", "assist_satellite.kitchen")
        listener = _listener(hass, on_listening, directory=directory)
        listener.handle(_state_change_event("assist_satellite.kitchen", "listening"))
        on_listening.assert_not_called()
        with pytest.raises(SatelliteNotFound):
            directory.resolve("dev-old")

    def test_entity_disappearance_removes_stale_mapping(self):
        hass = MagicMock()
        directory = SatelliteDirectory()
        directory.add("dev-kitchen", "assist_satellite.kitchen")
        listener = _listener(hass, MagicMock(), directory=directory)

        listener.handle(_state_change_event("assist_satellite.kitchen", None))

        with pytest.raises(SatelliteNotFound):
            directory.resolve("dev-kitchen")

    def test_registry_rebinding_moves_entity_to_current_device(self):
        hass = MagicMock()
        directory = SatelliteDirectory()
        directory.add("dev-old", "assist_satellite.kitchen")
        from homeassistant.helpers import entity_registry as er

        entry = MagicMock(device_id="dev-new")
        er.async_get = MagicMock(return_value=MagicMock(async_get=lambda _id: entry))
        listener = _listener(hass, MagicMock(), directory=directory)

        listener.handle(_state_change_event("assist_satellite.kitchen", "responding"))

        with pytest.raises(SatelliteNotFound):
            directory.resolve("dev-old")
        assert directory.resolve("dev-new") == "assist_satellite.kitchen"
        assert directory.state("dev-new") == "responding"

    def test_registry_only_device_rebind_refreshes_unchanged_state(self):
        hass = MagicMock()
        state = _state("assist_satellite.kitchen", "idle", changed_at=42.5)
        hass.states.get.return_value = state
        directory = SatelliteDirectory()
        directory.set_entity_state(
            "dev-old",
            "assist_satellite.kitchen",
            "idle",
            changed_at=42.5,
        )
        from homeassistant.helpers import entity_registry as er

        entry = MagicMock(device_id="dev-new")
        er.async_get = MagicMock(return_value=MagicMock(async_get=lambda _id: entry))
        listener = _listener(hass, MagicMock(), directory=directory)

        listener.handle_registry_update(_registry_event("assist_satellite.kitchen"))

        with pytest.raises(SatelliteNotFound):
            directory.resolve("dev-old")
        assert directory.resolve("dev-new") == "assist_satellite.kitchen"
        assert directory.state("dev-new") == "idle"

    @pytest.mark.parametrize("event_order", ["state_first", "registry_first"])
    def test_state_and_registry_update_interleavings_keep_current_binding(
        self, event_order,
    ):
        hass = MagicMock()
        state = _state("assist_satellite.kitchen", "responding")
        hass.states.get.return_value = state
        directory = SatelliteDirectory()
        from homeassistant.helpers import entity_registry as er

        entry = MagicMock(device_id="dev-old")
        er.async_get = MagicMock(return_value=MagicMock(async_get=lambda _id: entry))
        listener = _listener(hass, MagicMock(), directory=directory)
        listener.handle(_state_change_event("assist_satellite.kitchen", "responding"))
        entry.device_id = "dev-new"

        if event_order == "state_first":
            listener.handle(_state_change_event(
                "assist_satellite.kitchen", "responding",
            ))
            listener.handle_registry_update(
                _registry_event("assist_satellite.kitchen"),
            )
        else:
            listener.handle_registry_update(
                _registry_event("assist_satellite.kitchen"),
            )
            listener.handle(_state_change_event(
                "assist_satellite.kitchen", "responding",
            ))

        with pytest.raises(SatelliteNotFound):
            directory.resolve("dev-old")
        assert directory.resolve("dev-new") == "assist_satellite.kitchen"

    def test_registry_rename_removes_old_and_discovers_new_assist_entity(self):
        hass = MagicMock()
        new_state = _state("assist_satellite.den", "idle", changed_at=88.0)
        hass.states.get.return_value = new_state
        directory = SatelliteDirectory()
        directory.add("dev-old", "assist_satellite.kitchen")
        from homeassistant.helpers import entity_registry as er

        entry = MagicMock(device_id="dev-new")
        er.async_get = MagicMock(return_value=MagicMock(async_get=lambda _id: entry))
        listener = _listener(hass, MagicMock(), directory=directory)

        listener.handle_registry_update(_registry_event(
            "assist_satellite.den",
            old_entity_id="assist_satellite.kitchen",
        ))

        with pytest.raises(SatelliteNotFound):
            directory.resolve("dev-old")
        assert directory.resolve("dev-new") == "assist_satellite.den"

    def test_registry_remove_clears_binding_even_if_state_still_exists(self):
        hass = MagicMock()
        hass.states.get.return_value = _state("assist_satellite.kitchen", "idle")
        directory = SatelliteDirectory()
        directory.add("dev-k", "assist_satellite.kitchen")
        from homeassistant.helpers import entity_registry as er

        er.async_get = MagicMock(return_value=MagicMock(async_get=lambda _id: None))
        listener = _listener(hass, MagicMock(), directory=directory)

        listener.handle_registry_update(_registry_event(
            "assist_satellite.kitchen",
            action="remove",
        ))

        with pytest.raises(SatelliteNotFound):
            directory.resolve("dev-k")
