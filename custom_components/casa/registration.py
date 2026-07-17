"""assist_satellite state-listener session registration for Casa.

Sends an ``stt_start`` frame when a satellite starts listening. Since Casa
0.4x this only registers the voice scope for idle-sweep/dedup on the add-on
side — the add-on no longer prewarms the memory overlay on ``stt_start``.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from homeassistant.core import callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.event import TrackStates, async_track_state_change_filtered

from .const import TRANSPORT_SSE
from .delivery import SatelliteDirectory

_LOGGER = logging.getLogger(__name__)


class SessionRegistrationListener:
    """Registers the Casa voice session on every assist_satellite LISTENING transition."""

    def __init__(
        self,
        hass: Any,
        client: Any,
        transport: str,
        *,
        agent_role: str,
        directory: SatelliteDirectory,
    ) -> None:
        self._hass = hass
        self._client = client
        self._transport = transport
        self._agent_role = agent_role
        self._directory = directory
        self._logged_sse_noop = False
        self._tracker = None
        self._registry_unsubscribe = None

    def attach(self) -> None:
        """Subscribe to satellite state and registry changes. Tear down via detach()."""
        self._tracker = async_track_state_change_filtered(
            self._hass, TrackStates(False, set(), {"assist_satellite"}), self.handle,
        )
        self._registry_unsubscribe = self._hass.bus.async_listen(
            er.EVENT_ENTITY_REGISTRY_UPDATED,
            self.handle_registry_update,
        )
        for state in self._hass.states.async_all("assist_satellite"):
            self._update_directory(state)

    def detach(self) -> None:
        tracker = self._tracker
        registry_unsubscribe = self._registry_unsubscribe
        self._tracker = None
        self._registry_unsubscribe = None
        try:
            if tracker is not None:
                tracker.async_remove()
        finally:
            if registry_unsubscribe is not None:
                registry_unsubscribe()

    @callback
    def handle(self, event: Any) -> None:
        new_state = event.data.get("new_state")
        if new_state is None:
            old_state = event.data.get("old_state")
            entity_id = getattr(old_state, "entity_id", None)
            if isinstance(entity_id, str) and entity_id.startswith("assist_satellite."):
                self._directory.remove(entity_id)
            return
        entity_id = getattr(new_state, "entity_id", "")
        if not isinstance(entity_id, str) or not entity_id.startswith("assist_satellite."):
            return
        device_id = self._update_directory(new_state)
        if device_id is None or new_state.state != "listening":
            return
        if self._transport == TRANSPORT_SSE:
            if not self._logged_sse_noop:
                _LOGGER.debug("Casa session registration state=skipped reason=sse_transport")
                self._logged_sse_noop = True
            return

        self._hass.async_create_task(
            self._client.register_session(
                scope_id=device_id,
                transport=self._transport,
                agent_role=self._agent_role,
            )
        )

    @callback
    def handle_registry_update(self, event: Any) -> None:
        """Refresh Assist bindings changed without an HA state transition."""
        data = event.data
        entity_id = data.get("entity_id")
        old_entity_id = data.get("old_entity_id")
        if (
            isinstance(old_entity_id, str)
            and old_entity_id.startswith("assist_satellite.")
            and old_entity_id != entity_id
        ):
            self._directory.remove(old_entity_id)
        if not (
            isinstance(entity_id, str)
            and entity_id.startswith("assist_satellite.")
        ):
            return
        if data.get("action") == "remove":
            self._directory.remove(entity_id)
            return
        state = self._hass.states.get(entity_id)
        if state is None:
            self._directory.remove(entity_id)
            return
        self._update_directory(state)

    def _update_directory(self, state: Any) -> str | None:
        """Resolve the registry binding afresh and record this exact entity."""
        entity_id = getattr(state, "entity_id", "")
        if not isinstance(entity_id, str) or not entity_id.startswith("assist_satellite."):
            return None
        registry = er.async_get(self._hass)
        entry = registry.async_get(entity_id)
        if entry is None or entry.device_id is None:
            self._directory.remove(entity_id)
            return None
        changed = getattr(state, "last_changed", None)
        changed_at = (
            float(changed.timestamp())
            if changed is not None and callable(getattr(changed, "timestamp", None))
            else time.time()
        )
        device_id = entry.device_id
        self._directory.set_entity_state(
            device_id,
            entity_id,
            str(state.state),
            changed_at=changed_at,
        )
        return device_id
