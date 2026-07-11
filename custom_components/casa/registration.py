"""assist_satellite state-listener session registration for Casa.

Sends an ``stt_start`` frame when a satellite starts listening. Since Casa
0.4x this only registers the voice scope for idle-sweep/dedup on the add-on
side — the add-on no longer prewarms the memory overlay on ``stt_start``.
"""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.core import callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.event import TrackStates, async_track_state_change_filtered

from .const import TRANSPORT_SSE

_LOGGER = logging.getLogger(__name__)


class SessionRegistrationListener:
    """Registers the Casa voice session on every assist_satellite LISTENING transition."""

    def __init__(self, hass: Any, client: Any, transport: str) -> None:
        self._hass = hass
        self._client = client
        self._transport = transport
        self._logged_sse_noop = False
        self._tracker = None

    def attach(self) -> None:
        """Subscribe to assist_satellite state changes. Tear down via detach()."""
        self._tracker = async_track_state_change_filtered(
            self._hass, TrackStates(False, set(), {"assist_satellite"}), self.handle,
        )

    def detach(self) -> None:
        if self._tracker is not None:
            self._tracker.async_remove()
            self._tracker = None

    @callback
    def handle(self, event: Any) -> None:
        new_state = event.data.get("new_state")
        if new_state is None:
            return
        if getattr(new_state, "domain", None) != "assist_satellite":
            return
        if new_state.state != "listening":
            return
        if self._transport == TRANSPORT_SSE:
            if not self._logged_sse_noop:
                _LOGGER.debug("Registration listener active but transport=sse; skipping.")
                self._logged_sse_noop = True
            return

        entity_id = new_state.entity_id
        registry = er.async_get(self._hass)
        entry = registry.async_get(entity_id)
        if entry is None or entry.device_id is None:
            return
        scope_id = entry.device_id
        _LOGGER.debug("Registering voice session scope=%s", scope_id)
        self._hass.async_create_task(
            self._client.register_session(scope_id=scope_id, transport=self._transport)
        )
