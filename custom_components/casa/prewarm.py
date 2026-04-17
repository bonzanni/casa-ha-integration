"""assist_satellite state-listener prewarm for Casa."""

from __future__ import annotations

import logging
from typing import Any

from homeassistant.core import callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.event import async_track_state_change_event

from .const import TRANSPORT_SSE, TRANSPORT_WS

_LOGGER = logging.getLogger(__name__)


class PrewarmListener:
    """Fires Casa prewarm on every assist_satellite LISTENING transition."""

    def __init__(self, hass: Any, client: Any, transport: str) -> None:
        self._hass = hass
        self._client = client
        self._transport = transport
        self._logged_sse_noop = False
        self._unsub = None

    def attach(self) -> None:
        """Subscribe to state changes. Returns a tear-down callable via detach()."""
        self._unsub = async_track_state_change_event(
            self._hass, "*", self.handle,
        )

    def detach(self) -> None:
        if self._unsub is not None:
            self._unsub()
            self._unsub = None

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
                _LOGGER.debug("Prewarm listener active but transport=sse; skipping.")
                self._logged_sse_noop = True
            return

        entity_id = new_state.entity_id
        registry = er.async_get(self._hass)
        entry = registry.async_get(entity_id)
        if entry is None or entry.device_id is None:
            return
        scope_id = entry.device_id
        self._hass.async_create_task(
            self._client.prewarm(scope_id=scope_id, transport=self._transport)
        )
