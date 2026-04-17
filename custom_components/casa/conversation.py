"""Casa conversation entity for HA Assist pipeline."""

from __future__ import annotations

import logging
from typing import Literal

from homeassistant.components import conversation
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import MATCH_ALL
from homeassistant.helpers import device_registry as dr

from .const import (
    CONF_AGENT_ROLE, CONF_SESSION_MODE, CONF_TRANSPORT,
    DEFAULT_AGENT_ROLE, DEFAULT_SESSION_MODE, DEFAULT_TRANSPORT,
    DOMAIN, INTEGRATION_VERSION,
    SESSION_MODE_CONVERSATION, SESSION_MODE_DEVICE, SESSION_MODE_USER,
)

_LOGGER = logging.getLogger(__name__)


class CasaConversationEntity(conversation.ConversationEntity):
    _attr_has_entity_name = False
    _attr_name = "Casa Butler"

    def __init__(self, entry: ConfigEntry) -> None:
        self.entry = entry
        self._client = entry.runtime_data.client
        self._agent_role = entry.options.get(CONF_AGENT_ROLE, DEFAULT_AGENT_ROLE)
        self._session_mode = entry.options.get(CONF_SESSION_MODE, DEFAULT_SESSION_MODE)
        self._transport = entry.options.get(CONF_TRANSPORT, DEFAULT_TRANSPORT)
        self._attr_unique_id = entry.entry_id

    @property
    def supported_languages(self) -> list[str] | Literal["*"]:
        return MATCH_ALL

    @property
    def device_info(self) -> dr.DeviceInfo:
        return dr.DeviceInfo(
            identifiers={(DOMAIN, self.entry.entry_id)},
            name="Casa",
            manufacturer="Casa",
            model=f"Butler ({self._agent_role})",
            sw_version=INTEGRATION_VERSION,
            entry_type=dr.DeviceEntryType.SERVICE,
        )

    def _build_scope_id(self, user_input: conversation.ConversationInput) -> str:
        uid = user_input.context.user_id if user_input.context else None
        did = user_input.device_id
        cid = user_input.conversation_id
        if self._session_mode == SESSION_MODE_DEVICE:
            return did or uid or cid
        if self._session_mode == SESSION_MODE_USER:
            return uid or did or cid
        if self._session_mode == SESSION_MODE_CONVERSATION:
            return cid
        return did or uid or cid
