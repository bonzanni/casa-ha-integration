"""Casa conversation entity for HA Assist pipeline."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from typing import Literal

import aiohttp

from homeassistant.components import conversation
from homeassistant.components.conversation import ChatLog
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import MATCH_ALL
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import intent

from .api import AuthenticationError, BlockFrame, DoneFrame, ErrorFrame
from .const import (
    CONF_AGENT_ROLE, CONF_SESSION_MODE, CONF_TRANSPORT,
    DEFAULT_AGENT_ROLE, DEFAULT_SESSION_MODE, DEFAULT_TRANSPORT,
    DOMAIN, FALLBACK, INTEGRATION_VERSION, SILENT_STREAM_FALLBACK,
    SESSION_MODE_CONVERSATION, SESSION_MODE_DEVICE, SESSION_MODE_USER,
    TIMEOUT_TOTAL,
)

_LOGGER = logging.getLogger(__name__)


def _ha_context_payload(user_input: conversation.ConversationInput) -> dict:
    return {
        "device_id": user_input.device_id,
        "user_id": user_input.context.user_id if user_input.context else None,
        "language": user_input.language,
        "conversation_id": user_input.conversation_id,
    }


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

    async def _async_handle_message(
        self,
        user_input: conversation.ConversationInput,
        chat_log: ChatLog,
    ) -> conversation.ConversationResult:
        scope_id = self._build_scope_id(user_input)
        utterance_id = str(uuid.uuid4())
        saw_content = False
        error_frame: ErrorFrame | None = None

        async def _deltas() -> AsyncIterator[dict]:
            nonlocal saw_content, error_frame
            first = True
            try:
                async with asyncio.timeout(TIMEOUT_TOTAL):
                    async for frame in self._client.stream_utterance(
                        text=user_input.text,
                        agent_role=self._agent_role,
                        scope_id=scope_id,
                        utterance_id=utterance_id,
                        context=_ha_context_payload(user_input),
                        transport=self._transport,
                    ):
                        if isinstance(frame, ErrorFrame):
                            error_frame = frame
                            return
                        if isinstance(frame, BlockFrame):
                            if not frame.text:
                                _LOGGER.debug("Dropping empty block frame")
                                continue
                            saw_content = True
                            d: dict = {"content": frame.text}
                            if first:
                                d["role"] = "assistant"
                                first = False
                            yield d
                        # DoneFrame → loop ends naturally.
            except asyncio.TimeoutError:
                error_frame = ErrorFrame(kind_="timeout", spoken="")
            except AuthenticationError:
                self.entry.async_start_reauth(self.hass)
                error_frame = ErrorFrame(kind_="auth", spoken="")
            except (aiohttp.ClientError, aiohttp.ServerDisconnectedError):
                error_frame = ErrorFrame(kind_="connection", spoken="")

        async for _ in chat_log.async_add_delta_content_stream(
            user_input.agent_id, _deltas()
        ):
            pass

        if error_frame is not None:
            return self._error_result(user_input, error_frame)
        if not saw_content:
            return self._silent_stream_fallback(user_input)
        return conversation.async_get_result_from_chat_log(user_input, chat_log)

    def _error_result(self, user_input, error_frame: ErrorFrame):
        response = intent.IntentResponse(language=user_input.language)
        response.async_set_error(
            intent.IntentResponseErrorCode.FAILED_TO_HANDLE,
            error_frame.kind_,
        )
        response.async_set_speech(error_frame.spoken or FALLBACK, speech_type="plain")
        return conversation.ConversationResult(
            response=response,
            conversation_id=user_input.conversation_id,
        )

    def _silent_stream_fallback(self, user_input):
        response = intent.IntentResponse(language=user_input.language)
        response.async_set_speech(SILENT_STREAM_FALLBACK, speech_type="plain")
        return conversation.ConversationResult(
            response=response,
            conversation_id=user_input.conversation_id,
        )


async def async_setup_entry(hass, entry, async_add_entities):
    async_add_entities([CasaConversationEntity(entry)])
