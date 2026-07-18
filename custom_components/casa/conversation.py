"""Casa conversation entity for HA Assist pipeline."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import AsyncIterator, Callable
from typing import Literal

import aiohttp

from homeassistant.components import conversation
from homeassistant.components.conversation import ChatLog
from homeassistant.config_entries import ConfigEntry, ConfigSubentry
from homeassistant.const import MATCH_ALL
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import intent

from . import CasaAgentRuntime
from .api import AuthenticationError, BlockFrame, ErrorFrame, HandoffFrame
from .const import (
    DOMAIN,
    FALLBACK,
    INTEGRATION_VERSION,
    SILENT_STREAM_FALLBACK,
    SESSION_MODE_CONVERSATION,
    SESSION_MODE_DEVICE,
    SESSION_MODE_USER,
    SUBENTRY_TYPE_AGENT,
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
    _attr_supports_streaming = True

    def __init__(
        self,
        entry: ConfigEntry,
        subentry: ConfigSubentry,
        runtime: CasaAgentRuntime,
    ) -> None:
        self.entry = entry
        self._runtime = runtime
        self._client = runtime.client
        self._agent_role = runtime.role
        self._session_mode = runtime.session_mode
        self._transport = runtime.transport
        self._availability_unsubscribe: Callable[[], None] | None = None
        self._attr_name = subentry.title
        self._attr_unique_id = runtime.entity_unique_id

    @property
    def available(self) -> bool:
        """Return availability for this exact child runtime only."""
        return self._runtime.available

    @property
    def supported_languages(self) -> list[str] | Literal["*"]:
        return MATCH_ALL

    @property
    def device_info(self) -> dr.DeviceInfo:
        return dr.DeviceInfo(
            identifiers={(DOMAIN, self._runtime.entity_unique_id)},
            name="Casa",
            manufacturer="Casa",
            model=f"{self._runtime.name} ({self._agent_role})",
            sw_version=INTEGRATION_VERSION,
            entry_type=dr.DeviceEntryType.SERVICE,
        )

    async def async_added_to_hass(self) -> None:
        """Subscribe this entity to only its child runtime's availability."""
        await super().async_added_to_hass()
        self._availability_unsubscribe = (
            self._runtime.async_add_availability_listener(
                self.async_write_ha_state,
            )
        )

    async def async_will_remove_from_hass(self) -> None:
        """Release this entity's child-local availability subscription."""
        if self._availability_unsubscribe is not None:
            self._availability_unsubscribe()
            self._availability_unsubscribe = None
        await super().async_will_remove_from_hass()

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
        if self._client is None:
            return self._error_result(
                user_input,
                ErrorFrame(kind_="connection", spoken=""),
            )
        scope_id = self._build_scope_id(user_input)
        utterance_id = str(uuid.uuid4())
        saw_content = False
        error_frame: ErrorFrame | None = None
        handoff_frame: HandoffFrame | None = None

        stream = self._client.stream_utterance(
            text=user_input.text,
            agent_role=self._agent_role,
            scope_id=scope_id,
            utterance_id=utterance_id,
            context=_ha_context_payload(user_input),
            transport=self._transport,
        )

        async def _deltas(first_frame) -> AsyncIterator[dict]:
            nonlocal saw_content, error_frame, handoff_frame
            first = True
            frame = first_frame
            while frame is not None:
                if isinstance(frame, HandoffFrame):
                    handoff_frame = frame
                    return
                if isinstance(frame, ErrorFrame):
                    error_frame = frame
                    return
                if isinstance(frame, BlockFrame):
                    if not frame.text:
                        _LOGGER.debug("Dropping empty block frame")
                    else:
                        saw_content = True
                        d: dict = {"content": frame.text}
                        if first:
                            d["role"] = "assistant"
                            first = False
                        yield d
                try:
                    frame = await anext(stream)
                except StopAsyncIteration:
                    return

        try:
            async with asyncio.timeout(TIMEOUT_TOTAL):
                try:
                    first_frame = await anext(stream)
                except StopAsyncIteration:
                    first_frame = None
                if isinstance(first_frame, HandoffFrame):
                    handoff_frame = first_frame
                elif isinstance(first_frame, ErrorFrame):
                    error_frame = first_frame
                elif first_frame is not None:
                    async for _ in chat_log.async_add_delta_content_stream(
                        user_input.agent_id, _deltas(first_frame)
                    ):
                        pass
        except asyncio.TimeoutError:
            error_frame = ErrorFrame(kind_="timeout", spoken="")
        except AuthenticationError:
            self.entry.async_start_reauth(self.hass)
            error_frame = ErrorFrame(kind_="auth", spoken="")
        except (aiohttp.ClientError, OSError):
            error_frame = ErrorFrame(kind_="connection", spoken="")
        finally:
            await stream.aclose()

        if error_frame is not None:
            return self._error_result(user_input, error_frame)
        if handoff_frame is not None:
            response = intent.IntentResponse(language=user_input.language)
            response.async_set_speech(handoff_frame.text, speech_type="plain")
            return conversation.ConversationResult(
                response=response,
                conversation_id=user_input.conversation_id,
            )
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
    for subentry in entry.subentries.values():
        if subentry.subentry_type != SUBENTRY_TYPE_AGENT:
            continue
        runtime = entry.runtime_data.agents[subentry.subentry_id]
        async_add_entities(
            [CasaConversationEntity(entry, subentry, runtime)],
            config_subentry_id=subentry.subentry_id,
        )
