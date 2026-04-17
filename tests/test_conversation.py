"""Tests for CasaConversationEntity."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from custom_components.casa.api import BlockFrame, DoneFrame
from custom_components.casa.conversation import CasaConversationEntity
from custom_components.casa.const import (
    CONF_AGENT_ROLE, CONF_SESSION_MODE, CONF_TRANSPORT,
    DEFAULT_AGENT_ROLE, SESSION_MODE_DEVICE, SESSION_MODE_USER,
    SESSION_MODE_CONVERSATION, TRANSPORT_WS,
)


def _entity(session_mode=SESSION_MODE_DEVICE):
    entry = MagicMock()
    entry.entry_id = "entry-1"
    entry.options = {
        CONF_AGENT_ROLE: DEFAULT_AGENT_ROLE,
        CONF_SESSION_MODE: session_mode,
        CONF_TRANSPORT: TRANSPORT_WS,
    }
    entry.runtime_data = MagicMock()
    entry.runtime_data.client = MagicMock()
    return CasaConversationEntity(entry)


def _input(device_id=None, user_id=None, conversation_id="c-1"):
    ui = MagicMock()
    ui.device_id = device_id
    ui.conversation_id = conversation_id
    ui.context = MagicMock()
    ui.context.user_id = user_id
    ui.language = "en"
    ui.text = "hi"
    ui.agent_id = "agent-x"
    return ui


class TestScopeId:
    def test_device_mode_uses_device_id(self):
        ent = _entity(SESSION_MODE_DEVICE)
        assert ent._build_scope_id(_input(device_id="d-1")) == "d-1"

    def test_device_mode_falls_back_to_user(self):
        ent = _entity(SESSION_MODE_DEVICE)
        assert ent._build_scope_id(_input(device_id=None, user_id="u-1")) == "u-1"

    def test_device_mode_falls_back_to_conversation(self):
        ent = _entity(SESSION_MODE_DEVICE)
        assert ent._build_scope_id(_input(device_id=None, user_id=None)) == "c-1"

    def test_user_mode_uses_user_id(self):
        ent = _entity(SESSION_MODE_USER)
        assert ent._build_scope_id(_input(device_id="d-1", user_id="u-1")) == "u-1"

    def test_user_mode_falls_back_to_device(self):
        ent = _entity(SESSION_MODE_USER)
        assert ent._build_scope_id(_input(device_id="d-1", user_id=None)) == "d-1"

    def test_conversation_mode(self):
        ent = _entity(SESSION_MODE_CONVERSATION)
        assert ent._build_scope_id(_input(device_id="d-1", user_id="u-1")) == "c-1"


async def _aiter(frames):
    for f in frames:
        yield f


class _ChatLogCapture:
    """Fake ChatLog that records the deltas fed into its stream."""

    def __init__(self):
        self.deltas: list[dict] = []

    async def async_add_delta_content_stream(self, agent_id, deltas):
        async for d in deltas:
            self.deltas.append(d)
            yield d


class TestHandleMessageHappy:
    @pytest.mark.asyncio
    async def test_streams_blocks_as_deltas(self):
        ent = _entity()
        ent._client.stream_utterance = MagicMock(return_value=_aiter([
            BlockFrame(text="Hello", final=False),
            BlockFrame(text=" world", final=True),
            DoneFrame(),
        ]))
        ent._client.prewarm = AsyncMock()

        ui = _input(device_id="d-1")
        chat_log = _ChatLogCapture()
        result = await ent._async_handle_message(ui, chat_log)

        assert len(chat_log.deltas) == 2
        assert chat_log.deltas[0]["role"] == "assistant"
        assert chat_log.deltas[0]["content"] == "Hello"
        assert "role" not in chat_log.deltas[1]
        assert chat_log.deltas[1]["content"] == " world"

    @pytest.mark.asyncio
    async def test_forwards_context_and_scope(self):
        ent = _entity()
        seen = {}

        def spy(**kw):
            seen.update(kw)
            return _aiter([DoneFrame()])

        ent._client.stream_utterance = MagicMock(side_effect=spy)

        ui = _input(device_id="d-1", user_id="u-1")
        chat_log = _ChatLogCapture()
        await ent._async_handle_message(ui, chat_log)
        assert seen["scope_id"] == "d-1"
        assert seen["agent_role"] == "butler"
        assert seen["context"]["device_id"] == "d-1"
        assert seen["context"]["user_id"] == "u-1"
        assert seen["context"]["language"] == "en"
        assert seen["context"]["conversation_id"] == "c-1"
        assert seen["transport"] == "ws"
        assert "utterance_id" in seen
