"""Tests for CasaConversationEntity."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

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
