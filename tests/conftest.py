"""Shared test fixtures for Casa integration tests."""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest


HA_STUB_EXPORTS = frozenset({
    "homeassistant.core:callback", "homeassistant.core:HomeAssistant",
    "homeassistant.core:Event", "homeassistant.core:EventStateChangedData",
    "homeassistant.config_entries:ConfigFlow",
    "homeassistant.config_entries:OptionsFlow",
    "homeassistant.config_entries:ConfigEntry",
    "homeassistant.config_entries:ConfigFlowResult",
    "homeassistant.const:Platform", "homeassistant.const:MATCH_ALL",
    "homeassistant.const:EVENT_STATE_CHANGED",
    "homeassistant.exceptions:ConfigEntryAuthFailed",
    "homeassistant.exceptions:ConfigEntryNotReady",
    "homeassistant.helpers.aiohttp_client:async_get_clientsession",
    "homeassistant.helpers.event:async_track_state_change_event",
    "homeassistant.helpers.event:TrackStates",
    "homeassistant.helpers.event:async_track_state_change_filtered",
    "homeassistant.helpers.entity_registry:EVENT_ENTITY_REGISTRY_UPDATED",
    "homeassistant.helpers.entity_registry:async_get",
    "homeassistant.helpers.service_info.hassio:HassioServiceInfo",
    "homeassistant.helpers.device_registry:DeviceInfo",
    "homeassistant.helpers.device_registry:DeviceEntryType",
    "homeassistant.helpers.intent:IntentResponse",
    "homeassistant.helpers.intent:IntentResponseErrorCode",
    "homeassistant.components.conversation:ConversationEntity",
    "homeassistant.components.conversation:ConversationInput",
    "homeassistant.components.conversation:ConversationResult",
    "homeassistant.components.conversation:ChatLog",
    "homeassistant.components.conversation:async_get_result_from_chat_log",
    "homeassistant.components.conversation.chat_log:ChatLog",
    "homeassistant.components.assist_satellite:AssistSatelliteState",
    "homeassistant:core", "homeassistant:config_entries",
    "homeassistant:const", "homeassistant:exceptions", "homeassistant:helpers",
    "homeassistant:components", "homeassistant.helpers:aiohttp_client",
    "homeassistant.helpers:event", "homeassistant.helpers:entity_registry",
    "homeassistant.helpers:service_info",
    "homeassistant.helpers:device_registry", "homeassistant.helpers:intent",
    "homeassistant.helpers.service_info:hassio",
    "homeassistant.components:conversation",
    "homeassistant.components:assist_satellite",
})


def _make_stub_module(name: str) -> types.ModuleType:
    mod = types.ModuleType(name)
    sys.modules[name] = mod
    return mod


def _ensure_ha_stubs() -> None:
    if "homeassistant" in sys.modules:
        return

    ha = _make_stub_module("homeassistant")

    ha_core = _make_stub_module("homeassistant.core")
    ha_core.callback = lambda f: f
    ha_core.HomeAssistant = MagicMock
    ha_core.Event = MagicMock
    ha_core.EventStateChangedData = dict

    ha_ce = _make_stub_module("homeassistant.config_entries")

    class _ConfigFlowResult(dict):
        pass

    class _ConfigFlow:
        def __init_subclass__(cls, domain=None, **kw):
            super().__init_subclass__(**kw)
            if domain is not None:
                cls._domain = domain

        def __init__(self):
            self.hass = None
            self.context: dict = {}

        def async_show_form(self, *, step_id, data_schema=None, errors=None, **kw):
            return _ConfigFlowResult(type="form", step_id=step_id, data_schema=data_schema, errors=errors or {})

        def async_create_entry(self, *, title, data, **kw):
            return _ConfigFlowResult(type="create_entry", title=title, data=data)

        def async_abort(self, *, reason):
            return _ConfigFlowResult(type="abort", reason=reason)

        async def async_set_unique_id(self, unique_id):
            pass

        def _abort_if_unique_id_configured(self):
            pass

        def _get_reauth_entry(self):
            return MagicMock()

        def async_update_reload_and_abort(self, entry, *, data_updates=None, **kw):
            return _ConfigFlowResult(type="abort", reason="reauth_successful")

    class _OptionsFlow:
        def __init__(self):
            self.hass = None
            self.config_entry = MagicMock()
            self.config_entry.options = {}

        def async_show_form(self, *, step_id, data_schema=None, errors=None, **kw):
            return _ConfigFlowResult(type="form", step_id=step_id, data_schema=data_schema, errors=errors or {})

        def async_create_entry(self, *, data, **kw):
            return _ConfigFlowResult(type="create_entry", data=data)

        def add_suggested_values_to_schema(self, schema, suggested_values):
            return schema

    ha_ce.ConfigFlow = _ConfigFlow
    ha_ce.OptionsFlow = _OptionsFlow
    ha_ce.ConfigEntry = MagicMock
    ha_ce.ConfigFlowResult = _ConfigFlowResult

    ha_const = _make_stub_module("homeassistant.const")
    ha_const.Platform = MagicMock()
    ha_const.Platform.CONVERSATION = "conversation"
    ha_const.MATCH_ALL = "*"
    ha_const.EVENT_STATE_CHANGED = "state_changed"

    ha_exc = _make_stub_module("homeassistant.exceptions")
    ha_exc.ConfigEntryAuthFailed = type("ConfigEntryAuthFailed", (Exception,), {})
    ha_exc.ConfigEntryNotReady = type("ConfigEntryNotReady", (Exception,), {})

    ha_helpers = _make_stub_module("homeassistant.helpers")

    ha_aiohttp = _make_stub_module("homeassistant.helpers.aiohttp_client")
    ha_aiohttp.async_get_clientsession = MagicMock(return_value=MagicMock())

    ha_event = _make_stub_module("homeassistant.helpers.event")
    ha_event.async_track_state_change_event = MagicMock(return_value=lambda: None)

    class _TrackStates:
        def __init__(self, all_states, entities, domains):
            self.all_states = all_states
            self.entities = entities
            self.domains = domains

    ha_event.TrackStates = _TrackStates
    ha_event.async_track_state_change_filtered = MagicMock(return_value=MagicMock())

    ha_er = _make_stub_module("homeassistant.helpers.entity_registry")
    ha_er.EVENT_ENTITY_REGISTRY_UPDATED = "entity_registry_updated"
    ha_er.async_get = MagicMock(return_value=MagicMock())

    ha_si = _make_stub_module("homeassistant.helpers.service_info")
    ha_si_hassio = _make_stub_module("homeassistant.helpers.service_info.hassio")

    class _HassioServiceInfo:
        def __init__(self, config, name, slug, uuid):
            self.config = config
            self.name = name
            self.slug = slug
            self.uuid = uuid

    ha_si_hassio.HassioServiceInfo = _HassioServiceInfo

    ha_dr = _make_stub_module("homeassistant.helpers.device_registry")
    ha_dr.DeviceInfo = dict
    ha_dr.DeviceEntryType = MagicMock()
    ha_dr.DeviceEntryType.SERVICE = "service"

    ha_intent = _make_stub_module("homeassistant.helpers.intent")

    class _IntentResponse:
        def __init__(self, language=None):
            self.language = language
            self._speech: dict = {}
            self._error = None

        def async_set_speech(self, speech, speech_type="plain", extra_data=None):
            self._speech = {"speech": speech, "type": speech_type, "extra": extra_data}

        def async_set_error(self, code, message):
            self._error = (code, message)

    class _IntentResponseErrorCode:
        FAILED_TO_HANDLE = "failed_to_handle"
        NO_INTENT_MATCH = "no_intent_match"

    ha_intent.IntentResponse = _IntentResponse
    ha_intent.IntentResponseErrorCode = _IntentResponseErrorCode

    ha_components = _make_stub_module("homeassistant.components")

    ha_conv = _make_stub_module("homeassistant.components.conversation")

    def _unique_id_property(self):
        return getattr(self, "_attr_unique_id", None)

    ha_conv.ConversationEntity = type("ConversationEntity", (), {
        "_attr_has_entity_name": False,
        "_attr_name": None,
        "_attr_unique_id": None,
        "unique_id": property(_unique_id_property),
    })
    ha_conv.ConversationInput = MagicMock
    ha_conv.ConversationResult = MagicMock
    ha_conv.ChatLog = MagicMock

    def mock_get_result(user_input, chat_log):
        return {"type": "result", "conversation_id": getattr(user_input, "conversation_id", None)}

    ha_conv.async_get_result_from_chat_log = mock_get_result

    ha_conv_chatlog = _make_stub_module("homeassistant.components.conversation.chat_log")
    ha_conv_chatlog.ChatLog = ha_conv.ChatLog

    ha_ass = _make_stub_module("homeassistant.components.assist_satellite")

    class _AssistSatelliteState:
        LISTENING = type("_E", (), {"value": "listening"})()
        IDLE = type("_E", (), {"value": "idle"})()
        PROCESSING = type("_E", (), {"value": "processing"})()
        RESPONDING = type("_E", (), {"value": "responding"})()

    ha_ass.AssistSatelliteState = _AssistSatelliteState

    ha.core = ha_core
    ha.config_entries = ha_ce
    ha.const = ha_const
    ha.exceptions = ha_exc
    ha.helpers = ha_helpers
    ha_helpers.aiohttp_client = ha_aiohttp
    ha_helpers.event = ha_event
    ha_helpers.entity_registry = ha_er
    ha_helpers.service_info = ha_si
    ha_si.hassio = ha_si_hassio
    ha_helpers.device_registry = ha_dr
    ha_helpers.intent = ha_intent
    ha.components = ha_components
    ha_components.conversation = ha_conv
    ha_components.assist_satellite = ha_ass


_ensure_ha_stubs()


@pytest.fixture
def mock_session() -> MagicMock:
    return MagicMock()


class AsyncIteratorMock:
    """Mock for aiohttp response content (async iterator of bytes)."""

    def __init__(self, items: list[bytes]) -> None:
        self._items = iter(items)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._items)
        except StopIteration:
            raise StopAsyncIteration


@pytest.fixture
def async_iter():
    return AsyncIteratorMock
