"""Tests for per-subentry Casa conversation entities."""

from __future__ import annotations

import asyncio
from types import MappingProxyType
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from homeassistant.components import conversation as ha_conversation
from homeassistant.config_entries import ConfigSubentry

from custom_components.casa import CasaAgentRuntime
from custom_components.casa.api import (
    AuthenticationError,
    BlockFrame,
    ConnectionState,
    DoneFrame,
    ErrorFrame,
    HandoffFrame,
)
from custom_components.casa.conversation import (
    CasaConversationEntity,
    async_setup_entry,
)
from custom_components.casa.catalog import role_label
from custom_components.casa.const import (
    CONF_AGENT_NAME,
    CONF_IDLE_STABILITY_MS,
    CONF_ROLE,
    CONF_SESSION_MODE,
    CONF_TRANSPORT,
    DOMAIN,
    FALLBACK,
    SESSION_MODE_CONVERSATION,
    SESSION_MODE_DEVICE,
    SESSION_MODE_USER,
    SILENT_STREAM_FALLBACK,
    SUBENTRY_TYPE_AGENT,
    TRANSPORT_SSE,
    TRANSPORT_WS,
)


def _subentry(
    role: str = "butler",
    name: str = "Tina",
    *,
    subentry_id: str = "child-butler",
    session_mode: str = SESSION_MODE_DEVICE,
    transport: str = TRANSPORT_WS,
    subentry_type: str = SUBENTRY_TYPE_AGENT,
) -> ConfigSubentry:
    return ConfigSubentry(
        data=MappingProxyType({
            CONF_ROLE: role,
            CONF_AGENT_NAME: name,
            CONF_SESSION_MODE: session_mode,
            CONF_TRANSPORT: transport,
            CONF_IDLE_STABILITY_MS: 750,
        }),
        subentry_type=subentry_type,
        # Production keeps the role-derived Home Assistant identity separate
        # from Casa's mutable persona alias in CONF_AGENT_NAME.
        title=role_label(role),
        unique_id=role,
        subentry_id=subentry_id,
    )


def _runtime(
    subentry: ConfigSubentry,
    *,
    catalog_present: bool = True,
    client=None,
) -> CasaAgentRuntime:
    if client is None and catalog_present:
        client = MagicMock()
    return CasaAgentRuntime(
        parent_entry_id="entry-1",
        subentry_id=subentry.subentry_id,
        role=subentry.unique_id,
        name=subentry.data[CONF_AGENT_NAME],
        session_mode=subentry.data[CONF_SESSION_MODE],
        transport=subentry.data[CONF_TRANSPORT],
        idle_stability_ms=subentry.data[CONF_IDLE_STABILITY_MS],
        catalog_present=catalog_present,
        client=client,
        manager=None,
    )


def _entry(*children: tuple[ConfigSubentry, CasaAgentRuntime]):
    entry = MagicMock()
    entry.entry_id = "entry-1"
    entry.subentries = {
        subentry.subentry_id: subentry
        for subentry, _runtime_item in children
    }
    entry.runtime_data = MagicMock()
    entry.runtime_data.agents = {
        subentry.subentry_id: runtime
        for subentry, runtime in children
    }
    return entry


def _entity(
    session_mode: str = SESSION_MODE_DEVICE,
    *,
    transport: str = TRANSPORT_WS,
) -> CasaConversationEntity:
    subentry = _subentry(session_mode=session_mode, transport=transport)
    runtime = _runtime(subentry)
    entry = _entry((subentry, runtime))
    return CasaConversationEntity(entry, subentry, runtime)


def _input(device_id=None, user_id=None, conversation_id="c-1"):
    user_input = MagicMock()
    user_input.device_id = device_id
    user_input.conversation_id = conversation_id
    user_input.context = MagicMock()
    user_input.context.user_id = user_id
    user_input.language = "en"
    user_input.text = "hi"
    user_input.agent_id = "agent-x"
    return user_input


async def _aiter(frames):
    for frame in frames:
        yield frame


class _ChatLogCapture:
    """Fake ChatLog that records the deltas fed into its stream."""

    def __init__(self):
        self.deltas: list[dict] = []

    async def async_add_delta_content_stream(self, agent_id, deltas):
        async for delta in deltas:
            self.deltas.append(delta)
            yield delta


class TestPerSubentryEntities:
    @pytest.mark.asyncio
    async def test_setup_keeps_persona_metadata_under_stable_role_voice_identity(
        self,
        async_add_entities,
    ):
        butler = _subentry()
        concierge = _subentry(
            "concierge",
            "Gary",
            subentry_id="child-concierge",
            session_mode=SESSION_MODE_CONVERSATION,
            transport=TRANSPORT_SSE,
        )
        unrelated = _subentry(
            "ignored",
            "Ignored",
            subentry_id="child-ignored",
            subentry_type="other",
        )
        butler_runtime = _runtime(butler)
        concierge_runtime = _runtime(concierge)
        entry = _entry(
            (butler, butler_runtime),
            (concierge, concierge_runtime),
            (unrelated, _runtime(unrelated)),
        )

        await async_setup_entry(MagicMock(), entry, async_add_entities)

        assert len(async_add_entities.calls) == 2
        assert [call[2] for call in async_add_entities.calls] == [
            "child-butler",
            "child-concierge",
        ]
        tina = async_add_entities.calls[0][0][0]
        gary = async_add_entities.calls[1][0][0]
        assert tina._attr_name == "Voice"
        assert gary._attr_name == "Voice"
        assert tina._attr_has_entity_name is True
        assert gary._attr_has_entity_name is True
        assert tina.unique_id == "entry-1:butler"
        assert gary.unique_id == "entry-1:concierge"
        assert tina.device_info["identifiers"] == {(DOMAIN, "entry-1:butler")}
        assert gary.device_info["identifiers"] == {(DOMAIN, "entry-1:concierge")}
        assert tina.device_info["name"] == "Casa Butler"
        assert gary.device_info["name"] == "Casa Concierge"
        # Home Assistant uses the device plus entity name for the picker;
        # persona aliases remain model metadata rather than service identity.
        assert tina.device_info["model"] == "Tina (butler)"
        assert gary.device_info["model"] == "Gary (concierge)"
        assert tina._runtime is butler_runtime
        assert gary._runtime is concierge_runtime
        assert tina._client is butler_runtime.client
        assert gary._client is concierge_runtime.client
        assert tina._agent_role == "butler"
        assert gary._agent_role == "concierge"
        assert tina._session_mode == SESSION_MODE_DEVICE
        assert gary._session_mode == SESSION_MODE_CONVERSATION
        assert tina._transport == TRANSPORT_WS
        assert gary._transport == TRANSPORT_SSE

    @pytest.mark.asyncio
    async def test_each_entity_routes_only_to_its_fixed_runtime_client(
        self,
        async_add_entities,
    ):
        butler = _subentry()
        concierge = _subentry(
            "concierge",
            "Gary",
            subentry_id="child-concierge",
            session_mode=SESSION_MODE_CONVERSATION,
            transport=TRANSPORT_SSE,
        )
        tina_runtime = _runtime(butler)
        gary_runtime = _runtime(concierge)
        tina_runtime.client.stream_utterance = MagicMock(
            return_value=_aiter([DoneFrame()]),
        )
        gary_runtime.client.stream_utterance = MagicMock(
            return_value=_aiter([DoneFrame()]),
        )
        entry = _entry((butler, tina_runtime), (concierge, gary_runtime))
        await async_setup_entry(MagicMock(), entry, async_add_entities)
        tina = async_add_entities.calls[0][0][0]
        gary = async_add_entities.calls[1][0][0]
        user_input = _input(device_id="dev-k", user_id="user-1")

        await tina._async_handle_message(user_input, _ChatLogCapture())
        await gary._async_handle_message(user_input, _ChatLogCapture())

        tina_call = tina_runtime.client.stream_utterance.call_args.kwargs
        gary_call = gary_runtime.client.stream_utterance.call_args.kwargs
        assert tina_runtime.client.stream_utterance.call_count == 1
        assert gary_runtime.client.stream_utterance.call_count == 1
        assert tina_call["agent_role"] == "butler"
        assert tina_call["scope_id"] == "dev-k"
        assert tina_call["transport"] == TRANSPORT_WS
        assert gary_call["agent_role"] == "concierge"
        assert gary_call["scope_id"] == "c-1"
        assert gary_call["transport"] == TRANSPORT_SSE

    def test_catalog_rename_changes_model_but_not_stable_voice_identity(self):
        old_subentry = _subentry(name="Old Tina")
        old_runtime = _runtime(old_subentry)
        old_entity = CasaConversationEntity(
            _entry((old_subentry, old_runtime)),
            old_subentry,
            old_runtime,
        )
        renamed = _subentry(name="Tina")
        renamed_runtime = _runtime(renamed)
        renamed_entity = CasaConversationEntity(
            _entry((renamed, renamed_runtime)),
            renamed,
            renamed_runtime,
        )

        assert old_entity.unique_id == renamed_entity.unique_id == "entry-1:butler"
        assert old_entity._attr_name == "Voice"
        assert renamed_entity._attr_name == "Voice"
        assert old_entity.device_info["model"] == "Old Tina (butler)"
        assert renamed_entity.device_info["model"] == "Tina (butler)"

    @pytest.mark.asyncio
    async def test_missing_catalog_child_is_unavailable_without_client_fallback(self):
        missing = _subentry("legacy", "Legacy", subentry_id="child-legacy")
        sibling = _subentry()
        missing_runtime = _runtime(
            missing,
            catalog_present=False,
            client=None,
        )
        sibling_runtime = _runtime(sibling)
        sibling_runtime.client.stream_utterance = MagicMock(
            return_value=_aiter([DoneFrame()]),
        )
        entry = _entry((missing, missing_runtime), (sibling, sibling_runtime))
        entity = CasaConversationEntity(entry, missing, missing_runtime)

        assert entity.available is False
        result = await entity._async_handle_message(
            _input(device_id="dev-k"),
            _ChatLogCapture(),
        )

        assert result.response._speech["speech"] == FALLBACK
        sibling_runtime.client.stream_utterance.assert_not_called()

    @pytest.mark.asyncio
    async def test_availability_updates_only_the_subscribed_child_entity(
        self,
        monkeypatch,
    ):
        async def base_lifecycle_noop(_entity):
            return None

        monkeypatch.setattr(
            ha_conversation.ConversationEntity,
            "async_added_to_hass",
            base_lifecycle_noop,
            raising=False,
        )
        monkeypatch.setattr(
            ha_conversation.ConversationEntity,
            "async_will_remove_from_hass",
            base_lifecycle_noop,
            raising=False,
        )
        butler = _subentry()
        concierge = _subentry(
            "concierge",
            "Gary",
            subentry_id="child-concierge",
        )
        tina_runtime = _runtime(butler)
        gary_runtime = _runtime(concierge)
        tina = CasaConversationEntity(
            _entry((butler, tina_runtime)),
            butler,
            tina_runtime,
        )
        gary = CasaConversationEntity(
            _entry((concierge, gary_runtime)),
            concierge,
            gary_runtime,
        )
        tina.async_write_ha_state = MagicMock()
        gary.async_write_ha_state = MagicMock()

        await tina.async_added_to_hass()
        await gary.async_added_to_hass()
        tina_runtime.set_connection_state(ConnectionState.CONNECTED)

        assert tina.available is True
        assert gary.available is False
        tina.async_write_ha_state.assert_called_once_with()
        gary.async_write_ha_state.assert_not_called()

        await tina.async_will_remove_from_hass()
        tina_runtime.set_connection_state(ConnectionState.DISCONNECTED)
        gary_runtime.set_connection_state(ConnectionState.CONNECTED)
        tina.async_write_ha_state.assert_called_once_with()
        gary.async_write_ha_state.assert_called_once_with()


class TestScopeId:
    def test_device_mode_uses_device_id(self):
        entity = _entity(SESSION_MODE_DEVICE)
        assert entity._build_scope_id(_input(device_id="d-1")) == "d-1"

    def test_device_mode_falls_back_to_user(self):
        entity = _entity(SESSION_MODE_DEVICE)
        assert entity._build_scope_id(_input(device_id=None, user_id="u-1")) == "u-1"

    def test_device_mode_falls_back_to_conversation(self):
        entity = _entity(SESSION_MODE_DEVICE)
        assert entity._build_scope_id(_input(device_id=None, user_id=None)) == "c-1"

    def test_user_mode_uses_user_id(self):
        entity = _entity(SESSION_MODE_USER)
        assert entity._build_scope_id(_input(device_id="d-1", user_id="u-1")) == "u-1"

    def test_user_mode_falls_back_to_device(self):
        entity = _entity(SESSION_MODE_USER)
        assert entity._build_scope_id(_input(device_id="d-1", user_id=None)) == "d-1"

    def test_conversation_mode(self):
        entity = _entity(SESSION_MODE_CONVERSATION)
        assert entity._build_scope_id(_input(device_id="d-1", user_id="u-1")) == "c-1"


class TestHandleMessageHappy:
    @pytest.mark.asyncio
    async def test_handoff_returns_a_direct_spoken_result_without_chat_log_delta(self):
        entity = _entity()
        entity._client.stream_utterance = MagicMock(return_value=_aiter([
            HandoffFrame(
                handoff_id="handoff-1",
                text="Got it — I'll ask the judge. This may take a minute.",
            ),
        ]))
        chat_log = MagicMock()
        chat_log.async_add_delta_content_stream = AsyncMock()

        result = await entity._async_handle_message(_input(device_id="d-1"), chat_log)

        assert result.response._speech["speech"] == (
            "Got it — I'll ask the judge. This may take a minute."
        )
        chat_log.async_add_delta_content_stream.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_duplicate_handoff_has_one_user_visible_result(self):
        entity = _entity()
        entity._client.stream_utterance = MagicMock(return_value=_aiter([
            HandoffFrame(handoff_id="handoff-1", text="First acknowledgement."),
            HandoffFrame(handoff_id="handoff-1", text="Duplicate acknowledgement."),
        ]))
        chat_log = MagicMock()
        chat_log.async_add_delta_content_stream = AsyncMock()

        result = await entity._async_handle_message(_input(device_id="d-1"), chat_log)

        assert result.response._speech["speech"] == "First acknowledgement."
        chat_log.async_add_delta_content_stream.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_block_after_handoff_is_ignored(self):
        entity = _entity()
        entity._client.stream_utterance = MagicMock(return_value=_aiter([
            HandoffFrame(handoff_id="handoff-1", text="Handoff acknowledgement."),
            BlockFrame(text="Stale block", final=True),
            DoneFrame(),
        ]))
        chat_log = MagicMock()
        chat_log.async_add_delta_content_stream = AsyncMock()

        result = await entity._async_handle_message(_input(device_id="d-1"), chat_log)

        assert result.response._speech["speech"] == "Handoff acknowledgement."
        chat_log.async_add_delta_content_stream.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_streams_blocks_as_deltas(self):
        entity = _entity()
        entity._client.stream_utterance = MagicMock(return_value=_aiter([
            BlockFrame(text="Hello", final=False),
            BlockFrame(text=" world", final=True),
            DoneFrame(),
        ]))
        entity._client.register_session = AsyncMock()

        chat_log = _ChatLogCapture()
        await entity._async_handle_message(_input(device_id="d-1"), chat_log)

        assert len(chat_log.deltas) == 2
        assert chat_log.deltas[0]["role"] == "assistant"
        assert chat_log.deltas[0]["content"] == "Hello"
        assert "role" not in chat_log.deltas[1]
        assert chat_log.deltas[1]["content"] == " world"

    @pytest.mark.asyncio
    async def test_forwards_context_and_scope(self):
        entity = _entity()
        seen = {}

        def spy(**kwargs):
            seen.update(kwargs)
            return _aiter([DoneFrame()])

        entity._client.stream_utterance = MagicMock(side_effect=spy)

        user_input = _input(device_id="d-1", user_id="u-1")
        await entity._async_handle_message(user_input, _ChatLogCapture())
        assert seen["scope_id"] == "d-1"
        assert seen["agent_role"] == "butler"
        assert seen["context"]["device_id"] == "d-1"
        assert seen["context"]["user_id"] == "u-1"
        assert seen["context"]["language"] == "en"
        assert seen["context"]["conversation_id"] == "c-1"
        assert seen["transport"] == "ws"
        assert "utterance_id" in seen


class TestHandleMessageErrors:
    @pytest.mark.asyncio
    async def test_casa_error_with_spoken(self):
        entity = _entity()
        entity._client.stream_utterance = MagicMock(return_value=_aiter([
            ErrorFrame(kind_="timeout", spoken="[flat] That took too long."),
        ]))
        result = await entity._async_handle_message(
            _input(device_id="d-1"),
            _ChatLogCapture(),
        )
        assert "[flat] That took too long." in result.response._speech["speech"]
        assert result.response._speech["type"] == "plain"

    @pytest.mark.asyncio
    async def test_casa_error_empty_spoken_uses_fallback(self):
        entity = _entity()
        entity._client.stream_utterance = MagicMock(return_value=_aiter([
            ErrorFrame(kind_="sdk_error", spoken=""),
        ]))
        result = await entity._async_handle_message(
            _input(device_id="d-1"),
            _ChatLogCapture(),
        )
        assert result.response._speech["speech"] == FALLBACK

    @pytest.mark.asyncio
    async def test_authentication_triggers_reauth(self):
        entity = _entity()
        entity.entry.async_start_reauth = MagicMock()
        entity.hass = MagicMock()

        async def raising(**kwargs):
            raise AuthenticationError("bad secret")
            yield

        entity._client.stream_utterance = MagicMock(side_effect=raising)
        await entity._async_handle_message(
            _input(device_id="d-1"),
            _ChatLogCapture(),
        )
        entity.entry.async_start_reauth.assert_called_once_with(entity.hass)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "failure",
        [
            aiohttp.ClientError("offline"),
            ConnectionError("socket closed"),
            OSError("network unavailable"),
        ],
    )
    async def test_connection_failures_use_spoken_fallback(self, failure):
        entity = _entity()

        async def raising(**_kwargs):
            raise failure
            yield

        entity._client.stream_utterance = MagicMock(side_effect=raising)

        result = await entity._async_handle_message(
            _input(device_id="d-1"),
            _ChatLogCapture(),
        )

        assert result.response._speech["speech"] == FALLBACK

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "failure",
        [RuntimeError("programming defect"), asyncio.CancelledError()],
    )
    async def test_non_connection_failures_propagate(self, failure):
        entity = _entity()

        async def raising(**_kwargs):
            raise failure
            yield

        entity._client.stream_utterance = MagicMock(side_effect=raising)

        with pytest.raises(type(failure)):
            await entity._async_handle_message(
                _input(device_id="d-1"),
                _ChatLogCapture(),
            )

    @pytest.mark.asyncio
    async def test_zero_content_done_triggers_silent_fallback(self):
        entity = _entity()
        entity._client.stream_utterance = MagicMock(
            return_value=_aiter([DoneFrame()]),
        )
        result = await entity._async_handle_message(
            _input(device_id="d-1"),
            _ChatLogCapture(),
        )
        assert result.response._speech["speech"] == SILENT_STREAM_FALLBACK

    @pytest.mark.asyncio
    async def test_empty_block_text_is_skipped(self):
        entity = _entity()
        entity._client.stream_utterance = MagicMock(return_value=_aiter([
            BlockFrame(text="", final=False),
            BlockFrame(text="real", final=False),
            DoneFrame(),
        ]))
        chat_log = _ChatLogCapture()
        await entity._async_handle_message(_input(device_id="d-1"), chat_log)
        assert [delta["content"] for delta in chat_log.deltas] == ["real"]
