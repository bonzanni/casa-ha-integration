"""Tests for Casa voice-agent catalog validation."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from types import MappingProxyType, SimpleNamespace

import pytest

from homeassistant.config_entries import ConfigSubentry

from custom_components.casa.catalog import (
    CatalogValidationError,
    VoiceAgent,
    VoiceAgentCatalog,
    initial_subentry_data,
    parse_voice_agent_catalog,
    reconcile_catalog,
    role_label,
)
from custom_components.casa.const import (
    CONF_AGENT_NAME,
    CONF_IDLE_STABILITY_MS,
    CONF_ROLE,
    CONF_SESSION_MODE,
    CONF_TRANSPORT,
    DEFAULT_IDLE_STABILITY_MS,
    SESSION_MODE_CONVERSATION,
    SESSION_MODE_DEVICE,
    SUBENTRY_TYPE_AGENT,
    TRANSPORT_SSE,
    TRANSPORT_WS,
)


def _catalog(*role_names: str) -> VoiceAgentCatalog:
    assert len(role_names) % 2 == 0
    return VoiceAgentCatalog(
        schema_version=1,
        agents=tuple(
            VoiceAgent(role=role_names[index], name=role_names[index + 1])
            for index in range(0, len(role_names), 2)
        ),
    )


def test_parse_catalog_returns_sorted_immutable_agents():
    catalog = parse_voice_agent_catalog({
        "schema_version": 1,
        "agents": [
            {"role": "concierge", "name": "Gary"},
            {"role": "butler", "name": "Tina"},
        ],
    })

    assert catalog.schema_version == 1
    assert catalog.agents == (
        VoiceAgent(role="butler", name="Tina"),
        VoiceAgent(role="concierge", name="Gary"),
    )
    with pytest.raises(FrozenInstanceError):
        catalog.schema_version = 2
    with pytest.raises(FrozenInstanceError):
        catalog.agents[0].name = "Changed"


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"schema_version": 1, "agents": [], "extra": True},
        {"schema_version": True, "agents": []},
        {"schema_version": 2, "agents": []},
        {"schema_version": 1, "agents": "not-a-list"},
        {"schema_version": 1, "agents": [{"role": "Upper", "name": "X"}]},
        {"schema_version": 1, "agents": [{"role": "ok", "name": "bad\nname"}]},
        {
            "schema_version": 1,
            "agents": [
                {"role": "same", "name": "One"},
                {"role": "same", "name": "Two"},
            ],
        },
        {
            "schema_version": 1,
            "agents": [
                {"role": "ok", "name": "Name", "prompt": "private"},
            ],
        },
        {"schema_version": 1, "agents": ["not-an-object"]},
    ],
)
def test_parse_catalog_rejects_the_whole_invalid_response(payload):
    with pytest.raises(CatalogValidationError):
        parse_voice_agent_catalog(payload)


def test_parse_catalog_accepts_twenty_agents():
    catalog = parse_voice_agent_catalog({
        "schema_version": 1,
        "agents": [
            {"role": f"role_{index}", "name": f"Agent {index}"}
            for index in range(20)
        ],
    })

    assert len(catalog.agents) == 20


def test_parse_catalog_rejects_twenty_one_agents():
    payload = {
        "schema_version": 1,
        "agents": [
            {"role": f"role_{index}", "name": f"Agent {index}"}
            for index in range(21)
        ],
    }

    with pytest.raises(CatalogValidationError):
        parse_voice_agent_catalog(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("role", "a" * 64),
        ("name", "N" * 128),
    ],
)
def test_parse_catalog_accepts_bounded_role_and_name(field, value):
    agent = {"role": "valid", "name": "Valid"}
    agent[field] = value

    catalog = parse_voice_agent_catalog({"schema_version": 1, "agents": [agent]})

    assert getattr(catalog.agents[0], field) == value


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("role", "a" * 65),
        ("name", "N" * 129),
    ],
)
def test_parse_catalog_rejects_overlong_role_and_name(field, value):
    agent = {"role": "valid", "name": "Valid"}
    agent[field] = value

    with pytest.raises(CatalogValidationError):
        parse_voice_agent_catalog({"schema_version": 1, "agents": [agent]})


def test_initial_subentries_have_role_defaults():
    children = initial_subentry_data(
        _catalog("concierge", "Gary", "butler", "Tina"),
    )

    assert [(child["unique_id"], child["title"]) for child in children] == [
        ("butler", "Butler"),
        ("concierge", "Concierge"),
    ]
    assert children[0]["subentry_type"] == SUBENTRY_TYPE_AGENT
    assert children[0]["data"][CONF_ROLE] == "butler"
    assert children[0]["data"][CONF_AGENT_NAME] == "Tina"
    assert children[0]["data"][CONF_SESSION_MODE] == SESSION_MODE_DEVICE
    assert (
        children[1]["data"][CONF_SESSION_MODE]
        == SESSION_MODE_CONVERSATION
    )
    assert all(
        child["data"][CONF_TRANSPORT] == TRANSPORT_WS for child in children
    )
    assert all(
        child["data"][CONF_IDLE_STABILITY_MS] == DEFAULT_IDLE_STABILITY_MS
        for child in children
    )


def test_role_label_is_stable_and_human_readable():
    assert role_label("butler") == "Butler"
    assert role_label("mtg_judge") == "Mtg Judge"


def test_reconcile_adds_new_role_once_and_is_idempotent(hass):
    entry = SimpleNamespace(subentries={})
    catalog = _catalog("specialist", "Grace")

    reconcile_catalog(hass, entry, catalog)
    reconcile_catalog(hass, entry, catalog)

    assert len(hass.config_entries.added_subentries) == 1
    child = next(iter(entry.subentries.values()))
    assert child.unique_id == "specialist"
    assert child.title == "Specialist"
    assert child.subentry_type == SUBENTRY_TYPE_AGENT
    assert child.data[CONF_SESSION_MODE] == SESSION_MODE_DEVICE
    assert hass.config_entries.updated_subentries == []


def test_reconcile_rename_preserves_identity_and_user_settings(hass):
    child = ConfigSubentry(
        data=MappingProxyType({
            CONF_ROLE: "butler",
            CONF_AGENT_NAME: "Old Tina",
            CONF_SESSION_MODE: SESSION_MODE_CONVERSATION,
            CONF_TRANSPORT: TRANSPORT_SSE,
            CONF_IDLE_STABILITY_MS: 1250,
        }),
        subentry_type=SUBENTRY_TYPE_AGENT,
        title="Old Tina",
        unique_id="butler",
        subentry_id="stable-subentry-id",
    )
    entry = SimpleNamespace(subentries={child.subentry_id: child})

    catalog = _catalog("butler", "Tina")
    reconcile_catalog(hass, entry, catalog)
    reconcile_catalog(hass, entry, catalog)

    assert hass.config_entries.added_subentries == []
    assert len(hass.config_entries.updated_subentries) == 1
    assert child.subentry_id == "stable-subentry-id"
    assert child.unique_id == "butler"
    assert child.title == "Butler"
    assert child.data == {
        CONF_ROLE: "butler",
        CONF_AGENT_NAME: "Tina",
        CONF_SESSION_MODE: SESSION_MODE_CONVERSATION,
        CONF_TRANSPORT: TRANSPORT_SSE,
        CONF_IDLE_STABILITY_MS: 1250,
    }


def test_reconcile_never_removes_roles_missing_from_catalog(hass):
    child = ConfigSubentry(
        data=MappingProxyType({
            CONF_ROLE: "butler",
            CONF_AGENT_NAME: "Tina",
        }),
        subentry_type=SUBENTRY_TYPE_AGENT,
        title="Tina",
        unique_id="butler",
        subentry_id="stable-subentry-id",
    )
    entry = SimpleNamespace(subentries={child.subentry_id: child})

    reconcile_catalog(hass, entry, _catalog("concierge", "Gary"))

    assert "stable-subentry-id" in entry.subentries
    assert entry.subentries["stable-subentry-id"] is child
    assert len(hass.config_entries.added_subentries) == 1
    assert hass.config_entries.updated_subentries == []


def test_reconcile_complete_invalid_catalog_has_zero_mutations(hass):
    entry = SimpleNamespace(subentries={})
    invalid_catalog = VoiceAgentCatalog(
        schema_version=1,
        agents=(
            VoiceAgent(role="duplicate", name="One"),
            VoiceAgent(role="duplicate", name="Two"),
        ),
    )

    with pytest.raises(CatalogValidationError):
        reconcile_catalog(hass, entry, invalid_catalog)

    assert entry.subentries == {}
    assert hass.config_entries.added_subentries == []
    assert hass.config_entries.updated_subentries == []
