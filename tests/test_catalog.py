"""Tests for Casa voice-agent catalog validation."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from custom_components.casa.catalog import (
    CatalogValidationError,
    VoiceAgent,
    parse_voice_agent_catalog,
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
