"""Strict model and validation for Casa's voice-agent catalog."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

from homeassistant.config_entries import (
    ConfigEntry,
    ConfigSubentry,
    ConfigSubentryData,
)
from homeassistant.core import HomeAssistant

from .const import (
    CONF_AGENT_NAME,
    CONF_IDLE_STABILITY_MS,
    CONF_ROLE,
    CONF_SESSION_MODE,
    CONF_TRANSPORT,
    DEFAULT_IDLE_STABILITY_MS,
    MAX_VOICE_AGENT_NAME_LENGTH,
    MAX_VOICE_AGENTS,
    SESSION_MODE_CONVERSATION,
    SESSION_MODE_DEVICE,
    SUBENTRY_TYPE_AGENT,
    TRANSPORT_WS,
    VOICE_AGENT_CATALOG_SCHEMA_VERSION,
)

_ROLE_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")


@dataclass(frozen=True, slots=True)
class VoiceAgent:
    """One stable Casa voice-agent identity and its display name."""

    role: str
    name: str


@dataclass(frozen=True, slots=True)
class VoiceAgentCatalog:
    """A completely validated Casa voice-agent catalog."""

    schema_version: int
    agents: tuple[VoiceAgent, ...]


class CatalogValidationError(ValueError):
    """Raised when Casa's complete discovery response is unsafe to use."""


def role_label(role: str) -> str:
    """Return the stable human-readable label for a Casa role."""
    return role.replace("_", " ").replace("-", " ").title()


def parse_voice_agent_catalog(payload: Any) -> VoiceAgentCatalog:
    """Validate the complete schema-1 catalog and return immutable agents."""
    if not isinstance(payload, dict) or set(payload) != {"schema_version", "agents"}:
        raise CatalogValidationError("invalid_top_level")
    if (
        type(payload["schema_version"]) is not int
        or payload["schema_version"] != VOICE_AGENT_CATALOG_SCHEMA_VERSION
    ):
        raise CatalogValidationError("unsupported_schema")

    raw_agents = payload["agents"]
    if not isinstance(raw_agents, list) or len(raw_agents) > MAX_VOICE_AGENTS:
        raise CatalogValidationError("invalid_agent_count")

    agents: list[VoiceAgent] = []
    seen: set[str] = set()
    for raw in raw_agents:
        if not isinstance(raw, dict) or set(raw) != {"role", "name"}:
            raise CatalogValidationError("invalid_agent")

        role = raw["role"]
        name = raw["name"]
        if not isinstance(role, str) or _ROLE_RE.fullmatch(role) is None:
            raise CatalogValidationError("invalid_role")
        if role in seen:
            raise CatalogValidationError("duplicate_role")
        if (
            not isinstance(name, str)
            or name != name.strip()
            or not 1 <= len(name) <= MAX_VOICE_AGENT_NAME_LENGTH
            or any(unicodedata.category(char).startswith("C") for char in name)
        ):
            raise CatalogValidationError("invalid_name")

        seen.add(role)
        agents.append(VoiceAgent(role=role, name=name))

    agents.sort(key=lambda agent: agent.role)
    return VoiceAgentCatalog(
        schema_version=VOICE_AGENT_CATALOG_SCHEMA_VERSION,
        agents=tuple(agents),
    )


def agent_defaults(role: str) -> dict[str, Any]:
    """Return mutable per-agent settings for a newly discovered role."""
    return {
        CONF_SESSION_MODE: (
            SESSION_MODE_CONVERSATION
            if role == "concierge"
            else SESSION_MODE_DEVICE
        ),
        CONF_TRANSPORT: TRANSPORT_WS,
        CONF_IDLE_STABILITY_MS: DEFAULT_IDLE_STABILITY_MS,
    }


def agent_data(agent: VoiceAgent) -> dict[str, Any]:
    """Build one child entry's immutable identity and mutable defaults."""
    return {
        CONF_ROLE: agent.role,
        CONF_AGENT_NAME: agent.name,
        **agent_defaults(agent.role),
    }


def _validated_agents(catalog: VoiceAgentCatalog) -> tuple[VoiceAgent, ...]:
    """Revalidate a complete model before performing any HA mutation."""
    return parse_voice_agent_catalog({
        "schema_version": catalog.schema_version,
        "agents": [
            {"role": agent.role, "name": agent.name}
            for agent in catalog.agents
        ],
    }).agents


def initial_subentry_data(
    catalog: VoiceAgentCatalog,
) -> list[ConfigSubentryData]:
    """Create deterministic initial subentry data for a new parent entry."""
    return [
        {
            "data": agent_data(agent),
            "subentry_type": SUBENTRY_TYPE_AGENT,
            "title": role_label(agent.role),
            "unique_id": agent.role,
        }
        for agent in _validated_agents(catalog)
    ]


def reconcile_catalog(
    hass: HomeAssistant,
    entry: ConfigEntry,
    catalog: VoiceAgentCatalog,
) -> None:
    """Add and rename catalog children without deleting or resetting settings."""
    agents = _validated_agents(catalog)
    existing_by_role = {
        subentry.unique_id: subentry
        for subentry in entry.subentries.values()
        if (
            subentry.subentry_type == SUBENTRY_TYPE_AGENT
            and subentry.unique_id is not None
        )
    }

    for agent in agents:
        existing = existing_by_role.get(agent.role)
        if existing is None:
            subentry = ConfigSubentry(
                data=MappingProxyType(agent_data(agent)),
                subentry_type=SUBENTRY_TYPE_AGENT,
                title=role_label(agent.role),
                unique_id=agent.role,
            )
            hass.config_entries.async_add_subentry(entry, subentry)
            existing_by_role[agent.role] = subentry
            continue

        if (
            existing.title == role_label(agent.role)
            and existing.data.get(CONF_AGENT_NAME) == agent.name
        ):
            continue

        updated_data = {
            **existing.data,
            CONF_AGENT_NAME: agent.name,
        }
        hass.config_entries.async_update_subentry(
            entry,
            existing,
            data=MappingProxyType(updated_data),
            title=role_label(agent.role),
        )
