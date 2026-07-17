"""Strict model and validation for Casa's voice-agent catalog."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any

from .const import (
    MAX_VOICE_AGENT_NAME_LENGTH,
    MAX_VOICE_AGENTS,
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
