"""Contract tests for security and delivery guarantees in release docs."""

import json
import re
from pathlib import Path

import pytest


_ROOT = Path(__file__).parents[1]


def _normalized(filename: str) -> str:
    return " ".join((_ROOT / filename).read_text(encoding="utf-8").split())


@pytest.mark.parametrize("filename", ["README.md", "info.md"])
def test_websocket_security_boundary_is_explicit(filename):
    text = _normalized(filename)

    assert "HMAC of the empty HTTP upgrade body" in text
    assert "does not authenticate individual frames" in text
    assert "no encryption or cryptographic server authentication" in text


@pytest.mark.parametrize("filename", ["README.md", "info.md"])
def test_ack_loss_replay_boundary_is_explicit(filename):
    text = _normalized(filename)

    assert re.search(
        r"ordinary WebSocket reconnect.{0,240}"
        r"(?:without repeating|suppresses? (?:a )?replay)",
        text,
        re.IGNORECASE,
    )
    assert re.search(
        r"(?:summary may repeat|repeated summary remains possible).{0,260}"
        r"manager or integration process restart.{0,180}delivered-cache eviction",
        text,
        re.IGNORECASE,
    )
    assert re.search(
        r"(?:at least once|at-least-once).{0,320}"
        r"(?:silently lost|silent loss)",
        text,
        re.IGNORECASE,
    )

    assert "ordinary WebSocket reconnect replays" not in text
    assert "can never replay after a manager or integration process restart" not in text


def test_release_metadata_is_v050_with_existing_ha_minimum():
    manifest = json.loads(
        (_ROOT / "custom_components/casa/manifest.json").read_text(
            encoding="utf-8",
        ),
    )
    hacs = json.loads((_ROOT / "hacs.json").read_text(encoding="utf-8"))

    assert manifest["version"] == "0.5.0"
    assert hacs["homeassistant"] == "2026.4.0"


def test_readme_documents_catalog_parent_and_agent_children():
    text = _normalized("README.md")

    assert "one Casa parent" in text
    assert "separate conversation entities for Tina, Gary, and future" in text
    assert "There is no agent role field" in text
    assert "matching discovered agent" in text
    assert "Casa Butler" not in text


def test_readme_documents_parent_and_child_configuration_boundaries():
    text = _normalized("README.md")

    assert "Parent configuration" in text
    assert "Satellite entity overrides" in text
    assert "Per-agent reconfiguration" in text
    assert "Session mode" in text
    assert "Transport" in text
    assert "Assist idle stability" in text
    assert "recreated on the next reload" in text
    assert "exact host and port" in text
    assert "different aliases for the same host" in text


@pytest.mark.parametrize("filename", ["README.md", "info.md"])
def test_coordinated_voice_handoff_upgrade_is_explicit(filename):
    text = _normalized(filename)

    assert "Upgrade Casa to v0.89.0 before installing integration v0.5.0" in text
    assert "WebSocket protocol 2" in text
    assert "background_jobs" in text
    assert "satellite_announce" in text
    assert "voice_handoff" in text
    assert "fails closed before a job is created" in text
    assert "no legacy handoff fallback" in text
    assert "GET /api/voice/agents" in text
    assert "cannot create a new v0.5.0 entry" in text
    assert "existing v0.5.0 entry with retained children may load in degraded mode" in text
    assert "without catalog reconciliation" in text


@pytest.mark.parametrize("filename", ["README.md", "info.md"])
def test_handoff_user_experience_and_butler_boundary_are_explicit(filename):
    text = _normalized(filename)

    assert "HandoffFrame" in text
    assert "continue or cancel normally" in text
    assert "originating satellite is idle" in text
    assert "Butler" in text
    assert "direct" in text


def test_readme_e2e_matrix_covers_dynamic_agent_release_boundaries():
    text = _normalized("README.md")

    assert "Real-system E2E" in text
    assert "Controlled fault-injection acceptance" in text
    assert "authenticated catalog" in text
    assert "two conversation entities" in text
    assert "server utterance-to-first-text-block p95 below 1.5 seconds" in text
    assert "end-of-speech-to-first-audible-output p95 below 3.0 seconds" in text
    assert "Gary background result" in text
    assert "missing role remains unavailable" in text
    assert "isolated cleanup" in text
    assert "catalog fixture" in text
    assert "WebSocket protocol proxy" in text
    assert "Home Assistant task and socket introspection" in text
    assert "non-listening state into `LISTENING`" in text
    assert "repeated `LISTENING` state update" in text
    assert "must not send another registration" in text
    assert "connected WebSocket child registers" in text
    assert "SSE" in text
    assert "`cancel` frame" in text
    assert "sibling remains usable" in text
    assert "without duplicate reauth prompts" in text
    assert "without replay" in text
    assert "must never be silently lost" in text
    assert "listening device once" not in text
    assert "present WebSocket child registers" not in text
