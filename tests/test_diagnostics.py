"""Tests for strictly redacted Casa diagnostics."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from custom_components.casa import CasaAgentRuntime, CasaRuntimeData
from custom_components.casa.api import ConnectionState
from custom_components.casa.diagnostics import async_get_config_entry_diagnostics


def _child(
    *,
    subentry_id: str,
    role: str,
    name: str,
    transport: str,
    catalog_present: bool,
    background_capable: bool = False,
) -> CasaAgentRuntime:
    client = None
    manager = None
    if catalog_present:
        client = MagicMock()
        client.background_capable = background_capable
        manager = MagicMock()
    return CasaAgentRuntime(
        parent_entry_id="entry-1",
        subentry_id=subentry_id,
        role=role,
        name=name,
        session_mode="device",
        transport=transport,
        idle_stability_ms=750,
        catalog_present=catalog_present,
        client=client,
        manager=manager,
    )


def _entry():
    connected = _child(
        subentry_id="child-butler",
        role="butler",
        name="Tina",
        transport="ws",
        catalog_present=True,
        background_capable=True,
    )
    connected.set_connection_state(ConnectionState.CONNECTED)
    missing = _child(
        subentry_id="child-legacy",
        role="legacy",
        name="Legacy",
        transport="sse",
        catalog_present=False,
    )

    entry = MagicMock()
    entry.data = {
        "host": "PRIVATE_HOST_CANARY",
        "webhook_secret": "PRIVATE_SECRET_CANARY",
        "signature": "PRIVATE_SIGNATURE_CANARY",
        "raw_catalog": "PRIVATE_CATALOG_CANARY",
    }
    entry.options = {"prompt": "PRIVATE_PROMPT_CANARY"}
    entry.runtime_data = CasaRuntimeData(
        directory=MagicMock(),
        listener=MagicMock(),
        agents={
            connected.subentry_id: connected,
            missing.subentry_id: missing,
        },
        catalog_healthy=False,
    )

    connected.client._secret = "PRIVATE_CLIENT_SECRET_CANARY"
    connected.client._signature = "PRIVATE_CLIENT_SIGNATURE_CANARY"
    connected.manager._queues = {"PRIVATE_DEVICE_CANARY": MagicMock()}
    connected.manager._attempts = {
        ("PRIVATE_JOB_CANARY", "PRIVATE_ATTEMPT_CANARY"): MagicMock(
            spoken_text="PRIVATE_SPEECH_CANARY",
            result_text="PRIVATE_RESULT_CANARY",
        ),
    }
    return entry


@pytest.mark.asyncio
async def test_diagnostics_returns_only_the_health_allowlist():
    entry = _entry()

    result = await async_get_config_entry_diagnostics(MagicMock(), entry)

    assert result == {
        "catalog_healthy": False,
        "agents": [
            {
                "subentry_id": "child-butler",
                "role": "butler",
                "catalog_present": True,
                "transport": "ws",
                "connection_state": ConnectionState.CONNECTED,
                "background_capable": True,
            },
            {
                "subentry_id": "child-legacy",
                "role": "legacy",
                "catalog_present": False,
                "transport": "sse",
                "connection_state": ConnectionState.DISCONNECTED,
                "background_capable": False,
            },
        ],
    }


@pytest.mark.asyncio
async def test_serialized_diagnostics_excludes_every_sensitive_canary():
    result = await async_get_config_entry_diagnostics(MagicMock(), _entry())

    serialized = json.dumps(result)
    for canary in (
        "PRIVATE_HOST_CANARY",
        "PRIVATE_SECRET_CANARY",
        "PRIVATE_SIGNATURE_CANARY",
        "PRIVATE_CATALOG_CANARY",
        "PRIVATE_PROMPT_CANARY",
        "PRIVATE_CLIENT_SECRET_CANARY",
        "PRIVATE_CLIENT_SIGNATURE_CANARY",
        "PRIVATE_DEVICE_CANARY",
        "PRIVATE_JOB_CANARY",
        "PRIVATE_ATTEMPT_CANARY",
        "PRIVATE_SPEECH_CANARY",
        "PRIVATE_RESULT_CANARY",
    ):
        assert canary not in serialized


@pytest.mark.asyncio
@pytest.mark.parametrize("include_runtime_attribute", [False, True])
async def test_unloaded_diagnostics_are_empty_and_redacted(
    include_runtime_attribute,
):
    fields = {
        "data": {
            "host": "PRIVATE_UNLOADED_HOST_CANARY",
            "webhook_secret": "PRIVATE_UNLOADED_SECRET_CANARY",
        },
        "options": {"prompt": "PRIVATE_UNLOADED_PROMPT_CANARY"},
    }
    if include_runtime_attribute:
        fields["runtime_data"] = None
    entry = SimpleNamespace(**fields)

    result = await async_get_config_entry_diagnostics(MagicMock(), entry)

    assert result == {"catalog_healthy": False, "agents": []}
    serialized = json.dumps(result)
    assert "PRIVATE_UNLOADED_HOST_CANARY" not in serialized
    assert "PRIVATE_UNLOADED_SECRET_CANARY" not in serialized
    assert "PRIVATE_UNLOADED_PROMPT_CANARY" not in serialized
