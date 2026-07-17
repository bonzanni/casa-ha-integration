"""Contract tests for security and delivery guarantees in release docs."""

from pathlib import Path

import pytest


_ROOT = Path(__file__).parents[1]


@pytest.mark.parametrize("filename", ["README.md", "info.md"])
def test_websocket_security_boundary_is_explicit(filename):
    text = " ".join((_ROOT / filename).read_text(encoding="utf-8").split())

    assert "HMAC of the empty HTTP upgrade body" in text
    assert "does not authenticate individual frames" in text
    assert "no encryption or cryptographic server authentication" in text


@pytest.mark.parametrize("filename", ["README.md", "info.md"])
def test_ack_loss_replay_boundary_is_explicit(filename):
    text = " ".join((_ROOT / filename).read_text(encoding="utf-8").split())

    assert "ordinary WebSocket reconnect" in text
    assert "manager or integration process restart" in text
    assert "delivered-cache eviction" in text
