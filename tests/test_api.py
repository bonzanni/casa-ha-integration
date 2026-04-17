"""Tests for Casa API client."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from custom_components.casa.api import (
    BlockFrame,
    CasaApiClient,
    DoneFrame,
    ErrorFrame,
)


class TestCasaFrames:
    def test_block_frame_fields(self):
        f = BlockFrame(text="hello", final=False)
        assert f.text == "hello"
        assert f.final is False
        assert f.kind == "block"

    def test_done_frame_kind(self):
        assert DoneFrame().kind == "done"

    def test_error_frame_fields(self):
        f = ErrorFrame(kind_="timeout", spoken="slow")
        assert f.kind == "error"
        assert f.kind_ == "timeout"
        assert f.spoken == "slow"


@pytest.fixture
def api_client(mock_session: MagicMock) -> CasaApiClient:
    return CasaApiClient(
        session=mock_session,
        host="test-host",
        port=18065,
        webhook_secret="s3cret",
    )


class TestHealthCheck:
    @pytest.mark.asyncio
    async def test_healthy(self, api_client: CasaApiClient, mock_session: MagicMock):
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_session.get = AsyncMock(return_value=mock_resp)
        assert await api_client.health_check() is True
        call = mock_session.get.call_args
        assert call[0][0].endswith("/healthz")

    @pytest.mark.asyncio
    async def test_unhealthy(self, api_client: CasaApiClient, mock_session: MagicMock):
        mock_resp = AsyncMock()
        mock_resp.status = 503
        mock_session.get = AsyncMock(return_value=mock_resp)
        assert await api_client.health_check() is False


class TestStreamUtteranceSSE:
    @pytest.mark.asyncio
    async def test_parses_block_done(self, api_client: CasaApiClient, mock_session: MagicMock, async_iter):
        sse_lines = [
            b"event: block\n",
            b'data: {"text":"Hello","final":false}\n',
            b"\n",
            b"event: block\n",
            b'data: {"text":" world","final":true}\n',
            b"\n",
            b"event: done\n",
            b"data: {}\n",
            b"\n",
        ]
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.content = async_iter(sse_lines)
        mock_session.post = AsyncMock(return_value=mock_resp)

        frames = []
        async for f in api_client.stream_utterance(
            text="hi", agent_role="butler", scope_id="d-1",
            utterance_id="u-1", context={"device_id": "d-1"}, transport="sse",
        ):
            frames.append(f)

        kinds = [f.kind for f in frames]
        assert kinds == ["block", "block", "done"]
        assert frames[0].text == "Hello"
        assert frames[1].final is True

    @pytest.mark.asyncio
    async def test_parses_error(self, api_client: CasaApiClient, mock_session: MagicMock, async_iter):
        sse_lines = [
            b"event: error\n",
            b'data: {"kind":"timeout","spoken":"slow"}\n',
            b"\n",
        ]
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.content = async_iter(sse_lines)
        mock_session.post = AsyncMock(return_value=mock_resp)

        frames = []
        async for f in api_client.stream_utterance(
            text="hi", agent_role="butler", scope_id="d",
            utterance_id="u", context={}, transport="sse",
        ):
            frames.append(f)

        assert len(frames) == 1
        assert frames[0].kind == "error"
        assert frames[0].kind_ == "timeout"
        assert frames[0].spoken == "slow"

    @pytest.mark.asyncio
    async def test_sends_hmac_header_and_payload(self, api_client: CasaApiClient, mock_session: MagicMock, async_iter):
        sse_lines = [b"event: done\n", b"data: {}\n", b"\n"]
        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.raise_for_status = MagicMock()
        mock_resp.content = async_iter(sse_lines)
        mock_session.post = AsyncMock(return_value=mock_resp)

        async for _ in api_client.stream_utterance(
            text="hi", agent_role="butler", scope_id="d",
            utterance_id="u", context={"device_id": "d"}, transport="sse",
        ):
            pass

        call = mock_session.post.call_args
        assert call[0][0].endswith("/api/converse")
        body = call.kwargs["data"]
        headers = call.kwargs["headers"]
        import hashlib, hmac as _hmac
        expected = _hmac.new(b"s3cret", body, hashlib.sha256).hexdigest()
        assert headers["X-Webhook-Signature"] == expected
        parsed = json.loads(body)
        assert parsed["prompt"] == "hi"
        assert parsed["agent_role"] == "butler"
        assert parsed["scope_id"] == "d"
        assert parsed["context"]["device_id"] == "d"

    @pytest.mark.asyncio
    async def test_401_raises(self, api_client: CasaApiClient, mock_session: MagicMock):
        from custom_components.casa.api import AuthenticationError
        mock_resp = AsyncMock()
        mock_resp.status = 401
        mock_session.post = AsyncMock(return_value=mock_resp)
        with pytest.raises(AuthenticationError):
            async for _ in api_client.stream_utterance(
                text="hi", agent_role="butler", scope_id="d",
                utterance_id="u", context={}, transport="sse",
            ):
                pass
