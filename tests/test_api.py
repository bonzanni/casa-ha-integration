"""Tests for Casa API client."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import aiohttp
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


class _FakeWS:
    def __init__(self, outgoing=None):
        self.sent: list[dict] = []
        self._outgoing = list(outgoing or [])
        self.closed = False

    async def send_json(self, data):
        self.sent.append(data)

    async def close(self):
        self.closed = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._outgoing:
            raise StopAsyncIteration
        msg = self._outgoing.pop(0)
        return msg


class _FakeWSMsg:
    def __init__(self, type_name: str, data: str = ""):
        self.type = type("T", (), {"name": type_name})()
        self.data = data


class TestStreamUtteranceWS:
    @pytest.mark.asyncio
    async def test_ws_upgrade_hmac(self):
        import custom_components.casa.api as api_mod
        captured = {}

        session = MagicMock()

        async def fake_ws_connect(url, **kw):
            captured["url"] = url
            captured["headers"] = kw.get("headers")
            return _FakeWS(outgoing=[
                _FakeWSMsg("TEXT", json.dumps({
                    "type": "done", "utterance_id": "u-1",
                })),
            ])

        session.ws_connect = AsyncMock(side_effect=fake_ws_connect)
        client = api_mod.CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")

        frames = []
        async for f in client.stream_utterance(
            text="hi", agent_role="butler", scope_id="d",
            utterance_id="u-1", context={}, transport="ws",
        ):
            frames.append(f)

        import hashlib, hmac as _hmac
        expected = _hmac.new(b"sec", b"", hashlib.sha256).hexdigest()
        assert captured["headers"]["X-Webhook-Signature"] == expected
        assert captured["url"].endswith("/api/converse/ws")
        assert any(f.kind == "done" for f in frames)

    @pytest.mark.asyncio
    async def test_ws_401_raises(self):
        import custom_components.casa.api as api_mod
        session = MagicMock()
        err = aiohttp.WSServerHandshakeError(
            request_info=MagicMock(), history=(), status=401, message="Unauthorized",
        )
        session.ws_connect = AsyncMock(side_effect=err)
        client = api_mod.CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        with pytest.raises(api_mod.AuthenticationError):
            async for _ in client.stream_utterance(
                text="hi", agent_role="butler", scope_id="d",
                utterance_id="u", context={}, transport="ws",
            ):
                pass

    @pytest.mark.asyncio
    async def test_ws_demux_by_utterance_id(self):
        import custom_components.casa.api as api_mod
        session = MagicMock()
        ws = _FakeWS(outgoing=[
            _FakeWSMsg("TEXT", json.dumps({
                "type": "block", "utterance_id": "OTHER",
                "text": "ignored", "final": False,
            })),
            _FakeWSMsg("TEXT", json.dumps({
                "type": "block", "utterance_id": "u-mine",
                "text": "mine", "final": False,
            })),
            _FakeWSMsg("TEXT", json.dumps({
                "type": "done", "utterance_id": "u-mine",
            })),
        ])
        session.ws_connect = AsyncMock(return_value=ws)
        client = api_mod.CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        frames = []
        async for f in client.stream_utterance(
            text="hi", agent_role="butler", scope_id="d",
            utterance_id="u-mine", context={}, transport="ws",
        ):
            frames.append(f)
        texts = [f.text for f in frames if f.kind == "block"]
        assert texts == ["mine"]

    @pytest.mark.asyncio
    async def test_ws_sends_utterance_frame(self):
        import custom_components.casa.api as api_mod
        session = MagicMock()
        ws = _FakeWS(outgoing=[
            _FakeWSMsg("TEXT", json.dumps({"type": "done", "utterance_id": "u"})),
        ])
        session.ws_connect = AsyncMock(return_value=ws)
        client = api_mod.CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        async for _ in client.stream_utterance(
            text="hello", agent_role="butler", scope_id="dev-42",
            utterance_id="u", context={"language": "en"}, transport="ws",
        ):
            pass
        sent = ws.sent[0]
        assert sent["type"] == "utterance"
        assert sent["utterance_id"] == "u"
        assert sent["text"] == "hello"
        assert sent["agent_role"] == "butler"
        assert sent["scope_id"] == "dev-42"
        assert sent["context"]["language"] == "en"


class TestWsCancel:
    @pytest.mark.asyncio
    async def test_iterator_cancel_sends_cancel_frame(self):
        import custom_components.casa.api as api_mod
        session = MagicMock()
        ws = _FakeWS(outgoing=[
            _FakeWSMsg("TEXT", json.dumps({
                "type": "block", "utterance_id": "u", "text": "hi", "final": False,
            })),
            # intentionally no "done" — we cancel mid-stream.
        ])
        session.ws_connect = AsyncMock(return_value=ws)
        client = api_mod.CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")

        agen = client.stream_utterance(
            text="hi", agent_role="butler", scope_id="d",
            utterance_id="u", context={}, transport="ws",
        )
        first = await agen.__anext__()
        assert first.text == "hi"
        await agen.aclose()

        cancel_frames = [s for s in ws.sent if s.get("type") == "cancel"]
        assert cancel_frames
        assert cancel_frames[0]["utterance_id"] == "u"


class TestPrewarm:
    @pytest.mark.asyncio
    async def test_ws_prewarm_sends_stt_start(self):
        import custom_components.casa.api as api_mod
        session = MagicMock()
        ws = _FakeWS(outgoing=[])
        session.ws_connect = AsyncMock(return_value=ws)
        client = api_mod.CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        await client.prewarm(scope_id="dev-1", transport="ws")
        sent = [s for s in ws.sent if s.get("type") == "stt_start"]
        assert sent
        assert sent[0]["scope_id"] == "dev-1"
        assert sent[0]["session_key"] == "voice:dev-1"

    @pytest.mark.asyncio
    async def test_sse_prewarm_noops(self, api_client: CasaApiClient, mock_session: MagicMock):
        # SSE transport: prewarm must return without raising and without any network call.
        mock_session.post = AsyncMock()
        mock_session.ws_connect = AsyncMock()
        await api_client.prewarm(scope_id="dev-1", transport="sse")
        mock_session.post.assert_not_called()
        mock_session.ws_connect.assert_not_called()


class TestClose:
    @pytest.mark.asyncio
    async def test_close_closes_ws(self):
        import custom_components.casa.api as api_mod
        session = MagicMock()
        ws = _FakeWS(outgoing=[])
        session.ws_connect = AsyncMock(return_value=ws)
        client = api_mod.CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        await client._ensure_ws()
        await client.close()
        assert ws.closed is True
