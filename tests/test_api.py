"""Tests for Casa API client."""

from __future__ import annotations

import asyncio
import json
import logging
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
        import hashlib
        import hmac as _hmac
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
    _CLOSED = object()

    def __init__(self, outgoing=None, *, stay_open: bool = False, auto_done: bool = False):
        self.sent: list[dict] = []
        self._outgoing = list(outgoing or [])
        self._incoming: asyncio.Queue = asyncio.Queue()
        self._stay_open = stay_open
        self._auto_done = auto_done
        self.closed = False
        self.concurrent_send = 0
        self.max_concurrent_send = 0

    async def send_json(self, data):
        self.concurrent_send += 1
        self.max_concurrent_send = max(self.max_concurrent_send, self.concurrent_send)
        try:
            # Make overlapping writers observable to the concurrency test.
            await asyncio.sleep(0)
            self.sent.append(data)
            if self._auto_done and data.get("type") == "utterance":
                self.feed_json({"type": "done", "utterance_id": data["utterance_id"]})
        finally:
            self.concurrent_send -= 1

    async def close(self):
        self.closed = True
        self._incoming.put_nowait(self._CLOSED)

    def feed_json(self, frame: dict) -> None:
        self._incoming.put_nowait(_FakeWSMsg("TEXT", json.dumps(frame)))

    def feed_error(self, error: Exception) -> None:
        self._incoming.put_nowait(error)

    def disconnect(self) -> None:
        self.closed = True
        self._incoming.put_nowait(self._CLOSED)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._outgoing:
            return self._outgoing.pop(0)
        if not self._stay_open:
            raise StopAsyncIteration
        msg = await self._incoming.get()
        if msg is self._CLOSED:
            raise StopAsyncIteration
        if isinstance(msg, Exception):
            raise msg
        return msg


class _FakeWSMsg:
    def __init__(self, type_name: str, data: str = ""):
        self.type = type("T", (), {"name": type_name})()
        self.data = data


async def _wait_until(predicate, *, timeout: float = 1) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0)


async def _collect_ws_utterance(
    client: CasaApiClient,
    *,
    utterance_id: str = "u-1",
) -> list:
    return [
        frame
        async for frame in client.stream_utterance(
            text="hello",
            agent_role="concierge",
            scope_id="dev-1",
            utterance_id=utterance_id,
            context={},
            transport="ws",
        )
    ]


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

        import hashlib
        import hmac as _hmac
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
        ], stay_open=True)
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


class TestSessionRegistration:
    @pytest.mark.asyncio
    async def test_ws_registration_sends_stt_start(self):
        import custom_components.casa.api as api_mod
        session = MagicMock()
        ws = _FakeWS(outgoing=[])
        session.ws_connect = AsyncMock(return_value=ws)
        client = api_mod.CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        await client.register_session(
            scope_id="dev-1", transport="ws", agent_role="concierge",
        )
        sent = [s for s in ws.sent if s.get("type") == "stt_start"]
        assert sent
        assert sent[0]["scope_id"] == "dev-1"
        assert sent[0]["session_key"] == "voice:dev-1"
        assert sent[0]["agent_role"] == "concierge"

    @pytest.mark.asyncio
    async def test_sse_registration_noops(self, api_client: CasaApiClient, mock_session: MagicMock):
        # SSE transport: registration must return without raising and without any network call.
        mock_session.post = AsyncMock()
        mock_session.ws_connect = AsyncMock()
        await api_client.register_session(
            scope_id="dev-1", transport="sse", agent_role="concierge",
        )
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


class TestBackgroundDelivery:
    @pytest.mark.asyncio
    async def test_start_background_eagerly_connects_and_registers(self):
        session = MagicMock()
        ws = _FakeWS(stay_open=True)
        session.ws_connect = AsyncMock(return_value=ws)
        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")

        await client.start_background(
            route_id="entry-1",
            agent_role="concierge",
            job_handler=AsyncMock(),
        )

        assert ws.sent[0] == {
            "type": "voice_route_register",
            "protocol": 1,
            "route_id": "entry-1",
            "agent_role": "concierge",
            "capabilities": ["background_jobs", "satellite_announce"],
        }
        await client.close()

    @pytest.mark.asyncio
    async def test_old_casa_no_ack_keeps_sync_ws_and_disables_jobs(self):
        session = MagicMock()
        ws = _FakeWS(stay_open=True)
        session.ws_connect = AsyncMock(return_value=ws)
        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")

        await client.start_background(
            route_id="entry-1",
            agent_role="concierge",
            job_handler=AsyncMock(),
        )
        await asyncio.sleep(0)

        assert client.background_capable is False
        assert ws.closed is False
        assert client.reconnect_attempts_for_test == 0

        utterance = asyncio.create_task(_collect_ws_utterance(client))
        await _wait_until(lambda: any(frame.get("type") == "utterance" for frame in ws.sent))
        ws.feed_json({"type": "done", "utterance_id": "u-1"})
        frames = await utterance
        assert [frame.kind for frame in frames] == ["done"]
        await client.close()

    @pytest.mark.asyncio
    async def test_old_casa_no_ack_rejects_job_frames(self):
        session = MagicMock()
        ws = _FakeWS(stay_open=True)
        session.ws_connect = AsyncMock(return_value=ws)
        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        frame = {
            "type": "job_claimed",
            "protocol": 1,
            "job_id": "job-1",
            "delivery_attempt_id": "attempt-1",
        }

        try:
            await client.start_background(
                route_id="entry-1", agent_role="concierge", job_handler=AsyncMock(),
            )

            with pytest.raises(ConnectionError, match="background protocol"):
                await client.send_job_frame(frame)

            assert ws.sent == [{
                "type": "voice_route_register",
                "protocol": 1,
                "route_id": "entry-1",
                "agent_role": "concierge",
                "capabilities": ["background_jobs", "satellite_announce"],
            }]
        finally:
            await client.close()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("ack", "expected"),
        [
            (
                {
                    "type": "voice_route_registered",
                    "protocol": 1,
                    "accepted_capabilities": ["background_jobs", "satellite_announce"],
                },
                True,
            ),
            (
                {
                    "type": "voice_route_registered",
                    "protocol": "1",
                    "accepted_capabilities": ["background_jobs", "satellite_announce"],
                },
                False,
            ),
            (
                {
                    "type": "voice_route_registered",
                    "protocol": 1,
                    "accepted_capabilities": ["background_jobs"],
                },
                False,
            ),
        ],
    )
    async def test_only_protocol_one_ack_with_both_capabilities_enables_jobs(
        self, ack, expected,
    ):
        session = MagicMock()
        ws = _FakeWS(stay_open=True)
        session.ws_connect = AsyncMock(return_value=ws)
        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        await client.start_background(
            route_id="entry-1", agent_role="concierge", job_handler=AsyncMock(),
        )

        ws.feed_json(ack)
        await _wait_until(lambda: client._route_ack.is_set())

        assert client.background_capable is expected
        await client.close()

    @pytest.mark.asyncio
    async def test_job_frames_are_demuxed_before_utterance_ids(self):
        session = MagicMock()
        ws = _FakeWS(stay_open=True)
        session.ws_connect = AsyncMock(return_value=ws)
        handler = AsyncMock()
        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        await client.start_background(
            route_id="entry-1", agent_role="concierge", job_handler=handler,
        )
        ws.feed_json({
            "type": "voice_route_registered",
            "protocol": 1,
            "accepted_capabilities": ["background_jobs", "satellite_announce"],
        })
        await _wait_until(lambda: client.background_capable)

        utterance = asyncio.create_task(_collect_ws_utterance(client))
        await _wait_until(lambda: any(frame.get("type") == "utterance" for frame in ws.sent))
        job = {
            "type": "job_ready",
            "protocol": 1,
            "job_id": "job-1",
            "delivery_attempt_id": "attempt-1",
        }
        ws.feed_json(job)
        ws.feed_json({
            "type": "block",
            "utterance_id": "u-1",
            "text": "spoken response",
            "final": True,
        })
        ws.feed_json({"type": "done", "utterance_id": "u-1"})

        frames = await utterance
        handler.assert_awaited_once_with(job)
        assert [frame.kind for frame in frames] == ["block", "done"]
        assert frames[0].text == "spoken response"
        await client.close()

    @pytest.mark.asyncio
    async def test_only_current_socket_generation_dispatches_inbound_jobs(self):
        session = MagicMock()
        first = _FakeWS(stay_open=True)
        second = _FakeWS(stay_open=True)
        session.ws_connect = AsyncMock(side_effect=[first, second])
        received: list[dict] = []
        handled = asyncio.Event()

        async def handler(frame):
            received.append(frame)
            await client.send_job_frame({
                "type": "job_claimed",
                "protocol": 1,
                "job_id": frame["job_id"],
                "delivery_attempt_id": frame["delivery_attempt_id"],
            })
            handled.set()

        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        ack = {
            "type": "voice_route_registered",
            "protocol": 1,
            "accepted_capabilities": ["background_jobs", "satellite_announce"],
        }
        old_job = {
            "type": "job_ready",
            "protocol": 1,
            "job_id": "job-old-generation",
            "delivery_attempt_id": "attempt-old-generation",
        }
        current_job = {
            "type": "job_ready",
            "protocol": 1,
            "job_id": "job-current-generation",
            "delivery_attempt_id": "attempt-current-generation",
        }

        try:
            await client.start_background(
                route_id="entry-1", agent_role="concierge", job_handler=handler,
            )
            first.feed_json(ack)
            await _wait_until(lambda: client.background_capable)

            # Install and acknowledge generation 2 without ending generation 1's reader.
            first.closed = True
            await client._ensure_ws()
            second.feed_json(ack)
            await _wait_until(lambda: client.background_capable)

            first.feed_json(old_job)
            await _wait_until(lambda: first._incoming.empty())
            assert received == []

            second.feed_json(current_job)
            await asyncio.wait_for(handled.wait(), timeout=1)
            assert received == [current_job]
            assert second.sent[-1] == {
                "type": "job_claimed",
                "protocol": 1,
                "job_id": "job-current-generation",
                "delivery_attempt_id": "attempt-current-generation",
            }
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_authorization_and_denial_frames_only_reach_job_handler(self):
        session = MagicMock()
        ws = _FakeWS(stay_open=True)
        session.ws_connect = AsyncMock(return_value=ws)
        received: list[dict] = []

        async def handler(frame):
            received.append(frame)

        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        await client.start_background(
            route_id="entry-1", agent_role="concierge", job_handler=handler,
        )
        ws.feed_json({
            "type": "voice_route_registered",
            "protocol": 1,
            "accepted_capabilities": ["background_jobs", "satellite_announce"],
        })
        await _wait_until(lambda: client.background_capable)

        utterance = asyncio.create_task(_collect_ws_utterance(client))
        await _wait_until(lambda: any(frame.get("type") == "utterance" for frame in ws.sent))
        authorized = {
            "type": "job_delivery_authorized",
            "protocol": 1,
            "job_id": "job-1",
            "delivery_attempt_id": "attempt-1",
            "utterance_id": "u-1",
        }
        denied = {
            "type": "job_revoke",
            "protocol": 1,
            "job_id": "job-2",
            "delivery_attempt_id": "attempt-2",
            "utterance_id": "u-1",
            "reason": "stale_attempt",
        }
        ws.feed_json(authorized)
        ws.feed_json(denied)
        ws.feed_json({"type": "done", "utterance_id": "u-1"})

        frames = await utterance
        assert received == [authorized, denied]
        assert [frame.kind for frame in frames] == ["done"]
        await client.close()

    @pytest.mark.asyncio
    async def test_job_handler_exception_does_not_log_sensitive_text(self, caplog):
        session = MagicMock()
        ws = _FakeWS(stay_open=True)
        session.ws_connect = AsyncMock(return_value=ws)
        handler_called = asyncio.Event()
        exception_canary = "SECRET_EXCEPTION_CANARY_8e3cf1"
        frame_type_canary = "job_ready_SECRET_FRAME_TYPE_CANARY_17d21a"
        job_id_canary = "SECRET_JOB_ID_CANARY_f89b04"
        attempt_id_canary = "SECRET_ATTEMPT_ID_CANARY_c7c32a"
        result_canary = "SECRET_RESULT_CANARY_00ce11"
        spoken_canary = "SECRET_SPOKEN_CANARY_956eab"

        async def handler(_frame):
            handler_called.set()
            raise RuntimeError(exception_canary)

        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        await client.start_background(
            route_id="entry-1", agent_role="concierge", job_handler=handler,
        )
        ws.feed_json({
            "type": "voice_route_registered",
            "protocol": 1,
            "accepted_capabilities": ["background_jobs", "satellite_announce"],
        })
        await _wait_until(lambda: client.background_capable)
        ws.feed_json({
            "type": frame_type_canary,
            "protocol": 1,
            "job_id": job_id_canary,
            "delivery_attempt_id": attempt_id_canary,
            "result": result_canary,
            "spoken": spoken_canary,
        })
        await asyncio.wait_for(handler_called.wait(), timeout=1)
        await _wait_until(lambda: bool(caplog.records))

        assert "Background job frame handler failed" in caplog.text
        for canary in (
            exception_canary,
            frame_type_canary,
            job_id_canary,
            attempt_id_canary,
            result_canary,
            spoken_canary,
        ):
            assert canary not in caplog.text
        await client.close()

    @pytest.mark.asyncio
    async def test_reader_failure_does_not_log_sensitive_details(self, caplog):
        caplog.set_level(logging.DEBUG, logger="custom_components.casa.api")
        session = MagicMock()
        first = _FakeWS(stay_open=True)
        second = _FakeWS(stay_open=True)
        session.ws_connect = AsyncMock(side_effect=[first, second])
        canaries = (
            "SECRET_READER_CANARY",
            "ws://private-reader-host",
            "job_id=private-reader-job",
            "spoken=private-reader-result",
        )
        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")

        try:
            await client.start_background(
                route_id="entry-1", agent_role="concierge", job_handler=AsyncMock(),
            )
            first.feed_error(RuntimeError(" ".join(canaries)))
            await _wait_until(lambda: "Casa WebSocket reader failed" in caplog.text)

            record = next(
                record for record in caplog.records
                if record.getMessage() == "Casa WebSocket reader failed"
            )
            assert record.exc_info is None
            for canary in canaries:
                assert canary not in caplog.text
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_reconnect_failure_does_not_log_sensitive_details(
        self, caplog, monkeypatch,
    ):
        import custom_components.casa.api as api_mod

        caplog.set_level(logging.DEBUG, logger="custom_components.casa.api")
        sleep_started = asyncio.Event()

        async def blocked_sleep(_delay):
            sleep_started.set()
            await asyncio.Event().wait()

        monkeypatch.setattr(api_mod, "_sleep", blocked_sleep)
        session = MagicMock()
        ws = _FakeWS(stay_open=True)
        canaries = (
            "SECRET_RECONNECT_CANARY",
            "ws://private-reconnect-host",
            "job_id=private-reconnect-job",
            "spoken=private-reconnect-result",
        )
        session.ws_connect = AsyncMock(
            side_effect=[ws, RuntimeError(" ".join(canaries))],
        )
        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")

        try:
            await client.start_background(
                route_id="entry-1", agent_role="concierge", job_handler=AsyncMock(),
            )
            ws.disconnect()
            await asyncio.wait_for(sleep_started.wait(), timeout=1)

            record = next(
                record for record in caplog.records
                if record.getMessage()
                == "Casa WebSocket reconnect failed; retry scheduled"
            )
            assert record.exc_info is None
            for canary in canaries:
                assert canary not in caplog.text
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_cancel_failure_does_not_log_sensitive_details(self, caplog):
        caplog.set_level(logging.DEBUG, logger="custom_components.casa.api")
        session = MagicMock()
        ws = _FakeWS(outgoing=[
            _FakeWSMsg("TEXT", json.dumps({
                "type": "block", "utterance_id": "u-private",
                "text": "private spoken result", "final": False,
            })),
        ], stay_open=True)
        session.ws_connect = AsyncMock(return_value=ws)
        canaries = (
            "SECRET_CANCEL_CANARY",
            "ws://private-cancel-host",
            "utterance_id=u-private",
            "spoken=private-cancel-result",
        )
        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        utterance = client.stream_utterance(
            text="private prompt",
            agent_role="concierge",
            scope_id="dev-private",
            utterance_id="u-private",
            context={},
            transport="ws",
        )

        try:
            assert (await utterance.__anext__()).text == "private spoken result"
            ws.send_json = AsyncMock(side_effect=RuntimeError(" ".join(canaries)))
            await utterance.aclose()

            record = next(
                record for record in caplog.records
                if record.getMessage() == "Cancel frame failed; connection likely closed"
            )
            assert record.exc_info is None
            for canary in canaries:
                assert canary not in caplog.text
        finally:
            await client.close()

    @pytest.mark.asyncio
    async def test_all_ws_writes_share_one_global_writer(self):
        session = MagicMock()
        ws = _FakeWS(stay_open=True, auto_done=True)
        session.ws_connect = AsyncMock(return_value=ws)
        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        await client.start_background(
            route_id="entry-1", agent_role="concierge", job_handler=AsyncMock(),
        )
        ws.feed_json({
            "type": "voice_route_registered",
            "protocol": 1,
            "accepted_capabilities": ["background_jobs", "satellite_announce"],
        })
        await _wait_until(lambda: client.background_capable)

        await asyncio.gather(
            client.register_session(
                scope_id="dev-1", transport="ws", agent_role="concierge",
            ),
            _collect_ws_utterance(client),
            client.send_job_frame({
                "type": "job_claimed",
                "protocol": 1,
                "job_id": "job-1",
                "delivery_attempt_id": "attempt-1",
            }),
        )

        assert ws.max_concurrent_send == 1
        await client.close()

    @pytest.mark.asyncio
    async def test_reconnect_registers_once_on_every_socket_generation(self):
        session = MagicMock()
        first = _FakeWS(stay_open=True)
        second = _FakeWS(stay_open=True)
        session.ws_connect = AsyncMock(side_effect=[first, second])
        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        await client.start_background(
            route_id="entry-1", agent_role="concierge", job_handler=AsyncMock(),
        )

        first.disconnect()
        await _wait_until(lambda: session.ws_connect.await_count == 2)
        await _wait_until(lambda: len(second.sent) == 1)

        assert [frame["type"] for frame in first.sent] == ["voice_route_register"]
        assert [frame["type"] for frame in second.sent] == ["voice_route_register"]
        assert client.reconnect_attempts_for_test == 1
        await client.close()

    @pytest.mark.asyncio
    async def test_job_send_rechecks_negotiation_after_reconnect(self):
        session = MagicMock()
        first = _FakeWS(stay_open=True)
        second = _FakeWS(stay_open=True)
        session.ws_connect = AsyncMock(side_effect=[first, second])
        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        send_entered_ensure = asyncio.Event()
        release_ensure = asyncio.Event()
        send_task = None
        frame = {
            "type": "job_claimed",
            "protocol": 1,
            "job_id": "job-1",
            "delivery_attempt_id": "attempt-1",
        }

        try:
            await client.start_background(
                route_id="entry-1", agent_role="concierge", job_handler=AsyncMock(),
            )
            first.feed_json({
                "type": "voice_route_registered",
                "protocol": 1,
                "accepted_capabilities": ["background_jobs", "satellite_announce"],
            })
            await _wait_until(lambda: client.background_capable)

            original_ensure_ws = client._ensure_ws

            async def ensure_after_generation_switch():
                send_entered_ensure.set()
                await release_ensure.wait()
                return await original_ensure_ws()

            client._ensure_ws = ensure_after_generation_switch
            send_task = asyncio.create_task(client.send_job_frame(frame))
            await asyncio.wait_for(send_entered_ensure.wait(), timeout=1)

            first.closed = True
            release_ensure.set()
            with pytest.raises(ConnectionError, match="background protocol"):
                await send_task

            assert client.background_capable is False
            assert [sent["type"] for sent in second.sent] == ["voice_route_register"]

            second.feed_json({
                "type": "voice_route_registered",
                "protocol": 1,
                "accepted_capabilities": ["background_jobs", "satellite_announce"],
            })
            await _wait_until(lambda: client.background_capable)
            await client.send_job_frame(frame)

            assert second.sent == [
                {
                    "type": "voice_route_register",
                    "protocol": 1,
                    "route_id": "entry-1",
                    "agent_role": "concierge",
                    "capabilities": ["background_jobs", "satellite_announce"],
                },
                frame,
            ]
        finally:
            release_ensure.set()
            if send_task is not None and not send_task.done():
                send_task.cancel()
                await asyncio.gather(send_task, return_exceptions=True)
            await client.close()

    @pytest.mark.asyncio
    async def test_old_generation_closes_only_its_utterance_queues(self):
        session = MagicMock()
        first = _FakeWS(stay_open=True)
        second = _FakeWS(stay_open=True)
        session.ws_connect = AsyncMock(side_effect=[first, second])
        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        old_utterance = asyncio.create_task(_collect_ws_utterance(client))
        await _wait_until(lambda: any(frame.get("type") == "utterance" for frame in first.sent))

        # Install generation 2 before generation 1's reader reaches cleanup.
        first.closed = True
        await client._ensure_ws()
        first.disconnect()

        frames = await asyncio.wait_for(old_utterance, timeout=1)
        assert len(frames) == 1
        assert frames[0].kind == "error"
        assert frames[0].kind_ == "connection"
        await client.close()

    @pytest.mark.asyncio
    async def test_cancel_never_crosses_socket_generations(self):
        session = MagicMock()
        first = _FakeWS(outgoing=[
            _FakeWSMsg("TEXT", json.dumps({
                "type": "block", "utterance_id": "u-1", "text": "hi", "final": False,
            })),
        ], stay_open=True)
        second = _FakeWS(stay_open=True)
        session.ws_connect = AsyncMock(side_effect=[first, second])
        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        utterance = client.stream_utterance(
            text="hello",
            agent_role="concierge",
            scope_id="dev-1",
            utterance_id="u-1",
            context={},
            transport="ws",
        )
        assert (await utterance.__anext__()).text == "hi"

        first.closed = True
        await client._ensure_ws()
        await utterance.aclose()

        assert not any(frame.get("type") == "cancel" for frame in second.sent)
        await client.close()

    @pytest.mark.asyncio
    async def test_reconnect_backoff_grows_caps_and_resets_after_upgrade(self, monkeypatch):
        import custom_components.casa.api as api_mod

        delays: list[float] = []

        async def fake_sleep(delay):
            delays.append(delay)
            await asyncio.sleep(0)

        monkeypatch.setattr(api_mod, "_sleep", fake_sleep, raising=False)
        session = MagicMock()
        first = _FakeWS(stay_open=True)
        second = _FakeWS(stay_open=True)
        third = _FakeWS(stay_open=True)
        session.ws_connect = AsyncMock(side_effect=[
            first,
            ConnectionError("offline-1"),
            ConnectionError("offline-2"),
            ConnectionError("offline-3"),
            ConnectionError("offline-4"),
            ConnectionError("offline-5"),
            ConnectionError("offline-6"),
            ConnectionError("offline-7"),
            second,
            ConnectionError("offline-after-upgrade"),
            third,
        ])
        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        await client.start_background(
            route_id="entry-1", agent_role="concierge", job_handler=AsyncMock(),
        )

        first.disconnect()
        await _wait_until(lambda: len(second.sent) == 1)
        second.disconnect()
        await _wait_until(lambda: len(third.sent) == 1)

        assert delays == [1, 2, 4, 8, 16, 30, 30, 1]
        await client.close()

    @pytest.mark.asyncio
    async def test_without_background_start_new_casa_still_accepts_sync_voice(self):
        session = MagicMock()
        ws = _FakeWS(outgoing=[
            _FakeWSMsg("TEXT", json.dumps({"type": "done", "utterance_id": "u-1"})),
        ])
        session.ws_connect = AsyncMock(return_value=ws)
        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")

        frames = await _collect_ws_utterance(client)

        assert [frame.kind for frame in frames] == ["done"]
        assert [frame["type"] for frame in ws.sent] == ["utterance"]
        await client.close()

    @pytest.mark.asyncio
    async def test_close_active_background_reader_prevents_reconnect(self):
        session = MagicMock()
        ws = _FakeWS(stay_open=True)
        session.ws_connect = AsyncMock(return_value=ws)
        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        await client.start_background(
            route_id="entry-1", agent_role="concierge", job_handler=AsyncMock(),
        )

        await client.close()
        await asyncio.sleep(0)

        assert ws.closed is True
        assert session.ws_connect.await_count == 1
        assert client.background_capable is False
        assert client._ws_reader is None
        assert client._ws_supervisor is None

    @pytest.mark.asyncio
    async def test_close_cancels_outstanding_authorization_handler_future(self):
        session = MagicMock()
        ws = _FakeWS(stay_open=True)
        session.ws_connect = AsyncMock(return_value=ws)
        handler_started = asyncio.Event()
        authorization = asyncio.get_running_loop().create_future()

        async def handler(_frame):
            handler_started.set()
            await authorization

        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        await client.start_background(
            route_id="entry-1", agent_role="concierge", job_handler=handler,
        )
        ws.feed_json({
            "type": "voice_route_registered",
            "protocol": 1,
            "accepted_capabilities": ["background_jobs", "satellite_announce"],
        })
        await _wait_until(lambda: client.background_capable)
        ws.feed_json({
            "type": "job_delivery_authorized",
            "protocol": 1,
            "job_id": "job-1",
            "delivery_attempt_id": "attempt-1",
        })
        await asyncio.wait_for(handler_started.wait(), timeout=1)

        await client.close()

        assert authorization.cancelled()
        assert client._ws_reader is None
        assert client._ws_supervisor is None

    @pytest.mark.asyncio
    async def test_close_during_reconnect_backoff_stops_supervisor(self, monkeypatch):
        import custom_components.casa.api as api_mod

        monkeypatch.setattr(api_mod, "WS_RECONNECT_MIN", 0.05)
        monkeypatch.setattr(api_mod, "WS_RECONNECT_MAX", 0.05)
        session = MagicMock()
        ws = _FakeWS(stay_open=True)
        session.ws_connect = AsyncMock(side_effect=[ws, ConnectionError("offline")])
        client = CasaApiClient(session=session, host="h", port=1, webhook_secret="sec")
        await client.start_background(
            route_id="entry-1", agent_role="concierge", job_handler=AsyncMock(),
        )

        ws.disconnect()
        await _wait_until(lambda: session.ws_connect.await_count == 2)
        await client.close()
        await asyncio.sleep(0.06)

        assert session.ws_connect.await_count == 2
        assert client._ws_supervisor is None
