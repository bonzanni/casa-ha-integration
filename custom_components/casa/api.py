"""Casa add-on API client."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import aiohttp

from .const import (
    HEALTH_PATH,
    SSE_PATH,
    TIMEOUT_CONNECT,
    TIMEOUT_HEALTH,
    TIMEOUT_TOTAL,
    WS_PATH,
    WS_RECONNECT_MAX,
    WS_RECONNECT_MIN,
)

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class BlockFrame:
    text: str
    final: bool = False
    kind: str = "block"


@dataclass(frozen=True)
class DoneFrame:
    kind: str = "done"


@dataclass(frozen=True)
class ErrorFrame:
    kind_: str
    spoken: str
    kind: str = "error"


class AuthenticationError(Exception):
    """Raised when Casa returns 401."""


class CasaApiClient:
    """Async client for Casa add-on (SSE + WS)."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        host: str,
        port: int,
        webhook_secret: str,
    ) -> None:
        self._session = session
        self._base_url = f"http://{host}:{port}"
        self._ws_url = f"ws://{host}:{port}{WS_PATH}"
        self._secret = webhook_secret
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._ws_lock = asyncio.Lock()
        self._ws_reader: asyncio.Task | None = None
        self._ws_queues: dict[str, asyncio.Queue] = {}
        self._ws_backoff = WS_RECONNECT_MIN

    def _sign(self, body: bytes) -> str:
        return hmac.new(self._secret.encode(), body, hashlib.sha256).hexdigest()

    async def health_check(self) -> bool:
        async with asyncio.timeout(TIMEOUT_HEALTH):
            resp = await self._session.get(f"{self._base_url}{HEALTH_PATH}")
            return resp.status == 200

    async def stream_utterance(
        self,
        *,
        text: str,
        agent_role: str,
        scope_id: str,
        utterance_id: str,
        context: dict,
        transport: str = "ws",
    ) -> AsyncIterator[Any]:
        if transport == "sse":
            inner = self._stream_utterance_sse(
                text=text, agent_role=agent_role, scope_id=scope_id,
                utterance_id=utterance_id, context=context,
            )
        else:
            inner = self._stream_utterance_ws(
                text=text, agent_role=agent_role, scope_id=scope_id,
                utterance_id=utterance_id, context=context,
            )
        try:
            async for frame in inner:
                yield frame
        finally:
            await inner.aclose()

    async def _stream_utterance_sse(
        self, *, text, agent_role, scope_id, utterance_id, context,
    ) -> AsyncIterator[Any]:
        payload = {
            "prompt": text,
            "agent_role": agent_role,
            "scope_id": scope_id,
            "context": {"utterance_id": utterance_id, **context},
        }
        body = json.dumps(payload).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Signature": self._sign(body),
        }
        resp = await self._session.post(
            f"{self._base_url}{SSE_PATH}",
            data=body,
            headers=headers,
            timeout=aiohttp.ClientTimeout(connect=TIMEOUT_CONNECT, total=TIMEOUT_TOTAL),
        )
        if resp.status == 401:
            raise AuthenticationError("Invalid webhook secret")
        resp.raise_for_status()

        event: str | None = None
        async for raw in resp.content:
            line = raw.decode("utf-8").rstrip("\n").rstrip("\r")
            if not line:
                event = None
                continue
            if line.startswith("event:"):
                event = line[6:].strip()
                continue
            if line.startswith("data:"):
                data_str = line[5:].strip()
                try:
                    data = json.loads(data_str) if data_str else {}
                except json.JSONDecodeError:
                    continue
                if event == "block":
                    yield BlockFrame(text=data.get("text", ""), final=bool(data.get("final", False)))
                elif event == "error":
                    yield ErrorFrame(kind_=data.get("kind", "unknown"), spoken=data.get("spoken", ""))
                    return
                elif event == "done":
                    yield DoneFrame()
                    return
        # If SSE closes without `done`, synthesise one so callers terminate.
        yield DoneFrame()

    async def _ensure_ws(self) -> aiohttp.ClientWebSocketResponse:
        async with self._ws_lock:
            if self._ws is not None and not self._ws.closed:
                return self._ws
            headers = {"X-Webhook-Signature": self._sign(b"")}
            try:
                self._ws = await self._session.ws_connect(
                    self._ws_url, headers=headers, heartbeat=30,
                )
            except aiohttp.WSServerHandshakeError as err:
                if err.status == 401:
                    raise AuthenticationError("Invalid webhook secret") from err
                raise
            self._ws_backoff = WS_RECONNECT_MIN
            self._ws_reader = asyncio.create_task(self._read_loop(self._ws))
            return self._ws

    async def _read_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        try:
            async for msg in ws:
                if msg.type.name != "TEXT":
                    continue
                try:
                    frame = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue
                uid = frame.get("utterance_id")
                queue = self._ws_queues.get(uid) if uid else None
                if queue is None:
                    continue
                await queue.put(frame)
        finally:
            for q in list(self._ws_queues.values()):
                await q.put({"type": "__closed__"})

    async def _stream_utterance_ws(
        self, *, text, agent_role, scope_id, utterance_id, context,
    ) -> AsyncIterator[Any]:
        ws = await self._ensure_ws()
        queue: asyncio.Queue = asyncio.Queue()
        self._ws_queues[utterance_id] = queue
        terminated = False
        try:
            await ws.send_json({
                "type": "utterance",
                "utterance_id": utterance_id,
                "text": text,
                "agent_role": agent_role,
                "scope_id": scope_id,
                "context": context,
            })
            while True:
                frame = await queue.get()
                t = frame.get("type")
                if t == "__closed__":
                    terminated = True
                    yield ErrorFrame(kind_="connection", spoken="")
                    return
                if t == "block":
                    yield BlockFrame(text=frame.get("text", ""), final=bool(frame.get("final", False)))
                elif t == "error":
                    terminated = True
                    yield ErrorFrame(kind_=frame.get("kind", "unknown"), spoken=frame.get("spoken", ""))
                    return
                elif t == "done":
                    terminated = True
                    yield DoneFrame()
                    return
        finally:
            self._ws_queues.pop(utterance_id, None)
            if not terminated:
                ws = self._ws
                if ws is not None and not ws.closed:
                    try:
                        await ws.send_json({"type": "cancel", "utterance_id": utterance_id})
                    except Exception:
                        _LOGGER.debug("Cancel frame failed — connection likely closed", exc_info=True)

    async def register_session(self, scope_id: str, transport: str = "ws") -> None:
        """Send ``stt_start`` to register the voice scope for idle-sweep/dedup.

        The add-on stopped prewarming memory on ``stt_start`` in 0.4x; this
        frame now only ensures the session pool entry exists.
        """
        if transport != "ws":
            _LOGGER.debug("Session registration requested on SSE transport — no-op")
            return
        ws = await self._ensure_ws()
        await ws.send_json({
            "type": "stt_start",
            "session_key": f"voice:{scope_id}",
            "scope_id": scope_id,
            "context": {},
        })

    async def close(self) -> None:
        if self._ws_reader is not None and not self._ws_reader.done():
            self._ws_reader.cancel()
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        self._ws = None
        self._ws_reader = None

    async def probe_auth(self) -> None:
        """Verify the webhook secret by sending an HMAC-signed empty-prompt POST.

        Casa's SSE handler returns 400 "missing 'prompt'" when HMAC is valid
        and the body lacks a prompt, and 401 when HMAC is invalid. So a 400
        here is the success signal for auth; 401 raises AuthenticationError.
        """
        body = json.dumps({}).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Signature": self._sign(body),
        }
        async with asyncio.timeout(TIMEOUT_HEALTH):
            resp = await self._session.post(
                f"{self._base_url}{SSE_PATH}",
                data=body,
                headers=headers,
            )
        if resp.status == 401:
            raise AuthenticationError("Invalid webhook secret")
        # Any other status (400 expected, 200/404/etc. all count as "secret accepted").
