"""Casa add-on API client."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import aiohttp

from .const import (
    HEALTH_PATH,
    SSE_PATH,
    TIMEOUT_CONNECT,
    TIMEOUT_HEALTH,
    TIMEOUT_TOTAL,
    VOICE_ROUTE_CAPABILITIES,
    VOICE_ROUTE_PROTOCOL,
    WS_PATH,
    WS_RECONNECT_MAX,
    WS_RECONNECT_MIN,
)

_LOGGER = logging.getLogger(__name__)
_sleep = asyncio.sleep


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
        self._ws_write_lock = asyncio.Lock()
        self._ws_reader: asyncio.Task | None = None
        self._ws_supervisor: asyncio.Task | None = None
        self._ws_generation = 0
        self._ws_queues: dict[tuple[int, str], asyncio.Queue] = {}
        self._ws_backoff = WS_RECONNECT_MIN
        self._route_id: str | None = None
        self._route_agent_role: str | None = None
        self._registered_generation: int | None = None
        self._route_ack = asyncio.Event()
        self._accepted_capabilities: frozenset[str] = frozenset()
        self._job_handler: Callable[[dict], Awaitable[None]] | None = None
        self._closed = False
        self._reconnect_attempts = 0

    @property
    def background_capable(self) -> bool:
        """Whether the current socket negotiated the background protocol."""
        ws = self._ws
        return bool(
            not self._closed
            and ws is not None
            and not ws.closed
            and self._route_ack.is_set()
            and frozenset(VOICE_ROUTE_CAPABILITIES) <= self._accepted_capabilities
        )

    @property
    def reconnect_attempts_for_test(self) -> int:
        """Expose supervisor reconnect attempts for compatibility tests."""
        return self._reconnect_attempts

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
            if self._closed:
                raise RuntimeError("Casa API client is closed")
            if self._ws is not None and not self._ws.closed:
                return self._ws
            headers = {"X-Webhook-Signature": self._sign(b"")}
            try:
                ws = await self._session.ws_connect(
                    self._ws_url, headers=headers, heartbeat=30,
                )
            except aiohttp.WSServerHandshakeError as err:
                if err.status == 401:
                    raise AuthenticationError("Invalid webhook secret") from err
                raise
            if self._closed:
                await ws.close()
                raise RuntimeError("Casa API client is closed")

            self._ws = ws
            self._ws_generation += 1
            generation = self._ws_generation
            self._ws_backoff = WS_RECONNECT_MIN
            self._registered_generation = None
            self._route_ack.clear()
            self._accepted_capabilities = frozenset()
            reader = asyncio.create_task(self._read_loop(ws, generation))
            self._ws_reader = reader
            try:
                if self._route_id is not None:
                    await self._register_voice_route(ws, generation)
            except BaseException:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
                if not ws.closed:
                    await ws.close()
                if self._ws is ws and self._ws_generation == generation:
                    self._ws = None
                    self._ws_reader = None
                raise
            return ws

    async def _send_json(
        self,
        frame: dict,
        *,
        ws: aiohttp.ClientWebSocketResponse | None = None,
        generation: int | None = None,
    ) -> None:
        """Send through the sole writer after checking socket generation."""
        if ws is None:
            ws = await self._ensure_ws()
            generation = self._ws_generation
        if generation is None:
            raise RuntimeError("WebSocket generation is required")
        async with self._ws_write_lock:
            if (
                self._closed
                or self._ws is not ws
                or self._ws_generation != generation
                or ws.closed
            ):
                raise ConnectionError("Casa WebSocket generation is no longer active")
            await ws.send_json(frame)

    async def _register_voice_route(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        generation: int,
    ) -> None:
        if self._registered_generation == generation:
            return
        if self._route_id is None or self._route_agent_role is None:
            return
        await self._send_json({
            "type": "voice_route_register",
            "protocol": VOICE_ROUTE_PROTOCOL,
            "route_id": self._route_id,
            "agent_role": self._route_agent_role,
            "capabilities": list(VOICE_ROUTE_CAPABILITIES),
        }, ws=ws, generation=generation)
        self._registered_generation = generation

    async def _supervise_ws(self) -> None:
        """Reconnect a configured background route until the client closes."""
        while not self._closed:
            reader = self._ws_reader
            if reader is not None:
                try:
                    await reader
                except asyncio.CancelledError:
                    raise
                except Exception:
                    _LOGGER.debug("Casa WebSocket reader failed", exc_info=True)
            if self._closed:
                return

            self._reconnect_attempts += 1
            try:
                await self._ensure_ws()
            except asyncio.CancelledError:
                raise
            except Exception:
                if self._closed:
                    return
                delay = self._ws_backoff
                self._ws_backoff = min(self._ws_backoff * 2, WS_RECONNECT_MAX)
                _LOGGER.debug(
                    "Casa WebSocket reconnect failed; retrying in %s seconds",
                    delay,
                    exc_info=True,
                )
                await _sleep(delay)

    async def _read_loop(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        generation: int,
    ) -> None:
        try:
            async for msg in ws:
                if msg.type.name != "TEXT":
                    continue
                try:
                    frame = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue
                if not isinstance(frame, dict):
                    continue
                frame_type = frame.get("type")
                if frame_type == "voice_route_registered":
                    self._handle_route_ack(frame, ws, generation)
                    continue
                if isinstance(frame_type, str) and frame_type.startswith("job_"):
                    if self.background_capable and self._job_handler is not None:
                        try:
                            await self._job_handler(frame)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            _LOGGER.error(
                                "Background job frame handler failed type=%s",
                                frame_type,
                            )
                    continue
                uid = frame.get("utterance_id")
                queue = self._ws_queues.get((generation, uid)) if uid else None
                if queue is None:
                    continue
                await queue.put(frame)
        finally:
            if self._ws is ws and self._ws_generation == generation:
                self._ws = None
                self._route_ack.clear()
                self._accepted_capabilities = frozenset()
            for (queue_generation, _), queue in list(self._ws_queues.items()):
                if queue_generation == generation:
                    await queue.put({"type": "__closed__"})

    def _handle_route_ack(
        self,
        frame: dict,
        ws: aiohttp.ClientWebSocketResponse,
        generation: int,
    ) -> None:
        if self._ws is not ws or self._ws_generation != generation:
            return
        accepted = frame.get("accepted_capabilities")
        if (
            type(frame.get("protocol")) is int
            and frame["protocol"] == VOICE_ROUTE_PROTOCOL
            and isinstance(accepted, (list, tuple, set, frozenset))
            and all(isinstance(capability, str) for capability in accepted)
        ):
            self._accepted_capabilities = frozenset(accepted)
        else:
            self._accepted_capabilities = frozenset()
        self._route_ack.set()

    async def _stream_utterance_ws(
        self, *, text, agent_role, scope_id, utterance_id, context,
    ) -> AsyncIterator[Any]:
        ws = await self._ensure_ws()
        generation = self._ws_generation
        queue: asyncio.Queue = asyncio.Queue()
        queue_key = (generation, utterance_id)
        self._ws_queues[queue_key] = queue
        terminated = False
        try:
            await self._send_json({
                "type": "utterance",
                "utterance_id": utterance_id,
                "text": text,
                "agent_role": agent_role,
                "scope_id": scope_id,
                "context": context,
            }, ws=ws, generation=generation)
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
            if self._ws_queues.get(queue_key) is queue:
                self._ws_queues.pop(queue_key, None)
            if not terminated and not ws.closed:
                try:
                    await self._send_json({
                        "type": "cancel", "utterance_id": utterance_id,
                    }, ws=ws, generation=generation)
                except Exception:
                    _LOGGER.debug("Cancel frame failed — connection likely closed", exc_info=True)

    async def start_background(
        self,
        *,
        route_id: str,
        agent_role: str,
        job_handler: Callable[[dict], Awaitable[None]],
    ) -> None:
        """Eagerly connect and supervise a negotiated background voice route."""
        if self._closed:
            raise RuntimeError("Casa API client is closed")
        self._route_id = route_id
        self._route_agent_role = agent_role
        self._job_handler = job_handler
        ws = await self._ensure_ws()
        generation = self._ws_generation
        await self._register_voice_route(ws, generation)
        if self._ws_supervisor is None or self._ws_supervisor.done():
            self._ws_supervisor = asyncio.create_task(self._supervise_ws())

    async def send_job_frame(self, frame: dict) -> None:
        """Send a background-delivery frame through the global WS writer."""
        await self._send_json(frame)

    async def register_session(
        self,
        scope_id: str,
        transport: str = "ws",
        *,
        agent_role: str,
    ) -> None:
        """Send ``stt_start`` to register the voice scope for idle-sweep/dedup.

        The add-on stopped prewarming memory on ``stt_start`` in 0.4x; this
        frame now only ensures the session pool entry exists.
        """
        if transport != "ws":
            _LOGGER.debug("Session registration requested on SSE transport — no-op")
            return
        ws = await self._ensure_ws()
        generation = self._ws_generation
        await self._send_json({
            "type": "stt_start",
            "session_key": f"voice:{scope_id}",
            "scope_id": scope_id,
            "agent_role": agent_role,
            "context": {},
        }, ws=ws, generation=generation)

    async def close(self) -> None:
        self._closed = True
        ws = self._ws
        supervisor = self._ws_supervisor
        reader = self._ws_reader
        tasks = {
            task
            for task in (supervisor, reader)
            if task is not None and not task.done()
        }
        current = asyncio.current_task()
        for task in tasks:
            if task is not current:
                task.cancel()
        await asyncio.gather(
            *(task for task in tasks if task is not current),
            return_exceptions=True,
        )
        if ws is not None and not ws.closed:
            await ws.close()
        for queue in list(self._ws_queues.values()):
            await queue.put({"type": "__closed__"})
        self._ws = None
        self._ws_reader = None
        self._ws_supervisor = None
        self._registered_generation = None
        self._route_ack.clear()
        self._accepted_capabilities = frozenset()

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
