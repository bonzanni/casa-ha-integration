"""Casa add-on API client."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

import aiohttp

from .catalog import (
    CatalogValidationError,
    VoiceAgentCatalog,
    parse_voice_agent_catalog,
)
from .const import (
    HEALTH_PATH,
    SSE_PATH,
    TIMEOUT_CONNECT,
    TIMEOUT_HEALTH,
    TIMEOUT_TOTAL,
    VOICE_ROUTE_CAPABILITIES,
    VOICE_ROUTE_PROTOCOL,
    VOICE_AGENTS_PATH,
    WS_PATH,
    WS_RECONNECT_MAX,
    WS_RECONNECT_MIN,
)

_LOGGER = logging.getLogger(__name__)
_sleep = asyncio.sleep
_HANDOFF_VALUE_MAX_LENGTH = 512


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


@dataclass(frozen=True)
class HandoffFrame:
    handoff_id: str
    text: str
    kind: str = "handoff"


def _bounded_nonempty_handoff_value(value: Any) -> str | None:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > _HANDOFF_VALUE_MAX_LENGTH
    ):
        return None
    return value


def _parse_handoff_frame(frame: dict) -> HandoffFrame | None:
    handoff_id = _bounded_nonempty_handoff_value(frame.get("handoff_id"))
    text = _bounded_nonempty_handoff_value(frame.get("text"))
    if handoff_id is None or text is None:
        return None
    return HandoffFrame(handoff_id=handoff_id, text=text)


class AuthenticationError(Exception):
    """Raised when Casa returns 401."""


class CasaClientClosedError(ConnectionError):
    """Raised when an operation races or follows client shutdown."""


class ConnectionState(StrEnum):
    """Availability state of this client's WebSocket connection."""

    DISCONNECTED = "disconnected"
    CONNECTED = "connected"
    AUTH_FAILED = "auth_failed"


class CasaApiClient:
    """Async client for Casa add-on (SSE + WS)."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        host: str,
        port: int,
        webhook_secret: str,
        state_callback: Callable[[ConnectionState], None] | None = None,
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
        self._route_ack_generation: int | None = None
        self._accepted_capabilities: frozenset[str] = frozenset()
        self._job_handler: Callable[[dict], Awaitable[None]] | None = None
        self._state_callback = state_callback
        self._connection_state = ConnectionState.DISCONNECTED
        self._closed = False
        self._reconnect_attempts = 0

    @property
    def connected(self) -> bool:
        """Whether this client owns a current connected socket generation."""
        return (
            not self._closed
            and self._connection_state is ConnectionState.CONNECTED
        )

    def _notify_connection_state(
        self,
        state: ConnectionState,
        *,
        allow_closed: bool = False,
    ) -> None:
        """Publish a changed state without letting callbacks affect transport."""
        if self._closed and not allow_closed:
            return
        if (
            state is ConnectionState.DISCONNECTED
            and self._connection_state is ConnectionState.AUTH_FAILED
            and not allow_closed
        ):
            return
        if self._connection_state is state:
            return
        self._connection_state = state
        if self._state_callback is None:
            return
        try:
            self._state_callback(state)
        except Exception:
            _LOGGER.error("Casa connection state callback failed")

    @property
    def background_capable(self) -> bool:
        """Whether the current socket negotiated the background protocol."""
        return self._is_background_capable_for(self._ws, self._ws_generation)

    def _is_background_capable_for(
        self,
        ws: aiohttp.ClientWebSocketResponse | None,
        generation: int,
    ) -> bool:
        """Whether an exact socket generation negotiated background delivery."""
        return bool(
            not self._closed
            and ws is not None
            and self._ws is ws
            and self._ws_generation == generation
            and not ws.closed
            and self._route_ack.is_set()
            and self._route_ack_generation == generation
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

    async def fetch_voice_agents(self) -> VoiceAgentCatalog:
        """Fetch and validate Casa's authenticated voice-agent catalog."""
        headers = {"X-Webhook-Signature": self._sign(b"")}
        async with asyncio.timeout(TIMEOUT_HEALTH):
            response = await self._session.get(
                f"{self._base_url}{VOICE_AGENTS_PATH}",
                headers=headers,
            )
            if response.status == 401:
                response.release()
                raise AuthenticationError("Invalid webhook secret")
            response.raise_for_status()
            try:
                payload = await response.json()
            except (
                aiohttp.ContentTypeError,
                json.JSONDecodeError,
                UnicodeDecodeError,
            ) as err:
                raise CatalogValidationError("invalid_json") from err
            return parse_voice_agent_catalog(payload)

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
                raise CasaClientClosedError("Casa API client is closed")
            if self._connection_state is ConnectionState.AUTH_FAILED:
                raise AuthenticationError("Invalid webhook secret")
            if self._ws is not None and not self._ws.closed:
                return self._ws
            headers = {"X-Webhook-Signature": self._sign(b"")}
            try:
                ws = await self._session.ws_connect(
                    self._ws_url, headers=headers, heartbeat=30,
                )
            except aiohttp.WSServerHandshakeError as err:
                if err.status == 401:
                    self._notify_connection_state(ConnectionState.AUTH_FAILED)
                    raise AuthenticationError("Invalid webhook secret") from err
                raise
            if self._closed:
                await ws.close()
                raise CasaClientClosedError("Casa API client is closed")

            async with self._ws_write_lock:
                if self._closed:
                    await ws.close()
                    raise CasaClientClosedError("Casa API client is closed")
                self._ws = ws
                self._ws_generation += 1
                generation = self._ws_generation
                self._ws_backoff = WS_RECONNECT_MIN
                self._registered_generation = None
                self._route_ack.clear()
                self._route_ack_generation = None
                self._accepted_capabilities = frozenset()
                reader = asyncio.create_task(self._read_loop(ws, generation))
                self._ws_reader = reader
            try:
                if self._route_id is not None:
                    await self._register_voice_route(ws, generation)
                async with self._ws_write_lock:
                    if (
                        self._closed
                        or self._ws is not ws
                        or self._ws_generation != generation
                        or ws.closed
                    ):
                        raise ConnectionError(
                            "Casa WebSocket generation is no longer active",
                        )
                    self._notify_connection_state(ConnectionState.CONNECTED)
            except BaseException:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
                if not ws.closed:
                    await ws.close()
                async with self._ws_write_lock:
                    if self._ws is ws and self._ws_generation == generation:
                        self._ws = None
                        self._ws_reader = None
                        self._route_ack.clear()
                        self._route_ack_generation = None
                        self._accepted_capabilities = frozenset()
                        self._notify_connection_state(
                            ConnectionState.DISCONNECTED,
                        )
                raise
            return ws

    async def _send_json(
        self,
        frame: dict,
        *,
        ws: aiohttp.ClientWebSocketResponse | None = None,
        generation: int | None = None,
        require_background: bool = False,
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
            if require_background and (
                not self._route_ack.is_set()
                or self._route_ack_generation != generation
                or frozenset(VOICE_ROUTE_CAPABILITIES)
                - self._accepted_capabilities
            ):
                raise ConnectionError(
                    "Casa background protocol is not negotiated for this generation",
                )
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

    async def _retire_ws_generation(
        self,
        ws: aiohttp.ClientWebSocketResponse,
        generation: int,
    ) -> None:
        """Retire an exact failed generation so supervision can reconnect."""
        reader: asyncio.Task | None = None
        async with self._ws_write_lock:
            if self._ws is not ws or self._ws_generation != generation:
                return
            self._ws = None
            reader = self._ws_reader
            self._ws_reader = None
            self._registered_generation = None
            self._route_ack.clear()
            self._route_ack_generation = None
            self._accepted_capabilities = frozenset()
            self._notify_connection_state(ConnectionState.DISCONNECTED)

        if reader is not None and reader is not asyncio.current_task():
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)
        if not ws.closed:
            try:
                await ws.close()
            except Exception:
                _LOGGER.debug("Casa WebSocket retirement close failed")

    async def _supervise_ws(self) -> None:
        """Reconnect a configured background route until the client closes."""
        while not self._closed:
            reader = self._ws_reader
            if reader is not None:
                try:
                    await reader
                except asyncio.CancelledError:
                    current = asyncio.current_task()
                    if current is None or current.cancelling():
                        raise
                except Exception:
                    _LOGGER.debug("Casa WebSocket reader failed")
            if (
                self._closed
                or self._connection_state is ConnectionState.AUTH_FAILED
            ):
                return

            self._reconnect_attempts += 1
            try:
                await self._ensure_ws()
            except asyncio.CancelledError:
                raise
            except AuthenticationError:
                self._notify_connection_state(ConnectionState.AUTH_FAILED)
                return
            except Exception:
                if self._closed:
                    return
                delay = self._ws_backoff
                self._ws_backoff = min(self._ws_backoff * 2, WS_RECONNECT_MAX)
                _LOGGER.debug("Casa WebSocket reconnect failed; retry scheduled")
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
                    await self._handle_route_ack(frame, ws, generation)
                    continue
                if isinstance(frame_type, str) and frame_type.startswith("job_"):
                    if (
                        self._is_background_capable_for(ws, generation)
                        and self._job_handler is not None
                    ):
                        try:
                            await self._job_handler(frame)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            _LOGGER.error("Background job frame handler failed")
                    continue
                if frame_type == "handoff_received":
                    handoff = _parse_handoff_frame(frame)
                    uid = _bounded_nonempty_handoff_value(
                        frame.get("utterance_id"),
                    )
                    queue = self._ws_queues.get((generation, uid)) if uid else None
                    if handoff is not None and queue is not None:
                        await queue.put(handoff)
                    continue
                uid = frame.get("utterance_id")
                queue = self._ws_queues.get((generation, uid)) if uid else None
                if queue is None:
                    continue
                await queue.put(frame)
        finally:
            async with self._ws_write_lock:
                if self._ws is ws and self._ws_generation == generation:
                    self._ws = None
                    self._route_ack.clear()
                    self._route_ack_generation = None
                    self._accepted_capabilities = frozenset()
                    self._notify_connection_state(
                        ConnectionState.DISCONNECTED,
                    )
            for (queue_generation, _), queue in list(self._ws_queues.items()):
                if queue_generation == generation:
                    await queue.put({"type": "__closed__"})

    async def _handle_route_ack(
        self,
        frame: dict,
        ws: aiohttp.ClientWebSocketResponse,
        generation: int,
    ) -> None:
        async with self._ws_write_lock:
            if self._ws is not ws or self._ws_generation != generation or ws.closed:
                return
            accepted = frame.get("accepted_capabilities")
            if (
                type(frame.get("protocol")) is int
                and frame["protocol"] == VOICE_ROUTE_PROTOCOL
                and isinstance(accepted, (list, tuple, set, frozenset))
                and all(isinstance(capability, str) for capability in accepted)
                and frozenset(accepted) == frozenset(VOICE_ROUTE_CAPABILITIES)
            ):
                self._accepted_capabilities = frozenset(accepted)
            else:
                self._accepted_capabilities = frozenset()
            self._route_ack_generation = generation
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
            utterance = {
                "type": "utterance",
                "utterance_id": utterance_id,
                "text": text,
                "agent_role": agent_role,
                "scope_id": scope_id,
                "context": context,
            }
            device_id = context.get("device_id")
            if (
                isinstance(device_id, str)
                and device_id.strip()
                and len(device_id) <= 512
            ):
                utterance["device_id"] = device_id
            await self._send_json(utterance, ws=ws, generation=generation)
            while True:
                frame = await queue.get()
                if isinstance(frame, HandoffFrame):
                    terminated = True
                    yield frame
                    return
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
                    _LOGGER.debug("Cancel frame failed; connection likely closed")

    async def start_background(
        self,
        *,
        route_id: str,
        agent_role: str,
        job_handler: Callable[[dict], Awaitable[None]],
    ) -> None:
        """Eagerly connect and supervise a negotiated background voice route."""
        if self._closed:
            raise CasaClientClosedError("Casa API client is closed")
        self._route_id = route_id
        self._route_agent_role = agent_role
        self._job_handler = job_handler
        ws: aiohttp.ClientWebSocketResponse | None = None
        generation: int | None = None
        try:
            ws = await self._ensure_ws()
            generation = self._ws_generation
            await self._register_voice_route(ws, generation)
        except AuthenticationError:
            raise
        except (aiohttp.ClientError, OSError, asyncio.TimeoutError):
            if not self._closed:
                _LOGGER.debug(
                    "Casa background WebSocket startup failed; retry scheduled",
                )
            if ws is not None and generation is not None:
                await self._retire_ws_generation(ws, generation)
        if self._closed:
            raise CasaClientClosedError("Casa API client is closed")
        if self._ws_supervisor is None or self._ws_supervisor.done():
            self._ws_supervisor = asyncio.create_task(self._supervise_ws())

    async def send_job_frame(self, frame: dict) -> None:
        """Send a background-delivery frame through the global WS writer."""
        await self._send_json(frame, require_background=True)

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
        ws = self._ws
        generation = self._ws_generation
        if not self.connected or ws is None or ws.closed:
            return
        await self._send_json({
            "type": "stt_start",
            "session_key": f"voice:{scope_id}",
            "scope_id": scope_id,
            "agent_role": agent_role,
            "context": {},
        }, ws=ws, generation=generation)

    async def close(self) -> None:
        self._closed = True
        self._notify_connection_state(
            ConnectionState.DISCONNECTED,
            allow_closed=True,
        )
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
        async with self._ws_write_lock:
            if ws is not None and not ws.closed:
                await ws.close()
            self._ws = None
            self._ws_reader = None
            self._ws_supervisor = None
            self._registered_generation = None
            self._route_ack.clear()
            self._route_ack_generation = None
            self._accepted_capabilities = frozenset()
        for queue in list(self._ws_queues.values()):
            await queue.put({"type": "__closed__"})

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
