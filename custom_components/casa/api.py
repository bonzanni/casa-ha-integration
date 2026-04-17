"""Casa add-on API client."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
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
            async for frame in self._stream_utterance_sse(
                text=text, agent_role=agent_role, scope_id=scope_id,
                utterance_id=utterance_id, context=context,
            ):
                yield frame
        else:
            async for frame in self._stream_utterance_ws(
                text=text, agent_role=agent_role, scope_id=scope_id,
                utterance_id=utterance_id, context=context,
            ):
                yield frame

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

    async def _stream_utterance_ws(
        self, *, text, agent_role, scope_id, utterance_id, context,
    ) -> AsyncIterator[Any]:
        raise NotImplementedError  # Task 5 implements WS path
