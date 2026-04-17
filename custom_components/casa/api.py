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
