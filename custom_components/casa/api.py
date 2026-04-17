"""Casa add-on API client."""

from __future__ import annotations

from dataclasses import dataclass


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
