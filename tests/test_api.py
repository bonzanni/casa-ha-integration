"""Tests for Casa API client."""

from __future__ import annotations

import pytest

from custom_components.casa.api import (
    BlockFrame,
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
