"""Tests for Casa API client."""

from __future__ import annotations

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
