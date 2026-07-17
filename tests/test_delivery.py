"""Tests for Casa background delivery to Assist satellites."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from unittest.mock import AsyncMock, MagicMock

import pytest

from custom_components.casa.delivery import (
    BackgroundDeliveryManager,
    SatelliteAmbiguous,
    SatelliteDirectory,
    SatelliteNotFound,
)


class FakeClock:
    def __init__(self, now: float = 100.0) -> None:
        self.now = now
        self.sleeps: list[float] = []
        self._waiters: list[tuple[float, asyncio.Future]] = []

    async def sleep(self, delay: float) -> None:
        self.sleeps.append(delay)
        if delay <= 0:
            await asyncio.sleep(0)
            return
        future = asyncio.get_running_loop().create_future()
        self._waiters.append((self.now + delay, future))
        try:
            await future
        finally:
            self._waiters = [item for item in self._waiters if item[1] is not future]

    async def advance(self, seconds: float) -> None:
        self.now += seconds
        for deadline, future in list(self._waiters):
            if deadline <= self.now and not future.done():
                future.set_result(None)
        for _ in range(8):
            await asyncio.sleep(0)

    async def settle(self) -> None:
        for _ in range(8):
            await asyncio.sleep(0)


class FakeClient:
    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.manager: BackgroundDeliveryManager | None = None
        self.authorize = True
        self.on_send: Callable[[dict], Awaitable[None]] | None = None

    async def send_job_frame(self, frame: dict) -> None:
        self.sent.append(frame)
        if self.on_send is not None:
            await self.on_send(frame)
        if frame["type"] == "job_delivery_start" and self.authorize:
            assert self.manager is not None
            await self.manager.handle_frame({
                "type": "job_delivery_authorized",
                "protocol": 1,
                "job_id": frame["job_id"],
                "delivery_attempt_id": frame["delivery_attempt_id"],
            })


def job_ready(
    job_id: str,
    *,
    device: str = "dev-k",
    attempt_id: str | None = None,
    sequence: int = 1,
    **changes,
) -> dict:
    return {
        "type": "job_ready",
        "protocol": 1,
        "job_id": job_id,
        "delivery_attempt_id": attempt_id or f"attempt-{job_id}",
        "route_id": "entry-1",
        "origin_device_id": device,
        "spoken_text": "The ruling is no.",
        "ready_at": 90.0,
        "expires_at": 1000.0,
        "delivery_sequence": sequence,
        **changes,
    }


def sent_types(client: FakeClient) -> list[str]:
    return [frame["type"] for frame in client.sent]


@pytest.fixture
async def delivery_manager():
    clock = FakeClock()
    directory = SatelliteDirectory()
    directory.add("dev-k", "assist_satellite.kitchen")
    directory.add("dev-o", "assist_satellite.office")
    client = FakeClient()
    hass = MagicMock()
    hass.services.async_call = AsyncMock()
    manager = BackgroundDeliveryManager(
        hass,
        client,
        route_id="entry-1",
        directory=directory,
        idle_stability_ms=750,
        clock=clock,
    )
    client.manager = manager
    try:
        yield manager, clock
    finally:
        await manager.close()


class TestSatelliteDirectory:
    def test_tracks_device_entity_and_all_assist_states(self):
        directory = SatelliteDirectory()
        directory.add("dev-k", "assist_satellite.kitchen")

        directory.set_state("dev-k", "idle", changed_at=10.0)

        assert directory.resolve("dev-k") == "assist_satellite.kitchen"
        assert directory.state("dev-k") == "idle"
        assert directory.idle_since("dev-k") == 10.0

        for state in ("listening", "processing", "responding"):
            directory.set_state("dev-k", state, changed_at=11.0)
            assert directory.state("dev-k") == state
            assert directory.idle_since("dev-k") is None

    def test_refuses_ambiguous_device_without_override(self):
        directory = SatelliteDirectory()
        directory.add("dev-k", "assist_satellite.kitchen")
        directory.add("dev-k", "assist_satellite.kitchen_2")

        with pytest.raises(SatelliteAmbiguous):
            directory.resolve("dev-k")

    def test_uses_valid_configured_override(self):
        directory = SatelliteDirectory(overrides={
            "dev-k": "assist_satellite.kitchen_2",
        })
        directory.add("dev-k", "assist_satellite.kitchen")
        directory.add("dev-k", "assist_satellite.kitchen_2")

        assert directory.resolve("dev-k") == "assist_satellite.kitchen_2"

    @pytest.mark.parametrize(
        "override",
        ["assist_satellite.stale", "light.kitchen"],
    )
    def test_invalid_configured_override_fails_closed(self, override):
        directory = SatelliteDirectory(overrides={"dev-k": override})
        directory.add("dev-k", "assist_satellite.kitchen")

        with pytest.raises(SatelliteNotFound):
            directory.resolve("dev-k")

    def test_entity_rebinding_and_removal_clear_stale_mapping(self):
        directory = SatelliteDirectory()
        directory.add("dev-old", "assist_satellite.kitchen")

        directory.add("dev-new", "assist_satellite.kitchen")

        with pytest.raises(SatelliteNotFound):
            directory.resolve("dev-old")
        assert directory.resolve("dev-new") == "assist_satellite.kitchen"

        directory.remove("assist_satellite.kitchen")
        with pytest.raises(SatelliteNotFound):
            directory.resolve("dev-new")

    def test_ambiguous_device_retains_exact_entity_states_for_valid_override(self):
        directory = SatelliteDirectory(overrides={
            "dev-k": "assist_satellite.kitchen_2",
        })
        directory.set_entity_state(
            "dev-k", "assist_satellite.kitchen", "processing", changed_at=10.0,
        )
        directory.set_entity_state(
            "dev-k", "assist_satellite.kitchen_2", "idle", changed_at=11.0,
        )

        assert directory.resolve("dev-k") == "assist_satellite.kitchen_2"
        assert directory.state("dev-k") == "idle"
        assert directory.idle_since("dev-k") == 11.0


class TestStableIdleDelivery:
    async def test_already_stably_idle_announces_without_extra_750ms(
        self, delivery_manager,
    ):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)

        await manager.handle_frame(job_ready("job-1"))
        await manager.drain_for_test()

        manager.hass.services.async_call.assert_awaited_once_with(
            "assist_satellite",
            "announce",
            {
                "entity_id": "assist_satellite.kitchen",
                "message": "The ruling is no.",
            },
            blocking=True,
        )
        assert 0.75 not in clock.sleeps

    async def test_busy_satellite_renews_but_never_announces(self, delivery_manager):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "processing", changed_at=clock.now)

        await manager.handle_frame(job_ready("job-1"))
        await clock.settle()
        await clock.advance(20)

        assert "job_claim_renew" in sent_types(manager.client)
        manager.hass.services.async_call.assert_not_awaited()

    async def test_failed_renew_releases_worker_for_reoffered_attempt(
        self, delivery_manager,
    ):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "processing", changed_at=clock.now)
        first = job_ready("job-1", attempt_id="attempt-1")
        renew_failed = asyncio.Event()

        async def fail_renew(frame):
            if frame["type"] == "job_claim_renew":
                renew_failed.set()
                raise ConnectionError("PRIVATE_GENERATION_CANARY")

        manager.client.on_send = fail_renew
        await manager.handle_frame(first)
        await clock.settle()
        await clock.advance(5)
        await asyncio.wait_for(renew_failed.wait(), timeout=1)
        manager.client.on_send = None

        await manager.handle_frame(job_ready("job-1", attempt_id="attempt-2"))
        manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)
        await manager.drain_for_test()

        assert not any(
            frame["type"] == "job_delivery_start"
            and frame["delivery_attempt_id"] == "attempt-1"
            for frame in manager.client.sent
        )
        assert any(
            frame["type"] == "job_delivered"
            and frame["delivery_attempt_id"] == "attempt-2"
            for frame in manager.client.sent
        )
        manager.hass.services.async_call.assert_awaited_once()

    async def test_fresh_idle_transition_waits_750ms(self, delivery_manager):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "responding", changed_at=clock.now)
        await manager.handle_frame(job_ready("job-1"))
        await clock.settle()

        manager.directory.set_state("dev-k", "idle", changed_at=clock.now)
        await clock.advance(0.749)
        manager.hass.services.async_call.assert_not_awaited()

        await clock.advance(0.001)
        await manager.drain_for_test()
        manager.hass.services.async_call.assert_awaited_once()


class TestFinalRechecksAndRevocation:
    async def test_idle_to_listening_after_authorization_nacks_without_announce(
        self, delivery_manager,
    ):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)
        manager.client.authorize = False

        async def preempt_after_authorizing(frame):
            if frame["type"] != "job_delivery_start":
                return
            await manager.handle_frame({
                "type": "job_delivery_authorized",
                "protocol": 1,
                "job_id": frame["job_id"],
                "delivery_attempt_id": frame["delivery_attempt_id"],
            })
            manager.directory.set_state("dev-k", "listening", changed_at=clock.now)

        manager.client.on_send = preempt_after_authorizing
        await manager.handle_frame(job_ready("job-1"))
        await manager.drain_for_test()

        assert manager.client.sent[-1]["type"] == "job_nack"
        assert manager.client.sent[-1]["reason"] == "preempted_before_playback"
        manager.hass.services.async_call.assert_not_awaited()

    async def test_revoke_during_idle_wait_is_acked_without_announce(self, delivery_manager):
        manager, clock = delivery_manager
        offer = job_ready("job-1")
        manager.directory.set_state("dev-k", "processing", changed_at=clock.now)
        await manager.handle_frame(offer)
        await clock.settle()

        await manager.handle_frame({
            "type": "job_revoke",
            "protocol": 1,
            "job_id": offer["job_id"],
            "delivery_attempt_id": offer["delivery_attempt_id"],
            "reason": "cancelled",
        })
        await manager.drain_for_test()

        assert manager.client.sent[-1]["type"] == "job_revoked"
        manager.hass.services.async_call.assert_not_awaited()

    async def test_revoke_after_authorization_before_playback_is_acked(
        self, delivery_manager,
    ):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)
        manager.client.authorize = False

        async def authorize_then_revoke(frame):
            if frame["type"] != "job_delivery_start":
                return
            base = {
                "protocol": 1,
                "job_id": frame["job_id"],
                "delivery_attempt_id": frame["delivery_attempt_id"],
            }
            await manager.handle_frame({"type": "job_delivery_authorized", **base})
            await manager.handle_frame({"type": "job_revoke", "reason": "cancelled", **base})

        manager.client.on_send = authorize_then_revoke
        await manager.handle_frame(job_ready("job-1"))
        await manager.drain_for_test()

        assert manager.client.sent[-1]["type"] == "job_revoked"
        manager.hass.services.async_call.assert_not_awaited()

    async def test_revoke_during_blocking_playback_never_barges_in(self, delivery_manager):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)
        playback_started = asyncio.Event()
        release_playback = asyncio.Event()
        playback_cancelled = False

        async def blocking_announce(*_args, **_kwargs):
            nonlocal playback_cancelled
            playback_started.set()
            try:
                await release_playback.wait()
            except asyncio.CancelledError:
                playback_cancelled = True
                raise

        manager.hass.services.async_call = AsyncMock(side_effect=blocking_announce)
        offer = job_ready("job-1")
        await manager.handle_frame(offer)
        await asyncio.wait_for(playback_started.wait(), timeout=1)

        await manager.handle_frame({
            "type": "job_revoke",
            "protocol": 1,
            "job_id": offer["job_id"],
            "delivery_attempt_id": offer["delivery_attempt_id"],
            "reason": "cancelled",
        })
        await clock.settle()
        assert playback_cancelled is False
        assert "job_revoked" not in sent_types(manager.client)

        release_playback.set()
        await manager.drain_for_test()
        assert manager.client.sent[-1]["type"] == "job_delivered"

    async def test_revoke_while_locally_queued_acks_without_claim_or_playback(
        self, delivery_manager,
    ):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)
        first_started = asyncio.Event()
        release_first = asyncio.Event()

        async def blocking_first(_domain, _service, data, *, blocking):
            assert blocking is True
            if data["message"] == "first":
                first_started.set()
                await release_first.wait()

        manager.hass.services.async_call = AsyncMock(side_effect=blocking_first)
        first = job_ready("job-1", sequence=1, spoken_text="first")
        queued = job_ready("job-2", sequence=2, spoken_text="second")
        await manager.handle_frame(first)
        await manager.handle_frame(queued)
        await asyncio.wait_for(first_started.wait(), timeout=1)

        await manager.handle_frame({
            "type": "job_revoke",
            "protocol": 1,
            "job_id": queued["job_id"],
            "delivery_attempt_id": queued["delivery_attempt_id"],
            "reason": "cancelled",
        })
        release_first.set()
        await manager.drain_for_test()

        queued_types = [
            frame["type"] for frame in manager.client.sent
            if frame["job_id"] == "job-2"
        ]
        assert queued_types == ["job_revoked"]
        assert manager.hass.services.async_call.await_count == 1

    async def test_revoke_arriving_with_playback_started_is_already_too_late(
        self, delivery_manager,
    ):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)

        async def revoke_as_playback_starts(frame):
            if frame["type"] != "job_playback_started":
                return
            await manager.handle_frame({
                "type": "job_revoke",
                "protocol": 1,
                "job_id": frame["job_id"],
                "delivery_attempt_id": frame["delivery_attempt_id"],
                "reason": "cancelled",
            })

        manager.client.on_send = revoke_as_playback_starts
        await manager.handle_frame(job_ready("job-1"))
        await manager.drain_for_test()

        types = sent_types(manager.client)
        assert types[-2:] == ["job_playback_started", "job_delivered"]
        assert "job_nack" not in types
        assert "job_revoked" not in types
        manager.hass.services.async_call.assert_awaited_once()

    @pytest.mark.parametrize("invalidation", ["state", "rebind"])
    async def test_playback_started_send_revalidates_before_announce(
        self, delivery_manager, invalidation,
    ):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)

        async def invalidate_during_playback_started(frame):
            if frame["type"] != "job_playback_started":
                return
            if invalidation == "state":
                manager.directory.set_state(
                    "dev-k", "listening", changed_at=clock.now,
                )
            else:
                manager.directory.remove("assist_satellite.kitchen")
                manager.directory.add("dev-k", "assist_satellite.kitchen_2")
                manager.directory.set_state(
                    "dev-k", "idle", changed_at=clock.now - 10,
                )

        manager.client.on_send = invalidate_during_playback_started
        await manager.handle_frame(job_ready("job-1"))
        await manager.drain_for_test()

        assert sent_types(manager.client)[-1] == "job_playback_started"
        assert "job_nack" not in sent_types(manager.client)
        assert "job_revoked" not in sent_types(manager.client)
        manager.hass.services.async_call.assert_not_awaited()

    async def test_authorization_wait_is_bounded_to_ten_seconds(self, delivery_manager):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)
        manager.client.authorize = False
        await manager.handle_frame(job_ready("job-1"))
        await clock.settle()

        await clock.advance(9.999)
        assert "job_nack" not in sent_types(manager.client)
        await clock.advance(0.001)
        await manager.drain_for_test()
        assert manager.client.sent[-1] == {
            "type": "job_nack",
            "protocol": 1,
            "job_id": "job-1",
            "delivery_attempt_id": "attempt-job-1",
            "reason": "authorization_timeout",
        }

    async def test_final_recheck_rejects_a_different_entity_for_same_device(
        self, delivery_manager,
    ):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)
        manager.client.authorize = False

        async def rebind_after_authorizing(frame):
            if frame["type"] != "job_delivery_start":
                return
            await manager.handle_frame({
                "type": "job_delivery_authorized",
                "protocol": 1,
                "job_id": frame["job_id"],
                "delivery_attempt_id": frame["delivery_attempt_id"],
            })
            manager.directory.remove("assist_satellite.kitchen")
            manager.directory.add("dev-k", "assist_satellite.kitchen_2")
            manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)

        manager.client.on_send = rebind_after_authorizing
        await manager.handle_frame(job_ready("job-1"))
        await manager.drain_for_test()

        assert manager.client.sent[-1]["type"] == "job_nack"
        assert manager.client.sent[-1]["reason"] == "preempted_before_playback"
        manager.hass.services.async_call.assert_not_awaited()

    @pytest.mark.parametrize(
        ("change", "reason"),
        [("expire", "expired"), ("remove", "satellite_not_found")],
    )
    async def test_non_revoke_idle_wait_abort_nacks_with_its_own_reason(
        self, delivery_manager, change, reason,
    ):
        manager, clock = delivery_manager
        offer = job_ready("job-1", expires_at=105.0)
        manager.directory.set_state("dev-k", "processing", changed_at=clock.now)
        await manager.handle_frame(offer)
        await clock.settle()

        if change == "expire":
            await clock.advance(5)
        else:
            manager.directory.remove("assist_satellite.kitchen")
            await clock.settle()
        await manager.drain_for_test()

        assert manager.client.sent[-1]["type"] == "job_nack"
        assert manager.client.sent[-1]["reason"] == reason
        assert "job_revoked" not in sent_types(manager.client)


class TestPerDeviceWorkers:
    async def test_same_device_is_strict_fifo_and_mutex_covers_announce(
        self, delivery_manager,
    ):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        calls: list[str] = []

        async def announce(_domain, _service, data, *, blocking):
            assert blocking is True
            calls.append(data["message"])
            if data["message"] == "first":
                first_started.set()
                await release_first.wait()

        manager.hass.services.async_call = AsyncMock(side_effect=announce)
        await manager.handle_frame(job_ready("job-1", sequence=1, spoken_text="first"))
        await manager.handle_frame(job_ready("job-2", sequence=2, spoken_text="second"))
        await asyncio.wait_for(first_started.wait(), timeout=1)

        assert calls == ["first"]
        assert not any(
            frame["type"] == "job_claimed" and frame["job_id"] == "job-2"
            for frame in manager.client.sent
        )

        release_first.set()
        await manager.drain_for_test()
        assert calls == ["first", "second"]

    async def test_different_devices_continue_independently(self, delivery_manager):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)
        manager.directory.set_state("dev-o", "idle", changed_at=clock.now - 10)
        kitchen_started = asyncio.Event()
        release_kitchen = asyncio.Event()

        async def announce(_domain, _service, data, *, blocking):
            assert blocking is True
            if data["entity_id"] == "assist_satellite.kitchen":
                kitchen_started.set()
                await release_kitchen.wait()

        manager.hass.services.async_call = AsyncMock(side_effect=announce)
        await manager.handle_frame(job_ready("job-k", device="dev-k"))
        await manager.handle_frame(job_ready("job-o", device="dev-o"))
        await asyncio.wait_for(kitchen_started.wait(), timeout=1)
        await clock.settle()

        assert any(
            frame["type"] == "job_delivered" and frame["job_id"] == "job-o"
            for frame in manager.client.sent
        )
        release_kitchen.set()
        await manager.drain_for_test()

    async def test_ordinary_media_playback_is_deliberately_ignored(self, delivery_manager):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)
        manager.hass.states.get.return_value = MagicMock(state="playing")

        await manager.handle_frame(job_ready("job-1"))
        await manager.drain_for_test()

        manager.hass.services.async_call.assert_awaited_once()
        manager.hass.states.get.assert_not_called()

    async def test_delivered_reoffer_is_claimed_and_acked_without_replay(
        self, delivery_manager,
    ):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)
        await manager.handle_frame(job_ready("job-1", attempt_id="attempt-1"))
        await manager.drain_for_test()

        await manager.handle_frame(job_ready("job-1", attempt_id="attempt-2"))
        await manager.drain_for_test()

        manager.hass.services.async_call.assert_awaited_once()
        assert [
            frame["type"] for frame in manager.client.sent
            if frame["delivery_attempt_id"] == "attempt-2"
        ] == ["job_claimed", "job_delivered"]

    async def test_successful_audio_is_cached_before_delivered_write_failure(
        self, delivery_manager,
    ):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)

        async def fail_first_delivered(frame):
            if (
                frame["type"] == "job_delivered"
                and frame["delivery_attempt_id"] == "attempt-1"
            ):
                raise ConnectionError("PRIVATE_CONNECTION_CANARY")

        manager.client.on_send = fail_first_delivered
        await manager.handle_frame(job_ready("job-1", attempt_id="attempt-1"))
        await manager.drain_for_test()
        manager.client.on_send = None

        await manager.handle_frame(job_ready("job-1", attempt_id="attempt-2"))
        await manager.drain_for_test()

        manager.hass.services.async_call.assert_awaited_once()
        assert [
            frame["type"] for frame in manager.client.sent
            if frame["delivery_attempt_id"] == "attempt-2"
        ] == ["job_claimed", "job_delivered"]

    async def test_delivered_lru_is_bounded_to_exactly_256_ids(self, delivery_manager):
        manager, _ = delivery_manager
        for index in range(257):
            manager.remember_delivered_for_test(f"job-{index}")

        assert len(manager.delivered_ids_for_test) == 256
        assert "job-0" not in manager.delivered_ids_for_test
        assert "job-256" in manager.delivered_ids_for_test

    async def test_idle_worker_atomically_retires_all_per_device_structures(
        self, delivery_manager,
    ):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)

        await manager.handle_frame(job_ready("job-1"))
        await manager.drain_for_test()

        assert manager.worker_count_for_test == 0
        assert manager.queue_count_for_test == 0
        assert manager.mutex_count_for_test == 0
        assert manager.attempt_count_for_test == 0

    async def test_offer_interleaved_with_worker_completion_is_not_lost(
        self, delivery_manager,
    ):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)
        queued_followup = False

        async def queue_followup(frame):
            nonlocal queued_followup
            if frame["type"] == "job_delivered" and not queued_followup:
                queued_followup = True
                await manager.handle_frame(job_ready("job-2", sequence=2))

        manager.client.on_send = queue_followup
        await manager.handle_frame(job_ready("job-1"))
        await manager.drain_for_test()

        delivered_jobs = {
            frame["job_id"] for frame in manager.client.sent
            if frame["type"] == "job_delivered"
        }
        assert delivered_jobs == {"job-1", "job-2"}
        assert manager.worker_count_for_test == 0
        assert manager.queue_count_for_test == 0
        assert manager.mutex_count_for_test == 0

    async def test_close_cancels_and_awaits_manager_workers(self, delivery_manager):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "processing", changed_at=clock.now)
        await manager.handle_frame(job_ready("job-1"))
        await clock.settle()

        await manager.close()

        assert manager.worker_count_for_test == 0
        manager.hass.services.async_call.assert_not_awaited()

    async def test_close_cancels_and_awaits_mapping_nack_send_task(self, delivery_manager):
        manager, _ = delivery_manager
        send_started = asyncio.Event()
        send_cancelled = asyncio.Event()

        async def block_send(_frame):
            send_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                send_cancelled.set()
                raise

        manager.client.on_send = block_send
        await manager.handle_frame(job_ready("job-1", device="missing"))
        await asyncio.wait_for(send_started.wait(), timeout=1)

        await manager.close()

        assert send_cancelled.is_set()
        assert manager.owned_task_count_for_test == 0

    async def test_close_clears_all_payload_bearing_and_attempt_state(
        self, delivery_manager,
    ):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "processing", changed_at=clock.now)
        await manager.handle_frame(job_ready(
            "job-secret",
            spoken_text="SECRET_CLOSE_PAYLOAD_CANARY",
        ))
        await clock.settle()
        manager.remember_delivered_for_test("job-previous")

        await manager.close()

        assert "SECRET_CLOSE_PAYLOAD_CANARY" not in repr(vars(manager))
        assert manager.worker_count_for_test == 0
        assert manager.owned_task_count_for_test == 0
        assert manager.queue_count_for_test == 0
        assert manager.mutex_count_for_test == 0
        assert manager.attempt_count_for_test == 0
        assert manager.delivered_ids_for_test == frozenset()


class TestProtocolAndPrivacy:
    @pytest.mark.parametrize(
        "changes",
        [
            {"protocol": True},
            {"protocol": 1.0},
            {"route_id": "entry-other"},
            {"expires_at": 100.0},
            {"ready_at": float("nan")},
            {"ready_at": float("inf")},
            {"expires_at": float("nan")},
            {"expires_at": float("inf")},
            {"ready_at": 1000.0, "expires_at": 1000.0},
            {"ready_at": 1001.0, "expires_at": 1000.0},
            {"delivery_sequence": 0},
            {"spoken_text": None},
        ],
    )
    async def test_invalid_ready_frames_do_not_mutate_or_send(self, delivery_manager, changes):
        manager, clock = delivery_manager
        manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)

        await manager.handle_frame(job_ready("job-1", **changes))
        await clock.settle()

        assert manager.client.sent == []
        manager.hass.services.async_call.assert_not_awaited()

    def test_runtime_idle_stability_rejects_bool(self):
        with pytest.raises(ValueError, match="idle stability"):
            BackgroundDeliveryManager(
                MagicMock(),
                FakeClient(),
                route_id="entry-1",
                directory=SatelliteDirectory(),
                idle_stability_ms=True,
            )

    @pytest.mark.parametrize(
        ("device", "reason"),
        [("missing", "satellite_not_found"), ("dev-k", "satellite_ambiguous")],
    )
    async def test_resolution_failure_nacks_with_static_reason(
        self, delivery_manager, device, reason,
    ):
        manager, clock = delivery_manager
        if reason == "satellite_ambiguous":
            manager.directory.add("dev-k", "assist_satellite.kitchen_2")

        await manager.handle_frame(job_ready("job-1", device=device))
        await clock.settle()

        assert manager.client.sent[-1]["type"] == "job_nack"
        assert manager.client.sent[-1]["reason"] == reason

    async def test_logs_never_include_payload_or_exception_canaries(
        self, delivery_manager, caplog,
    ):
        manager, clock = delivery_manager
        result_canary = "SECRET_SPOKEN_RESULT_CANARY"
        exception_canary = "SECRET_EXCEPTION_CANARY"
        manager.directory.set_state("dev-k", "idle", changed_at=clock.now - 10)
        manager.hass.services.async_call = AsyncMock(
            side_effect=RuntimeError(exception_canary),
        )

        with caplog.at_level(logging.DEBUG, logger="custom_components.casa.delivery"):
            await manager.handle_frame(job_ready("job-1", spoken_text=result_canary))
            await manager.drain_for_test()

        assert result_canary not in caplog.text
        assert exception_canary not in caplog.text
        assert all(record.exc_info is None for record in caplog.records)
