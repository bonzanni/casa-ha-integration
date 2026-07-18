"""Background delivery of completed Casa voice jobs."""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections import OrderedDict, defaultdict
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any


_LOGGER = logging.getLogger(__name__)
_PROTOCOL = 1
_LEASE_RENEW_SECONDS = 5.0
_AUTHORIZATION_TIMEOUT_SECONDS = 10.0
_DELIVERED_LRU_SIZE = 256
_ASSIST_BUSY_STATES = frozenset({"listening", "processing", "responding"})


class _SystemClock:
    @property
    def now(self) -> float:
        return time.time()

    async def sleep(self, delay: float) -> None:
        await asyncio.sleep(delay)


@dataclass
class _Delivery:
    job_id: str
    attempt_id: str
    device_id: str
    entity_id: str
    spoken_text: str
    expires_at: float
    sequence: int
    revoked: asyncio.Event
    lease_lost: asyncio.Event
    phase: str = "ready"


@dataclass
class _PlaybackSlot:
    lock: asyncio.Lock
    users: int = 0


class SatelliteResolutionError(Exception):
    """Base class for fail-closed satellite resolution errors."""


class SatelliteNotFound(SatelliteResolutionError):
    """Raised when a device has no currently valid Assist satellite."""


class SatelliteAmbiguous(SatelliteResolutionError):
    """Raised when a device has multiple Assist satellites without an override."""


class SatelliteDirectory:
    """Current in-memory mapping from HA devices to Assist satellite entities."""

    def __init__(self, *, overrides: Mapping[str, str] | None = None) -> None:
        self._overrides = dict(overrides or {})
        self._device_entities: dict[str, set[str]] = defaultdict(set)
        self._entity_devices: dict[str, str] = {}
        self._entity_states: dict[str, str] = {}
        self._entity_idle_since: dict[str, float] = {}
        self._device_events: dict[str, asyncio.Event] = {}
        self._playback_slots: dict[str, _PlaybackSlot] = {}

    def _event(self, device_id: str) -> asyncio.Event:
        event = self._device_events.get(device_id)
        if event is None:
            event = asyncio.Event()
            self._device_events[device_id] = event
        return event

    def change_event(self, device_id: str) -> asyncio.Event:
        """Return the process-local event pulsed by mapping or state changes."""
        return self._event(device_id)

    def discard_change_event(
        self,
        device_id: str,
        event: asyncio.Event,
    ) -> None:
        """Discard one exact unused signal without removing a newer generation."""
        if self._device_events.get(device_id) is event:
            self._device_events.pop(device_id, None)

    def _notify(self, device_id: str | None, *, retire: bool = False) -> None:
        if device_id is None:
            return
        event = self._device_events.pop(device_id, None)
        if not retire:
            self._device_events[device_id] = asyncio.Event()
        if event is not None:
            event.set()

    def add(self, device_id: str, entity_id: str) -> None:
        """Add or rebind one current Assist satellite registry entity."""
        previous_device = self._entity_devices.get(entity_id)
        if previous_device is not None and previous_device != device_id:
            self._device_entities[previous_device].discard(entity_id)
            previous_device_retired = not self._device_entities[previous_device]
            if previous_device_retired:
                self._device_entities.pop(previous_device, None)
            self._notify(previous_device, retire=previous_device_retired)
        self._entity_devices[entity_id] = device_id
        self._device_entities[device_id].add(entity_id)
        self._notify(device_id)

    def remove(self, entity_id: str) -> None:
        """Remove an entity and every state datum associated with it."""
        device_id = self._entity_devices.pop(entity_id, None)
        device_retired = False
        if device_id is not None:
            self._device_entities[device_id].discard(entity_id)
            if not self._device_entities[device_id]:
                self._device_entities.pop(device_id, None)
                device_retired = True
        self._entity_states.pop(entity_id, None)
        self._entity_idle_since.pop(entity_id, None)
        self._notify(device_id, retire=device_retired)

    def resolve(self, device_id: str) -> str:
        """Resolve exactly one currently valid entity, applying overrides safely."""
        entities = self._device_entities.get(device_id, set())
        override = self._overrides.get(device_id)
        if override is not None:
            if (
                override.startswith("assist_satellite.")
                and override in entities
                and self._entity_devices.get(override) == device_id
            ):
                return override
            raise SatelliteNotFound
        if not entities:
            raise SatelliteNotFound
        if len(entities) != 1:
            raise SatelliteAmbiguous
        return next(iter(entities))

    def set_state(self, device_id: str, state: str, *, changed_at: float) -> None:
        """Record every Assist state and the start of an idle transition."""
        entity_id = self.resolve(device_id)
        self.set_entity_state(device_id, entity_id, state, changed_at=changed_at)

    def set_entity_state(
        self,
        device_id: str,
        entity_id: str,
        state: str,
        *,
        changed_at: float,
    ) -> None:
        """Record state for an exact current registry binding, even if ambiguous."""
        self.add(device_id, entity_id)
        previous = self._entity_states.get(entity_id)
        self._entity_states[entity_id] = state
        if state == "idle":
            if previous != "idle":
                self._entity_idle_since[entity_id] = float(changed_at)
        else:
            self._entity_idle_since.pop(entity_id, None)
        self._notify(device_id)

    def state(self, device_id: str) -> str | None:
        """Return the current state of the device's resolved satellite."""
        return self._entity_states.get(self.resolve(device_id))

    def idle_since(self, device_id: str) -> float | None:
        """Return when the resolved satellite most recently entered idle."""
        return self._entity_idle_since.get(self.resolve(device_id))

    @asynccontextmanager
    async def playback_slot(self, device_id: str) -> AsyncIterator[None]:
        """Serialize one device's playback across every agent manager."""
        slot = self._playback_slots.get(device_id)
        if slot is None:
            slot = _PlaybackSlot(lock=asyncio.Lock())
            self._playback_slots[device_id] = slot
        slot.users += 1
        try:
            async with slot.lock:
                yield
        finally:
            slot.users -= 1
            if (
                slot.users == 0
                and self._playback_slots.get(device_id) is slot
            ):
                self._playback_slots.pop(device_id, None)

    @property
    def playback_slot_count_for_test(self) -> int:
        """Expose retained shared playback slots for lifecycle assertions."""
        return len(self._playback_slots)


class BackgroundDeliveryManager:
    """Reconstructible HA-side workers for Casa's durable delivery offers."""

    def __init__(
        self,
        hass: Any,
        client: Any,
        *,
        route_id: str,
        directory: SatelliteDirectory,
        idle_stability_ms: int = 750,
        clock: Any | None = None,
    ) -> None:
        if type(idle_stability_ms) is not int or not 0 <= idle_stability_ms <= 5000:
            raise ValueError("idle stability must be between 0 and 5000 ms")
        self.hass = hass
        self.client = client
        self.route_id = route_id
        self.directory = directory
        self._idle_stability_s = idle_stability_ms / 1000
        self._clock = clock or _SystemClock()
        self._queues: dict[str, asyncio.Queue[_Delivery]] = {}
        self._workers: dict[str, asyncio.Task] = {}
        self._attempts: dict[tuple[str, str], _Delivery] = {}
        self._authorization: dict[tuple[str, str], asyncio.Future[bool]] = {}
        self._delivered: OrderedDict[str, None] = OrderedDict()
        self._owned_tasks: set[asyncio.Task] = set()
        self._closed = False

    async def handle_frame(self, frame: dict) -> None:
        """Validate and dispatch one background frame without doing long work."""
        if self._closed or not isinstance(frame, dict):
            return
        frame_type = frame.get("type")
        if frame_type == "job_ready":
            delivery = self._parse_ready(frame)
            if delivery is None:
                return
            key = (delivery.job_id, delivery.attempt_id)
            if key in self._attempts:
                return
            if delivery.job_id not in self._delivered:
                try:
                    delivery.entity_id = self.directory.resolve(delivery.device_id)
                except SatelliteAmbiguous:
                    self._spawn_send(self._frame(delivery, "job_nack", reason="satellite_ambiguous"))
                    return
                except SatelliteNotFound:
                    self._spawn_send(self._frame(delivery, "job_nack", reason="satellite_not_found"))
                    return
            self._attempts[key] = delivery
            queue = self._queues.setdefault(delivery.device_id, asyncio.Queue())
            queue.put_nowait(delivery)
            worker = self._workers.get(delivery.device_id)
            if worker is None or worker.done():
                self._workers[delivery.device_id] = asyncio.create_task(
                    self._worker(delivery.device_id),
                )
            return

        key = self._frame_key(frame)
        if key is None:
            return
        if frame_type == "job_delivery_authorized":
            future = self._authorization.get(key)
            if future is not None and not future.done():
                future.set_result(True)
        elif frame_type == "job_revoke":
            delivery = self._attempts.get(key)
            if delivery is not None and delivery.phase != "playing":
                delivery.revoked.set()
                future = self._authorization.get(key)
                if future is not None and not future.done():
                    future.set_result(False)

    def _parse_ready(self, frame: dict) -> _Delivery | None:
        if type(frame.get("protocol")) is not int or frame["protocol"] != _PROTOCOL:
            return None
        if frame.get("route_id") != self.route_id:
            return None
        job_id = frame.get("job_id")
        attempt_id = frame.get("delivery_attempt_id")
        device_id = frame.get("origin_device_id")
        spoken_text = frame.get("spoken_text")
        ready_at = frame.get("ready_at")
        expires_at = frame.get("expires_at")
        sequence = frame.get("delivery_sequence")
        if not all(
            isinstance(value, str) and bool(value.strip()) and len(value) <= 512
            for value in (job_id, attempt_id, device_id)
        ):
            return None
        if not isinstance(spoken_text, str) or not spoken_text:
            return None
        if (
            isinstance(ready_at, bool)
            or not isinstance(ready_at, (int, float))
            or isinstance(expires_at, bool)
            or not isinstance(expires_at, (int, float))
            or type(sequence) is not int
            or sequence < 1
        ):
            return None
        try:
            ready_at_float = float(ready_at)
            expires_at_float = float(expires_at)
        except (OverflowError, ValueError):
            return None
        if (
            not math.isfinite(ready_at_float)
            or not math.isfinite(expires_at_float)
            or ready_at_float >= expires_at_float
            or expires_at_float <= self._clock.now
        ):
            return None
        return _Delivery(
            job_id=job_id,
            attempt_id=attempt_id,
            device_id=device_id,
            entity_id="",
            spoken_text=spoken_text,
            expires_at=expires_at_float,
            sequence=sequence,
            revoked=asyncio.Event(),
            lease_lost=asyncio.Event(),
        )

    @staticmethod
    def _frame_key(frame: dict) -> tuple[str, str] | None:
        if type(frame.get("protocol")) is not int or frame["protocol"] != _PROTOCOL:
            return None
        job_id = frame.get("job_id")
        attempt_id = frame.get("delivery_attempt_id")
        if not isinstance(job_id, str) or not isinstance(attempt_id, str):
            return None
        return job_id, attempt_id

    async def _worker(self, device_id: str) -> None:
        queue = self._queues[device_id]
        while True:
            delivery = await queue.get()
            try:
                await self._deliver(delivery)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - payload and exception details stay out of logs
                _LOGGER.warning("Casa background delivery stopped reason=worker_error")
            finally:
                self._attempts.pop((delivery.job_id, delivery.attempt_id), None)
                queue.task_done()
            if queue.empty():
                current = asyncio.current_task()
                if (
                    self._queues.get(device_id) is queue
                    and self._workers.get(device_id) is current
                ):
                    self._queues.pop(device_id, None)
                    self._workers.pop(device_id, None)
                    return

    async def _deliver(self, delivery: _Delivery) -> None:
        if delivery.revoked.is_set():
            await self._ack_revoked(delivery)
            return
        if delivery.job_id in self._delivered:
            await self._send(self._frame(delivery, "job_claimed"))
            await self._send(self._frame(delivery, "job_delivered"))
            return

        delivery.phase = "claimed"
        await self._send(self._frame(delivery, "job_claimed"))
        renewer = asyncio.create_task(self._renew_claim(delivery))
        try:
            async with self.directory.playback_slot(delivery.device_id):
                await self._deliver_in_playback_slot(delivery)
        finally:
            renewer.cancel()
            await asyncio.gather(renewer, return_exceptions=True)

    async def _deliver_in_playback_slot(self, delivery: _Delivery) -> None:
        """Wait, authorize, and announce while holding the shared device slot."""
        if not await self._wait_for_stable_idle(delivery):
            await self._abort_idle_wait(delivery)
            return
        if not self._has_current_stable_idle_claim(delivery):
            await self._abort_preplay(delivery)
            return
        key = (delivery.job_id, delivery.attempt_id)
        authorization = asyncio.get_running_loop().create_future()
        self._authorization[key] = authorization
        delivery.phase = "authorizing"
        try:
            await self._send(self._frame(delivery, "job_delivery_start"))
            authorized = await self._wait_authorization(delivery, authorization)
        finally:
            self._authorization.pop(key, None)
        if not authorized:
            if delivery.lease_lost.is_set():
                return
            if delivery.revoked.is_set():
                await self._ack_revoked(delivery)
            else:
                await self._nack(delivery, "authorization_timeout")
            return
        delivery.phase = "authorized"
        if not self._has_current_stable_idle_claim(delivery):
            await self._abort_preplay(delivery)
            return
        delivery.phase = "playing"
        await self._send(self._frame(delivery, "job_playback_started"))
        if not self._has_current_stable_idle_claim(
            delivery,
            require_preplay_state=False,
        ):
            await self._abort_preplay(delivery)
            return
        announce_started = self._clock.now
        await self.hass.services.async_call(
            "assist_satellite",
            "announce",
            {
                "entity_id": delivery.entity_id,
                "message": delivery.spoken_text,
            },
            blocking=True,
        )
        _LOGGER.debug(
            "Casa background announce announce_ms=%d state=complete",
            max(0, int((self._clock.now - announce_started) * 1000)),
        )
        self._remember_delivered(delivery.job_id)
        await self._send(self._frame(delivery, "job_delivered"))

    async def _renew_claim(self, delivery: _Delivery) -> None:
        renew_count = 0
        while True:
            await self._clock.sleep(_LEASE_RENEW_SECONDS)
            try:
                await self._send(self._frame(delivery, "job_claim_renew"))
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - generation loss has no safe payload to log
                delivery.lease_lost.set()
                return
            renew_count += 1
            _LOGGER.debug("Casa background lease renew_count=%d", renew_count)

    async def _wait_for_stable_idle(self, delivery: _Delivery) -> bool:
        reset_count = 0
        started = self._clock.now
        while (
            not self._expired(delivery)
            and not delivery.revoked.is_set()
            and not delivery.lease_lost.is_set()
        ):
            event = self.directory.change_event(delivery.device_id)
            try:
                if self.directory.resolve(delivery.device_id) != delivery.entity_id:
                    return False
                state = self.directory.state(delivery.device_id)
                idle_since = self.directory.idle_since(delivery.device_id)
            except SatelliteResolutionError:
                self.directory.discard_change_event(delivery.device_id, event)
                return False
            if event.is_set():
                continue
            if state == "idle" and idle_since is not None:
                remaining = self._idle_stability_s - (self._clock.now - idle_since)
                if remaining <= 0:
                    _LOGGER.debug(
                        "Casa background idle idle_wait_ms=%d stability_reset_count=%d state=idle",
                        max(0, int((self._clock.now - started) * 1000)),
                        reset_count,
                    )
                    return True
                changed = await self._wait_event_or_timeout(
                    event,
                    delivery,
                    min(remaining, delivery.expires_at - self._clock.now),
                )
                if changed:
                    try:
                        if (
                            self.directory.resolve(delivery.device_id)
                            == delivery.entity_id
                            and self.directory.state(delivery.device_id)
                            in _ASSIST_BUSY_STATES
                        ):
                            reset_count += 1
                    except SatelliteResolutionError:
                        pass
            else:
                await self._wait_event_or_timeout(
                    event,
                    delivery,
                    delivery.expires_at - self._clock.now,
                )
        return False

    async def _abort_idle_wait(self, delivery: _Delivery) -> None:
        if delivery.lease_lost.is_set():
            return
        if delivery.revoked.is_set():
            await self._ack_revoked(delivery)
            return
        if self._expired(delivery):
            await self._nack(delivery, "expired")
            return
        try:
            self.directory.resolve(delivery.device_id)
        except SatelliteAmbiguous:
            await self._nack(delivery, "satellite_ambiguous")
            return
        except SatelliteNotFound:
            await self._nack(delivery, "satellite_not_found")
            return
        await self._nack(delivery, "preempted_before_playback")

    async def _abort_preplay(self, delivery: _Delivery) -> None:
        if delivery.lease_lost.is_set():
            return
        if delivery.revoked.is_set():
            await self._ack_revoked(delivery)
            return
        await self._nack(delivery, "preempted_before_playback")

    async def _wait_authorization(
        self,
        delivery: _Delivery,
        authorization: asyncio.Future[bool],
    ) -> bool:
        timeout = asyncio.create_task(self._clock.sleep(_AUTHORIZATION_TIMEOUT_SECONDS))
        revoked = asyncio.create_task(delivery.revoked.wait())
        lease_lost = asyncio.create_task(delivery.lease_lost.wait())
        try:
            done, _ = await asyncio.wait(
                {authorization, timeout, revoked, lease_lost},
                return_when=asyncio.FIRST_COMPLETED,
            )
            return authorization in done and authorization.result()
        finally:
            for task in (timeout, revoked, lease_lost):
                task.cancel()
            await asyncio.gather(
                timeout, revoked, lease_lost, return_exceptions=True,
            )

    async def _wait_event_or_timeout(
        self,
        changed: asyncio.Event,
        delivery: _Delivery,
        timeout_s: float,
    ) -> bool:
        change_task = asyncio.create_task(changed.wait())
        revoke_task = asyncio.create_task(delivery.revoked.wait())
        lease_lost_task = asyncio.create_task(delivery.lease_lost.wait())
        timeout_task = asyncio.create_task(self._clock.sleep(max(0, timeout_s)))
        try:
            done, _ = await asyncio.wait(
                {change_task, revoke_task, lease_lost_task, timeout_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            return change_task in done
        finally:
            for task in (
                change_task, revoke_task, lease_lost_task, timeout_task,
            ):
                task.cancel()
            await asyncio.gather(
                change_task,
                revoke_task,
                lease_lost_task,
                timeout_task,
                return_exceptions=True,
            )

    def _has_current_stable_idle_claim(
        self,
        delivery: _Delivery,
        *,
        require_preplay_state: bool = True,
    ) -> bool:
        """Verify this slot still holds the exact, stably idle satellite."""
        event = self.directory.change_event(delivery.device_id)
        try:
            if require_preplay_state and (
                self._expired(delivery)
                or delivery.revoked.is_set()
                or delivery.lease_lost.is_set()
            ):
                return False
            if event.is_set() or (
                self.directory.resolve(delivery.device_id) != delivery.entity_id
            ):
                return False
            state = self.directory.state(delivery.device_id)
            idle_since = self.directory.idle_since(delivery.device_id)
            return bool(
                not event.is_set()
                and state == "idle"
                and idle_since is not None
                and self._clock.now - idle_since >= self._idle_stability_s
            )
        except SatelliteResolutionError:
            return False
        finally:
            self.directory.discard_change_event(delivery.device_id, event)

    def _expired(self, delivery: _Delivery) -> bool:
        return self._clock.now >= delivery.expires_at

    @staticmethod
    def _frame(delivery: _Delivery, frame_type: str, **fields: str) -> dict:
        return {
            "type": frame_type,
            "protocol": _PROTOCOL,
            "job_id": delivery.job_id,
            "delivery_attempt_id": delivery.attempt_id,
            **fields,
        }

    async def _nack(self, delivery: _Delivery, reason: str) -> None:
        await self._send(self._frame(delivery, "job_nack", reason=reason))

    async def _ack_revoked(self, delivery: _Delivery) -> None:
        await self._send(self._frame(delivery, "job_revoked"))

    async def _send(self, frame: dict) -> None:
        await self.client.send_job_frame(frame)

    def _spawn_send(self, frame: dict) -> None:
        task = asyncio.create_task(self._send(frame))
        self._owned_tasks.add(task)
        task.add_done_callback(self._owned_send_done)

    def _owned_send_done(self, task: asyncio.Task) -> None:
        self._owned_tasks.discard(task)
        self._consume_send_result(task)

    @staticmethod
    def _consume_send_result(task: asyncio.Task) -> None:
        if task.cancelled():
            return
        try:
            task.result()
        except Exception:  # noqa: BLE001 - no exception details in logs
            _LOGGER.warning("Casa background frame send failed reason=connection")

    def _remember_delivered(self, job_id: str) -> None:
        self._delivered[job_id] = None
        self._delivered.move_to_end(job_id)
        while len(self._delivered) > _DELIVERED_LRU_SIZE:
            self._delivered.popitem(last=False)

    def remember_delivered_for_test(self, job_id: str) -> None:
        """Populate the reconstructible delivered cache in bounded-LRU tests."""
        self._remember_delivered(job_id)

    @property
    def delivered_ids_for_test(self) -> frozenset[str]:
        """Expose only cache membership, never result or spoken payload data."""
        return frozenset(self._delivered)

    @property
    def worker_count_for_test(self) -> int:
        """Expose manager-owned worker count for lifecycle assertions."""
        return len(self._workers)

    @property
    def queue_count_for_test(self) -> int:
        """Expose manager-owned device queue count for lifecycle assertions."""
        return len(self._queues)

    @property
    def mutex_count_for_test(self) -> int:
        """Expose directory-owned device playback slots for compatibility tests."""
        return self.directory.playback_slot_count_for_test

    @property
    def attempt_count_for_test(self) -> int:
        """Expose manager-owned active attempt count for lifecycle assertions."""
        return len(self._attempts)

    @property
    def owned_task_count_for_test(self) -> int:
        """Expose detached manager task count for lifecycle assertions."""
        return len(self._owned_tasks)

    async def drain_for_test(self) -> None:
        """Wait until every currently queued device delivery is complete."""
        await asyncio.gather(*(queue.join() for queue in self._queues.values()))

    async def close(self) -> None:
        """Cancel and await all manager-owned workers before API shutdown."""
        if self._closed:
            return
        self._closed = True
        for future in self._authorization.values():
            if not future.done():
                future.cancel()
        tasks = [
            task
            for task in (*self._workers.values(), *self._owned_tasks)
            if not task.done()
        ]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._authorization.clear()
        self._attempts.clear()
        self._queues.clear()
        self._workers.clear()
        self._delivered.clear()
        self._owned_tasks.clear()


__all__ = [
    "BackgroundDeliveryManager",
    "SatelliteAmbiguous",
    "SatelliteDirectory",
    "SatelliteNotFound",
    "SatelliteResolutionError",
]
