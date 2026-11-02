"""Lease renewal for one owned Request (Requirements 2.4, 2.7, 7.7).

A ``LeaseKeeper`` renews its Lease every ``lease_duration / 4`` (shorter than the
one-third bound of Req 2.4). The Lease is treated as lost when:

* the State_Store rejects a renewal with ``LEASE_LOST`` (another Manager holds it
  or it expired), or
* renewals keep failing because the State_Store is unreachable and the Lease
  expiry time has passed (Req 2.7).

On loss the keeper sets :attr:`LeaseKeeper.lost`, cancels the attached Request
task (so no further dispatches or state transitions happen for the Request), and
calls the optional ``on_lost`` callback.

``stop_renewing`` supports Req 7.7: the keeper stops without releasing the Lease,
so the Request becomes claimable by another Manager once the Lease expires.

This module depends on the State_Store only through the small
:class:`LeaseStore` protocol and never imports a Redis client (import-linter
contract). Any exception raised by a renewal call is treated as "State_Store
unreachable"; normal script outcomes arrive as :class:`StoreStatus` values.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from enum import StrEnum
from typing import Protocol

from das.clock import Clock, SystemClock
from das.models import Lease
from das.observability import get_logger
from das.observability.metrics import LeaseLossReason
from das.state import RenewResult, StoreStatus, WriteResult

log = get_logger(__name__)

RENEWALS_PER_DURATION = 4


class LeaseStore(Protocol):
    """The State_Store operations a ``LeaseKeeper`` needs."""

    async def renew_lease(self, lease: Lease, duration: float) -> RenewResult: ...

    async def release_lease(self, lease: Lease) -> WriteResult: ...


class LeaseEnd(StrEnum):
    """Why a ``LeaseKeeper`` stopped renewing."""

    LOST_REJECTED = "lost_rejected"  # renewal answered LEASE_LOST
    LOST_UNREACHABLE = "lost_unreachable"  # State_Store unreachable past expiry
    REQUEST_TERMINAL = "request_terminal"  # Request already Completed/Failed/Cancelled
    STOPPED = "stopped"  # stop_renewing() (Req 7.7)
    RELEASED = "released"  # release() after the Request finished


LOST_REASONS = frozenset({LeaseEnd.LOST_REJECTED, LeaseEnd.LOST_UNREACHABLE})


def renew_interval(lease_duration: float) -> float:
    """Renewal period: a quarter of the Lease duration (Req 2.4)."""
    return lease_duration / RENEWALS_PER_DURATION


class LeaseLossMetrics(Protocol):
    """The metric a ``LeaseKeeper`` records (``ManagerMetrics`` satisfies it)."""

    def lease_lost(self, reason: LeaseLossReason) -> None: ...


#: ``das_lease_losses_total{reason}`` for each loss kind (Req 3.4).
LOSS_METRIC_REASON: dict[LeaseEnd, LeaseLossReason] = {
    LeaseEnd.LOST_REJECTED: "held_by_other",
    LeaseEnd.LOST_UNREACHABLE: "store_unreachable",
}


class HeldLeases:
    """Leases this Manager holds and that haven't expired, for ``das_leases_held`` (Req 3.5).

    A ``LeaseKeeper`` joins on construction (the acquire succeeded) and leaves when
    its Lease is released or lost. A keeper that stopped renewing (Req 7.7) still
    counts until its local deadline passes. :meth:`count` is read at scrape time
    (``set_function``), so the gauge is always current.
    """

    def __init__(self) -> None:
        self._keepers: set[LeaseKeeper] = set()

    def add(self, keeper: LeaseKeeper) -> None:
        self._keepers.add(keeper)

    def discard(self, keeper: LeaseKeeper) -> None:
        self._keepers.discard(keeper)

    def count(self) -> int:
        """Number of held, unexpired Leases; expired keepers are dropped."""
        expired = [k for k in tuple(self._keepers) if not k.held]
        for keeper in expired:
            self._keepers.discard(keeper)
        return len(self._keepers)


class LeaseKeeper:
    """Keeps one Lease alive while its Request task runs."""

    def __init__(
        self,
        store: LeaseStore,
        lease: Lease,
        lease_duration: float,
        *,
        clock: Clock | None = None,
        task: asyncio.Task[object] | None = None,
        on_lost: Callable[[Lease, LeaseEnd], object] | None = None,
        acquired_at: float | None = None,
        metrics: LeaseLossMetrics | None = None,
        held: HeldLeases | None = None,
    ) -> None:
        """``acquired_at`` is the ``clock.monotonic()`` reading taken just before the
        acquire call was sent; it defaults to now. ``metrics`` records
        ``das_lease_losses_total`` and ``held`` tracks ``das_leases_held`` (Req 3.4, 3.5)."""
        if lease_duration <= 0:
            raise ValueError("lease_duration must be positive")
        self._store = store
        self._lease = lease
        self._duration = float(lease_duration)
        self._interval = renew_interval(self._duration)
        self._clock = clock or SystemClock()
        self._task = task
        self._on_lost = on_lost
        # Local (monotonic) deadline by which the Lease has surely expired. It is
        # measured from *before* the acquiring/renewing call was sent, so it never
        # lies later than the Redis-side expiry.
        start = self._clock.monotonic() if acquired_at is None else acquired_at
        self._deadline = start + self._duration
        self._stop = asyncio.Event()
        self.lost = asyncio.Event()
        self.ended: LeaseEnd | None = None
        self._runner: asyncio.Task[LeaseEnd] | None = None
        self._metrics = metrics
        self._held = held
        self._released = False
        if held is not None:
            held.add(self)

    # ------------------------------------------------------------------ accessors

    @property
    def lease(self) -> Lease:
        """The Lease with the latest known expiry (owner and epoch never change)."""
        return self._lease

    @property
    def interval(self) -> float:
        return self._interval

    @property
    def is_lost(self) -> bool:
        return self.lost.is_set()

    @property
    def renewing(self) -> bool:
        return self.ended is None

    @property
    def held(self) -> bool:
        """True while the Lease is neither lost nor released and its local deadline
        hasn't passed (Req 3.5)."""
        if self.is_lost or self._released:
            return False
        return self._clock.monotonic() < self._deadline

    def attach(self, task: asyncio.Task[object]) -> None:
        """Set the Request task to cancel when the Lease is lost."""
        self._task = task
        if self.is_lost and not task.done():
            task.cancel()

    # ------------------------------------------------------------------ lifecycle

    def start(self) -> asyncio.Task[LeaseEnd]:
        """Run :meth:`run` in a background task (idempotent)."""
        if self._runner is None:
            self._runner = asyncio.create_task(
                self.run(), name=f"lease-keeper:{self._lease.request_id}"
            )
        return self._runner

    async def run(self) -> LeaseEnd:
        """Renew until the Lease is lost, renewal is stopped, or the Request ends."""
        while self.ended is None:
            now = self._clock.monotonic()
            # Wake no later than the local deadline so an unreachable store is
            # detected as soon as the Lease has expired.
            delay = min(self._interval, max(0.0, self._deadline - now))
            if await self._wait_stop(delay):
                break
            await self._renew_once()
        assert self.ended is not None
        return self.ended

    async def _wait_stop(self, delay: float) -> bool:
        """Sleep ``delay`` on the injected clock; True when stop was requested."""
        if self._stop.is_set():
            return True
        sleeper = asyncio.ensure_future(self._clock.sleep(delay))
        stopper = asyncio.ensure_future(self._stop.wait())
        try:
            await asyncio.wait({sleeper, stopper}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for fut in (sleeper, stopper):
                if not fut.done():
                    fut.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await fut
        return self._stop.is_set()

    async def _renew_once(self) -> None:
        sent_at = self._clock.monotonic()
        try:
            result = await self._store.renew_lease(self._lease, self._duration)
        except Exception as exc:  # State_Store unreachable (connection/timeout/OS errors)
            if self.ended is not None:
                return
            if self._clock.monotonic() >= self._deadline:
                log.warning(
                    "lease_lost_unreachable",
                    request_id=self._lease.request_id,
                    epoch=self._lease.epoch,
                    error=type(exc).__name__,
                )
                self._mark_lost(LeaseEnd.LOST_UNREACHABLE)
            else:
                log.warning(
                    "lease_renew_failed",
                    request_id=self._lease.request_id,
                    epoch=self._lease.epoch,
                    error=type(exc).__name__,
                )
            return
        if self.ended is not None:  # stopped while the call was in flight
            return
        if result.status is StoreStatus.OK:
            self._deadline = sent_at + self._duration
            if result.expires_at is not None:
                self._lease = self._lease.model_copy(update={"expires_at": result.expires_at})
        elif result.status is StoreStatus.REQUEST_TERMINAL:
            log.info("lease_request_terminal", request_id=self._lease.request_id)
            self._finish(LeaseEnd.REQUEST_TERMINAL)
        else:
            log.warning(
                "lease_lost_rejected",
                request_id=self._lease.request_id,
                epoch=self._lease.epoch,
                status=str(result.status),
            )
            self._mark_lost(LeaseEnd.LOST_REJECTED)

    def _finish(self, reason: LeaseEnd) -> None:
        if self.ended is None:
            self.ended = reason
        self._stop.set()

    def _mark_lost(self, reason: LeaseEnd) -> None:
        if self.ended is not None:
            return
        self._finish(reason)
        self.lost.set()
        if self._held is not None:
            self._held.discard(self)
        if self._metrics is not None:  # exactly once per Lease: ended guards re-entry (Req 3.4)
            try:
                self._metrics.lease_lost(LOSS_METRIC_REASON[reason])
            except Exception:
                log.exception("lease_metric_failed", request_id=self._lease.request_id)
        if self._task is not None and not self._task.done():
            self._task.cancel()
        if self._on_lost is not None:
            try:
                self._on_lost(self._lease, reason)
            except Exception:
                log.exception("lease_on_lost_failed", request_id=self._lease.request_id)

    def mark_rejected(self) -> None:
        """A Lease-guarded write answered ``LEASE_LOST``: the Lease is lost exactly like
        a rejected renewal (``held_by_other``, Req 3.4), but the Request task is not
        cancelled because the driver is already stopping. No-op once ended."""
        task, self._task = self._task, None
        try:
            self._mark_lost(LeaseEnd.LOST_REJECTED)
        finally:
            self._task = task

    def stop_renewing(self) -> None:
        """Stop renewing without releasing; the Lease then expires (Req 7.7)."""
        if self.ended is None:
            log.info("lease_stop_renewing", request_id=self._lease.request_id)
        self._finish(LeaseEnd.STOPPED)

    async def release(self) -> WriteResult | None:
        """Stop renewing and release the Lease (Request finished). Best effort."""
        was_lost = self.is_lost
        self._finish(LeaseEnd.RELEASED)
        self._released = True
        if self._held is not None:
            self._held.discard(self)
        await self.wait_closed()
        if was_lost:
            return None
        try:
            return await self._store.release_lease(self._lease)
        except Exception as exc:
            log.warning(
                "lease_release_failed",
                request_id=self._lease.request_id,
                error=type(exc).__name__,
            )
            return None

    async def wait_closed(self) -> None:
        """Wait for the background renewal task (if started) to finish."""
        if self._runner is not None and self._runner is not asyncio.current_task():
            with contextlib.suppress(asyncio.CancelledError):
                await self._runner


__all__ = [
    "LOSS_METRIC_REASON",
    "LOST_REASONS",
    "HeldLeases",
    "LeaseEnd",
    "LeaseKeeper",
    "LeaseLossMetrics",
    "LeaseStore",
    "renew_interval",
]
