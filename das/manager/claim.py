"""Claiming of Pending and orphaned Requests (Requirements 2.1, 2.3, 2.5).

``ClaimLoop`` runs every second. Each tick it asks the State_Store for claimable
Requests (``das:requests:pending`` plus Requests in ``das:leases:expiry`` whose
expiry has passed) and tries ``acquire_lease`` on each one it does not already
own. ``acquire_lease`` is a single-winner Lua script, so concurrent Managers never
both succeed (Req 2.2). A 1 s period meets the 5 s bound of Req 2.1 and the 10 s
takeover bound of Req 2.5.

Every won Lease is handed to ``on_claimed(lease)``, where the RequestDriver is
started. If ``on_claimed`` returns an ``asyncio.Future``/``Task``, the Request
counts as owned until it finishes; otherwise call :meth:`ClaimLoop.done` when the
Request is no longer owned. ``max_concurrent`` caps how many Requests this
Manager owns at once.

The loop depends on the State_Store only through :class:`ClaimStore` and never
imports a Redis client (import-linter contract).
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from das.clock import Clock, SystemClock
from das.models import Lease
from das.observability import get_logger
from das.observability.metrics import LeaseKind
from das.state import AcquireResult, StoreStatus, WriteResult

log = get_logger(__name__)

CLAIM_PERIOD = 1.0
DEFAULT_SCAN_LIMIT = 100


class ClaimStore(Protocol):
    """The State_Store operations a ``ClaimLoop`` needs."""

    async def claimable_request_ids(self, limit: int = 100) -> list[str]: ...

    async def acquire_lease(self, rid: str, manager_id: str, duration: float) -> AcquireResult: ...

    async def release_lease(self, lease: Lease) -> WriteResult: ...


class LeaseAcquireMetrics(Protocol):
    """The metric a ``ClaimLoop`` records (``ManagerMetrics`` satisfies it)."""

    def lease_acquired(self, kind: LeaseKind) -> None: ...


OnClaimed = Callable[[Lease], "Awaitable[object] | object"]


class ClaimLoop:
    """Periodically claims Requests that have no valid Lease."""

    def __init__(
        self,
        store: ClaimStore,
        manager_id: str,
        lease_duration: float,
        on_claimed: OnClaimed,
        *,
        clock: Clock | None = None,
        period: float = CLAIM_PERIOD,
        max_concurrent: int | None = None,
        scan_limit: int = DEFAULT_SCAN_LIMIT,
        metrics: LeaseAcquireMetrics | None = None,
    ) -> None:
        if not manager_id:
            raise ValueError("manager_id must be non-empty")
        if lease_duration <= 0:
            raise ValueError("lease_duration must be positive")
        if period <= 0:
            raise ValueError("period must be positive")
        if max_concurrent is not None and max_concurrent < 1:
            raise ValueError("max_concurrent must be at least 1")
        self._store = store
        self._manager_id = manager_id
        self._duration = float(lease_duration)
        self._on_claimed = on_claimed
        self._clock = clock or SystemClock()
        self._period = float(period)
        self._max = max_concurrent
        self._scan_limit = scan_limit
        self._owned: set[str] = set()
        self._stop = asyncio.Event()
        self._paused = False
        self._metrics = metrics

    # ------------------------------------------------------------------ ownership

    @property
    def owned(self) -> frozenset[str]:
        """Request_IDs this Manager currently treats as owned."""
        return frozenset(self._owned)

    def capacity(self) -> int | None:
        """Free slots, or None when unlimited."""
        return None if self._max is None else max(0, self._max - len(self._owned))

    def done(self, rid: str) -> None:
        """Forget ``rid`` (its Request task finished or its Lease was lost)."""
        self._owned.discard(rid)

    # ------------------------------------------------------------------ loop

    def stop(self) -> None:
        self._stop.set()

    # ------------------------------------------------------------------ drain

    @property
    def paused(self) -> bool:
        return self._paused

    def pause(self) -> None:
        """Stop acquiring new Leases from the next acquire on (drain, Req 17.4).

        The loop keeps running (it stays a live Critical_Loop) but each tick is a
        no-op; Requests already owned are untouched.
        """
        self._paused = True

    def resume(self) -> None:
        """Acquire Leases again from the next tick (undrain, Req 17.8)."""
        self._paused = False

    async def run(self) -> None:
        """Tick every ``period`` seconds until :meth:`stop`."""
        while not self._stop.is_set():
            try:
                await self.tick()
            except Exception as exc:  # State_Store unreachable; retry next tick
                log.warning("claim_scan_failed", error=type(exc).__name__)
            if self._stop.is_set():
                break
            await self._clock.sleep(self._period)

    async def tick(self) -> list[Lease]:
        """One scan: try to acquire every claimable Request; return the Leases won."""
        free = self.capacity()
        if free == 0 or self._paused:
            return []
        ids = await self._store.claimable_request_ids(self._scan_limit)
        won: list[Lease] = []
        for rid in ids:
            if self._stop.is_set() or self._paused:
                break
            if rid in self._owned:
                continue
            if free is not None and len(won) >= free:
                break
            lease = await self._try_acquire(rid)
            if lease is not None and await self._hand_off(lease):
                won.append(lease)
        return won

    async def _try_acquire(self, rid: str) -> Lease | None:
        try:
            result = await self._store.acquire_lease(rid, self._manager_id, self._duration)
        except Exception as exc:
            log.warning("claim_acquire_failed", request_id=rid, error=type(exc).__name__)
            return None
        if result.status is StoreStatus.OK and result.lease is not None:
            log.info(
                "lease_acquired", request_id=rid, epoch=result.lease.epoch, kind=result.kind
            )
            self._record_acquired(result.kind)
            return result.lease
        # HELD: another Manager won; NOT_FOUND / REQUEST_TERMINAL: nothing to claim.
        return None

    def _record_acquired(self, kind: str | None) -> None:
        """``das_lease_acquisitions_total{kind}`` with the script's ``kind`` (Req 3.3)."""
        if self._metrics is None:
            return
        value: LeaseKind = "takeover" if kind == "takeover" else "initial"
        try:
            self._metrics.lease_acquired(value)
        except Exception:
            log.exception("lease_metric_failed")

    async def _hand_off(self, lease: Lease) -> bool:
        rid = lease.request_id
        self._owned.add(rid)
        try:
            outcome = self._on_claimed(lease)
            if inspect.isawaitable(outcome) and not isinstance(outcome, asyncio.Future):
                outcome = await outcome
        except Exception:
            log.exception("claim_handoff_failed", request_id=rid)
            self._owned.discard(rid)
            await self._release(lease)
            return False
        if isinstance(outcome, asyncio.Future):

            def _done(_f: asyncio.Future[Any], rid: str = rid) -> None:
                self.done(rid)

            outcome.add_done_callback(_done)
        return True

    async def _release(self, lease: Lease) -> None:
        try:
            await self._store.release_lease(lease)
        except Exception as exc:
            log.warning(
                "claim_release_failed", request_id=lease.request_id, error=type(exc).__name__
            )


__all__ = ["CLAIM_PERIOD", "ClaimLoop", "ClaimStore", "LeaseAcquireMetrics", "OnClaimed"]
