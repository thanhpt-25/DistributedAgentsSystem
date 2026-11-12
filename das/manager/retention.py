"""Retention_Sweeper and Request_Index backfill of the Manager_Agent (Req 14, 15.10).

* :class:`RetentionSweeper` runs once per ``retention.sweep_interval`` (first sweep
  right after start) while ``retention.enabled`` is true. It reads Redis ``TIME``,
  picks up to ``retention.batch_size`` candidates from ``das:requests:finished``,
  ``das:requests:inbox_purge`` and ``das:dlq:terminal`` and calls ``purge_request``,
  ``purge_inbox`` and ``purge_dead_letter`` for each. The scripts re-check every
  condition with their own ``TIME`` (Req 14.4, 14.5), so the shell's ``now`` only
  selects candidates. Each ``DELETED`` reply adds one to
  ``das_retention_deleted_total{kind}``; only the script that deleted an item gets
  ``DELETED``, so an item is counted once across the Manager_Group (Req 14.10).
  A State_Store error ends the current sweep, logs at most one warning per 60 s and
  the next interval retries (Req 14.13). Not a Critical_Loop.
* :class:`IndexBackfill` adds every existing (v1) Request to the Request_Index once:
  ``SET das:requests:index:backfill_lock <agent> NX PX 60000`` renewed every 20 s,
  ``SCAN MATCH das:req:* TYPE hash`` (suffixed keys skipped), ``index_request`` per
  Request (which also stamps a missing Finished_At, Req 14.9), then
  ``SET das:requests:index:ready 1`` (Req 15.10).

All waiting goes through the injected :class:`~das.clock.Clock`.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from das.clock import Clock
from das.config.schema import RetentionConfig
from das.manager.samplers import _PeriodicLoop
from das.observability import get_logger
from das.observability.metrics import ManagerMetrics, RetentionKind
from das.observability.ratelimit import RateLimitedLog
from das.state import PURGE_DELETED

log = get_logger(__name__)

WARNING_PERIOD = 60.0
BACKFILL_LOCK_TTL = 60.0
BACKFILL_RENEW_INTERVAL = 20.0
BACKFILL_RETRY_INTERVAL = 5.0
#: Reply of ``purge_inbox`` / ``purge_dead_letter`` when an item was deleted.
DELETED = "DELETED"


class RetentionStore(Protocol):
    """The State_Store calls the sweeper needs (``StateStore`` satisfies it)."""

    async def server_time_ms(self) -> int: ...
    async def finished_candidates(self, now_ms: int, ttl_ms: int, limit: int) -> list[str]: ...
    async def inbox_candidates(self, now_ms: int, ttl_ms: int, limit: int) -> list[str]: ...
    async def dlq_terminal_candidates(
        self, now_ms: int, ttl_ms: int, limit: int
    ) -> list[str]: ...
    async def purge_request(self, rid: str, ttl_ms: int) -> str: ...
    async def purge_inbox(self, rid: str, ttl_ms: int) -> str: ...
    async def purge_dead_letter(self, dlid: str, ttl_ms: int) -> str: ...


@dataclass(slots=True)
class SweepResult:
    """Outcome of one sweep: items deleted per kind, and whether it ran to the end."""

    deleted: dict[RetentionKind, int] = field(
        default_factory=lambda: {"request": 0, "inbox": 0, "dlq_record": 0}
    )
    completed: bool = True


class RetentionSweeper(_PeriodicLoop):
    """Batch deletion of expired Requests, inboxes and Dead_Letter_Records (Req 14)."""

    def __init__(
        self,
        store: RetentionStore,
        metrics: ManagerMetrics,
        clock: Clock,
        config: RetentionConfig,
    ) -> None:
        super().__init__(clock, config.sweep_interval)
        self._store = store
        self._metrics = metrics
        self._config = config
        self._warn = RateLimitedLog(clock, WARNING_PERIOD)

    @property
    def enabled(self) -> bool:
        return self._config.enabled

    async def run(self) -> None:
        if not self._config.enabled:  # Req 14.12: never deletes anything
            return
        await super().run()

    def _phases(
        self,
    ) -> list[
        tuple[
            RetentionKind,
            int,
            Callable[[int, int, int], Awaitable[list[str]]],
            Callable[[str, int], Awaitable[str]],
        ]
    ]:
        c, s = self._config, self._store
        return [
            ("request", c.terminal_request_ttl * 1000, s.finished_candidates, s.purge_request),
            ("inbox", c.inbox_ttl * 1000, s.inbox_candidates, s.purge_inbox),
            ("dlq_record", c.dlq_record_ttl * 1000, s.dlq_terminal_candidates, s.purge_dead_letter),
        ]

    async def sample_once(self) -> SweepResult:
        """One sweep; a State_Store error abandons the rest of it (Req 14.13)."""
        result = SweepResult()
        if not self._config.enabled:
            return result
        batch = self._config.batch_size
        try:
            now_ms = await self._store.server_time_ms()
            for kind, ttl_ms, candidates, purge in self._phases():
                for item in await candidates(now_ms, ttl_ms, batch):
                    if await purge(item, ttl_ms) in (PURGE_DELETED, DELETED):
                        result.deleted[kind] += 1
                        self._metrics.retention_deleted(kind, 1)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            result.completed = False
            self._warn.warning(log, "sweep", "retention_sweep_failed", error=type(exc).__name__)
        return result


class BackfillStore(Protocol):
    """The State_Store calls the backfill needs (``StateStore`` satisfies it)."""

    async def index_ready(self) -> bool: ...
    async def set_index_ready(self) -> None: ...
    async def acquire_backfill_lock(self, owner: str, ttl_ms: int) -> bool: ...
    async def renew_backfill_lock(self, owner: str, ttl_ms: int) -> bool: ...
    async def release_backfill_lock(self, owner: str) -> bool: ...
    def scan_request_ids(self): ...  # AsyncIterator[str]
    async def index_request(self, rid: str) -> bool: ...


class TenantBackfillStore(Protocol):
    """The State_Store calls the Tenant_Backfill needs (``StateStore`` satisfies it)."""

    async def index_ready(self) -> bool: ...
    async def tenant_index_ready(self) -> bool: ...
    async def set_tenant_index_ready(self) -> None: ...
    async def acquire_tenant_backfill_lock(self, owner: str, ttl_ms: int) -> bool: ...
    async def renew_tenant_backfill_lock(self, owner: str, ttl_ms: int) -> bool: ...
    async def release_tenant_backfill_lock(self, owner: str) -> bool: ...
    def scan_request_index(self): ...  # AsyncIterator[list[str]]
    def scan_dead_letter_index(self): ...  # AsyncIterator[list[str]]
    async def tenant_backfill_requests(self, rids: list[str]) -> int: ...
    async def tenant_backfill_dead_letters(self, dlids: list[str]) -> int: ...


class IndexBackfill:
    """One-time Request_Index backfill under a renewed ``SET NX PX`` lock (Req 15.10).

    The lock, renewal and retry logic is shared with :class:`TenantIndexBackfill`, which overrides the small
    ``_is_ready`` ... ``_log_ready`` hooks below with the Tenant_Backfill's own flag, lock and work."""

    _FAILED_EVENT = "request_index_backfill_failed"

    def __init__(
        self,
        store: Any,  # BackfillStore, or TenantBackfillStore for TenantIndexBackfill
        clock: Clock,
        owner: str,
        *,
        lock_ttl: float = BACKFILL_LOCK_TTL,
        renew_interval: float = BACKFILL_RENEW_INTERVAL,
        retry_interval: float = BACKFILL_RETRY_INTERVAL,
    ) -> None:
        self._store = store
        self._clock = clock
        self._owner = owner
        self._lock_ttl_ms = int(lock_ttl * 1000)
        self._renew_interval = float(renew_interval)
        self._retry_interval = float(retry_interval)
        self._warn = RateLimitedLog(clock, WARNING_PERIOD)
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    # -- hooks: what differs between the Request_Index and the Tenant_Index backfill ------------------

    async def _is_ready(self) -> bool:
        return bool(await self._store.index_ready())

    async def _can_start(self) -> bool:
        return True

    async def _acquire(self) -> bool:
        return bool(await self._store.acquire_backfill_lock(self._owner, self._lock_ttl_ms))

    async def _renew(self) -> bool:
        return bool(await self._store.renew_backfill_lock(self._owner, self._lock_ttl_ms))

    async def _release(self) -> None:
        await self._store.release_backfill_lock(self._owner)

    async def _fill(self) -> int:
        count = 0
        async for rid in self._store.scan_request_ids():
            await self._store.index_request(rid)
            count += 1
        return count

    async def _mark_ready(self) -> None:
        await self._store.set_index_ready()

    def _log_ready(self, count: int) -> None:
        log.info("request_index_backfilled", requests=count)

    async def run_once(self) -> bool:
        """True when the index is ready (already, or after this backfill); False when
        another agent holds the lock. State_Store errors propagate."""
        if await self._is_ready():
            return True
        if not await self._can_start() or not await self._acquire():
            return False
        renewer = asyncio.ensure_future(self._renew_loop())
        try:
            count = await self._fill()
            await self._mark_ready()
            self._log_ready(count)
        finally:
            renewer.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await renewer
            with contextlib.suppress(Exception):
                await self._release()
        return True

    async def run(self) -> None:
        """Retry every ``retry_interval`` until the index is ready or ``stop``."""
        while not self._stop.is_set():
            try:
                if await self.run_once():
                    return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._warn.warning(log, "backfill", self._FAILED_EVENT, error=type(exc).__name__)
            if await self._wait(self._retry_interval):
                return

    async def _renew_loop(self) -> None:
        while True:
            await self._clock.sleep(self._renew_interval)
            try:
                held = await self._renew()
            except Exception as exc:
                self._warn.warning(
                    log, "renew", "backfill_lock_renew_failed", error=type(exc).__name__
                )
                continue
            if not held:
                # index_request is idempotent, so a concurrent backfill is still safe.
                self._warn.warning(log, "lost", "backfill_lock_lost")

    async def _wait(self, seconds: float) -> bool:
        sleeper = asyncio.ensure_future(self._clock.sleep(seconds))
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


class TenantIndexBackfill(IndexBackfill):
    """One-time Tenant_Backfill (Phase 5 Req 9.2): files every Request and Dead_Letter_Record in its tenant's
    index twins under ``das:tenant:index:backfill_lock``, then sets ``das:tenant:index:ready``.

    It copies from the global indexes, so it waits for the Phase 2 Request_Index backfill to finish first.
    ``tenant_backfill.lua`` is idempotent, so a second agent that loses the lock mid-run is harmless."""

    _FAILED_EVENT = "tenant_index_backfill_failed"

    async def _is_ready(self) -> bool:
        return bool(await self._store.tenant_index_ready())

    async def _can_start(self) -> bool:
        return bool(await self._store.index_ready())

    async def _acquire(self) -> bool:
        return bool(await self._store.acquire_tenant_backfill_lock(self._owner, self._lock_ttl_ms))

    async def _renew(self) -> bool:
        return bool(await self._store.renew_tenant_backfill_lock(self._owner, self._lock_ttl_ms))

    async def _release(self) -> None:
        await self._store.release_tenant_backfill_lock(self._owner)

    async def _fill(self) -> int:
        count = 0
        # Requests first: a record is filed only when its Request exists.
        async for rids in self._store.scan_request_index():
            count += await self._store.tenant_backfill_requests(rids)
        async for dlids in self._store.scan_dead_letter_index():
            count += await self._store.tenant_backfill_dead_letters(dlids)
        return count

    async def _mark_ready(self) -> None:
        await self._store.set_tenant_index_ready()

    def _log_ready(self, count: int) -> None:
        log.info("tenant_index_backfilled", items=count)


__all__ = [
    "BACKFILL_LOCK_TTL",
    "BACKFILL_RENEW_INTERVAL",
    "BackfillStore",
    "IndexBackfill",
    "RetentionStore",
    "RetentionSweeper",
    "SweepResult",
    "TenantBackfillStore",
    "TenantIndexBackfill",
]
