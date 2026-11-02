"""Periodic queue-depth and Agent_Registry gauges of the Manager_Agent (Req 3.1, 3.2, 3.6, 3.7, 3.10).

* :class:`QueueDepthSampler` reads the ready count of ``das.results``, ``das.dlq``
  and ``das.jobs.<cap>`` for every Capability advertised in the Agent_Registry at
  sampling time, once per ``metrics.queue_sample_interval`` (first sample right
  after start). Each queue has its own 5 s timeout; a failed or slow sample bumps
  ``das_queue_depth_sample_errors_total{queue}``, keeps the previous gauge value
  (or no series at all if the queue was never sampled), logs at most one warning
  per queue per 60 s and doesn't affect the other queues (Req 3.2).
* :class:`RegistryGauges` reads the Agent_Registry once per heartbeat interval and
  sets ``das_registry_agents{role, status}`` for all six role/status pairs (0 when
  empty) and ``das_capability_workers{capability}`` (0 for a Capability declared
  only by non-Available Workers). A failed read or one slower than 2 s keeps both
  gauges, bumps ``das_registry_sample_errors_total`` and logs at most one warning
  per 60 s (Req 3.10).

Neither is a Critical_Loop: errors are logged and the loop keeps going. All
waiting goes through the injected :class:`~das.clock.Clock`.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Iterable
from typing import Protocol

from das.clock import Clock
from das.models import AgentRecord
from das.observability import get_logger
from das.observability.metrics import (
    REGISTRY_ROLES,
    REGISTRY_STATUSES,
    ManagerMetrics,
    RegistryRole,
    RegistryStatus,
)
from das.observability.ratelimit import RateLimitedLog
from das.registry.availability import AVAILABLE, WORKER_ROLE, advertised_capabilities
from das.registry.registry import RegistrySnapshot
from das.transport.base import QueueInspector, queue_name_for

log = get_logger(__name__)

QUEUE_SAMPLE_TIMEOUT = 5.0
REGISTRY_SAMPLE_TIMEOUT = 2.0
WARNING_PERIOD = 60.0
FIXED_DESTINATIONS: tuple[str, ...] = ("results", "dlq")
ROLES: tuple[RegistryRole, ...] = ("manager", "worker")
STATUSES: tuple[RegistryStatus, ...] = ("Available", "Unavailable", "Draining")


class RegistryReader(Protocol):
    """The Agent_Registry read the samplers need (``AgentRegistry`` satisfies it)."""

    async def snapshot(self) -> RegistrySnapshot: ...


async def within[T](clock: Clock, awaitable: Awaitable[T], timeout: float) -> T:
    """Await ``awaitable`` for at most ``timeout`` seconds of ``clock`` time.

    Raises :class:`TimeoutError` (after cancelling the call) when the clock's
    sleep finishes first. A call that is already done wins over the timer.
    """
    call = asyncio.ensure_future(awaitable)
    timer = asyncio.ensure_future(clock.sleep(timeout))
    try:
        await asyncio.wait({call, timer}, return_when=asyncio.FIRST_COMPLETED)
    except BaseException:
        call.cancel()
        raise
    finally:
        if not timer.done():
            timer.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await timer
    if call.done():
        return call.result()
    call.cancel()
    with contextlib.suppress(asyncio.CancelledError, Exception):
        await call
    raise TimeoutError(f"no answer within {timeout} s")


class _PeriodicLoop:
    """``run`` = sample, then wait ``interval`` (interruptible by ``stop``)."""

    def __init__(self, clock: Clock, interval: float) -> None:
        if interval <= 0:
            raise ValueError("interval must be positive")
        self._clock = clock
        self._interval = float(interval)
        self._stop = asyncio.Event()

    @property
    def interval(self) -> float:
        return self._interval

    def stop(self) -> None:
        self._stop.set()

    async def sample_once(self) -> object:  # pragma: no cover - overridden
        raise NotImplementedError

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.sample_once()
            except asyncio.CancelledError:
                raise
            except Exception:  # never a Critical_Loop: log and keep sampling
                log.exception("sampler_failed", sampler=type(self).__name__)
            if await self._wait():
                break

    async def _wait(self) -> bool:
        """Sleep one interval on the clock; True when stop was requested."""
        if self._stop.is_set():
            return True
        sleeper = asyncio.ensure_future(self._clock.sleep(self._interval))
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


def sampled_destinations(capabilities: Iterable[str]) -> list[str]:
    """``results``, ``dlq`` and ``jobs/<cap>`` for each Capability, in a stable order."""
    return [*FIXED_DESTINATIONS, *(f"jobs/{cap}" for cap in sorted(set(capabilities)))]


class QueueDepthSampler(_PeriodicLoop):
    """``das_queue_depth{queue}`` every ``interval`` seconds (Req 3.1, 3.2)."""

    def __init__(
        self,
        inspector: QueueInspector,
        registry: RegistryReader,
        metrics: ManagerMetrics,
        clock: Clock,
        *,
        interval: float = 15.0,
        queue_timeout: float = QUEUE_SAMPLE_TIMEOUT,
        registry_timeout: float = REGISTRY_SAMPLE_TIMEOUT,
    ) -> None:
        super().__init__(clock, interval)
        self._inspector = inspector
        self._registry = registry
        self._metrics = metrics
        self._queue_timeout = float(queue_timeout)
        self._registry_timeout = float(registry_timeout)
        self._warn = RateLimitedLog(clock, WARNING_PERIOD)
        # Last successful registry read; used when the registry can't be read.
        self._capabilities: frozenset[str] = frozenset()

    async def _current_capabilities(self) -> frozenset[str]:
        try:
            snapshot = await within(self._clock, self._registry.snapshot(), self._registry_timeout)
        except Exception as exc:
            # RegistryGauges counts registry errors; reuse the last known set here.
            self._warn.warning(
                log, ("registry",), "queue_sample_registry_failed", error=type(exc).__name__
            )
            return self._capabilities
        self._capabilities = advertised_capabilities(snapshot.agents)
        return self._capabilities

    async def sample_once(self) -> dict[str, int]:
        """Sample every queue once; returns ``{queue name: depth}`` of the successes."""
        capabilities = await self._current_capabilities()
        depths: dict[str, int] = {}
        for destination in sampled_destinations(capabilities):
            queue = queue_name_for(destination)
            try:
                depth = await within(
                    self._clock, self._inspector.ready_count(destination), self._queue_timeout
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._metrics.queue_depth_sample_error(queue)
                self._warn.warning(
                    log,
                    ("queue", queue),
                    "queue_depth_sample_failed",
                    queue=queue,
                    error=type(exc).__name__,
                )
                continue
            self._metrics.queue_depth(queue, int(depth))
            depths[queue] = int(depth)
        return depths


def registry_counts(
    agents: Iterable[AgentRecord],
) -> tuple[dict[tuple[RegistryRole, RegistryStatus], int], dict[str, int]]:
    """Pure: (agents per (role, status) for all 6 pairs, Available Workers per Capability).

    The Capability map has every Capability declared by any Worker, 0 when none of
    its Workers is Available (Req 3.6, 3.7). Agents of other roles (gateway) are
    ignored.
    """
    by_status: dict[tuple[RegistryRole, RegistryStatus], int] = {
        (role, status): 0 for role in ROLES for status in STATUSES
    }
    workers: dict[str, int] = {}
    for agent in agents:
        key = (agent.role, agent.status)
        if agent.role in REGISTRY_ROLES and agent.status in REGISTRY_STATUSES:
            by_status[key] += 1  # type: ignore[index]
        if agent.role == WORKER_ROLE:
            available = agent.status == AVAILABLE
            for cap in agent.capabilities:
                workers[cap] = workers.get(cap, 0) + (1 if available else 0)
    return by_status, workers


class RegistryGauges(_PeriodicLoop):
    """``das_registry_agents`` and ``das_capability_workers`` every heartbeat (Req 3.6, 3.7, 3.10)."""

    def __init__(
        self,
        registry: RegistryReader,
        metrics: ManagerMetrics,
        clock: Clock,
        *,
        interval: float,
        timeout: float = REGISTRY_SAMPLE_TIMEOUT,
    ) -> None:
        super().__init__(clock, interval)
        self._registry = registry
        self._metrics = metrics
        self._timeout = float(timeout)
        self._warn = RateLimitedLog(clock, WARNING_PERIOD)
        self._reported_caps: set[str] = set()

    async def sample_once(self) -> bool:
        """One read; True when the gauges were updated."""
        try:
            snapshot = await within(self._clock, self._registry.snapshot(), self._timeout)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._metrics.registry_sample_error()
            self._warn.warning(log, "registry", "registry_sample_failed", error=type(exc).__name__)
            return False
        by_status, workers = registry_counts(snapshot.agents)
        for (role, status), count in by_status.items():
            self._metrics.registry_agents(role, status, count)
        # A Capability no Worker declares any more drops to 0 instead of going stale.
        for cap in self._reported_caps - workers.keys():
            workers[cap] = 0
        for cap in sorted(workers):
            self._metrics.capability_workers(cap, workers[cap])
        self._reported_caps = set(workers)
        return True


__all__ = [
    "QUEUE_SAMPLE_TIMEOUT",
    "REGISTRY_SAMPLE_TIMEOUT",
    "QueueDepthSampler",
    "RegistryGauges",
    "RegistryReader",
    "registry_counts",
    "sampled_destinations",
    "within",
]
