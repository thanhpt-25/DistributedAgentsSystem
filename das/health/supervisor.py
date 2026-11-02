"""Critical_Loop supervision and event-loop watchdog for the Liveness_Probe (Req 16.2, 16.9, 16.10).

:class:`LoopSupervisor` tracks the asyncio tasks of an agent's Critical_Loops. A
loop counts as *ended* once its task is done for any reason (returned, raised an
unhandled exception, or was cancelled). After :meth:`LoopSupervisor.mark_shutting_down`
the agent is in graceful shutdown, and loops stopped by that shutdown do not make
the Liveness_Probe fail.

:class:`EventLoopWatchdog` runs a tiny task that records ``last_tick`` every
second using the injected :class:`~das.clock.Clock`. If the event loop is blocked,
the tick stops advancing and :meth:`EventLoopWatchdog.stale` reports ``True``
once more than 5 seconds have passed since the last tick.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

from das.clock import Clock
from das.observability.logging import get_logger

WATCHDOG_INTERVAL = 1.0
WATCHDOG_STALE_AFTER = 5.0


class LoopSupervisor:
    """Registry of Critical_Loop tasks for the Liveness_Probe."""

    def __init__(self, log: Any = None) -> None:
        self._log = log if log is not None else get_logger("das.health.supervisor")
        self._tasks: dict[str, asyncio.Task[Any]] = {}
        self._shutting_down = False

    def register(self, name: str, task: asyncio.Task[Any]) -> None:
        """Track ``task`` as the Critical_Loop called ``name`` (a later call replaces it)."""
        if not name:
            raise ValueError("loop name must be non-empty")
        self._tasks[name] = task

        def _done(t: asyncio.Task[Any], n: str = name) -> None:
            self._on_done(n, t)

        task.add_done_callback(_done)

    def register_subscription(self, name: str, subscription: Any) -> asyncio.Task[None]:
        """Track a consumer subscription as the Critical_Loop ``name``.

        A subscription has no task of its own, so this registers a watcher task that
        ends when the subscription is cancelled (``wait_closed``). Objects without
        ``wait_closed`` are treated as running until the watcher is cancelled.
        Returns the watcher task; the caller cancels it at shutdown.
        """
        task = asyncio.create_task(
            wait_subscription_closed(subscription), name=f"das-watch-{name}"
        )
        self.register(name, task)
        return task

    def _on_done(self, name: str, task: asyncio.Task[Any]) -> None:
        if self._tasks.get(name) is not task:
            return
        try:
            if task.cancelled():
                exc: BaseException | None = None
                outcome = "cancelled"
            else:
                exc = task.exception()  # also marks the exception as retrieved
                outcome = "error" if exc is not None else "exited"
            if self._shutting_down:
                return
            self._log.error(
                "critical_loop_ended",
                loop=name,
                outcome=outcome,
                error_type=type(exc).__name__ if exc is not None else None,
            )
        except Exception:  # logging must never break the event loop
            pass

    @property
    def names(self) -> list[str]:
        """Names of every registered Critical_Loop, in registration order."""
        return list(self._tasks)

    def ended(self) -> list[str]:
        """Names of Critical_Loops whose task has exited, raised, or been cancelled."""
        return [name for name, task in self._tasks.items() if task.done()]

    def mark_shutting_down(self) -> None:
        """Record that graceful shutdown has started (Req 16.9)."""
        self._shutting_down = True

    @property
    def shutting_down(self) -> bool:
        return self._shutting_down


async def wait_subscription_closed(subscription: Any) -> None:
    """Return once ``subscription`` is cancelled; wait forever if it cannot tell."""
    wait = getattr(subscription, "wait_closed", None)
    if callable(wait):
        await wait()
    else:
        await asyncio.Event().wait()


class EventLoopWatchdog:
    """Records ``last_tick`` every ``interval`` seconds from inside the event loop."""

    def __init__(
        self,
        clock: Clock,
        *,
        interval: float = WATCHDOG_INTERVAL,
        stale_after: float = WATCHDOG_STALE_AFTER,
    ) -> None:
        if interval <= 0:
            raise ValueError("interval must be > 0")
        if stale_after <= 0:
            raise ValueError("stale_after must be > 0")
        self._clock = clock
        self._interval = float(interval)
        self._stale_after = float(stale_after)
        self._last_tick = clock.monotonic()
        self._task: asyncio.Task[None] | None = None

    @property
    def last_tick(self) -> float:
        return self._last_tick

    def tick(self) -> None:
        """Record a scheduled check as completed now."""
        self._last_tick = self._clock.monotonic()

    def stale(self, now: float) -> bool:
        """True when more than ``stale_after`` seconds passed since the last tick."""
        return now - self._last_tick > self._stale_after

    async def run(self) -> None:
        """Tick forever; cancel the task to stop."""
        while True:
            self.tick()
            await self._clock.sleep(self._interval)

    def start(self) -> asyncio.Task[None]:
        """Start :meth:`run` as a background task (idempotent)."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run(), name="das-event-loop-watchdog")
        return self._task

    async def stop(self) -> None:
        task, self._task = self._task, None
        if task is None:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task


__all__ = [
    "WATCHDOG_INTERVAL",
    "WATCHDOG_STALE_AFTER",
    "EventLoopWatchdog",
    "LoopSupervisor",
    "wait_subscription_closed",
]
