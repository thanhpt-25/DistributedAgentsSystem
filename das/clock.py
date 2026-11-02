"""Injectable time source.

Code that waits or reads the time takes a ``Clock`` so that backoff, capability
wait, heartbeat staleness, and timeout logic can run instantly in tests.

Lease expiry and heartbeat staleness in production use Redis server time; this
clock covers local timing (sleeps, local deadlines, log timestamps).
"""

from __future__ import annotations

import asyncio
import time
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """Source of time and sleeping."""

    def now(self) -> float:
        """Wall-clock time as seconds since the Unix epoch (UTC)."""
        ...

    def monotonic(self) -> float:
        """Monotonic seconds, for measuring intervals and durations."""
        ...

    async def sleep(self, seconds: float) -> None:
        """Suspend the caller for ``seconds`` (negative values count as zero)."""
        ...


class SystemClock:
    """Real time backed by :mod:`time` and :func:`asyncio.sleep`."""

    def now(self) -> float:
        return time.time()

    def monotonic(self) -> float:
        return time.monotonic()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(max(0.0, seconds))


class FakeClock:
    """Deterministic clock for tests.

    ``sleep`` records the requested duration, advances the clock by that
    amount, and yields once to the event loop, so code under test makes
    progress without real waiting. ``advance`` moves time forward manually.
    Wall and monotonic time move together.
    """

    def __init__(self, start: float = 1_700_000_000.0, monotonic_start: float = 0.0) -> None:
        self._now = float(start)
        self._mono = float(monotonic_start)
        self.sleeps: list[float] = []

    def now(self) -> float:
        return self._now

    def monotonic(self) -> float:
        return self._mono

    def advance(self, seconds: float) -> None:
        """Move time forward by ``seconds``; time never moves backwards."""
        if seconds < 0:
            raise ValueError("FakeClock cannot move backwards")
        self._now += seconds
        self._mono += seconds

    async def sleep(self, seconds: float) -> None:
        seconds = max(0.0, float(seconds))
        self.sleeps.append(seconds)
        self.advance(seconds)
        await asyncio.sleep(0)


__all__ = ["Clock", "FakeClock", "SystemClock"]
