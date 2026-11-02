"""Per-key rate limiting for warning logs (Requirements 1.6, 1.9, 3.2, 3.10).

A :class:`RateLimitedLog` lets the first event for a key through and then at
most one more event per ``period`` seconds for that key, measured with the
injected :class:`~das.clock.Clock` (``monotonic``). Keys are independent.
"""

from __future__ import annotations

import threading
from collections.abc import Hashable
from typing import Any

from das.clock import Clock

DEFAULT_PERIOD = 60.0


class RateLimitedLog:
    """At most one log entry per key per ``period`` seconds."""

    def __init__(self, clock: Clock, period: float = DEFAULT_PERIOD) -> None:
        if period < 0:
            raise ValueError("period must be >= 0")
        self._clock = clock
        self._period = float(period)
        self._last: dict[Hashable, float] = {}
        self._lock = threading.Lock()

    @property
    def period(self) -> float:
        return self._period

    def allow(self, key: Hashable) -> bool:
        """Return True (and remember the time) if an entry for ``key`` may be logged now."""
        now = self._clock.monotonic()
        with self._lock:
            last = self._last.get(key)
            if last is not None and now - last < self._period:
                return False
            self._last[key] = now
            return True

    def warning(self, log: Any, key: Hashable, event: str, **fields: Any) -> bool:
        """Log ``event`` at warning level through ``log`` if allowed; return whether it logged."""
        if not self.allow(key):
            return False
        try:
            log.warning(event, **fields)
        except Exception:  # logging must never break the caller
            pass
        return True


__all__ = ["DEFAULT_PERIOD", "RateLimitedLog"]
