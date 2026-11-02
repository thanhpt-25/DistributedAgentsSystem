"""In-flight Job limiter shared by all Capability subscriptions (Requirement 5.3).

Satisfies :class:`das.transport.base.InFlightLimiterLike`. A slot is taken
before a dispatch Message is pulled and released when the delivery is settled.
"""
from __future__ import annotations

import asyncio
from collections import deque


class InFlightLimiter:
    """Counting semaphore with capacity ``concurrency_limit``.

    ``acquire`` waits FIFO for a free slot; ``try_acquire`` never waits and
    does not jump ahead of queued waiters; ``release`` raises if it would
    push the free count above capacity.
    """

    def __init__(self, concurrency_limit: int) -> None:
        if (
            isinstance(concurrency_limit, bool)
            or not isinstance(concurrency_limit, int)
            or not (1 <= concurrency_limit <= 100)
        ):
            raise ValueError("concurrency_limit must be an integer from 1 to 100")
        self._capacity = concurrency_limit
        self._in_use = 0
        self._waiters: deque[asyncio.Future[None]] = deque()

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def in_use(self) -> int:
        return self._in_use

    @property
    def available(self) -> int:
        return self._capacity - self._in_use

    def try_acquire(self) -> bool:
        """Take a slot without waiting; return False when none is free."""
        if self._in_use >= self._capacity or self._has_waiters():
            return False
        self._in_use += 1
        return True

    async def acquire(self) -> None:
        if self.try_acquire():
            return
        fut: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append(fut)
        try:
            await fut
        except asyncio.CancelledError:
            # If the slot was handed to us just before cancellation, pass it on.
            if fut.done() and not fut.cancelled():
                self._in_use -= 1
                self._wake_next()
            raise
        finally:
            try:
                self._waiters.remove(fut)
            except ValueError:
                pass

    def release(self) -> None:
        if self._in_use <= 0:
            raise RuntimeError("InFlightLimiter released more times than acquired")
        self._in_use -= 1
        self._wake_next()

    def _has_waiters(self) -> bool:
        return any(not w.done() for w in self._waiters)

    def _wake_next(self) -> None:
        while self._in_use < self._capacity:
            fut = next((w for w in self._waiters if not w.done()), None)
            if fut is None:
                return
            # Hand the slot directly to the waiter so try_acquire can't steal it.
            self._in_use += 1
            fut.set_result(None)


__all__ = ["InFlightLimiter"]
