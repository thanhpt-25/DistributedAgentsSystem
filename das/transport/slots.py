"""How many unsettled deliveries a subscription may hold (Phase 7 Req 2.1, 3.4), for the pull-style adapters (HTTP,
Kafka) that hand Messages to handlers themselves: the subscription's own ``max_in_flight`` and the shared limiter's
(``SubscribeOptions.limiter``), so a slow handler never holds more than its share."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class Slots:
    max_in_flight: int | None
    limiter: Any
    used: int = 0
    _freed: asyncio.Event = field(default_factory=asyncio.Event)

    async def acquire_first(self) -> None:
        """Wait for one slot (the subscription's own, then the shared limiter's) and take it."""
        while self.max_in_flight is not None and self.used >= self.max_in_flight:
            self._freed.clear()
            await self._freed.wait()
        if self.limiter is not None:
            await self.limiter.acquire()
        self.used += 1

    def try_more(self) -> bool:
        """Take one more slot if one is free right now."""
        if self.max_in_flight is not None and self.used >= self.max_in_flight:
            return False
        if self.limiter is not None and not self.limiter.try_acquire():
            return False
        self.used += 1
        return True

    def release(self) -> None:
        self.used = max(0, self.used - 1)
        if self.limiter is not None:
            self.limiter.release()
        self._freed.set()


__all__ = ["Slots"]
