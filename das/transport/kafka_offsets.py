"""Which offset may be committed (das-scale-resilience task 10.1; Phase 7 Req 3.4, 3.6).

Kafka commits a high-water mark: committing offset ``n`` says every message below ``n`` is done. Messages are handled
concurrently and acknowledged in any order, so committing the offset of the message that was just acknowledged could
commit past one that is still being worked on, and a crash would lose it. :class:`OffsetTracker` keeps, per partition,
the offsets handed out and the ones acknowledged, and says what may be committed: the lowest offset still in flight, or
one past the highest delivered when nothing is.

Pure: no I/O, no clock. The adapter (``das.transport.kafka``) owns the commits.
"""

from __future__ import annotations

from collections.abc import Hashable


class OffsetTracker:
    """Per partition key: delivered offsets (in increasing order, as Kafka hands them out) and acknowledged ones."""

    def __init__(self) -> None:
        self._in_flight: dict[Hashable, set[int]] = {}
        self._highest: dict[Hashable, int] = {}
        self._committed: dict[Hashable, int] = {}

    def deliver(self, key: Hashable, offset: int) -> None:
        """Record that the message at ``offset`` was handed to a handler. An offset below what is committed is a
        duplicate of something finished, and is ignored."""
        if offset < self._committed.get(key, 0):
            return
        self._in_flight.setdefault(key, set()).add(offset)
        self._highest[key] = max(self._highest.get(key, -1), offset)

    def ack(self, key: Hashable, offset: int) -> int | None:
        """Record that ``offset`` is finished. Returns the offset to commit when that moves the commit point forward
        (the next offset to read), otherwise ``None``. An offset that was not delivered, or is acknowledged twice,
        changes nothing."""
        pending = self._in_flight.get(key)
        if pending is None or offset not in pending:
            return None
        pending.discard(offset)
        point = min(pending) if pending else self._highest[key] + 1
        if point <= self._committed.get(key, 0):
            return None
        self._committed[key] = point
        return point

    def in_flight(self, key: Hashable | None = None) -> int:
        """Delivered and not yet acknowledged: for one partition, or for all of them."""
        if key is not None:
            return len(self._in_flight.get(key, ()))
        return sum(len(offsets) for offsets in self._in_flight.values())

    def committed(self, key: Hashable) -> int | None:
        """The last offset this tracker said to commit for ``key``, or ``None`` when it never has."""
        return self._committed.get(key)

    def forget(self, key: Hashable) -> int:
        """Drop everything about a partition that was taken away (a rebalance); its messages go to another consumer.
        Returns how many in-flight messages that abandoned."""
        abandoned = len(self._in_flight.pop(key, ()))
        self._highest.pop(key, None)
        self._committed.pop(key, None)
        return abandoned


__all__ = ["OffsetTracker"]
