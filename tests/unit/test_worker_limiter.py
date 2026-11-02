"""InFlightLimiter: capacity, FIFO hand-off, over-release guard (Req 5.3)."""
from __future__ import annotations

import asyncio

import pytest

from das.transport.base import InFlightLimiterLike
from das.worker.limiter import InFlightLimiter


def test_satisfies_transport_protocol() -> None:
    limiter: InFlightLimiterLike = InFlightLimiter(1)
    assert callable(limiter.try_acquire) and callable(limiter.release)


@pytest.mark.parametrize("bad", [0, 101, -1, True, 2.5, "3"])
def test_rejects_invalid_capacity(bad: object) -> None:
    with pytest.raises(ValueError):
        InFlightLimiter(bad)  # type: ignore[arg-type]


def test_try_acquire_up_to_capacity() -> None:
    limiter = InFlightLimiter(2)
    assert limiter.try_acquire()
    assert limiter.try_acquire()
    assert not limiter.try_acquire()
    assert limiter.in_use == 2 and limiter.available == 0
    limiter.release()
    assert limiter.try_acquire()


def test_over_release_raises() -> None:
    limiter = InFlightLimiter(1)
    with pytest.raises(RuntimeError):
        limiter.release()
    assert limiter.try_acquire()
    limiter.release()
    with pytest.raises(RuntimeError):
        limiter.release()
    assert limiter.in_use == 0


async def test_acquire_waits_until_release() -> None:
    limiter = InFlightLimiter(1)
    await limiter.acquire()
    waiter = asyncio.create_task(limiter.acquire())
    await asyncio.sleep(0)
    assert not waiter.done()
    limiter.release()
    await asyncio.wait_for(waiter, 1)
    assert limiter.in_use == 1


async def test_try_acquire_does_not_steal_from_waiter() -> None:
    limiter = InFlightLimiter(1)
    assert limiter.try_acquire()
    waiter = asyncio.create_task(limiter.acquire())
    await asyncio.sleep(0)
    limiter.release()  # slot handed to the waiter
    assert not limiter.try_acquire()
    await asyncio.wait_for(waiter, 1)
    assert limiter.in_use == 1


async def test_waiters_served_fifo() -> None:
    limiter = InFlightLimiter(1)
    assert limiter.try_acquire()
    order: list[int] = []

    async def take(i: int) -> None:
        await limiter.acquire()
        order.append(i)

    tasks = [asyncio.create_task(take(i)) for i in range(3)]
    await asyncio.sleep(0)
    for _ in range(3):
        limiter.release()
        await asyncio.sleep(0)
    await asyncio.wait_for(asyncio.gather(*tasks), 1)
    assert order == [0, 1, 2]


async def test_cancelled_waiter_does_not_leak_slot() -> None:
    limiter = InFlightLimiter(1)
    assert limiter.try_acquire()
    waiter = asyncio.create_task(limiter.acquire())
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    limiter.release()
    assert limiter.in_use == 0
    assert limiter.try_acquire()


async def test_waiter_cancelled_after_handoff_passes_slot_on() -> None:
    limiter = InFlightLimiter(1)
    assert limiter.try_acquire()
    first = asyncio.create_task(limiter.acquire())
    second = asyncio.create_task(limiter.acquire())
    await asyncio.sleep(0)
    limiter.release()  # hands slot to `first` ...
    first.cancel()  # ... which is cancelled before it resumes
    with pytest.raises(asyncio.CancelledError):
        await first
    await asyncio.wait_for(second, 1)
    assert limiter.in_use == 1
