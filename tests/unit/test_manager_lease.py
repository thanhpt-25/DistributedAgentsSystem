"""Unit tests for LeaseKeeper (task 14.1; Requirements 2.4, 2.7, 7.7)."""

from __future__ import annotations

import asyncio

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from das.clock import FakeClock
from das.manager.lease import LeaseEnd, LeaseKeeper, renew_interval
from das.models import Lease
from das.state import RenewResult, StateStore, WriteResult, keys

DURATION = 30.0


class FlakyStore:
    """Delegates to a real StateStore; renewals raise while ``down`` is True."""

    def __init__(self, store: StateStore) -> None:
        self.store = store
        self.down = False
        self.renew_calls = 0

    async def renew_lease(self, lease: Lease, duration: float) -> RenewResult:
        self.renew_calls += 1
        if self.down:
            raise RedisConnectionError("connection refused")
        return await self.store.renew_lease(lease, duration)

    async def release_lease(self, lease: Lease) -> WriteResult:
        return await self.store.release_lease(lease)


async def until(cond, steps: int = 5000) -> None:
    for _ in range(steps):
        if cond():
            return
        await asyncio.sleep(0)
    raise AssertionError("condition not reached")


async def expire(fake_redis, rid: str) -> None:
    """Force the stored Lease to be expired in Redis time."""
    sec, _ = await fake_redis.time()
    past = int(sec) - 1
    await fake_redis.hset(keys.lease(rid), "expires_at", f"{past}.000000")
    await fake_redis.zadd(keys.LEASES_EXPIRY, {rid: past})


async def acquired(state_store: StateStore, owner: str = "m1") -> Lease:
    rid = await state_store.create_request("hello")
    result = await state_store.acquire_lease(rid, owner, DURATION)
    assert result.ok and result.lease is not None
    return result.lease


async def forever() -> None:
    await asyncio.Event().wait()


def test_renew_interval_is_shorter_than_a_third() -> None:
    for duration in (5, 30, 300):
        assert renew_interval(duration) == duration / 4
        assert renew_interval(duration) < duration / 3


async def test_renews_every_quarter_and_extends_expiry(state_store, fake_clock: FakeClock):
    lease = await acquired(state_store)
    flaky = FlakyStore(state_store)
    keeper = LeaseKeeper(flaky, lease, DURATION, clock=fake_clock)
    keeper.start()
    await until(lambda: flaky.renew_calls >= 3)
    keeper.stop_renewing()
    await keeper.wait_closed()

    assert fake_clock.sleeps[:3] == [7.5, 7.5, 7.5]
    assert not keeper.is_lost
    assert keeper.lease.expires_at >= lease.expires_at
    stored = await state_store.get_lease(lease.request_id)
    assert stored is not None and stored.owner == "m1" and stored.epoch == lease.epoch
    assert stored.expires_at == pytest.approx(keeper.lease.expires_at)


async def test_rejected_renewal_marks_lost_and_cancels_task(
    state_store, fake_redis, fake_clock: FakeClock
):
    lease = await acquired(state_store)
    await expire(fake_redis, lease.request_id)
    taken = await state_store.acquire_lease(lease.request_id, "m2", DURATION)
    assert taken.ok

    lost_calls: list[LeaseEnd] = []
    task = asyncio.create_task(forever())
    keeper = LeaseKeeper(
        state_store,
        lease,
        DURATION,
        clock=fake_clock,
        task=task,
        on_lost=lambda _lease, reason: lost_calls.append(reason),
    )
    assert await keeper.run() is LeaseEnd.LOST_REJECTED
    assert keeper.lost.is_set()
    assert lost_calls == [LeaseEnd.LOST_REJECTED]
    with pytest.raises(asyncio.CancelledError):
        await task
    # The new owner's Lease is untouched.
    stored = await state_store.get_lease(lease.request_id)
    assert stored is not None and stored.owner == "m2"


async def test_unreachable_marks_lost_only_after_expiry(state_store, fake_clock: FakeClock):
    lease = await acquired(state_store)
    flaky = FlakyStore(state_store)
    flaky.down = True
    task = asyncio.create_task(forever())
    keeper = LeaseKeeper(flaky, lease, DURATION, clock=fake_clock, task=task)
    start = fake_clock.monotonic()

    assert await keeper.run() is LeaseEnd.LOST_UNREACHABLE
    assert fake_clock.monotonic() - start == pytest.approx(DURATION)
    assert flaky.renew_calls == 4  # failures at 7.5, 15, 22.5 kept the lease; 30 lost it
    assert keeper.is_lost
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_transient_outage_before_expiry_recovers(state_store, fake_clock: FakeClock):
    lease = await acquired(state_store)
    flaky = FlakyStore(state_store)
    flaky.down = True
    keeper = LeaseKeeper(flaky, lease, DURATION, clock=fake_clock)
    keeper.start()
    await until(lambda: flaky.renew_calls >= 2)  # 15 s of outage
    flaky.down = False
    await until(lambda: flaky.renew_calls >= 4)
    keeper.stop_renewing()
    await keeper.wait_closed()
    assert not keeper.is_lost
    assert keeper.ended is LeaseEnd.STOPPED


async def test_stop_renewing_keeps_lease_and_task(state_store, fake_clock: FakeClock):
    lease = await acquired(state_store)
    task = asyncio.create_task(forever())
    keeper = LeaseKeeper(state_store, lease, DURATION, clock=fake_clock, task=task)
    keeper.start()
    keeper.stop_renewing()
    await keeper.wait_closed()

    assert keeper.ended is LeaseEnd.STOPPED and not keeper.is_lost
    assert not task.done()
    stored = await state_store.get_lease(lease.request_id)
    assert stored is not None and stored.owner == "m1"  # not released: expires on its own
    task.cancel()


async def test_release_frees_request_for_other_managers(state_store, fake_clock: FakeClock):
    lease = await acquired(state_store)
    keeper = LeaseKeeper(state_store, lease, DURATION, clock=fake_clock)
    keeper.start()
    result = await keeper.release()
    assert result is not None and result.ok
    assert keeper.ended is LeaseEnd.RELEASED
    assert lease.request_id in await state_store.claimable_request_ids()
    assert (await state_store.acquire_lease(lease.request_id, "m2", DURATION)).ok


async def test_terminal_request_stops_without_loss(state_store, fake_clock: FakeClock):
    lease = await acquired(state_store)
    assert (await state_store.cancel_request(lease.request_id)).ok
    task = asyncio.create_task(forever())
    keeper = LeaseKeeper(state_store, lease, DURATION, clock=fake_clock, task=task)
    assert await keeper.run() is LeaseEnd.REQUEST_TERMINAL
    assert not keeper.is_lost and not task.done()
    task.cancel()


async def test_attach_after_loss_cancels(state_store, fake_redis, fake_clock: FakeClock):
    lease = await acquired(state_store)
    await expire(fake_redis, lease.request_id)
    keeper = LeaseKeeper(state_store, lease, DURATION, clock=fake_clock)
    await keeper.run()
    assert keeper.is_lost
    task = asyncio.create_task(forever())
    keeper.attach(task)
    with pytest.raises(asyncio.CancelledError):
        await task
