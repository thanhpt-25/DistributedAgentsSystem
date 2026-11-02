"""Unit tests for ClaimLoop (task 14.1; Requirements 2.1, 2.2, 2.3, 2.5)."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from redis.exceptions import ConnectionError as RedisConnectionError

from das.clock import FakeClock
from das.manager.claim import ClaimLoop
from das.models import Lease
from das.state import StateStore, keys

DURATION = 30.0


async def expire(fake_redis, rid: str) -> None:
    sec, _ = await fake_redis.time()
    past = int(sec) - 1
    await fake_redis.hset(keys.lease(rid), "expires_at", f"{past}.000000")
    await fake_redis.zadd(keys.LEASES_EXPIRY, {rid: past})


def collector() -> tuple[list[Lease], Callable[[Lease], None]]:
    got: list[Lease] = []
    return got, got.append


async def test_claims_pending_request(state_store: StateStore, fake_clock: FakeClock):
    rid = await state_store.create_request("hi")
    got, on_claimed = collector()
    loop = ClaimLoop(state_store, "m1", DURATION, on_claimed, clock=fake_clock)

    won = await loop.tick()
    assert [lease.request_id for lease in won] == [rid]
    assert got == won
    lease = got[0]
    assert lease.owner == "m1" and lease.epoch == 1
    assert lease.expires_at - await state_store.server_time() <= DURATION
    assert loop.owned == {rid}
    assert rid not in await state_store.claimable_request_ids()
    assert await loop.tick() == []  # nothing left to claim


async def test_concurrent_managers_single_winner(state_store: StateStore, fake_clock: FakeClock):
    rids = [await state_store.create_request(f"p{i}") for i in range(5)]
    got1, cb1 = collector()
    got2, cb2 = collector()
    m1 = ClaimLoop(state_store, "m1", DURATION, cb1, clock=fake_clock)
    m2 = ClaimLoop(state_store, "m2", DURATION, cb2, clock=fake_clock)
    await asyncio.gather(m1.tick(), m2.tick())

    claimed = [lease.request_id for lease in got1 + got2]
    assert sorted(claimed) == sorted(rids)  # every Request claimed exactly once
    for lease in got1 + got2:
        stored = await state_store.get_lease(lease.request_id)
        assert stored is not None and stored.owner == lease.owner


async def test_takes_over_expired_lease(state_store: StateStore, fake_redis, fake_clock):
    rid = await state_store.create_request("hi")
    first = await state_store.acquire_lease(rid, "m1", DURATION)
    assert first.ok
    got, on_claimed = collector()
    m2 = ClaimLoop(state_store, "m2", DURATION, on_claimed, clock=fake_clock)

    assert await m2.tick() == []  # still held
    await expire(fake_redis, rid)
    won = await m2.tick()
    assert [lease.request_id for lease in won] == [rid]
    assert won[0].owner == "m2" and won[0].epoch == 2


async def test_max_concurrent_caps_ownership(state_store: StateStore, fake_clock: FakeClock):
    rids = [await state_store.create_request(f"p{i}") for i in range(3)]
    got, on_claimed = collector()
    loop = ClaimLoop(state_store, "m1", DURATION, on_claimed, clock=fake_clock, max_concurrent=2)

    assert len(await loop.tick()) == 2
    assert await loop.tick() == []
    loop.done(got[0].request_id)
    assert len(await loop.tick()) == 1
    assert sorted(lease.request_id for lease in got) == sorted(rids)


async def test_returned_task_tracks_ownership(state_store: StateStore, fake_clock: FakeClock):
    await state_store.create_request("hi")
    gate = asyncio.Event()

    async def driver(_lease: Lease) -> None:
        await gate.wait()

    loop = ClaimLoop(
        state_store, "m1", DURATION, lambda lease: asyncio.create_task(driver(lease)),
        clock=fake_clock,
    )
    won = await loop.tick()
    assert loop.owned == {won[0].request_id}
    gate.set()
    for _ in range(10):
        await asyncio.sleep(0)
    assert loop.owned == frozenset()


async def test_failed_handoff_releases_lease(state_store: StateStore, fake_clock: FakeClock):
    rid = await state_store.create_request("hi")

    def boom(_lease: Lease) -> None:
        raise RuntimeError("driver failed to start")

    loop = ClaimLoop(state_store, "m1", DURATION, boom, clock=fake_clock)
    assert await loop.tick() == []
    assert loop.owned == frozenset()
    assert await state_store.get_lease(rid) is None
    assert rid in await state_store.claimable_request_ids()


class FlakyScan:
    def __init__(self, store: StateStore, failures: int) -> None:
        self.store = store
        self.failures = failures

    async def claimable_request_ids(self, limit: int = 100) -> list[str]:
        if self.failures > 0:
            self.failures -= 1
            raise RedisConnectionError("unreachable")
        return await self.store.claimable_request_ids(limit)

    async def acquire_lease(self, rid, manager_id, duration):
        return await self.store.acquire_lease(rid, manager_id, duration)

    async def release_lease(self, lease):
        return await self.store.release_lease(lease)


async def test_run_ticks_every_second_and_survives_outage(
    state_store: StateStore, fake_clock: FakeClock
):
    rid = await state_store.create_request("hi")
    got, on_claimed = collector()
    loop = ClaimLoop(FlakyScan(state_store, failures=2), "m1", DURATION, on_claimed, clock=fake_clock)
    runner = asyncio.create_task(loop.run())
    for _ in range(100):
        if got:
            break
        await asyncio.sleep(0)
    loop.stop()
    await asyncio.wait_for(runner, 1)

    assert [lease.request_id for lease in got] == [rid]
    assert fake_clock.sleeps[:2] == [1.0, 1.0]  # claimed on the 3rd tick, 2 s in
