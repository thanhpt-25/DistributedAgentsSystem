"""Lease metrics (task 11.2; Req 3.3, 3.4, 3.5): acquisitions by kind, losses by reason, held gauge."""

from __future__ import annotations

import asyncio

from redis.exceptions import ConnectionError as RedisConnectionError

from das.clock import FakeClock
from das.manager.claim import ClaimLoop
from das.manager.lease import HeldLeases, LeaseKeeper
from das.models import Lease
from das.observability.metrics import ManagerMetrics, MetricsRecorder
from das.state import RenewResult, StateStore, StoreStatus, WriteResult, keys

DURATION = 30.0


class ListLog:
    def warning(self, *_a, **_k) -> None: ...


def new_metrics() -> tuple[MetricsRecorder, ManagerMetrics]:
    rec = MetricsRecorder(clock=FakeClock(), log=ListLog())
    return rec, ManagerMetrics(rec)


def acquisitions(rec: MetricsRecorder, kind: str) -> float:
    return rec.registry.get_sample_value("das_lease_acquisitions_total", {"kind": kind}) or 0.0


def losses(rec: MetricsRecorder, reason: str) -> float:
    return rec.registry.get_sample_value("das_lease_losses_total", {"reason": reason}) or 0.0


async def expire(fake_redis, rid: str) -> None:
    sec, _ = await fake_redis.time()
    past = int(sec) - 1
    await fake_redis.hset(keys.lease(rid), "expires_at", f"{past}.000000")
    await fake_redis.zadd(keys.LEASES_EXPIRY, {rid: past})


class DownStore:
    """Every renewal fails as if the State_Store were unreachable."""

    async def renew_lease(self, lease: Lease, duration: float) -> RenewResult:
        raise RedisConnectionError("connection refused")

    async def release_lease(self, lease: Lease) -> WriteResult:  # pragma: no cover
        raise RedisConnectionError("connection refused")


# ---------------------------------------------------------------- acquisitions


async def test_acquisition_kinds_initial_reacquire_and_takeover(
    state_store: StateStore, fake_redis, fake_clock: FakeClock
) -> None:
    rec, metrics = new_metrics()
    rid = await state_store.create_request("hi")
    m1 = ClaimLoop(state_store, "m1", DURATION, lambda _l: None, clock=fake_clock, metrics=metrics)
    m2 = ClaimLoop(state_store, "m2", DURATION, lambda _l: None, clock=fake_clock, metrics=metrics)

    assert len(await m1.tick()) == 1  # never leased before: initial
    assert acquisitions(rec, "initial") == 1

    assert await m2.tick() == []  # HELD: rejected acquire leaves the counter alone
    assert acquisitions(rec, "initial") == 1 and acquisitions(rec, "takeover") == 0

    await expire(fake_redis, rid)
    m1.done(rid)
    assert len(await m1.tick()) == 1  # same agent re-acquires: initial
    assert acquisitions(rec, "initial") == 2

    await expire(fake_redis, rid)
    assert len(await m2.tick()) == 1  # previous epoch held by m1: takeover
    assert acquisitions(rec, "takeover") == 1
    assert acquisitions(rec, "initial") == 2


# ---------------------------------------------------------------- losses and held gauge


async def test_rejected_renewal_counts_held_by_other_once(
    state_store: StateStore, fake_redis, fake_clock: FakeClock
) -> None:
    rec, metrics = new_metrics()
    held = HeldLeases()
    metrics.leases_held(held.count)
    rid = await state_store.create_request("hi")
    lease = (await state_store.acquire_lease(rid, "m1", DURATION)).lease
    assert lease is not None
    keeper = LeaseKeeper(
        state_store, lease, DURATION, clock=fake_clock, metrics=metrics, held=held
    )
    assert rec.registry.get_sample_value("das_leases_held") == 1

    await expire(fake_redis, rid)
    assert (await state_store.acquire_lease(rid, "m2", DURATION)).ok
    await keeper.run()

    assert keeper.is_lost
    assert losses(rec, "held_by_other") == 1 and losses(rec, "store_unreachable") == 0
    assert rec.registry.get_sample_value("das_leases_held") == 0
    keeper._mark_lost(keeper.ended)  # a second call never double counts
    await keeper.release()
    assert losses(rec, "held_by_other") == 1


async def test_unreachable_store_counts_store_unreachable(fake_clock: FakeClock) -> None:
    rec, metrics = new_metrics()
    held = HeldLeases()
    lease = Lease(request_id="r1", owner="m1", epoch=1, expires_at=0.0)
    keeper = LeaseKeeper(DownStore(), lease, DURATION, clock=fake_clock, metrics=metrics, held=held)
    await keeper.run()
    assert losses(rec, "store_unreachable") == 1 and losses(rec, "held_by_other") == 0
    assert held.count() == 0


async def test_held_counts_live_leases_and_drops_released_and_expired(
    fake_clock: FakeClock,
) -> None:
    held = HeldLeases()

    class OkStore:
        async def renew_lease(self, lease: Lease, duration: float) -> RenewResult:  # pragma: no cover
            raise AssertionError("not started")

        async def release_lease(self, lease: Lease) -> WriteResult:
            return WriteResult(StoreStatus.OK)

    keepers = [
        LeaseKeeper(
            OkStore(), Lease(request_id=f"r{i}", owner="m1", epoch=1, expires_at=0.0),
            DURATION, clock=fake_clock, held=held,
        )
        for i in range(3)
    ]
    assert held.count() == 3
    await keepers[0].release()
    assert held.count() == 2
    keepers[1].stop_renewing()  # Req 7.7: still held until it expires
    assert held.count() == 2
    fake_clock.advance(DURATION)
    assert held.count() == 0
    await asyncio.sleep(0)
