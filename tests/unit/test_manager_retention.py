"""Unit tests for RetentionSweeper and IndexBackfill (task 11.3;
Req 14.3, 14.6-14.10, 14.12, 14.13, 15.10)."""

from __future__ import annotations

import asyncio
import itertools

import pytest

from das.clock import FakeClock
from das.config.schema import RetentionConfig
from das.manager import retention
from das.manager.retention import IndexBackfill, RetentionSweeper
from das.models import RequestStatus
from das.models.dlq import DeadLetterFields
from das.observability.metrics import ManagerMetrics, MetricsRecorder
from das.state import StateStore, keys

T0 = 1_700_000_000_000
HOUR_MS = 3_600_000


class ListLog:
    def __init__(self) -> None:
        self.warnings: list[tuple[str, dict]] = []
        self.infos: list[tuple[str, dict]] = []

    def warning(self, event: str, **fields) -> None:
        self.warnings.append((event, fields))

    def info(self, event: str, **fields) -> None:
        self.infos.append((event, fields))

    def exception(self, event: str, **fields) -> None:  # pragma: no cover - not expected
        self.warnings.append((event, fields))


@pytest.fixture
def log(monkeypatch) -> ListLog:
    fake = ListLog()
    monkeypatch.setattr(retention, "log", fake)
    return fake


@pytest.fixture
def recorder() -> MetricsRecorder:
    return MetricsRecorder(clock=FakeClock(), log=ListLog())


def deleted(rec: MetricsRecorder, kind: str) -> float:
    return rec.registry.get_sample_value("das_retention_deleted_total", {"kind": kind}) or 0.0


def config(**kw) -> RetentionConfig:
    base = dict(
        enabled=True,
        terminal_request_ttl=7200,
        inbox_ttl=60,
        dlq_record_ttl=3600,
        sweep_interval=10,
        batch_size=2,
    )
    base.update(kw)
    return RetentionConfig(**base)


def counter_ids(prefix: str = "r"):
    c = itertools.count()
    return lambda: f"{prefix}{next(c):03d}"


async def cancelled_with_inbox(store: StateStore, n: int) -> list[str]:
    rids = []
    for i in range(n):
        rid = await store.create_request(f"p{i}")
        assert (await store.cancel_request(rid)).ok
        await store.record_inbox_event(rid, "job_result", "{}")
        rids.append(rid)
    return rids


# ---------------------------------------------------------------- RetentionSweeper


async def test_sweep_deletes_in_batches_and_counts(fake_redis_time, recorder, log):
    store = StateStore((await _client(fake_redis_time)), id_factory=counter_ids())
    fake_redis_time.set_ms(T0)
    rids = await cancelled_with_inbox(store, 3)
    pending = await store.create_request("still pending")
    dlid = "d" * 32
    await store.create_dead_letter(
        DeadLetterFields(dead_letter_id=dlid, reason="malformed", message_id="m-1")
    )
    await store.discard_dead_letter(dlid)
    r = store.client
    sweeper = RetentionSweeper(store, ManagerMetrics(recorder), FakeClock(), config())

    # Inbox TTL passed, request and DLQ TTL not yet: only inboxes go (Req 14.7).
    fake_redis_time.set_ms(T0 + 60_000)
    res = await sweeper.sample_once()
    assert res.completed and res.deleted == {"request": 0, "inbox": 2, "dlq_record": 0}
    assert [await r.exists(keys.inbox(x)) for x in rids] == [0, 0, 1]
    assert all([await store.get_status(x) is RequestStatus.CANCELLED for x in rids])

    # Every TTL passed: batch_size bounds each kind per sweep (Req 14.3, 14.6, 14.8).
    fake_redis_time.set_ms(T0 + 2 * HOUR_MS)
    res = await sweeper.sample_once()
    assert res.deleted == {"request": 2, "inbox": 1, "dlq_record": 1}
    res = await sweeper.sample_once()
    assert res.deleted == {"request": 1, "inbox": 0, "dlq_record": 0}
    for x in rids:
        for k in keys.request_keys(x):
            assert await r.exists(k) == 0, k
    assert await store.get_dead_letter(dlid) is None
    assert await store.get_status(pending) is RequestStatus.PENDING
    assert (deleted(recorder, "request"), deleted(recorder, "inbox")) == (3.0, 3.0)
    assert deleted(recorder, "dlq_record") == 1.0
    assert log.warnings == []


async def test_concurrent_sweepers_count_each_item_once(fake_redis_time, recorder, log):
    store = StateStore((await _client(fake_redis_time)), id_factory=counter_ids())
    fake_redis_time.set_ms(T0)
    await cancelled_with_inbox(store, 4)
    fake_redis_time.set_ms(T0 + 2 * HOUR_MS)
    metrics = ManagerMetrics(recorder)
    cfg = config(batch_size=10)
    a = RetentionSweeper(store, metrics, FakeClock(), cfg)
    b = RetentionSweeper(StateStore(store.client), metrics, FakeClock(), cfg)
    ra, rb = await asyncio.gather(a.sample_once(), b.sample_once())
    assert ra.deleted["request"] + rb.deleted["request"] == 4
    assert deleted(recorder, "request") == 4.0  # Req 14.10


class StubStore:
    """Candidates come from fixed lists; ``fail_on`` makes that purge raise."""

    def __init__(self, fail_on: str | None = None) -> None:
        self.fail_on = fail_on
        self.calls: list[str] = []

    async def server_time_ms(self) -> int:
        self.calls.append("time")
        return T0

    async def finished_candidates(self, now_ms, ttl_ms, limit):
        return ["r1", "r2", "r3"][:limit]

    async def inbox_candidates(self, now_ms, ttl_ms, limit):
        self.calls.append("inbox_candidates")
        return ["r9"]

    async def dlq_terminal_candidates(self, now_ms, ttl_ms, limit):
        return []

    async def purge_request(self, rid, ttl_ms):
        self.calls.append(f"purge_request:{rid}")
        if rid == self.fail_on:
            raise ConnectionError("redis down")
        return "DELETED"

    async def purge_inbox(self, rid, ttl_ms):
        self.calls.append(f"purge_inbox:{rid}")
        return "DELETED"

    async def purge_dead_letter(self, dlid, ttl_ms):  # pragma: no cover - no candidates
        return "KEEP"


async def test_store_error_abandons_rest_of_sweep_and_rate_limits_warning(recorder, log):
    store = StubStore(fail_on="r2")
    clock = FakeClock()
    sweeper = RetentionSweeper(store, ManagerMetrics(recorder), clock, config(batch_size=3))

    res = await sweeper.sample_once()
    assert not res.completed
    assert res.deleted["request"] == 1
    assert "purge_request:r3" not in store.calls
    assert "inbox_candidates" not in store.calls  # rest of the sweep skipped (Req 14.13)
    assert deleted(recorder, "request") == 1.0  # the failed purge is not counted
    assert [w[0] for w in log.warnings] == ["retention_sweep_failed"]

    await sweeper.sample_once()
    assert len(log.warnings) == 1  # at most one warning per 60 s
    clock.advance(60)
    await sweeper.sample_once()
    assert len(log.warnings) == 2


async def test_run_retries_next_interval_after_error(recorder, log):
    store = StubStore(fail_on="r1")
    clock = FakeClock()
    sweeper = RetentionSweeper(store, ManagerMetrics(recorder), clock, config(sweep_interval=30))
    task = asyncio.create_task(sweeper.run())
    for _ in range(20):
        await asyncio.sleep(0)
    sweeper.stop()
    await asyncio.wait_for(task, 1)
    assert store.calls.count("time") >= 2
    assert all(s == 30 for s in clock.sleeps)


class ExplodingStore:
    def __getattr__(self, name):  # any State_Store access is a test failure
        raise AssertionError(f"retention disabled but store.{name} was used")


async def test_disabled_sweeper_never_touches_the_store(recorder, log):
    clock = FakeClock()
    sweeper = RetentionSweeper(
        ExplodingStore(), ManagerMetrics(recorder), clock, config(enabled=False)
    )
    assert not sweeper.enabled
    await asyncio.wait_for(sweeper.run(), 1)  # returns immediately (Req 14.12)
    res = await sweeper.sample_once()
    assert res.deleted == {"request": 0, "inbox": 0, "dlq_record": 0}
    assert clock.sleeps == []


# ---------------------------------------------------------------- IndexBackfill


async def _client(fake_redis_time):
    from fakeredis import FakeServer
    from fakeredis.aioredis import FakeRedis

    return FakeRedis(server=FakeServer(), decode_responses=True)


async def seed_v1(store: StateStore) -> None:
    r = store.client
    await r.hset(
        keys.request("v1done"),
        mapping={"status": "Completed", "prompt": "x", "created_at": "1700000000.5"},
    )
    await r.hset(
        keys.request("v1run"),
        mapping={"status": "Running", "prompt": "y", "created_at": "1700000001"},
    )
    # Suffixed keys (and non-hash keys) are not Requests.
    await r.hset(keys.jobs("v1run"), mapping={"a": "{}"})
    await r.rpush(keys.transitions("v1run"), "{}")
    await r.set(keys.request("weird"), "not a hash")


async def test_backfill_indexes_v1_requests_and_sets_ready(fake_redis_time, log):
    store = StateStore(await _client(fake_redis_time))
    fake_redis_time.set_ms(T0 + 5000)
    await seed_v1(store)
    r = store.client
    assert sorted({x async for x in store.scan_request_ids(count=1)}) == ["v1done", "v1run"]

    backfill = IndexBackfill(store, FakeClock(), "manager-1")
    await asyncio.wait_for(backfill.run(), 2)

    assert await store.index_ready()
    assert await r.zscore(keys.REQUESTS_INDEX, "v1done") == T0 + 500
    assert await r.zscore(keys.requests_by_status("Running"), "v1run") == T0 + 1000
    # A terminal v1 Request gets Finished_At (Req 14.9).
    assert int(await r.hget(keys.request("v1done"), keys.F_FINISHED_AT)) == T0 + 5000
    assert await r.zcard(keys.REQUESTS_INDEX) == 2
    assert await r.exists(keys.REQUESTS_INDEX_BACKFILL_LOCK) == 0  # released
    assert await backfill.run_once()  # already ready: nothing to do


async def test_backfill_waits_while_another_agent_holds_the_lock(fake_redis_time, log):
    store = StateStore(await _client(fake_redis_time))
    await seed_v1(store)
    r = store.client
    assert await store.acquire_backfill_lock("other", 60_000)
    assert not await store.acquire_backfill_lock("manager-1", 60_000)  # NX
    assert 0 < await r.pttl(keys.REQUESTS_INDEX_BACKFILL_LOCK) <= 60_000

    clock = FakeClock()
    backfill = IndexBackfill(store, clock, "manager-1")
    assert not await backfill.run_once()
    assert not await store.index_ready()
    assert await r.zcard(keys.REQUESTS_INDEX) == 0

    # The other agent finishes: the waiting one sees the ready flag and stops.
    task = asyncio.create_task(backfill.run())
    await asyncio.sleep(0)
    await store.set_index_ready()
    await asyncio.wait_for(task, 2)
    assert await r.zcard(keys.REQUESTS_INDEX) == 0
    assert not await store.release_backfill_lock("manager-1")  # not the holder
    assert await r.get(keys.REQUESTS_INDEX_BACKFILL_LOCK) == "other"


class SlowIndexStore:
    """Delegates to a StateStore; each index_request takes 15 s of clock time."""

    def __init__(self, store: StateStore, clock: FakeClock) -> None:
        self._store = store
        self._clock = clock
        self.renewals: list[tuple[str, int]] = []

    def __getattr__(self, name):
        return getattr(self._store, name)

    async def index_request(self, rid: str) -> bool:
        for _ in range(15):
            await asyncio.sleep(0)
            self._clock.advance(1)
        return await self._store.index_request(rid)

    async def renew_backfill_lock(self, owner: str, ttl_ms: int) -> bool:
        self.renewals.append((owner, ttl_ms))
        return await self._store.renew_backfill_lock(owner, ttl_ms)


async def test_backfill_renews_lock_every_20_seconds(fake_redis_time, log):
    store = StateStore(await _client(fake_redis_time))
    await seed_v1(store)
    clock = FakeClock()
    slow = SlowIndexStore(store, clock)
    assert await IndexBackfill(slow, clock, "manager-1").run_once()
    assert slow.renewals and all(x == ("manager-1", 60_000) for x in slow.renewals)
    assert 20.0 in clock.sleeps
    assert await store.index_ready()


async def test_backfill_lock_renew_and_release_check_owner(state_store):
    r = state_store.client
    assert await state_store.acquire_backfill_lock("a", 1000)
    assert not await state_store.renew_backfill_lock("b", 60_000)
    assert await r.pttl(keys.REQUESTS_INDEX_BACKFILL_LOCK) <= 1000
    assert await state_store.renew_backfill_lock("a", 60_000)
    assert await r.pttl(keys.REQUESTS_INDEX_BACKFILL_LOCK) > 1000
    assert not await state_store.release_backfill_lock("b")
    assert await state_store.release_backfill_lock("a")
    assert await r.exists(keys.REQUESTS_INDEX_BACKFILL_LOCK) == 0
