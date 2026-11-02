# Feature: das-observability-operations, Property 25: Liveness của retention và đếm đúng một lần
"""Property 25: retention liveness and exactly-once counting.

For any state with N expired terminal Requests (Completed, Failed with a Discarded
Dead_Letter_Record, Cancelled; with or without an inbox stream), ``batch_size`` 1-50
and 1-10 Manager_Agents sweeping concurrently with a reachable State_Store (fakeredis
running the real ``purge_*`` Lua scripts, Redis ``TIME`` from ``FakeRedisTime``):

- after ``ceil(N / batch_size) + 1`` sweep intervals no expired Request is left, in
  any interval at least one Manager sweeps and the others may or may not (Req 14.6);
- the sum of ``das_retention_deleted_total{kind}`` over every Manager equals the
  number of items actually deleted per ``kind``, observed independently as the key
  existing before and gone after the purge call (Req 14.10).

Draining is not modelled separately: ``ManagerAgent.drain`` pauses the ClaimLoop
only and never stops the ``RetentionSweeper`` loop, so a Draining Manager sweeps
exactly like an Available one.

Noise that must not slow liveness down: Pending Requests and fresh terminal
Requests (their inbox may expire during the run and is counted as ``inbox``).

**Validates: Requirements 14.6, 14.10**
"""

from __future__ import annotations

import asyncio
import itertools
import math
import random
from dataclasses import dataclass, field

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import event, given, note
from hypothesis import strategies as st

from das.clock import FakeClock
from das.config.schema import RetentionConfig
from das.manager.retention import RetentionSweeper
from das.models import FinalResult, JobRecord, JobState, RequestStatus
from das.observability.metrics import ManagerMetrics, MetricsRecorder
from das.state import StateStore, TransitionBatch, keys
from tests.fixtures.redis_time import FakeRedisTime
from tests.unit.test_state_phase2_dlq import FINAL, fields
from tests.unit.test_state_store import PLAN

T0 = 1_700_000_000_000
REQUEST_TTL_S = 3600
INBOX_TTL_S = 60
DLQ_TTL_S = 3600
SWEEP_INTERVAL_S = 10
KINDS = ("request", "inbox", "dlq_record")
COMPLETED = FinalResult(summary="done", jobs=[])


class _NullLog:
    def warning(self, *a, **k) -> None: ...
    def info(self, *a, **k) -> None: ...
    def exception(self, *a, **k) -> None: ...


@dataclass(frozen=True)
class Item:
    status: RequestStatus  # terminal status to reach
    inbox: bool
    discard_dlq: bool  # Failed only: Discarded Dead_Letter_Record (else stays Pending)


@dataclass(frozen=True)
class Scenario:
    expired: list[Item]
    fresh: list[Item]
    pending: int
    batch_size: int
    managers: int
    # One bitmask per interval: which Managers sweep in it (made non-empty).
    rounds: list[int]
    seed: int


TERMINAL = [RequestStatus.COMPLETED, RequestStatus.FAILED, RequestStatus.CANCELLED]
items = st.builds(Item, st.sampled_from(TERMINAL), st.booleans(), st.booleans())


@st.composite
def scenarios(draw) -> Scenario:
    batch = draw(st.integers(1, 50))
    expired = draw(st.lists(items, max_size=0) | st.lists(items, min_size=1, max_size=40))
    n_rounds = math.ceil(len(expired) / batch) + 1
    managers = draw(st.integers(1, 10))
    return Scenario(
        expired=expired,
        fresh=draw(st.lists(items, max_size=4)),
        pending=draw(st.integers(0, 3)),
        batch_size=batch,
        managers=managers,
        rounds=draw(
            st.lists(
                st.integers(0, 2**managers - 1), min_size=n_rounds, max_size=n_rounds
            )
        ),
        seed=draw(st.integers(0, 2**32 - 1)),
    )


class ObservedStore:
    """``StateStore`` view for one Manager: purge calls are recorded as truth.

    Each purge runs under a shared lock with a key-existence check before and after,
    so "actually deleted" is observed from Redis, not from the script reply. Random
    yields before the lock and in the candidate reads interleave the Managers.
    """

    def __init__(self, store: StateStore, lock: asyncio.Lock, truth: dict, rng: random.Random):
        self._s = store
        self._lock = lock
        self._truth = truth
        self._rng = rng

    async def _yield(self) -> None:
        for _ in range(self._rng.randint(0, 3)):
            await asyncio.sleep(0)

    async def server_time_ms(self) -> int:
        await self._yield()
        return await self._s.server_time_ms()

    async def finished_candidates(self, now_ms, ttl_ms, limit):
        await self._yield()
        return await self._s.finished_candidates(now_ms, ttl_ms, limit)

    async def inbox_candidates(self, now_ms, ttl_ms, limit):
        await self._yield()
        return await self._s.inbox_candidates(now_ms, ttl_ms, limit)

    async def dlq_terminal_candidates(self, now_ms, ttl_ms, limit):
        await self._yield()
        return await self._s.dlq_terminal_candidates(now_ms, ttl_ms, limit)

    async def _observed(self, kind: str, key: str, call) -> str:
        await self._yield()
        r = self._s.client
        async with self._lock:
            before = await r.exists(key)
            reply = await call()
            after = await r.exists(key)
        if before and not after:
            self._truth[kind] += 1
        return reply

    async def purge_request(self, rid: str, ttl_ms: int) -> str:
        return await self._observed(
            "request", keys.request(rid), lambda: self._s.purge_request(rid, ttl_ms)
        )

    async def purge_inbox(self, rid: str, ttl_ms: int) -> str:
        return await self._observed(
            "inbox", keys.inbox(rid), lambda: self._s.purge_inbox(rid, ttl_ms)
        )

    async def purge_dead_letter(self, dlid: str, ttl_ms: int) -> str:
        return await self._observed(
            "dlq_record", keys.dlq(dlid), lambda: self._s.purge_dead_letter(dlid, ttl_ms)
        )


@dataclass
class Builder:
    store: StateStore
    dlids: itertools.count = field(default_factory=itertools.count)

    async def make(self, item: Item) -> str:
        s = self.store
        rid = await s.create_request("hello")
        if item.inbox:
            await s.record_inbox_event(rid, "job_result", "{}")
        if item.status is RequestStatus.CANCELLED:
            assert (await s.cancel_request(rid)).ok
            return rid
        acq = await s.acquire_lease(rid, "builder", 3600)
        assert acq.ok, acq
        lease = acq.lease
        assert (await s.commit_plan(lease, PLAN)).ok
        if item.status is RequestStatus.COMPLETED:
            assert (await s.finalize(lease, COMPLETED)).ok
            return rid
        job = JobRecord(job=PLAN.jobs[0], state=JobState.FAILED, attempt=1, reason="x")
        assert (await s.apply_transition(lease, TransitionBatch(jobs={"a": job}))).ok
        assert (await s.fail_request(lease, "job a failed", FINAL)).ok
        dlid = f"{next(self.dlids):032d}"
        assert (await s.create_dead_letter(fields(dlid, rid))).created
        if item.discard_dlq:
            assert (await s.discard_dead_letter(dlid)).outcome == "accepted"
        return rid


def deleted(rec: MetricsRecorder, kind: str) -> float:
    return rec.registry.get_sample_value("das_retention_deleted_total", {"kind": kind}) or 0.0


async def scenario(sc: Scenario) -> None:
    rng = random.Random(sc.seed)
    with FakeRedisTime(start=T0 / 1000) as redis_time:
        store = StateStore(FakeRedis(server=FakeServer(), decode_responses=True))
        try:
            b = Builder(store)
            expired = [await b.make(it) for it in sc.expired]
            # Past every TTL for the expired set; fresh noise finishes now.
            now_ms = T0 + REQUEST_TTL_S * 1000 + 1000
            redis_time.set_ms(now_ms)
            fresh = [await b.make(it) for it in sc.fresh]
            pending = [await store.create_request("p") for _ in range(sc.pending)]

            cfg = RetentionConfig(
                enabled=True,
                terminal_request_ttl=REQUEST_TTL_S,
                inbox_ttl=INBOX_TTL_S,
                dlq_record_ttl=DLQ_TTL_S,
                sweep_interval=SWEEP_INTERVAL_S,
                batch_size=sc.batch_size,
            )
            lock = asyncio.Lock()
            truth = dict.fromkeys(KINDS, 0)
            recorders = [
                MetricsRecorder(clock=FakeClock(), log=_NullLog()) for _ in range(sc.managers)
            ]
            sweepers = [
                RetentionSweeper(
                    ObservedStore(StateStore(store.client), lock, truth, rng),
                    ManagerMetrics(rec),
                    FakeClock(),
                    cfg,
                )
                for rec in recorders
            ]
            reported = dict.fromkeys(KINDS, 0)
            for i, mask in enumerate(sc.rounds):
                active = [m for m in range(sc.managers) if mask >> m & 1] or [
                    mask % sc.managers
                ]
                rng.shuffle(active)
                results = await asyncio.gather(*(sweepers[m].sample_once() for m in active))
                for res in results:
                    assert res.completed  # State_Store reachable: every sweep finishes
                    for k in KINDS:
                        reported[k] += res.deleted[k]
                note(f"interval {i}: managers {active}, truth {truth}")
                redis_time.advance(SWEEP_INTERVAL_S)

            # Req 14.6: liveness within ceil(N / batch_size) + 1 intervals.
            r = store.client
            for rid in expired:
                for k in keys.request_keys(rid):
                    assert await r.exists(k) == 0, (rid, k)
            left = await store.finished_candidates(
                await store.server_time_ms(), REQUEST_TTL_S * 1000, 10_000
            )
            assert set(left).isdisjoint(expired), left
            # Noise is untouched (not expired, or not terminal).
            for rid in fresh:
                assert await store.get_status(rid) is not None
            for rid in pending:
                assert await store.get_status(rid) is RequestStatus.PENDING

            # Req 14.10: counted exactly once across the Manager_Group.
            assert truth["request"] == len(expired)
            counted = {k: sum(deleted(rec, k) for rec in recorders) for k in KINDS}
            assert counted == {k: float(v) for k, v in truth.items()}, (counted, truth)
            assert reported == truth
            event(f"managers={sc.managers}")
            event("expired" if expired else "no expired")
            if truth["inbox"]:
                event("inbox deleted")
            if truth["dlq_record"]:
                event("dlq_record deleted")
        finally:
            await store.client.aclose()


@given(scenarios())
def test_retention_liveness_and_exactly_once_count(sc: Scenario) -> None:
    asyncio.run(scenario(sc))
