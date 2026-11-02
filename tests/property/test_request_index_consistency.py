# Feature: das-observability-operations, Property 26: Lọc đúng danh sách Request và Request_Index nhất quán
"""Property 26: Request listing filters correctly and the Request_Index stays consistent.

For any sequence of State_Store operations (create, commit_plan, finalize Completed /
Failed, cancel, Replay that reopens a Failed Request, reopen, purge, index backfill
of a v1 Request) run through the real Lua scripts on fakeredis, after every operation:

- each ``das:requests:by_status:{s}`` holds exactly the rids whose current status is
  ``s`` and ``das:requests:index`` holds exactly the rids that still exist, all scored
  by the Request's created_at in ms (Req 15.11);
- for any valid filter (``status`` 1-5 values, optional ``created_after`` inclusive /
  ``created_before`` exclusive) every Request returned on every page satisfies every
  filter condition at read time (Req 15.2, 15.5).

**Validates: Requirements 15.2, 15.5, 15.11**
"""

from __future__ import annotations

import asyncio
import itertools
from dataclasses import dataclass, field

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import event, given, note
from hypothesis import strategies as st

from das.models import FinalResult, JobRecord, JobState, Lease, RequestStatus
from das.state import StateStore, TransitionBatch, keys
from tests.fixtures.redis_time import FakeRedisTime
from tests.unit.test_state_phase2_dlq import FINAL, fields
from tests.unit.test_state_store import PLAN

T0 = 1_700_000_000_000
STATUSES = list(RequestStatus)
COMPLETED = FinalResult(summary="done", jobs=[])

OPS = ["create", "commit", "complete", "fail", "cancel", "replay", "reopen", "purge", "v1",
       "reindex"]


@dataclass(frozen=True)
class Filter:
    statuses: tuple[RequestStatus, ...] | None
    created_after_ms: int | None
    created_before_ms: int | None
    limit: int


@dataclass(frozen=True)
class Op:
    kind: str
    target: int  # index into the rids created so far (modulo)
    tick_ms: int  # Redis time advance before the operation
    v1_status: RequestStatus
    query: Filter


def _bounds(pair: tuple[int | None, int | None]) -> tuple[int | None, int | None]:
    a, b = pair
    if a is not None and b is not None and a >= b:
        a, b = b, a + 1 if a == b else a
    return a, b


filters = st.builds(
    lambda statuses, window, limit: Filter(statuses, *_bounds(window), limit),
    st.none() | st.lists(st.sampled_from(STATUSES), min_size=1, max_size=5).map(tuple),
    st.tuples(
        st.none() | st.integers(T0 - 1, T0 + 40),
        st.none() | st.integers(T0 - 1, T0 + 40),
    ),
    st.integers(1, 5),
)

ops = st.builds(
    Op,
    # Lifecycle steps weighted up so Failed -> Replay -> reopen chains are reached often.
    kind=st.sampled_from([*OPS, "commit", "fail", "fail", "replay", "replay", "reopen"]),
    target=st.integers(0, 50),
    tick_ms=st.integers(0, 2),
    v1_status=st.sampled_from(STATUSES),
    query=filters,
)


@dataclass
class Harness:
    store: StateStore
    clock: FakeRedisTime
    now_ms: int = T0
    rids: list[str] = field(default_factory=list)
    leases: dict[str, Lease] = field(default_factory=dict)
    has_plan: set[str] = field(default_factory=set)
    attempts: dict[str, int] = field(default_factory=dict)
    dead_letter: dict[str, str] = field(default_factory=dict)  # rid -> Pending dlid
    pending_replay: dict[str, str] = field(default_factory=dict)  # rid -> unapplied dlid
    dlids: itertools.count = field(default_factory=itertools.count)
    v1_ids: itertools.count = field(default_factory=itertools.count)

    async def status(self, rid: str) -> RequestStatus | None:
        return await self.store.get_status(rid)

    async def lease_for(self, rid: str) -> Lease:
        res = await self.store.acquire_lease(rid, "m1", 3600)
        assert res.ok, res
        self.leases[rid] = res.lease
        return res.lease

    async def apply_pending_replay(self, rid: str, lease: Lease) -> None:
        dlid = self.pending_replay.pop(rid, None)
        if dlid is not None:
            batch = TransitionBatch(replays_applied=[dlid])
            assert (await self.store.apply_transition(lease, batch)).ok

    def applicable(self, kind: str, rid: str, status: RequestStatus | None) -> bool:
        running_owned = status is RequestStatus.RUNNING and rid in self.leases
        return {
            "commit": status is RequestStatus.PENDING and rid not in self.has_plan,
            "complete": running_owned,
            "fail": running_owned,
            "cancel": status in (RequestStatus.PENDING, RequestStatus.RUNNING),
            "replay": status is RequestStatus.FAILED and rid in self.dead_letter,
            "reopen": status is RequestStatus.PENDING and rid in self.has_plan,
            "purge": True,
            "reindex": True,
        }[kind]

    async def pick(self, op: Op) -> str | None:
        """The ``op.target``-th rid (mod count) whose state allows ``op.kind``."""
        candidates = [
            rid for rid in self.rids if self.applicable(op.kind, rid, await self.status(rid))
        ]
        return candidates[op.target % len(candidates)] if candidates else None

    async def run(self, op: Op) -> str:
        """Apply ``op`` when its precondition holds; returns what was actually done."""
        self.now_ms += op.tick_ms
        self.clock.set_ms(self.now_ms)
        s = self.store
        if op.kind == "create" or not self.rids:
            self.rids.append(await s.create_request("hello"))
            return "create"
        if op.kind == "v1":
            # A Request as written by v1 (no index entry, no finished_at), then backfilled.
            rid = f"v1-{next(self.v1_ids):03d}"
            await s.client.hset(
                keys.request(rid),
                mapping={
                    "status": op.v1_status.value,
                    "prompt": "old",
                    "created_at": f"{self.now_ms / 1000:.6f}",
                },
            )
            assert await s.index_request(rid)
            self.rids.append(rid)
            return "v1"
        rid = await self.pick(op)
        if rid is None:
            return "noop"
        status = await self.status(rid)
        lease = self.leases.get(rid)
        if op.kind == "commit" and status is RequestStatus.PENDING and rid not in self.has_plan:
            lease = await self.lease_for(rid)
            assert (await s.commit_plan(lease, PLAN)).ok
            self.has_plan.add(rid)
            return "commit"
        if op.kind == "complete" and status is RequestStatus.RUNNING and lease is not None:
            await self.apply_pending_replay(rid, lease)
            assert (await s.finalize(lease, COMPLETED)).ok
            del self.leases[rid]
            return "complete"
        if op.kind == "fail" and status is RequestStatus.RUNNING and lease is not None:
            await self.apply_pending_replay(rid, lease)
            attempt = self.attempts.get(rid, 0) + 1
            self.attempts[rid] = attempt
            job = JobRecord(job=PLAN.jobs[0], state=JobState.FAILED, attempt=attempt, reason="x")
            assert (await s.apply_transition(lease, TransitionBatch(jobs={"a": job}))).ok
            assert (await s.fail_request(lease, "job a failed", FINAL)).ok
            del self.leases[rid]
            dlid = f"{next(self.dlids):032d}"
            assert (await s.create_dead_letter(fields(dlid, rid, attempt=attempt))).created
            self.dead_letter[rid] = dlid
            return "fail"
        if op.kind == "cancel" and status in (RequestStatus.PENDING, RequestStatus.RUNNING):
            assert (await s.cancel_request(rid)).ok
            self.leases.pop(rid, None)
            self.pending_replay.pop(rid, None)
            return "cancel"
        if op.kind == "replay" and status is RequestStatus.FAILED and rid in self.dead_letter:
            dlid = self.dead_letter.pop(rid)
            res = await s.replay_dead_letter(dlid)
            assert res.accepted, res
            self.pending_replay[rid] = dlid
            return "replay"
        if op.kind == "reopen" and status is RequestStatus.PENDING and rid in self.has_plan:
            lease = await self.lease_for(rid)
            assert (await s.reopen_request(lease)).ok
            return "reopen"
        if op.kind == "purge":
            outcome = await s.purge_request(rid, 0)
            return f"purge:{outcome}"
        if op.kind == "reindex":
            await s.index_request(rid)
            return "reindex"
        return "noop"


def created_ms(raw: str) -> int:
    sec, _, frac = raw.partition(".")
    return int(sec) * 1000 + int((frac + "000")[:3])


async def check_index(h: Harness) -> dict[str, tuple[RequestStatus, int]]:
    """Req 15.11: index and by_status sets match the stored Requests exactly."""
    r = h.store.client
    truth: dict[str, tuple[RequestStatus, int]] = {}
    for rid in h.rids:
        status, created = await r.hmget(keys.request(rid), keys.F_STATUS, "created_at")
        if status is not None:
            truth[rid] = (RequestStatus(status), created_ms(created))
    index = dict(await r.zrange(keys.REQUESTS_INDEX, 0, -1, withscores=True))
    assert index == {rid: ms for rid, (_s, ms) in truth.items()}
    for s in STATUSES:
        members = dict(await r.zrange(keys.requests_by_status(s.value), 0, -1, withscores=True))
        assert members == {rid: ms for rid, (st_, ms) in truth.items() if st_ is s}, s
    return truth


async def check_filter(h: Harness, q: Filter, truth: dict) -> int:
    """Req 15.2, 15.5: every item on every page satisfies every filter condition."""
    after = None
    returned = 0
    for _ in range(200):
        page = await h.store.list_requests(
            statuses=q.statuses,
            created_after_ms=q.created_after_ms,
            created_before_ms=q.created_before_ms,
            limit=q.limit,
            after=after,
        )
        assert len(page.items) <= q.limit
        for it in page.items:
            assert it.request_id in truth
            status, ms = truth[it.request_id]
            assert it.status is status  # current status at read time
            assert it.created_at_ms == ms
            if q.statuses is not None:
                assert status in q.statuses
            if q.created_after_ms is not None:
                assert ms >= q.created_after_ms
            if q.created_before_ms is not None:
                assert ms < q.created_before_ms
        returned += len(page.items)
        if page.next_after is None:
            return returned
        after = page.next_after
    raise AssertionError("pagination did not end")


async def scenario(sequence: list[Op]) -> None:
    with FakeRedisTime(start=T0 / 1000) as clock:
        store = StateStore(FakeRedis(server=FakeServer(), decode_responses=True))
        try:
            h = Harness(store, clock)
            for op in sequence:
                done = await h.run(op)
                event(done)
                note(f"{op.kind} -> {done}; query={op.query}")
                truth = await check_index(h)
                returned = await check_filter(h, op.query, truth)
                event("non-empty listing" if returned else "empty listing")
        finally:
            await store.client.aclose()


@given(st.lists(ops, min_size=10, max_size=40))
def test_listing_filters_and_request_index_consistent(sequence: list[Op]) -> None:
    asyncio.run(scenario(sequence))
