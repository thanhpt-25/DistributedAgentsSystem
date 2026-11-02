# Feature: das-client-api, Property 3: Active_Set equals non-terminal Requests
"""Property 3: Active_Set equals non-terminal Requests.

For any sequence of 1 to 20 operations (real ``das/state/lua`` scripts through
``StateStore`` on a fresh fakeredis server per example, ``TIME`` from
:class:`FakeRedisTime`):

- ``create(fp)`` with fp from a pool of two fingerprints (``submit_request``) or
  ``None`` (a pre-upgrade Request through ``create_request``);
- ``finalize(i, Completed|Failed)`` (planning first when needed; a Failed finalize
  leaves Job ``a`` Failed so the Request can be replayed);
- ``cancel(i)``;
- ``replay(i)`` of a Dead_Letter_Record for the Failed Job of a Failed Request, which
  reopens it (skipped for any other Request);
- ``purge(i)`` with Redis time advanced past the terminal TTL,

after every operation each ``das:client:{fp}:active`` equals the set of that fp's
Requests that still exist and are non-terminal, and Requests without ``client_fp``
are in no Active_Set.

**Validates: Requirements 3.1, 3.3, 3.4, 3.5, 3.6, 3.7**
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import event, given
from hypothesis import strategies as st

from das.models import FinalResult, JobRecord, JobState, RequestStatus
from das.state import (
    PURGE_DELETED,
    PURGE_KEEP,
    ClientRequestSpec,
    StateStore,
    StoreStatus,
    TransitionBatch,
    keys,
)
from tests.fixtures.redis_time import FakeRedisTime
from tests.unit.test_state_phase2_dlq import fields
from tests.unit.test_state_store import PLAN

FPS = ("aaaaaaaa", "bbbbbbbb")
TTL_MS = 60_000
TERMINAL = {RequestStatus.COMPLETED, RequestStatus.FAILED, RequestStatus.CANCELLED}
FINAL = FinalResult(summary="done", jobs=[])

ops = st.lists(
    st.one_of(
        st.tuples(st.just("create"), st.sampled_from([*FPS, None])),
        st.tuples(st.just("finalize"), st.integers(0, 19),
                  st.sampled_from([RequestStatus.COMPLETED, RequestStatus.FAILED])),
        st.tuples(st.just("cancel"), st.integers(0, 19)),
        st.tuples(st.just("replay"), st.integers(0, 19)),
        st.tuples(st.just("purge"), st.integers(0, 19)),
    ),
    min_size=1,
    max_size=20,
)


@dataclass
class Req:
    rid: str
    fp: str | None
    status: RequestStatus = RequestStatus.PENDING
    exists: bool = True
    has_plan: bool = False
    failed_attempt: int | None = None  # attempt of Job ``a`` while it is Failed
    pending_replays: list[str] = field(default_factory=list)


class Scenario:
    def __init__(self, store: StateStore, clock: FakeRedisTime) -> None:
        self.store = store
        self.clock = clock
        self.reqs: list[Req] = []
        self.dl_seq = 0

    async def create(self, fp: str | None) -> None:
        if fp is None:
            rid = await self.store.create_request("p")
        else:
            res = await self.store.submit_request("p", ClientRequestSpec(fingerprint=fp))
            assert res.outcome == "created" and res.request_id is not None
            rid = res.request_id
        self.reqs.append(Req(rid, fp))

    async def finalize(self, r: Req, target: RequestStatus) -> None:
        if not r.exists or r.status in TERMINAL:
            return
        lease = (await self.store.acquire_lease(r.rid, "m1", 3600.0)).lease
        assert lease is not None
        if r.has_plan:  # reopened by a Replay
            assert (await self.store.reopen_request(lease)).ok
        else:
            assert (await self.store.commit_plan(lease, PLAN)).ok
            r.has_plan = True
        if target is RequestStatus.FAILED:
            attempt = (r.failed_attempt or 0) + 1
            failed = JobRecord(job=PLAN.jobs[0], state=JobState.FAILED, attempt=attempt, reason="x")
            batch = TransitionBatch(jobs={"a": failed}, replays_applied=tuple(r.pending_replays))
            assert (await self.store.apply_transition(lease, batch)).ok
            r.failed_attempt = attempt
            assert (await self.store.fail_request(lease, "boom")).ok
        else:
            batch = TransitionBatch(replays_applied=tuple(r.pending_replays))
            assert (await self.store.apply_transition(lease, batch)).ok
            r.failed_attempt = None
            assert (await self.store.finalize(lease, FINAL)).ok
        r.pending_replays = []
        r.status = target

    async def cancel(self, r: Req) -> None:
        res = await self.store.cancel_request(r.rid)
        if not r.exists:
            assert res.status is StoreStatus.NOT_FOUND
        elif r.status in TERMINAL:
            assert res.status is StoreStatus.TERMINAL
        else:
            assert res.ok
            r.status = RequestStatus.CANCELLED
            r.pending_replays = []

    async def replay(self, r: Req) -> None:
        if not r.exists or r.status is not RequestStatus.FAILED or r.failed_attempt is None:
            return
        self.dl_seq += 1
        dlid = f"{self.dl_seq:032x}"
        assert (await self.store.create_dead_letter(fields(dlid, r.rid, attempt=r.failed_attempt))).created
        assert (await self.store.replay_dead_letter(dlid)).outcome == "accepted"
        r.status = RequestStatus.PENDING
        r.pending_replays.append(dlid)
        event("replay reopened a Failed Request")

    async def purge(self, r: Req) -> None:
        self.clock.advance(TTL_MS / 1000 + 1)
        outcome = await self.store.purge_request(r.rid, TTL_MS)
        if r.exists and r.status in TERMINAL:
            assert outcome == PURGE_DELETED
            r.exists = False
            event(f"purged a {'client' if r.fp else 'pre-upgrade'} Request")
        else:
            assert outcome == PURGE_KEEP

    async def check(self) -> None:
        r = self.store.client
        for req in self.reqs:
            got = await self.store.get_status(req.rid)
            assert got == (req.status if req.exists else None), req
        for fp in FPS:
            expected = {q.rid for q in self.reqs if q.fp == fp and q.exists and q.status not in TERMINAL}
            assert await r.smembers(keys.client_active(fp)) == expected, fp
        allowed = {keys.client_active(fp) for fp in FPS}
        assert set(await r.keys(keys.client_active("*"))) <= allowed


async def _run(operations: list[tuple], clock: FakeRedisTime) -> None:
    sc = Scenario(StateStore(FakeRedis(server=FakeServer(), decode_responses=True)), clock)
    for op in operations:
        name = op[0]
        if name == "create":
            await sc.create(op[1])
        elif sc.reqs:
            req = sc.reqs[op[1] % len(sc.reqs)]
            if name == "finalize":
                await sc.finalize(req, op[2])
            else:
                await getattr(sc, name)(req)
        await sc.check()


@given(operations=ops)
def test_active_set_equals_non_terminal_requests(operations: list[tuple]) -> None:
    with FakeRedisTime() as clock:
        asyncio.run(_run(operations, clock))
