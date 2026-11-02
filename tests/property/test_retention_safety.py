# Feature: das-observability-operations, Property 24: An toàn của retention
"""Property 24: retention safety.

For any interleaving of Request creation, state transitions (commit_plan, reopen,
finalize Completed / Failed, cancel), Replay / Discard of Dead_Letter_Records, Redis
clock advances, removal of ``finished_at`` (a v1 Request) and ``RetentionSweeper``
sweeps on 1 to 10 Managers (separate clients on one fakeredis server, real Lua
scripts, ``TIME`` from :class:`FakeRedisTime`):

- no Request that is Pending or Running when ``purge_request`` runs is deleted, and no
  terminal Request is deleted before ``finished_at + terminal_request_ttl``; a
  terminal Request past that point *is* deleted by the script (exact outcome);
- a deleted Request leaves no key behind (``request_keys``, the Request_Index,
  ``finished``, ``inbox_purge``, ``pending`` and ``leases:expiry``), checked right
  after the script returns;
- a terminal Request without ``finished_at`` is stamped with Redis ``TIME`` and not
  deleted in the same sweep;
- an inbox stream is only deleted for a terminal Request past ``inbox_ttl``, and every
  other key of that Request stays;
- a ``Pending`` Dead_Letter_Record is never deleted; a deleted record is gone from
  ``list_dead_letters`` (``GET /v1/admin/dlq``).

Operations are also injected *between* a sweeper's candidate selection and its
``purge_*`` call (e.g. a Replay reopening a Failed candidate, or a re-finish), which
is the race the scripts' own ``TIME`` re-check guards against (Req 14.4).

**Validates: Requirements 14.3, 14.4, 14.5, 14.7, 14.8, 14.9**
"""

from __future__ import annotations

import asyncio
import itertools
from dataclasses import dataclass, field

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import event, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, rule

from das.clock import FakeClock
from das.config.schema import RetentionConfig
from das.manager.retention import RetentionSweeper
from das.models import FinalResult, JobRecord, JobState, Lease, RequestStatus
from das.observability.metrics import ManagerMetrics, NoopRecorder
from das.state import (
    PURGE_DELETED,
    PURGE_KEEP,
    PURGE_STAMPED,
    StateStore,
    TransitionBatch,
    keys,
)
from das.state.store import purge_request_keys
from tests.fixtures.redis_time import FakeRedisTime
from tests.unit.test_state_phase2_dlq import FINAL, fields
from tests.unit.test_state_store import PLAN, result

T0_MS = 1_700_000_000_000
TERMINAL = {RequestStatus.COMPLETED, RequestStatus.FAILED, RequestStatus.CANCELLED}
COMPLETED = FinalResult(summary="done", jobs=[])
DLQ_STATUSES = ("Pending", "Replayed", "Discarded")

OPS = (
    "create", "commit", "reopen", "complete", "fail", "cancel", "replay", "discard",
    "clear_finished", "orphan_dlq",
)
ops = st.tuples(st.sampled_from(OPS), st.integers(0, 50))


@dataclass
class Req:
    status: RequestStatus = RequestStatus.PENDING
    finished: int | None = None
    inbox: bool = True
    has_plan: bool = False
    attempt: int = 0
    pending_replays: set[str] = field(default_factory=set)


@dataclass
class DL:
    rid: str | None
    status: str = "Pending"
    changed: int | None = None


class GuardedStore:
    """``RetentionStore`` for one Manager: candidates pass through; each ``purge_*``
    first runs any injected operations, then checks the script's reply against the
    model under the machine's lock (so model and Redis never diverge mid-check)."""

    def __init__(self, machine: RetentionSafety, store: StateStore) -> None:
        self.m = machine
        self.s = store

    async def server_time_ms(self) -> int:
        return await self.s.server_time_ms()

    async def finished_candidates(self, now_ms: int, ttl_ms: int, limit: int) -> list[str]:
        return await self.s.finished_candidates(now_ms, ttl_ms, limit)

    async def inbox_candidates(self, now_ms: int, ttl_ms: int, limit: int) -> list[str]:
        return await self.s.inbox_candidates(now_ms, ttl_ms, limit)

    async def dlq_terminal_candidates(self, now_ms: int, ttl_ms: int, limit: int) -> list[str]:
        return await self.s.dlq_terminal_candidates(now_ms, ttl_ms, limit)

    async def _guarded(self, candidate: str | None, check, item: str, ttl_ms: int) -> str:
        async with self.m.lock:
            try:
                await self.m.run_injected(candidate)
                return await check(self.s, item, ttl_ms)
            except BaseException as exc:  # the sweeper swallows errors; keep them
                self.m.errors.append(exc)
                raise

    async def purge_request(self, rid: str, ttl_ms: int) -> str:
        return await self._guarded(rid, self.m.checked_purge_request, rid, ttl_ms)

    async def purge_inbox(self, rid: str, ttl_ms: int) -> str:
        return await self._guarded(rid, self.m.checked_purge_inbox, rid, ttl_ms)

    async def purge_dead_letter(self, dlid: str, ttl_ms: int) -> str:
        return await self._guarded(None, self.m.checked_purge_dead_letter, dlid, ttl_ms)


class RetentionSafety(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.now_ms = T0_MS
        self.rids = itertools.count(1)
        self.errors: list[BaseException] = []  # raised inside a sweep (it swallows them)
        self.redis_time = FakeRedisTime(start=T0_MS / 1000).install()
        self.loop = asyncio.new_event_loop()
        self.server = FakeServer()
        self.clients: list[FakeRedis] = []
        self.store = self._new_store()  # the "API / Manager logic" side
        self.sweepers: list[RetentionSweeper] = []
        self.cfg: RetentionConfig | None = None
        self.lock: asyncio.Lock | None = None
        self.reqs: dict[str, Req] = {}
        self.deleted: set[str] = set()
        self.dls: dict[str, DL] = {}
        self.dl_deleted: set[str] = set()
        self.leases: dict[str, Lease] = {}
        self.injected: list[tuple[str, int]] = []
        self.stamped_this_sweep: set[str] = set()
        self.focus: str | None = None  # restricts op() targets to the candidate in a race
        self.dlids = itertools.count()

    # ------------------------------------------------------------------ plumbing
    def _new_store(self) -> StateStore:
        client = FakeRedis(server=self.server, decode_responses=True)
        self.clients.append(client)
        # Deterministic Request_IDs: candidates tie on finished_at and are then ordered
        # by Request_ID, so random IDs would make replays of a test case diverge.
        return StateStore(client, id_factory=lambda: f"{next(self.rids):032x}")

    def run(self, coro):
        return self.loop.run_until_complete(coro)

    def teardown(self) -> None:
        try:
            for c in self.clients:
                self.run(c.aclose())
            self.loop.close()
        finally:
            self.redis_time.uninstall()

    @property
    def r(self) -> FakeRedis:
        return self.store.client

    def advance_ms(self, ms: int) -> None:
        self.now_ms += ms
        self.redis_time.set_ms(self.now_ms)

    # ------------------------------------------------------------------ setup
    @initialize(
        managers=st.integers(1, 10),
        request_ttl=st.sampled_from([3600, 7200]),
        inbox_ttl=st.sampled_from([60, 600, 3600]),
        dlq_ttl=st.sampled_from([3600, 7200]),
        batch=st.integers(1, 5),
    )
    def setup(self, managers: int, request_ttl: int, inbox_ttl: int, dlq_ttl: int, batch: int):
        self.cfg = RetentionConfig(
            enabled=True,
            terminal_request_ttl=request_ttl,
            inbox_ttl=inbox_ttl,
            dlq_record_ttl=dlq_ttl,
            batch_size=batch,
        )
        self.lock = asyncio.Lock()
        for _ in range(managers):
            self.sweepers.append(
                RetentionSweeper(
                    GuardedStore(self, self._new_store()),
                    ManagerMetrics(NoopRecorder()),
                    FakeClock(),
                    self.cfg,
                )
            )

    # ------------------------------------------------------------------ operations
    def _alive(self, pred) -> list[str]:
        return [
            rid for rid, q in self.reqs.items()
            if pred(q) and (self.focus is None or rid == self.focus)
        ]

    def _pick(self, items: list[str], target: int) -> str | None:
        return items[target % len(items)] if items else None

    async def _lease(self, rid: str) -> Lease:
        res = await self.store.acquire_lease(rid, "m1", 3600)
        if res.ok:
            self.leases[rid] = res.lease
        assert rid in self.leases, res
        return self.leases[rid]

    async def _apply_pending_replays(self, rid: str, lease: Lease) -> None:
        q = self.reqs[rid]
        if q.pending_replays:
            batch = TransitionBatch(replays_applied=sorted(q.pending_replays))
            assert (await self.store.apply_transition(lease, batch)).ok
            q.pending_replays.clear()

    async def op(self, kind: str, target: int) -> str:
        s = self.store
        P, R = RequestStatus.PENDING, RequestStatus.RUNNING
        if kind == "create" or not self.reqs:
            rid = await s.create_request("hello")
            assert await s.record_result_inbox(rid, result(rid, "a")) is not None
            self.reqs[rid] = Req()
            return "create"
        if kind == "commit":
            rid = self._pick(self._alive(lambda q: q.status is P and not q.has_plan), target)
            if rid is None:
                return "noop"
            assert (await s.commit_plan(await self._lease(rid), PLAN)).ok
            self.reqs[rid].status, self.reqs[rid].has_plan = R, True
            return kind
        if kind == "reopen":
            rid = self._pick(self._alive(lambda q: q.status is P and q.has_plan), target)
            if rid is None:
                return "noop"
            assert (await s.reopen_request(await self._lease(rid))).ok
            self.reqs[rid].status = R
            return kind
        if kind in ("complete", "fail"):
            rid = self._pick(self._alive(lambda q: q.status is R), target)
            if rid is None:
                return "noop"
            q = self.reqs[rid]
            lease = await self._lease(rid)
            await self._apply_pending_replays(rid, lease)
            if kind == "complete":
                res = await s.finalize(lease, COMPLETED)
                assert res.ok, res
                q.status = RequestStatus.COMPLETED
            else:
                q.attempt += 1
                job = JobRecord(job=PLAN.jobs[0], state=JobState.FAILED, attempt=q.attempt,
                                reason="x")
                assert (await s.apply_transition(lease, TransitionBatch(jobs={"a": job}))).ok
                res = await s.fail_request(lease, "job a failed", FINAL)
                assert res.ok, res
                q.status = RequestStatus.FAILED
                dlid = f"{next(self.dlids):032d}"
                assert (await s.create_dead_letter(fields(dlid, rid, attempt=q.attempt))).created
                self.dls[dlid] = DL(rid)
            assert res.finished_at_ms == self.now_ms
            q.finished = self.now_ms  # Req 14.2: overwrites an earlier Finished_At
            self.leases.pop(rid, None)
            return kind
        if kind == "cancel":
            rid = self._pick(self._alive(lambda q: q.status in (P, R)), target)
            if rid is None:
                return "noop"
            q = self.reqs[rid]
            res = await s.cancel_request(rid)
            assert res.ok and res.finished_at_ms == self.now_ms, res
            q.status, q.finished = RequestStatus.CANCELLED, self.now_ms
            for dlid in q.pending_replays:  # Req 13.15: unapplied Replays back to Pending
                rec = self.dls.get(dlid)
                if rec is None:  # Replayed record already purged (Req 14.8): stays gone
                    continue
                rec.status, rec.changed = "Pending", self.now_ms
            q.pending_replays.clear()
            self.leases.pop(rid, None)
            return kind
        if kind in ("replay", "replay_failed", "discard"):
            pending = [d for d, rec in self.dls.items() if rec.status == "Pending"]
            if kind == "replay_failed":  # reopen a Failed Request (often a purge candidate)
                pending = [
                    d for d in pending
                    if self.dls[d].rid in self.reqs
                    and (self.focus is None or self.dls[d].rid == self.focus)
                    and self.reqs[self.dls[d].rid].status is RequestStatus.FAILED
                ]
                kind = "replay"
            dlid = self._pick(pending, target)
            if dlid is None:
                return "noop"
            rec = self.dls[dlid]
            if kind == "discard":
                assert (await s.discard_dead_letter(dlid)).outcome == "accepted"
                rec.status, rec.changed = "Discarded", self.now_ms
                return kind
            res = await s.replay_dead_letter(dlid)
            if not res.accepted:
                return f"replay:{res.outcome}"
            rec.status, rec.changed = "Replayed", self.now_ms
            q = self.reqs[res.request_id]
            q.pending_replays.add(dlid)
            q.inbox = True  # the replay event is appended to the inbox
            if res.request_status_before is RequestStatus.FAILED:
                q.status, q.finished = P, None  # reopened (Req 13.2)
            return "replay"
        if kind == "clear_finished":
            rid = self._pick(
                self._alive(lambda q: q.status in TERMINAL and q.finished is not None), target
            )
            if rid is None:
                return "noop"
            # A v1 Request: terminal without finished_at (the candidate entry stays).
            await self.r.hdel(keys.request(rid), keys.F_FINISHED_AT)
            self.reqs[rid].finished = None
            return kind
        if kind == "orphan_dlq":
            rid = self._pick(sorted(self.deleted), target)
            dlid = f"{next(self.dlids):032d}"
            assert (await s.create_dead_letter(fields(dlid, rid))).created
            self.dls[dlid] = DL(rid)
            return kind
        raise AssertionError(kind)

    async def run_injected(self, candidate: str | None = None) -> None:
        if not self.injected:
            return
        kind, target = self.injected.pop(0)
        if kind == "tick":
            self.advance_ms(target)
            return
        if kind == "race":
            # Act on the very Request about to be purged (selected with an older view).
            if candidate is None:
                return
            plan = (["replay_failed"], ["replay_failed", "reopen", "complete"],
                    ["clear_finished"])[target % 3]
            self.focus = candidate
            try:
                done = [await self.op(k, 0) for k in plan]
            finally:
                self.focus = None
            event(f"race:{'+'.join(done)}")
            return
        event(f"injected:{await self.op(kind, target)}")

    # ------------------------------------------------------------------ checked purges
    async def checked_purge_request(self, s: StateStore, rid: str, ttl_ms: int) -> str:
        q = self.reqs.get(rid)
        if q is None or q.status not in TERMINAL:
            expected = PURGE_KEEP  # Req 14.5: never a Pending/Running (or gone) Request
        elif q.finished is None:
            expected = PURGE_STAMPED  # Req 14.9
        elif q.finished + ttl_ms > self.now_ms:
            expected = PURGE_KEEP  # Req 14.5: not before finished_at + ttl
        else:
            expected = PURGE_DELETED  # Req 14.3
        got = await s.purge_request(rid, ttl_ms)
        event(f"purge_request:{got}")
        assert got == expected, (rid, q, self.now_ms, ttl_ms, got)
        if got == PURGE_STAMPED:
            q.finished = self.now_ms
            self.stamped_this_sweep.add(rid)
        elif got == PURGE_DELETED:
            assert rid not in self.stamped_this_sweep  # Req 14.9: not in the stamping sweep
            del self.reqs[rid]
            self.deleted.add(rid)
            self.leases.pop(rid, None)
            await self.assert_request_gone(rid)  # Req 14.4: nothing partial left
        return got

    async def checked_purge_inbox(self, s: StateStore, rid: str, ttl_ms: int) -> str:
        q = self.reqs.get(rid)
        eligible = (
            q is not None
            and q.status in TERMINAL
            and q.finished is not None
            and q.finished + ttl_ms <= self.now_ms
        )
        got = await s.purge_inbox(rid, ttl_ms)
        event(f"purge_inbox:{got}")
        assert got == ("DELETED" if eligible and q.inbox else "KEEP"), (rid, q, got)
        if got == "DELETED":
            q.inbox = False
            assert await self.r.exists(keys.request(rid)) == 1  # Req 14.7: the rest stays
            assert await self.r.exists(keys.jobs(rid)) == int(q.has_plan)
        return got

    async def checked_purge_dead_letter(self, s: StateStore, dlid: str, ttl_ms: int) -> str:
        rec = self.dls.get(dlid)
        eligible = (
            rec is not None
            and rec.status in ("Replayed", "Discarded")
            and rec.changed is not None
            and rec.changed + ttl_ms <= self.now_ms
        )
        got = await s.purge_dead_letter(dlid, ttl_ms)
        event(f"purge_dead_letter:{got}")
        assert got == ("DELETED" if eligible else "KEEP"), (dlid, rec, self.now_ms, got)
        if got == "DELETED":
            del self.dls[dlid]
            self.dl_deleted.add(dlid)
        return got

    async def assert_request_gone(self, rid: str) -> None:
        names = purge_request_keys(rid)
        n = len(keys.request_keys(rid))
        per_request, zsets = names[:n], names[n:]
        assert await self.r.exists(*per_request) == 0, rid
        for z in zsets:
            assert await self.r.zscore(z, rid) is None, (rid, z)

    # ------------------------------------------------------------------ rules
    @rule(op=ops)
    def operate(self, op: tuple[str, int]) -> None:
        event(f"op:{self.run(self.op(*op))}")

    @rule(kind=st.sampled_from(["complete", "fail", "fail", "cancel", "cancel_pending"]))
    def finished_request(self, kind: str) -> None:
        """Shortcut to a fresh terminal Request (target -1 = the newest candidate)."""

        async def go() -> None:
            await self.op("create", 0)
            if kind != "cancel_pending":
                await self.op("commit", -1)
            await self.op("cancel" if kind.startswith("cancel") else kind, -1)

        self.run(go())

    @rule(ms=st.sampled_from([1, 999, 30_000, 59_999, 60_000, 600_000, 3_599_999,
                              3_600_000, 7_200_000]))
    def advance(self, ms: int) -> None:
        self.advance_ms(ms)

    @rule(
        data=st.data(),
        injected=st.lists(
            st.one_of(
                ops,
                # Races with a selected candidate: reopen it, re-finish it, cancel.
                st.tuples(st.sampled_from(["replay_failed", "reopen", "complete", "fail",
                                           "cancel", "clear_finished"]), st.integers(0, 50)),
                st.tuples(st.just("race"), st.integers(0, 2)),
                st.tuples(st.just("race"), st.integers(0, 2)),
                st.tuples(st.just("tick"), st.integers(1, 2_000)),
            ),
            max_size=4,
        ),
    )
    def sweep(self, data: st.DataObject, injected: list[tuple[str, int]]) -> None:
        """Several Managers sweep concurrently; ``injected`` operations run between a
        sweeper's candidate selection and its purge scripts."""
        n = len(self.sweepers)
        chosen = data.draw(
            st.lists(st.integers(0, n - 1), min_size=1, max_size=n, unique=True), label="mgrs"
        )
        self.injected = list(injected)
        self.stamped_this_sweep = set()

        async def go():
            return await asyncio.gather(*(self.sweepers[i].sample_once() for i in chosen))

        results = self.run(go())
        if self.errors:
            raise self.errors[0]
        for res in results:
            assert res.completed
        self.injected = []

    # ------------------------------------------------------------------ invariants
    @invariant()
    def model_matches_store(self) -> None:
        self.run(self._check())

    async def _check(self) -> None:
        r = self.r
        for rid, q in self.reqs.items():
            status, finished = await r.hmget(keys.request(rid), keys.F_STATUS, keys.F_FINISHED_AT)
            assert status == q.status.value, (rid, status, q)
            assert (None if finished is None else int(finished)) == q.finished, (rid, q)
            assert await r.exists(keys.inbox(rid)) == int(q.inbox), (rid, q)
            assert await r.exists(keys.jobs(rid)) == int(q.has_plan), (rid, q)
        for rid in self.deleted:
            await self.assert_request_gone(rid)
        for dlid, rec in self.dls.items():
            assert await r.hget(keys.dlq(dlid), "status") == rec.status, (dlid, rec)
        for dlid in self.dl_deleted:
            assert await r.exists(keys.dlq(dlid)) == 0
            for z in (keys.DLQ_INDEX, keys.DLQ_TERMINAL,
                      keys.dlq_by_reason("retries_exhausted"),
                      *(keys.dlq_by_status(st_) for st_ in DLQ_STATUSES)):
                assert await r.zscore(z, dlid) is None, (dlid, z)
        # Req 14.8: listing shows exactly the surviving records (Pending ones included).
        listed: list[str] = []
        after = None
        while True:
            page = await self.store.list_dead_letters(limit=100, after=after)
            listed += [d.dead_letter_id for d in page.records]
            if page.next_after is None:
                break
            after = page.next_after
        assert sorted(listed) == sorted(self.dls)


RetentionSafety.TestCase.settings = settings(
    RetentionSafety.TestCase.settings, stateful_step_count=50
)
test_retention_safety = RetentionSafety.TestCase
