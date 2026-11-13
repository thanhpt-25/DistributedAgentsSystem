"""A model of the platform for the tenant properties 2 and 3 (das-multi-tenant tasks 3.6, 3.7).

:class:`World` runs one operation at a time against the real State_Store (the Lua scripts on fakeredis, with a
controllable Redis ``TIME``) and keeps what the operations imply: for each tenant and UTC day the counters the
Usage_Rollup must hold. The two ``check_*`` methods compare the store with the model; the properties call them after
every operation. Operations that do not apply to the current state (a plan for a Request that is not Pending) are
skipped, so any generated sequence is valid."""

from __future__ import annotations

import itertools
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import strategies as st

from das.models import JobRecord, JobState, Lease
from das.models.dlq import DEAD_LETTER_REASONS, RECORD_STATUSES
from das.models.usage import UsageRecord
from das.state import ClientRequestSpec, StateStore, StoreStatus, TransitionBatch, keys
from tests.fixtures.redis_time import FakeRedisTime
from tests.unit.test_state_phase2_dlq import FINAL as FAILED_FINAL
from tests.unit.test_state_phase2_dlq import everything, fields
from tests.unit.test_state_phase2_writes import FINAL, usage
from tests.unit.test_state_store import PLAN, result

FP = "cbabecbd"
A, B = "team-a", "team-b"
TENANTS = (None, A, B)  # None: created without a tenant, so the default tenant
ALL_TENANTS = ("default", A, B)
REQUEST_STATUSES = ("Pending", "Running", "Completed", "Failed", "Cancelled")
START_MS = int(datetime(2026, 11, 12, 12, 0, tzinfo=UTC).timestamp() * 1000)
DAY_SECONDS = 86_400
LONG_LEASE = 10_000_000  # seconds: a Lease never expires during a sequence, even across ticks

KINDS = (
    "create", "create", "create", "plan", "plan", "job_result", "finalize", "fail", "fail_dlq", "cancel",
    "replay", "discard", "purge", "stale", "tick",
)
operations = st.lists(
    st.tuples(
        st.sampled_from(KINDS),
        st.integers(0, 7),  # which Request
        st.sampled_from(TENANTS),  # tenant of a created Request
        st.integers(1, 5),  # tokens in
        st.integers(1, 4),  # tokens out
    ),
    min_size=1,
    max_size=25,
)
#: Operations without a clock tick, for the tests that need two Worlds to stay in step on one clock.
steady_operations = st.lists(
    st.tuples(
        st.sampled_from([k for k in dict.fromkeys(KINDS) if k != "tick"]),
        st.integers(0, 7),
        st.sampled_from(TENANTS),
        st.integers(1, 5),
        st.integers(1, 4),
    ),
    max_size=25,
)
#: Every kind of operation, twice, over all three tenants: a sequence both properties always run, so a fault in the
#: handling of any one operation cannot hide behind the luck of the random draw.
EVERY_KIND = [
    (kind, i, TENANTS[i % len(TENANTS)], 1 + i % 5, 1 + i % 4)
    for _ in range(2)
    for i, kind in enumerate(dict.fromkeys(KINDS))
]


@dataclass
class Rec:
    rid: str
    tenant: str
    status: str  # a Request_Status, or "Purged"
    lease: Lease | None = None
    last_lease: Lease | None = None
    dlid: str | None = None  # the latest record of this Request
    dlids: list[str] = field(default_factory=list)  # every record ever filed for it: a replayed Request can fail again
    dl_status: str | None = None
    applied: set[tuple[str, int]] = field(default_factory=set)


def day_of(seconds: float) -> str:
    return datetime.fromtimestamp(seconds, UTC).strftime("%Y%m%d")


class World:
    """``phase4`` makes every Request a Default_Tenant one whatever tenant an operation names, with or without a
    client key, as data written before Phase 5 is. ``ids`` makes Request ids deterministic, so two Worlds fed the same
    operations hold the same keys."""

    def __init__(
        self, clock: FakeRedisTime, *, phase4: bool = False, ids: Callable[[], str] | None = None
    ) -> None:
        redis = FakeRedis(server=FakeServer(), decode_responses=True)
        self.store = StateStore(redis, id_factory=ids) if ids is not None else StateStore(redis)
        self.phase4 = phase4
        self.clock = clock
        self.recs: list[Rec] = []
        self.expected: Counter[tuple[str, str, str]] = Counter()  # (tenant, day, rollup field) -> value
        self._ids = itertools.count(1)

    # ------------------------------------------------------------------ the model's bookkeeping

    def _count(self, tenant: str, name: str, n: int = 1) -> None:
        self.expected[(tenant, day_of(self.clock.now()), name)] += n

    def _tokens(self, tenant: str, record: UsageRecord) -> None:
        for direction, n in (
            ("in", record.usage.input_tokens),
            ("out", record.usage.output_tokens),
            ("total", record.usage.total_tokens),
        ):
            self._count(tenant, f"tokens/{direction}/{record.provider}/{record.model}", n)

    def adopt(self, rid: str, tenant: str) -> None:
        """A Request created outside the model (through the Gateway): from here on the model knows it."""
        self.recs.append(Rec(rid, tenant, "Pending"))
        self._count(tenant, "submitted")

    def _pick(self, target: int, *statuses: str) -> Rec | None:
        pool = [r for r in self.recs if r.status in statuses]
        return pool[target % len(pool)] if pool else None

    # ------------------------------------------------------------------ operations

    async def apply(self, kind: str, target: int, tenant: str | None, n: int, m: int) -> None:
        await getattr(self, f"_{kind}")(target, tenant, n, m)

    async def _create(self, target: int, tenant: str | None, n: int, m: int) -> Rec:
        if tenant is None:
            rid = await self.store.create_request("prompt")
        else:
            spec_tenant = None if self.phase4 else tenant
            res = await self.store.submit_request("prompt", ClientRequestSpec(fingerprint=FP, tenant=spec_tenant))
            assert res.request_id is not None
            rid = res.request_id
        effective = "default" if self.phase4 else (tenant or "default")
        rec = Rec(rid, effective, "Pending")
        self.recs.append(rec)
        self._count(effective, "submitted")
        return rec

    # Every operation finds the Request it needs or builds one, so a generated sequence reaches the states that
    # matter (a dead letter to replay, a terminal Request to purge) instead of mostly skipping.

    async def _ensure_pending(self, target: int, tenant: str | None, n: int, m: int) -> Rec:
        return self._pick(target, "Pending") or await self._create(target, tenant, n, m)

    async def _ensure_running(self, target: int, tenant: str | None, n: int, m: int) -> Rec:
        rec = self._pick(target, "Running")
        if rec is None:
            rec = await self._plan(target, tenant, n, m)
        assert rec.lease is not None
        return rec

    async def _ensure_dead_letter(self, target: int, tenant: str | None, n: int, m: int) -> Rec:
        pool = [r for r in self.recs if r.status == "Failed" and r.dlid and r.dl_status == "Pending"]
        return pool[target % len(pool)] if pool else await self._fail_dlq(target, tenant, n, m)

    async def _plan(self, target: int, tenant: str | None, n: int, m: int) -> Rec:
        rec = await self._ensure_pending(target, tenant, n, m)
        lease = (await self.store.acquire_lease(rec.rid, "m1", LONG_LEASE)).lease
        assert lease is not None
        planning = usage("plan", i=n, o=m)
        assert (await self.store.commit_plan(lease, PLAN, [planning])).ok
        rec.status, rec.lease = "Running", lease
        self._tokens(rec.tenant, planning)
        return rec

    async def _job_result(self, target: int, tenant: str | None, n: int, m: int) -> None:
        rec = await self._ensure_running(target, tenant, n, m)
        assert rec.lease is not None
        attempt = 1 + m % 2
        batch = TransitionBatch(
            results=[result(rec.rid, "a", attempt)], usage=[usage("job", i=n, o=m, job_id="a", attempt=attempt)]
        )
        assert (await self.store.apply_transition(rec.lease, batch)).ok
        if ("a", attempt) not in rec.applied:  # a duplicate result adds no usage (Property 3)
            rec.applied.add(("a", attempt))
            self._tokens(rec.tenant, batch.usage[0])

    async def _finalize(self, target: int, tenant: str | None, n: int, m: int) -> None:
        rec = await self._ensure_running(target, tenant, n, m)
        assert rec.lease is not None
        summary = usage("summarize", i=n, o=m)
        assert (await self.store.finalize(rec.lease, FINAL, usage=[summary])).ok
        self._finish(rec, "Completed")
        self._tokens(rec.tenant, summary)

    async def _fail(self, target: int, tenant: str | None, n: int, m: int) -> None:
        rec = await self._ensure_running(target, tenant, n, m)
        assert rec.lease is not None
        summary = usage("summarize", i=n, o=m)
        assert (await self.store.fail_request(rec.lease, "boom", FAILED_FINAL, usage=[summary])).ok
        self._finish(rec, "Failed")
        self._tokens(rec.tenant, summary)

    async def _fail_dlq(self, target: int, tenant: str | None, n: int, m: int) -> Rec:
        rec = await self._ensure_running(target, tenant, n, m)
        assert rec.lease is not None
        failed = JobRecord(job=PLAN.jobs[0], state=JobState.FAILED, attempt=1, reason="x")
        skipped = JobRecord(job=PLAN.jobs[1], state=JobState.SKIPPED, reason="dep failed")
        assert (await self.store.apply_transition(rec.lease, TransitionBatch(jobs={"a": failed, "b": skipped}))).ok
        assert (await self.store.fail_request(rec.lease, "job a failed", FAILED_FINAL)).ok
        self._finish(rec, "Failed")
        rec.dlid = f"{next(self._ids):032x}"
        assert (await self.store.create_dead_letter(fields(rec.dlid, rid=rec.rid))).created
        rec.dlids.append(rec.dlid)
        rec.dl_status = "Pending"
        return rec

    async def _cancel(self, target: int, tenant: str | None, n: int, m: int) -> Rec:
        rec = self._pick(target, "Pending", "Running") or await self._create(target, tenant, n, m)
        assert (await self.store.cancel_request(rec.rid, prices=None)).ok
        self._finish(rec, "Cancelled")
        return rec

    async def _replay(self, target: int, tenant: str | None, n: int, m: int) -> None:
        rec = await self._ensure_dead_letter(target, tenant, n, m)
        assert rec.dlid is not None
        assert (await self.store.replay_dead_letter(rec.dlid)).outcome == "accepted"
        rec.dl_status = "Replayed"
        lease = (await self.store.acquire_lease(rec.rid, "m1", LONG_LEASE)).lease
        assert lease is not None
        assert (await self.store.reopen_request(lease)).ok
        assert (await self.store.apply_transition(lease, TransitionBatch(replays_applied=[rec.dlid]))).ok
        rec.status, rec.lease = "Running", lease

    async def _discard(self, target: int, tenant: str | None, n: int, m: int) -> None:
        rec = await self._ensure_dead_letter(target, tenant, n, m)
        assert rec.dlid is not None
        assert (await self.store.discard_dead_letter(rec.dlid)).outcome == "accepted"
        rec.dl_status = "Discarded"

    async def _purge(self, target: int, tenant: str | None, n: int, m: int) -> None:
        rec = self._pick(target, "Completed", "Failed", "Cancelled") or await self._cancel(target, tenant, n, m)
        assert await self.store.purge_request(rec.rid, ttl_ms=0) == "DELETED"
        rec.status = "Purged"

    async def _stale(self, target: int, tenant: str | None, n: int, m: int) -> None:
        """Writes that must be rejected: the whole store stays exactly as it was (Properties 2 and 3)."""
        rec = self._pick(target, "Running", "Completed", "Failed", "Cancelled") or await self._ensure_running(
            target, tenant, n, m
        )
        before = await everything(self.store)
        if rec.status == "Running" and rec.lease is not None:
            stranger = rec.lease.model_copy(update={"owner": "intruder"})
            assert (await self.store.commit_plan(stranger, PLAN, [usage("plan")])).status is StoreStatus.LEASE_LOST
            rejected = await self.store.finalize(stranger, FINAL, usage=[usage("summarize")])
            assert rejected.status is StoreStatus.LEASE_LOST
        else:  # terminal
            if rec.last_lease is not None:
                late = await self.store.finalize(rec.last_lease, FINAL, usage=[usage("summarize")])
                assert late.status in (StoreStatus.LEASE_LOST, StoreStatus.REQUEST_TERMINAL)
            assert (await self.store.cancel_request(rec.rid, prices=None)).status is StoreStatus.TERMINAL
        assert await everything(self.store) == before

    async def _tick(self, target: int, tenant: str | None, n: int, m: int) -> None:
        self.clock.advance(DAY_SECONDS * (1 + target % 2))

    def _finish(self, rec: Rec, status: str) -> None:
        rec.status, rec.last_lease, rec.lease = status, rec.lease or rec.last_lease, None
        self._count(rec.tenant, f"finished/{status}")

    # ------------------------------------------------------------------ Property 2: the indexes mirror the global ones

    async def check_mirror(self) -> None:
        r = self.store.client

        async def zset(key: str) -> dict[str, float]:
            return {str(member): score for member, score in await r.zrange(key, 0, -1, withscores=True)}

        def of(members: dict[str, float], owner: dict[str, str | None], tenant: str) -> dict[str, float]:
            return {m: s for m, s in members.items() if owner[m] == tenant}

        index = await zset(keys.REQUESTS_INDEX)
        owner: dict[str, str | None] = {}
        status: dict[str, str] = {}
        for rid in index:
            owner[rid] = await r.hget(keys.request(rid), "tenant") or "default"
            status[rid] = await r.hget(keys.request(rid), "status")
        for tenant in ALL_TENANTS:
            assert await zset(keys.tenant_requests(tenant)) == of(index, owner, tenant), (tenant, "index")
            for s in REQUEST_STATUSES:
                got = await zset(keys.tenant_requests_by_status(tenant, s))
                assert got == of(await zset(keys.requests_by_status(s)), owner, tenant), (tenant, s)
            active = {rid for rid in of(index, owner, tenant) if status[rid] in ("Pending", "Running")}
            assert {str(m) for m in await r.smembers(keys.tenant_active(tenant))} == active, (tenant, "active")

        dlq = await zset(keys.DLQ_INDEX)
        dl_owner = {dlid: await r.hget(keys.dlq(dlid), "tenant") for dlid in dlq}  # None: no tenant, no twins
        for tenant in ALL_TENANTS:
            assert await zset(keys.tenant_dlq_index(tenant)) == of(dlq, dl_owner, tenant), (tenant, "dlq index")  # type: ignore[arg-type]
            for s in RECORD_STATUSES:
                got = await zset(keys.tenant_dlq_by_status(tenant, s))
                assert got == of(await zset(keys.dlq_by_status(s)), dl_owner, tenant), (tenant, "dlq", s)  # type: ignore[arg-type]
            for reason in DEAD_LETTER_REASONS:
                got = await zset(keys.tenant_dlq_by_reason(tenant, reason))
                assert got == of(await zset(keys.dlq_by_reason(reason)), dl_owner, tenant), (tenant, "reason", reason)  # type: ignore[arg-type]

    # ------------------------------------------------------------------ Property 3: the usage rollup is exact

    async def check_usage(self) -> None:
        r = self.store.client
        want: dict[tuple[str, str], dict[str, str]] = {}
        for (tenant, day, name), value in self.expected.items():
            want.setdefault((tenant, day), {})[name] = str(value)
        got: dict[tuple[str, str], dict[str, str]] = {}
        for key in await r.keys("das:tenant:*:usage:*"):
            _, _, tenant, _, day = str(key).split(":")
            got[(tenant, day)] = await r.hgetall(key)
        assert got == want
        for key in got:
            assert 0 < await r.ttl(keys.tenant_usage(*key)) <= 400 * DAY_SECONDS  # the 400-day default retention
