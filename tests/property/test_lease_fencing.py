# Feature: distributed-agent-system, Property 13: Lease mutual exclusion and fencing
"""Property 13: Lease mutual exclusion and fencing.

For any number of Manager_Agents (2..10) and any interleaved sequence of acquire,
renew, release, guarded-write, and clock-advance operations executed against the
real Lua scripts (fakeredis):

- At every point at most one Manager holds an unexpired Lease on a Request.
- Each successful renew sets ``expires_at = now + duration``.
- Every guarded write by a Manager whose ``(owner, epoch)`` does not match the
  current unexpired Lease returns ``LEASE_LOST`` and leaves the Request, Job, and
  Lease state unchanged.

Redis ``TIME`` is driven by a fake server clock (patched into fakeredis), so lease
expiry is exercised without real sleeps. Time and durations are multiples of 0.25 s
on a whole-second base, which are exact in binary floating point, so expiry
boundaries (``expires_at == now``) are tested exactly.

**Validates: Requirements 2.2, 2.4, 2.7, 3.12**
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest import mock

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from fakeredis.commands_mixins import server_mixin
from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    initialize,
    invariant,
    precondition,
    rule,
)

from das.models import JobPlan, JobRecord, JobState, Lease, PlannedJob, RequestStatus
from das.state import StateStore, StoreStatus, TransitionBatch, keys

START = 1_700_000_000.0  # whole seconds, so every quarter-second step is exact

PLAN = JobPlan(
    jobs=[
        PlannedJob(job_id="a", instruction="research x", capabilities=["research"]),
        PlannedJob(job_id="b", instruction="sum", capabilities=["summarize"], depends_on=["a"]),
    ]
)

durations = st.sampled_from([1.0, 2.0, 3.0, 5.0])
advances = st.integers(min_value=1, max_value=24).map(lambda q: q * 0.25)  # 0.25 .. 6 s


class LeaseMachine(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.now = START
        self._patch = mock.patch.object(
            server_mixin, "time", SimpleNamespace(time=lambda: self.now)
        )
        self._patch.start()
        self.loop = asyncio.new_event_loop()
        self.redis = FakeRedis(server=FakeServer(), decode_responses=True)
        self.store = StateStore(self.redis)
        self.rid = self.run(self.store.create_request("hello"))
        # Model of the stored lease: owner/expires_at are None when released.
        self.owner: str | None = None
        self.epoch = 0
        self.expires_at: float | None = None
        self.managers: list[str] = []
        self.tokens: dict[str, Lease] = {}  # last lease each Manager obtained (maybe stale)
        self.writes = 0

    def run(self, coro):
        return self.loop.run_until_complete(coro)

    def teardown(self) -> None:
        try:
            self.run(self.redis.aclose())
            self.loop.close()
        finally:
            self._patch.stop()

    # ------------------------------------------------------------------ helpers

    def held(self) -> bool:
        return self.owner is not None and self.expires_at is not None and self.expires_at > self.now

    def valid(self, token: Lease) -> bool:
        return self.held() and token.owner == self.owner and token.epoch == self.epoch

    async def _raw_state(self) -> tuple:
        r = self.redis
        return (
            await r.hgetall(keys.request(self.rid)),
            await r.hgetall(keys.jobs(self.rid)),
            await r.hgetall(keys.lease(self.rid)),
            await r.lrange(keys.transitions(self.rid), 0, -1),
            await r.zrange(keys.LEASES_EXPIRY, 0, -1, withscores=True),
            await r.zrange(keys.REQUESTS_PENDING, 0, -1, withscores=True),
        )

    def guarded_write(self, token: Lease) -> None:
        before = self.run(self._raw_state())
        status = self.run(self.store.get_status(self.rid))
        if status is RequestStatus.PENDING:
            res = self.run(self.store.commit_plan(token, PLAN))
        else:
            self.writes += 1
            record = JobRecord(job=PLAN.jobs[0], state=JobState.WAITING, reason=f"w{self.writes}")
            res = self.run(self.store.apply_transition(token, TransitionBatch(jobs={"a": record})))
        if self.valid(token):
            assert res.status is StoreStatus.OK, res
        else:
            assert res.status is StoreStatus.LEASE_LOST, (token, res)
            assert self.run(self._raw_state()) == before

    # ------------------------------------------------------------------ rules

    @initialize(n=st.integers(min_value=2, max_value=10))
    def make_managers(self, n: int) -> None:
        self.managers = [f"m{i}" for i in range(n)]

    @rule(data=st.data(), duration=durations)
    def acquire(self, data, duration: float) -> None:
        m = data.draw(st.sampled_from(self.managers), label="manager")
        res = self.run(self.store.acquire_lease(self.rid, m, duration))
        if self.held():
            assert res.status is StoreStatus.HELD, res
            return
        assert res.status is StoreStatus.OK, res
        assert res.lease.owner == m
        assert res.lease.epoch == self.epoch + 1  # fencing token strictly increases
        assert res.lease.expires_at == self.now + duration
        self.owner, self.epoch, self.expires_at = m, res.lease.epoch, res.lease.expires_at
        self.tokens[m] = res.lease

    @precondition(lambda self: self.tokens)
    @rule(data=st.data(), duration=durations)
    def renew(self, data, duration: float) -> None:
        m = data.draw(st.sampled_from(sorted(self.tokens)), label="manager")
        token = self.tokens[m]
        before = self.run(self._raw_state())
        res = self.run(self.store.renew_lease(token, duration))
        if self.valid(token):
            assert res.status is StoreStatus.OK, res
            assert res.expires_at == self.now + duration
            self.expires_at = res.expires_at
            self.tokens[m] = token.model_copy(update={"expires_at": res.expires_at})
        else:
            assert res.status is StoreStatus.LEASE_LOST, res
            assert self.run(self._raw_state()) == before

    @precondition(lambda self: self.tokens)
    @rule(data=st.data())
    def release(self, data) -> None:
        m = data.draw(st.sampled_from(sorted(self.tokens)), label="manager")
        token = self.tokens[m]
        before = self.run(self._raw_state())
        res = self.run(self.store.release_lease(token))
        # Release is fenced on (owner, epoch) only; releasing one's own expired lease is harmless.
        if self.owner is not None and token.owner == self.owner and token.epoch == self.epoch:
            assert res.status is StoreStatus.OK, res
            self.owner = self.expires_at = None
        else:
            assert res.status is StoreStatus.LEASE_LOST, res
            assert self.run(self._raw_state()) == before

    @precondition(lambda self: self.tokens)
    @rule(data=st.data())
    def write_with_own_token(self, data) -> None:
        m = data.draw(st.sampled_from(sorted(self.tokens)), label="manager")
        self.guarded_write(self.tokens[m])

    @rule(data=st.data(), epoch_delta=st.integers(min_value=-2, max_value=2))
    def write_with_forged_token(self, data, epoch_delta: int) -> None:
        """A Manager presents an (owner, epoch) it never acquired, e.g. guessing the epoch."""
        m = data.draw(st.sampled_from(self.managers), label="manager")
        epoch = max(1, self.epoch + epoch_delta)
        token = Lease(request_id=self.rid, owner=m, epoch=epoch, expires_at=self.now + 60)
        self.guarded_write(token)

    @rule(dt=advances)
    def advance_clock(self, dt: float) -> None:
        self.now += dt

    # ------------------------------------------------------------------ invariants

    @invariant()
    def at_most_one_unexpired_holder(self) -> None:
        stored = self.run(self.store.get_lease(self.rid))
        if self.owner is None:
            assert stored is None
        else:
            assert stored is not None
            assert (stored.owner, stored.epoch, stored.expires_at) == (
                self.owner,
                self.epoch,
                self.expires_at,
            )
        holders = [
            m
            for m, t in self.tokens.items()
            if stored is not None
            and stored.expires_at > self.now
            and (t.owner, t.epoch) == (stored.owner, stored.epoch)
        ]
        assert len(holders) <= 1, holders


LeaseMachine.TestCase.settings = settings(
    LeaseMachine.TestCase.settings, stateful_step_count=30
)
TestLeaseMutualExclusionAndFencing = LeaseMachine.TestCase
