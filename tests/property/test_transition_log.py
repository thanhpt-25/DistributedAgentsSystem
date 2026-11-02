"""Property test for the Job transition log (task 9.4).

# Feature: distributed-agent-system, Property 25: Transition log matches state changes

A stateful machine drives the real Lua scripts on fakeredis: one plan commit, then any
mix of ``apply_transition`` batches, lease handovers between Managers
(``release_lease`` + ``acquire_lease``), stale-lease writes, and finally deletion. A
model of the expected log is kept alongside and compared after every step.

**Validates: Requirements 13.7, 13.8**
"""

from __future__ import annotations

import asyncio
from typing import Any

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    initialize,
    invariant,
    precondition,
    rule,
)

from das.models import JobPlan, JobRecord, JobState, Lease, PlannedJob
from das.state import StateStore, StoreStatus, TransitionBatch, keys

MANAGERS = ("m1", "m2", "m3")
LEASE_SECONDS = 300.0  # never expires during a run; handover is explicit

# (job index, new state, attempt, worker_id)
changes = st.lists(
    st.tuples(
        st.integers(min_value=0, max_value=3),
        st.sampled_from(list(JobState)),
        st.integers(min_value=0, max_value=3),
        st.sampled_from([None, "w1", "w2"]),
    ),
    min_size=0,
    max_size=4,
    unique_by=lambda c: c[0],
)


class TransitionLogMachine(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.loop = asyncio.new_event_loop()
        self.client = self._run(self._make_client())
        self.store = StateStore(self.client)
        self.rid = ""
        self.lease: Lease | None = None
        self.stale: list[Lease] = []
        self.plan: JobPlan | None = None
        self.records: dict[str, JobRecord] = {}
        # Expected entries: (job_id, from_state, to_state, attempt, worker_id)
        self.expected: list[tuple[str, JobState | None, JobState, int, str | None]] = []
        self.deleted = False

    def _run(self, coro: Any) -> Any:
        return self.loop.run_until_complete(coro)

    @staticmethod
    async def _make_client() -> FakeRedis:
        return FakeRedis(server=FakeServer(), decode_responses=True)

    def teardown(self) -> None:
        try:
            self._run(self.client.aclose())
        finally:
            self.loop.close()

    # ------------------------------------------------------------------ setup

    @initialize(n_jobs=st.integers(min_value=1, max_value=4))
    def commit_plan(self, n_jobs: int) -> None:
        self.rid = self._run(self.store.create_request("prompt"))
        acq = self._run(self.store.acquire_lease(self.rid, MANAGERS[0], LEASE_SECONDS))
        assert acq.ok
        self.lease = acq.lease
        self.plan = JobPlan(
            jobs=[
                PlannedJob(job_id=f"j{i}", instruction=f"do {i}", capabilities=["research"])
                for i in range(n_jobs)
            ]
        )
        res = self._run(self.store.commit_plan(self.lease, self.plan))
        assert res.ok
        for job in self.plan.jobs:
            self.records[job.job_id] = JobRecord(job=job, state=JobState.WAITING)
            self.expected.append((job.job_id, None, JobState.WAITING, 0, None))

    # ------------------------------------------------------------------ rules

    def _batch(self, change_list) -> tuple[dict[str, JobRecord], list]:
        assert self.plan is not None
        jobs: dict[str, JobRecord] = {}
        new_entries = []
        n = len(self.plan.jobs)
        seen: set[str] = set()
        for idx, state, attempt, worker in change_list:
            jid = self.plan.jobs[idx % n].job_id
            if jid in seen:
                continue
            seen.add(jid)
            old = self.records[jid]
            rec = old.model_copy(update={"state": state, "attempt": attempt, "worker_id": worker})
            jobs[jid] = rec
            if old.state != state:
                new_entries.append((jid, old.state, state, attempt, worker))
        return jobs, new_entries

    @precondition(lambda self: not self.deleted)
    @rule(change_list=changes)
    def apply(self, change_list) -> None:
        jobs, new_entries = self._batch(change_list)
        before = len(self.expected)
        res = self._run(self.store.apply_transition(self.lease, TransitionBatch(jobs=jobs)))
        assert res.ok, res
        assert res.transitions_written == len(new_entries)
        self.records.update(jobs)
        self.expected.extend(new_entries)
        # Every entry from this batch carries the batch's Redis-time stamp.
        stored = self._run(self.store.get_transitions(self.rid))
        assert all(e.at == res.at for e in stored[before:])

    @precondition(lambda self: not self.deleted)
    @rule(target_idx=st.integers(min_value=0, max_value=len(MANAGERS) - 1))
    def handover(self, target_idx: int) -> None:
        assert self.lease is not None
        released = self._run(self.store.release_lease(self.lease))
        assert released.ok
        acq = self._run(self.store.acquire_lease(self.rid, MANAGERS[target_idx], LEASE_SECONDS))
        assert acq.ok, acq
        assert acq.lease.epoch > self.lease.epoch
        self.stale.append(self.lease)
        self.lease = acq.lease

    @precondition(lambda self: not self.deleted and bool(self.stale))
    @rule(data=st.data(), change_list=changes)
    def stale_write(self, data, change_list) -> None:
        old = data.draw(st.sampled_from(self.stale))
        jobs, _ = self._batch(change_list)
        res = self._run(self.store.apply_transition(old, TransitionBatch(jobs=jobs)))
        assert res.status is StoreStatus.LEASE_LOST
        # Nothing changes: Job records stay as modelled; the invariant checks the log.
        snap = self._run(self.store.load_snapshot(self.rid))
        assert snap.jobs == self.records

    @precondition(lambda self: not self.deleted)
    @rule()
    def delete(self) -> None:
        self._run(self.store.delete_request(self.rid))
        self.deleted = True
        self.expected = []

    @precondition(lambda self: self.deleted)
    @rule(change_list=changes)
    def write_after_delete(self, change_list) -> None:
        # The lease went with the Request, so no write can recreate the log.
        jobs, _ = self._batch(change_list)
        res = self._run(self.store.apply_transition(self.lease, TransitionBatch(jobs=jobs)))
        assert not res.ok

    # ------------------------------------------------------------------ invariants

    @invariant()
    def log_matches_state_changes(self) -> None:
        if not self.rid:
            return
        stored = self._run(self.store.get_transitions(self.rid))
        got = [(e.job_id, e.from_state, e.to_state, e.attempt, e.worker_id) for e in stored]
        assert got == self.expected
        stamps = [e.at for e in stored]
        assert stamps == sorted(stamps)
        if self.deleted:
            assert not self._run(self.client.exists(keys.transitions(self.rid)))


# Inherits the loaded profile (>= 100 examples); short runs keep it fast.
TransitionLogMachine.TestCase.settings = settings(stateful_step_count=20)
TestTransitionLog = TransitionLogMachine.TestCase
