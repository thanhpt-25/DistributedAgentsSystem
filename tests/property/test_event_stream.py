# Feature: das-client-api, Property 6: Event_Stream mirrors persisted changes
"""Property 6: Event_Stream mirrors persisted changes.

For any Job_Plan (``job_plans(max_jobs=5)``) and any sequence of store operations on
one Request (real ``das/state/lua`` scripts through ``StateStore`` on a fresh fakeredis
server per example, ``TIME`` from :class:`FakeRedisTime`): create, commit plan,
transition batches moving random Jobs to random states, an optional stale-lease batch,
finalize (Completed or Failed) or cancel, a batch after the terminal write, and
optionally a Replay of the Failed Request followed either by a cancel before any
Manager resumes it, or by ``reopen_request`` and a second finalize:

- the Event_Stream holds one ``request.created``, then one ``request.planned`` per
  commit (equal to ``planned_event_data``), one ``job.transition`` per transition-log
  entry in the same order and with equal data (``transition_event_data``), one
  ``request.reopened`` per Replay that reopens the Failed Request (written by the
  Replay, so also when it is cancelled before resumption; ``reopen_request`` adds
  nothing), and exactly one Terminal_Event after each terminal
  write (equal to ``terminal_event_data``), which is the last event while the Request
  is terminal;
- operations answered ``LEASE_LOST`` or ``REQUEST_TERMINAL`` add nothing;
- no event ``data`` contains the prompt or a Job instruction.

**Validates: Requirements 6.1, 6.2, 6.3, 6.4**
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import given
from hypothesis import strategies as st

from das.client.events import (
    TERMINAL_EVENT_TYPES,
    planned_event_data,
    reopened_event_data,
    terminal_event_data,
    terminal_event_type,
    transition_event_data,
)
from das.models import FinalResult, JobPlan, JobState, Lease, RequestStatus
from das.state import ClientRequestSpec, StateStore, StoreStatus, TransitionBatch, keys
from tests.fixtures.redis_time import FakeRedisTime
from tests.strategies import job_plans
from tests.unit.test_state_phase2_dlq import fields

PROMPT = "PROMPT-SECRET-c41d9e"
DLID = "d" * 32
FINAL = FinalResult(summary="done", jobs=[])

# One batch: (job index, new state, attempt, worker_id) per Job it touches.
batches = st.lists(
    st.lists(
        st.tuples(
            st.integers(0, 4),
            st.sampled_from(list(JobState)),
            st.integers(0, 3),
            st.sampled_from([None, "w1", "w2"]),
        ),
        max_size=4,
    ),
    max_size=4,
)


@dataclass(frozen=True)
class Scenario:
    plan: JobPlan
    webhook: bool
    batches: list[list[tuple[int, JobState, int, str | None]]]
    stale_batch: bool
    end: str  # "Completed" | "Failed" | "Cancelled"
    replay: bool  # only with end == "Failed"
    after_replay: str  # "cancel" (before resumption) | "resume" (reopen + finalize)


@st.composite
def scenarios(draw: st.DrawFn) -> Scenario:
    plan = draw(job_plans(max_jobs=5))
    # Distinctive instructions, so their absence from event data is meaningful.
    plan = JobPlan(jobs=[
        j.model_copy(update={"instruction": f"INSTR-SECRET-{i}-{j.job_id}"}) for i, j in enumerate(plan.jobs)
    ])
    end = draw(st.sampled_from(["Completed", "Failed", "Cancelled"]))
    return Scenario(
        plan=plan,
        webhook=draw(st.booleans()),
        batches=draw(batches),
        stale_batch=draw(st.booleans()),
        end=end,
        replay=end == "Failed" and draw(st.booleans()),
        after_replay=draw(st.sampled_from(["cancel", "resume"])),
    )


class Run:
    def __init__(self, store: StateStore, sc: Scenario) -> None:
        self.store, self.sc = store, sc
        self.expected: list[tuple[str, dict[str, Any]]] = []
        self.log_seen = 0
        self.rid = ""

    async def stream(self) -> list[tuple[str, str, dict[str, Any]]]:
        return [
            (sid, f["type"], json.loads(f["data"]))
            for sid, f in await self.store.client.xrange(keys.events(self.rid))
        ]

    async def expect_transitions(self) -> None:
        log = await self.store.get_transitions(self.rid)
        for entry in log[self.log_seen:]:
            self.expected.append(("job.transition", transition_event_data(entry)))
        self.log_seen = len(log)

    async def expect_terminal(self, status: RequestStatus, error: str | None) -> None:
        usage = await self.store.load_usage(self.rid)
        self.expected.append(
            (terminal_event_type(status), terminal_event_data(status, error, usage, None, None))
        )

    async def check(self) -> None:
        got = await self.stream()
        assert [(t, d) for _, t, d in got] == self.expected
        status = await self.store.get_status(self.rid)
        if status in (RequestStatus.COMPLETED, RequestStatus.FAILED, RequestStatus.CANCELLED):
            assert got[-1][1] == terminal_event_type(status)
            if self.sc.webhook:
                hook = await self.store.client.hgetall(keys.webhook(self.rid))
                assert hook["event_id"] == got[-1][0] and hook["state"] == "pending"

    async def batch(self, lease: Lease, items: list[tuple[int, JobState, int, str | None]]) -> StoreStatus:
        snap = await self.store.load_snapshot(self.rid)
        recs = list(snap.jobs.values())
        jobs = {}
        for idx, state, attempt, worker in items:
            rec = recs[idx % len(recs)]
            jobs[rec.job.job_id] = rec.model_copy(update={"state": state, "attempt": attempt, "worker_id": worker})
        return (await self.store.apply_transition(lease, TransitionBatch(jobs=jobs))).status

    async def unchanged_after(self, status: StoreStatus, *expected: StoreStatus) -> None:
        assert status in expected
        await self.check()  # nothing appended

    async def go(self) -> None:
        sc, store = self.sc, self.store
        spec = ClientRequestSpec(
            fingerprint="aaaaaaaa",
            webhook_url="https://hooks.example.test/x" if sc.webhook else None,
            webhook_secret_name="s" if sc.webhook else None,
        )
        res = await store.submit_request(PROMPT, spec)
        assert res.request_id is not None
        self.rid = res.request_id
        self.expected.append(("request.created", {"status": "Pending"}))
        await self.check()

        lease = (await store.acquire_lease(self.rid, "m1", 300)).lease
        assert lease is not None
        assert (await store.commit_plan(lease, sc.plan)).ok
        self.expected.append(("request.planned", planned_event_data(sc.plan)))
        await self.expect_transitions()
        await self.check()

        for items in sc.batches:
            assert await self.batch(lease, items) is StoreStatus.OK
            await self.expect_transitions()
            await self.check()

        if sc.stale_batch:
            stale = lease.model_copy(update={"epoch": lease.epoch + 1})
            items = [(i, JobState.READY, 1, None) for i in range(len(sc.plan.jobs))]
            await self.unchanged_after(await self.batch(stale, items), StoreStatus.LEASE_LOST)

        if sc.end == "Cancelled":
            assert (await store.cancel_request(self.rid)).ok
            await self.expect_transitions()
            await self.expect_terminal(RequestStatus.CANCELLED, None)
        else:
            if sc.replay:
                # Job 0 Failed with attempt >= 1, so its dead letter can be replayed.
                assert await self.batch(lease, [(0, JobState.FAILED, 1, "w1")]) is StoreStatus.OK
                await self.expect_transitions()
            status = RequestStatus(sc.end)
            if status is RequestStatus.COMPLETED:
                assert (await store.finalize(lease, FINAL)).ok
                await self.expect_terminal(status, None)
            else:
                assert (await store.fail_request(lease, "boom")).ok
                await self.expect_terminal(status, "boom")
        await self.check()

        # A Manager write on the terminal Request adds nothing: REQUEST_TERMINAL after a
        # cancel (the Lease survives), LEASE_LOST after finalize (which releases it).
        await self.unchanged_after(await self.batch(lease, [(0, JobState.READY, 1, None)]),
                                   StoreStatus.REQUEST_TERMINAL, StoreStatus.LEASE_LOST)

        if sc.replay:
            first_job = sc.plan.jobs[0].job_id
            assert (await store.create_dead_letter(fields(DLID, self.rid, job_id=first_job))).created
            assert (await store.replay_dead_letter(DLID)).outcome == "accepted"
            # The Replay records the reopen in the same write (Req 6.4).
            self.expected.append(("request.reopened", reopened_event_data()))
            await self.check()
        if sc.replay and sc.after_replay == "cancel":
            # Cancelled before any Manager resumes it: still one Terminal_Event after the reopen.
            assert (await store.cancel_request(self.rid)).ok
            await self.expect_transitions()
            await self.expect_terminal(RequestStatus.CANCELLED, None)
            await self.check()
        elif sc.replay:
            first_job = sc.plan.jobs[0].job_id
            lease2 = (await store.acquire_lease(self.rid, "m2", 300)).lease
            assert lease2 is not None
            assert (await store.reopen_request(lease2)).ok
            await self.check()  # resuming appends no Event (Req 6.4)
            snap = await store.load_snapshot(self.rid)
            ready = snap.jobs[first_job].model_copy(update={"state": JobState.READY})
            batch = TransitionBatch(jobs={first_job: ready}, tag=DLID, replays_applied=[DLID])
            assert (await store.apply_transition(lease2, batch)).ok
            await self.expect_transitions()
            await self.check()
            assert (await store.finalize(lease2, FINAL)).ok
            await self.expect_terminal(RequestStatus.COMPLETED, None)
            await self.check()

        got = await self.stream()
        terminals = sum(t in TERMINAL_EVENT_TYPES for _, t, _ in got)
        assert terminals == (2 if sc.replay else 1)
        if sc.replay:
            types_ = [t for _, t, _ in got]
            reopen_at = types_.index("request.reopened")
            assert sum(t in TERMINAL_EVENT_TYPES for t in types_[reopen_at + 1:]) == 1
        secrets = [PROMPT, *(j.instruction for j in sc.plan.jobs)]
        raw = [f["data"] for _, f in await store.client.xrange(keys.events(self.rid))]
        for text in raw:
            for secret in secrets:
                assert secret not in text


async def _check(sc: Scenario) -> None:
    client = FakeRedis(server=FakeServer(), decode_responses=True)
    try:
        with FakeRedisTime():
            await Run(StateStore(client), sc).go()
    finally:
        await client.aclose()


@given(scenarios())
def test_event_stream_mirrors_persisted_changes(sc: Scenario) -> None:
    """**Validates: Requirements 6.1, 6.2, 6.3, 6.4**"""
    asyncio.run(_check(sc))
