# Feature: das-observability-operations, Property 23: Loại bỏ Job_Result cũ sau Replay
"""Property 23: stale Job_Results are discarded after a Replay.

For any interleaving of Replays of a Job, Job_Results of every attempt (including
pre-Replay attempts arriving late) and duplicate deliveries, every Job_Result whose
attempt is lower than the Job's current attempt count (at the time it is applied) is
discarded: the Job state is unchanged, no result is written and no Usage_Record is
produced. A replayed Job reaches a terminal state only through a result of its current
attempt, or through dead-letter when ``attempt_limit`` is reached.

Each event goes through ``RequestMachine.step`` on a snapshot loaded from the store
and the decision is committed with the real ``apply_transition.lua`` (fakeredis[lua]);
the checks read the store back after every commit.

**Validates: Requirements 13.10**
"""

from __future__ import annotations

import asyncio

from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from hypothesis import HealthCheck, event, given, settings
from hypothesis import strategies as st

from das.manager.machine import (
    TIMER_RETRY,
    JobResultReceived,
    MachineConfig,
    PlanCommitted,
    PublishConfirmed,
    PublishDeadLetter,
    ReplayRequested,
    RequestSnapshot,
    StepContext,
    TimerFired,
    attempt_limit,
    step,
)
from das.models import (
    AgentRecord,
    JobPlan,
    JobResultPayload,
    JobState,
    PlannedJob,
    RequestStatus,
    TokenUsage,
)
from das.state import StateStore

JOB = "a"
PLAN = JobPlan(
    jobs=[
        PlannedJob(job_id=JOB, instruction="research x", capabilities=["research"]),
        PlannedJob(job_id="b", instruction="sum", capabilities=["summarize"], depends_on=[JOB]),
    ]
)
AGENTS = (
    AgentRecord(
        agent_id="w1",
        role="worker",
        host="h",
        capabilities=frozenset({"research", "summarize"}),
        status="Available",
        last_heartbeat=0.0,
    ),
)
META = {"provider": "openai", "model": "m"}
# Weighted toward failures so the Job reaches dead-letter and gets replayed.
STATUSES = ["Failed"] * 3 + ["TimedOut", "Succeeded", "Cancelled"]
OPS = st.sampled_from(["result"] * 4 + ["duplicate"] * 2 + ["replay"] * 2 + ["retry", "confirm"])


def _result(rid: str, attempt: int, status: str, n: int) -> JobResultPayload:
    return JobResultPayload(
        request_id=rid,
        job_id=JOB,
        attempt=attempt,
        worker_id="w1",
        status=status,
        content=f"out {attempt}/{n}" if status == "Succeeded" else "",
        error=None if status == "Succeeded" else f"err {attempt}/{n}",
        usage=TokenUsage(input_tokens=n + 1, output_tokens=1, total_tokens=n + 2),
        duration_ms=1,
    )


async def _scenario(data: st.DataObject, max_attempts: int) -> None:
    redis = FakeRedis(server=FakeServer(), decode_responses=True)
    store = StateStore(redis)
    rid = await store.create_request("p")
    lease = (await store.acquire_lease(rid, "m1", 3600.0)).lease
    assert lease is not None
    assert (await store.commit_plan(lease, PLAN)).ok
    cfg = MachineConfig(max_attempts=max_attempts)

    async def load() -> tuple[RequestSnapshot, int]:
        stored = await store.load_snapshot(rid, include_usage=True)
        assert stored is not None
        return RequestSnapshot.from_stored(stored), len(stored.usage or [])

    async def commit(ev: object) -> object:
        snap, _ = await load()
        ctx = StepContext(now=await store.server_time(), config=cfg, agents=AGENTS)
        d = step(snap, ev, ctx)
        if not d.is_empty:
            assert (await store.apply_transition(lease, d.updates)).ok
        return d

    await commit(PlanCommitted())
    sent: list[JobResultPayload] = []
    replays = 0
    stale_seen = 0
    for n in range(data.draw(st.integers(5, 30), label="steps")):
        before, usage_before = await load()
        assert before.status is RequestStatus.RUNNING
        rec = before.jobs[JOB]
        cur = rec.attempt
        dead = rec.state is JobState.FAILED and not any(
            t.kind == TIMER_RETRY and t.job_id == JOB for t in before.timers
        )
        # Steer toward Replays of a dead-lettered Job so attempts climb past the limit.
        op = "replay" if dead and data.draw(st.integers(0, 3)) > 0 else data.draw(OPS)
        ev: object
        if op == "duplicate" and sent:
            ev = JobResultReceived(data.draw(st.sampled_from(sent)), META)
        elif op in ("result", "duplicate"):
            attempt = data.draw(st.sampled_from([cur] * 3 + list(range(1, cur + 2))))
            res = _result(rid, attempt, data.draw(st.sampled_from(STATUSES)), n)
            sent.append(res)
            ev = JobResultReceived(res, META)
        elif op == "replay":
            attempt = data.draw(st.sampled_from([cur, cur, cur - 1, cur + 1]))
            ev = ReplayRequested("dl" + str(n).zfill(30), JOB, attempt)
        elif op == "retry":
            ev = TimerFired(TIMER_RETRY, JOB, cur)
        else:
            ev = PublishConfirmed(JOB, cur)

        d = await commit(ev)
        after, usage_after = await load()
        new = after.jobs[JOB]
        if isinstance(ev, ReplayRequested) and new.attempt == cur + 1:
            replays += 1
        if isinstance(ev, JobResultReceived) and replays and ev.result.attempt < cur:
            event("pre-replay or stale result after a replay")

        if isinstance(ev, JobResultReceived):
            res = ev.result
            key = (res.job_id, res.attempt)
            accepted = (
                res.attempt == cur
                and key not in before.results
                and rec.state in (JobState.READY, JobState.DISPATCHED)
            )
            if res.attempt < cur:
                stale_seen += 1
            if not accepted:
                # Discarded: Job unchanged, no result written, no Usage_Record.
                assert new == rec, (res.attempt, cur, rec.state, new.state)
                assert after.results.get(key) == before.results.get(key)
                assert usage_after == usage_before
                assert not d.updates.usage and not d.updates.results and d.changes == ()
                assert after.jobs == before.jobs
            else:
                assert after.results[key] == res
                assert usage_after == usage_before + 1
        else:
            # Only accepted results produce usage.
            assert usage_after == usage_before

        # A terminal Job state is reached only by a current-attempt result, or
        # dead-letter at the attempt limit.
        retry_pending = any(t.kind == TIMER_RETRY and t.job_id == JOB for t in after.timers)
        terminal = new.state in (JobState.SUCCEEDED, JobState.CANCELLED) or (
            new.state is JobState.FAILED and not retry_pending
        )
        if terminal and (new.state is not rec.state or new.attempt != rec.attempt):
            assert isinstance(ev, JobResultReceived), (ev, rec.state, new.state)
            assert ev.result.attempt == cur == new.attempt
            if new.state is JobState.FAILED:
                assert new.attempt >= attempt_limit(new, cfg)
                assert any(
                    isinstance(e, PublishDeadLetter) and e.job_id == JOB for e in d.effects
                )

    event(f"replays applied: {min(replays, 3)}")
    event("stale results seen" if stale_seen else "no stale results")
    await redis.aclose()


@settings(
    max_examples=150,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
@given(data=st.data(), max_attempts=st.integers(1, 3))
def test_stale_results_discarded_after_replay(data: st.DataObject, max_attempts: int) -> None:
    asyncio.run(_scenario(data, max_attempts))
