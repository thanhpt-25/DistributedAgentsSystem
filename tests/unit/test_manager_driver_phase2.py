"""RequestDriver Phase 2: Replay, reopen, usage and release
(das-observability-operations task 10.3; Req 9.1, 9.3, 9.4, 13.2-13.5, 13.15, 17.5).

Same real pieces as ``test_manager_driver``: fakeredis running the Lua scripts, the
Redis-backed registry, MemoryTransport and the Planner/Summarizer over FakeAdapter.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json

from das.manager.driver import DriverOutcome
from das.manager.machine import MachineConfig
from das.manager.planner import PlannerConfig
from das.models import JobState, RequestStatus
from das.state import INBOX_REPLAY, StateStore, StoreStatus
from tests.unit.test_manager_driver import (
    ONE_JOB,
    TIMEOUT,
    TWO_JOBS,
    _status_of,
    _true,
    gated,
    make_env,
    steal_lease,
    wait_until,
)
from tests.unit.test_state_phase2_dlq import fields

DL = "a" * 32


def ops(records) -> list[str]:
    return [r.operation for r in records]


# --------------------------------------------------------------------------- usage


async def test_completed_request_persists_plan_job_and_summarize_usage(
    state_store: StateStore, fake_redis
):
    env = await make_env(state_store, fake_redis)
    rid, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED
    usage = await state_store.load_usage(rid)
    assert sorted(ops(usage)) == ["job", "job", "plan", "summarize"]
    plan = next(u for u in usage if u.operation == "plan")
    summary = next(u for u in usage if u.operation == "summarize")
    assert (plan.provider, plan.model) == ("fake", "m-1")  # Req 9.1
    assert (summary.provider, summary.model) == ("fake", "m-1")  # Req 9.3
    jobs = sorted((u.job_id, u.attempt) for u in usage if u.operation == "job")
    assert jobs == [("a", 1), ("b", 1)]  # Req 9.2


async def test_rejected_plan_persists_usage_of_every_attempt(state_store: StateStore, fake_redis):
    bad = json.dumps({"jobs": [{"job_id": "a", "instruction": "x", "capabilities": ["paint"]}]})
    env = await make_env(
        state_store, fake_redis, plan_outcomes=(bad, bad),
        tweak=lambda d: dataclasses.replace(d, planner_config=PlannerConfig(planning_retries=1)),
    )
    rid, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.FAILED
    assert ops(await state_store.load_usage(rid)) == ["plan", "plan"]  # Req 9.1


async def test_lease_loss_drops_summarizer_usage(state_store: StateStore, fake_redis):
    gate, responder = gated()
    env = await make_env(
        state_store, fake_redis, plan_outcomes=(ONE_JOB,),
        summary_outcomes=(responder("late"),), lease_duration=1.0,
    )
    rid, task = await env.submit_and_claim()
    await wait_until(lambda: _true(len(env.summary_llm.calls) == 1))
    await steal_lease(fake_redis, state_store, rid)
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.LEASE_LOST
    gate.set()
    assert sorted(ops(await state_store.load_usage(rid))) == ["job", "plan"]  # Req 9.4


# --------------------------------------------------------------------------- replay


async def test_replay_of_failed_request_reopens_and_completes(
    state_store: StateStore, fake_redis
):
    env = await make_env(
        state_store, fake_redis, summary_outcomes=("after replay",),
        machine_config=MachineConfig(max_attempts=1),
    )
    env.worker.status = "Failed"
    rid, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.FAILED
    assert (await state_store.create_dead_letter(fields(DL, rid=rid))).created
    replay = await state_store.replay_dead_letter(DL)
    assert replay.outcome == "accepted"
    assert await state_store.get_status(rid) is RequestStatus.PENDING  # Req 13.2

    env.worker.status = "Succeeded"
    planner_calls = len(env.planner_llm.calls)
    acquired = await state_store.acquire_lease(rid, "m1", env.drivers.lease_duration)
    assert acquired.ok and acquired.lease is not None
    task = env.drivers.on_claimed(acquired.lease)
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED

    assert len(env.planner_llm.calls) == planner_calls  # reopened, not re-planned (D7)
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.status is RequestStatus.COMPLETED
    assert stored.jobs["a"].state is JobState.SUCCEEDED and stored.jobs["a"].attempt == 2
    assert stored.jobs["b"].state is JobState.SUCCEEDED  # Skipped reset to Waiting (13.4)
    assert stored.final_result is not None and stored.final_result.summary == "after replay"
    assert (await state_store.get_dead_letter(DL)).status == "Replayed"
    assert not await fake_redis.smembers(f"das:req:{rid}:pending_replays")


async def test_finalize_replay_pending_drains_inbox_and_applies_replay(
    state_store: StateStore, fake_redis
):
    env = await make_env(
        state_store, fake_redis, summary_outcomes=("after replay",),
        machine_config=MachineConfig(max_attempts=1),
    )
    env.worker.status = "Failed"
    real_finalize = state_store.finalize
    replies: list[StoreStatus] = []

    async def finalize_with_racing_replay(lease, *args, **kwargs):
        if not replies:  # Gateway accepts a Replay just before the owner finalizes (D8)
            assert (await state_store.create_dead_letter(fields(DL, rid=lease.request_id))).created
            assert (await state_store.replay_dead_letter(DL)).outcome == "accepted"
            env.worker.status = "Succeeded"
        result = await real_finalize(lease, *args, **kwargs)
        replies.append(result.status)
        return result

    state_store.finalize = finalize_with_racing_replay  # type: ignore[method-assign]
    rid, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED
    assert replies == [StoreStatus.REPLAY_PENDING, StoreStatus.OK]
    stored = await state_store.load_snapshot(rid, include_transitions=True)
    assert stored is not None and stored.status is RequestStatus.COMPLETED
    assert stored.jobs["a"].attempt == 2 and stored.jobs["a"].state is JobState.SUCCEEDED
    tagged = [t for t in stored.transitions or [] if t.dead_letter_id == DL]
    assert tagged  # replay transitions carry the Dead_Letter_ID (Req 13.4)


async def test_replay_event_for_a_non_failed_job_is_reverted(
    state_store: StateStore, fake_redis
):
    env = await make_env(state_store, fake_redis, plan_outcomes=(ONE_JOB,))
    env.worker.mode = "hold"
    rid, task = await env.submit_and_claim()
    await wait_until(lambda: _status_of(state_store, rid, "a", JobState.DISPATCHED))
    # Inject a replay event whose Job is not Failed (as if it raced with a retry).
    assert (await state_store.create_dead_letter(fields(DL, rid=rid))).created
    await fake_redis.hset(f"das:dlq:{DL}", "status", "Replayed")
    await fake_redis.sadd(f"das:req:{rid}:pending_replays", DL)
    await fake_redis.xadd(
        f"das:req:{rid}:inbox",
        {"kind": INBOX_REPLAY,
         "data": json.dumps({"dead_letter_id": DL, "job_id": "a", "attempt": 1})},
    )
    env.drivers.notify(rid)

    async def reverted() -> bool:
        return not await fake_redis.smembers(f"das:req:{rid}:pending_replays")

    await wait_until(reverted)
    assert (await fake_redis.hget(f"das:dlq:{DL}", "status")) == "Pending"  # Req 13.15
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.jobs["a"].state is JobState.DISPATCHED
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


# --------------------------------------------------------------------------- release


async def test_request_release_mid_execution_releases_lease(state_store: StateStore, fake_redis):
    env = await make_env(state_store, fake_redis)
    env.worker.mode = "hold"
    rid, task = await env.submit_and_claim()
    await wait_until(lambda: _status_of(state_store, rid, "a", JobState.DISPATCHED))
    assert env.drivers.request_release(rid)
    assert await asyncio.wait_for(task, 5.0) is DriverOutcome.RELEASED  # Req 17.5
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.status is RequestStatus.RUNNING
    assert stored.lease is None
    assert stored.jobs["a"].state is JobState.DISPATCHED
    assert env.drivers.request_release(rid) is False  # no longer owned


async def test_request_release_abandons_in_flight_planning(state_store: StateStore, fake_redis):
    gate, responder = gated()
    env = await make_env(state_store, fake_redis, plan_outcomes=(responder(TWO_JOBS),))
    rid, task = await env.submit_and_claim()
    await wait_until(lambda: _true(len(env.planner_llm.calls) == 1))
    assert env.drivers.request_release_all() == 1
    assert await asyncio.wait_for(task, 5.0) is DriverOutcome.RELEASED
    gate.set()
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.status is RequestStatus.PENDING
    assert stored.plan is None and stored.lease is None
    assert await state_store.load_usage(rid) == []
