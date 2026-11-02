"""Unit tests for the atomic State_Store writes (task 9.5).

``commit_plan`` must persist the Job_Plan and status Running together or not at all
(Req 3.9), and ``finalize`` must write the Final_Result before the Completed status
(Req 7.4).

Redis runs a script in isolation but does not roll back writes made before a runtime
error, so these tests check two things: every rejection path leaves the stored state
byte-for-byte unchanged, and the write order is such that a failure at any write never
exposes Running without the plan, or Completed without the Final_Result. Faults are
injected by prefixing the real script with a shim that wraps ``redis.call``.
"""

from __future__ import annotations

import asyncio
import json

import pytest
from redis.exceptions import ResponseError

from das.client.events import planned_event_data
from das.models import (
    FinalResult,
    JobPlan,
    JobRecord,
    JobReport,
    JobState,
    Lease,
    PlannedJob,
    RequestStatus,
)
from das.state import StateStore, StoreStatus, keys
from das.state.scripts import script_source
from das.state.store import commit_plan_keys, finalize_keys

PLAN = JobPlan(
    jobs=[
        PlannedJob(job_id="a", instruction="research x", capabilities=["research"]),
        PlannedJob(job_id="b", instruction="sum", capabilities=["summarize"], depends_on=["a"]),
        PlannedJob(job_id="c", instruction="check", capabilities=["review"], depends_on=["b"]),
    ]
)

FINAL = FinalResult(
    summary="done",
    jobs=[JobReport(job_id="a", state=JobState.SUCCEEDED, worker_id="w1")],
)

WRITE_COMMANDS = {"HSET", "HDEL", "DEL", "RPUSH", "ZADD", "ZREM", "XADD", "SET"}
TRACE_KEY = "test:trace"

# Wraps redis.call: logs every write to TRACE_KEY and raises on the Nth write
# (ARGV-independent: N comes from the 'test:fail_at' key; 0 disables the fault).
SHIM = f"""
local __rc = redis.call
local __fail_at = tonumber(__rc('GET', 'test:fail_at') or '0')
local __writes = 0
local __W = {{ {", ".join(f"{c}=true" for c in sorted(WRITE_COMMANDS))} }}
redis.call = function(cmd, ...)
  if __W[string.upper(cmd)] then
    __writes = __writes + 1
    if __writes == __fail_at then
      error('injected fault at write ' .. __writes)
    end
    __rc('RPUSH', '{TRACE_KEY}', table.concat({{ cmd, ... }}, ' '))
  end
  return __rc(cmd, ...)
end
"""


async def dump_state(store: StateStore, rid: str) -> dict:
    """Raw contents of every key that belongs to the Request plus the shared sets."""
    r = store.client
    return {
        "request": await r.hgetall(keys.request(rid)),
        "jobs": await r.hgetall(keys.jobs(rid)),
        "transitions": await r.lrange(keys.transitions(rid), 0, -1),
        "lease": await r.hgetall(keys.lease(rid)),
        "pending": await r.zrange(keys.REQUESTS_PENDING, 0, -1, withscores=True),
        "expiry": await r.zrange(keys.LEASES_EXPIRY, 0, -1, withscores=True),
    }


def commit_args(lease: Lease, plan: JobPlan) -> tuple[list[str], list]:
    rid = lease.request_id
    args: list = [rid, lease.owner, lease.epoch, plan.model_dump_json(), "", json.dumps(planned_event_data(plan))]
    for job in plan.jobs:
        args += [job.job_id, JobRecord(job=job, state=JobState.WAITING).model_dump_json()]
    return commit_plan_keys(rid), args


def finalize_args(lease: Lease, final: FinalResult) -> tuple[list[str], list]:
    rid = lease.request_id
    keys_ = finalize_keys(rid, RequestStatus.COMPLETED)
    return keys_, [rid, lease.owner, lease.epoch, "Completed", final.model_dump_json(), "", ""]


async def pending_with_lease(store: StateStore) -> tuple[str, Lease]:
    rid = await store.create_request("hello")
    acq = await store.acquire_lease(rid, "m1", 30)
    assert acq.ok
    return rid, acq.lease


async def running_with_lease(store: StateStore) -> tuple[str, Lease]:
    rid, lease = await pending_with_lease(store)
    assert (await store.commit_plan(lease, PLAN)).ok
    return rid, lease


# ------------------------------------------------------------------ commit_plan: success


async def test_commit_plan_with_lease_writes_plan_and_running_together(state_store: StateStore):
    rid, lease = await pending_with_lease(state_store)
    res = await state_store.commit_plan(lease, PLAN)
    assert res.ok
    snap = await state_store.load_snapshot(rid, include_transitions=True)
    assert snap.status is RequestStatus.RUNNING
    assert snap.plan == PLAN
    assert list(snap.jobs) == ["a", "b", "c"]
    assert all(rec.state is JobState.WAITING for rec in snap.jobs.values())
    assert [t.job_id for t in snap.transitions] == ["a", "b", "c"]
    # Every transition carries the timestamp returned by the same script run.
    assert {t.at for t in snap.transitions} == {res.at}
    assert snap.lease == lease  # commit does not touch the lease


# ------------------------------------------------------------------ commit_plan: no lease


def _foreign(lease: Lease, **update) -> Lease:
    return lease.model_copy(update=update)


@pytest.mark.parametrize(
    "variant",
    ["never_acquired", "wrong_owner", "stale_epoch", "released", "expired"],
)
async def test_commit_plan_without_valid_lease_writes_nothing(state_store: StateStore, variant):
    rid = await state_store.create_request("hello")
    if variant == "never_acquired":
        lease = Lease(request_id=rid, owner="m1", epoch=1, expires_at=9e12)
    else:
        duration = 0.05 if variant == "expired" else 30
        lease = (await state_store.acquire_lease(rid, "m1", duration)).lease
        if variant == "wrong_owner":
            lease = _foreign(lease, owner="m2")
        elif variant == "stale_epoch":
            lease = _foreign(lease, epoch=lease.epoch - 1)
        elif variant == "released":
            assert (await state_store.release_lease(lease)).ok
        else:
            await asyncio.sleep(0.1)
    before = await dump_state(state_store, rid)

    res = await state_store.commit_plan(lease, PLAN)

    assert res.status is StoreStatus.LEASE_LOST
    assert await dump_state(state_store, rid) == before
    snap = await state_store.load_snapshot(rid)
    assert snap.status is RequestStatus.PENDING and snap.plan is None and snap.jobs == {}


async def test_commit_plan_on_cancelled_request_writes_nothing(state_store: StateStore):
    rid, lease = await pending_with_lease(state_store)
    assert (await state_store.cancel_request(rid)).ok
    before = await dump_state(state_store, rid)
    res = await state_store.commit_plan(lease, PLAN)
    assert res.status is StoreStatus.REQUEST_TERMINAL
    assert await dump_state(state_store, rid) == before


async def test_commit_plan_twice_keeps_first_plan(state_store: StateStore):
    rid, lease = await running_with_lease(state_store)
    before = await dump_state(state_store, rid)
    other = JobPlan(jobs=[PlannedJob(job_id="z", instruction="other", capabilities=["x"])])
    res = await state_store.commit_plan(lease, other)
    assert res.status is StoreStatus.INVALID_STATE
    assert await dump_state(state_store, rid) == before


# ------------------------------------------------------------------ commit_plan: failures


async def test_commit_plan_malformed_job_json_writes_nothing(state_store: StateStore):
    """A record that fails to decode mid-list must not leave earlier Jobs behind."""
    rid, lease = await pending_with_lease(state_store)
    keys_, args = commit_args(lease, PLAN)
    args[-1] = '{"job": not json'  # the last Job's record is malformed
    script = state_store.client.register_script(script_source("commit_plan"))
    before = await dump_state(state_store, rid)

    # Real Redis surfaces a script error as ResponseError; fakeredis may re-raise the
    # decoder's own exception. Either way the script must abort before any write.
    with pytest.raises(Exception):  # noqa: B017
        await script(keys=keys_, args=args)

    assert await dump_state(state_store, rid) == before


async def test_commit_plan_fault_at_any_write_never_exposes_running_without_plan(fake_redis):
    """Inject a failure at each write in turn: Running always comes with the full plan."""
    store = StateStore(fake_redis)
    script = fake_redis.register_script(SHIM + script_source("commit_plan"))

    rid, lease = await pending_with_lease(store)
    keys_, args = commit_args(lease, PLAN)
    await script(keys=keys_, args=args)
    total_writes = await fake_redis.llen(TRACE_KEY)
    assert total_writes > len(PLAN.jobs)

    for fail_at in range(1, total_writes + 1):
        rid, lease = await pending_with_lease(store)
        keys_, args = commit_args(lease, PLAN)
        await fake_redis.set("test:fail_at", fail_at)
        with pytest.raises(ResponseError, match="injected fault"):
            await script(keys=keys_, args=args)
        await fake_redis.delete("test:fail_at")
        snap = await store.load_snapshot(rid)
        # Plan and status are set by one command after every Job is written, so a
        # failure either leaves the Request Pending without a plan or Running with all of it.
        if snap.status is RequestStatus.RUNNING:
            assert snap.plan == PLAN, f"fault at write {fail_at}"
            assert list(snap.jobs) == PLAN.job_ids(), f"fault at write {fail_at}"
        else:
            assert snap.status is RequestStatus.PENDING, f"fault at write {fail_at}"
            assert snap.plan is None, f"fault at write {fail_at}"


async def test_commit_plan_writes_plan_and_status_in_one_command(fake_redis):
    store = StateStore(fake_redis)
    script = fake_redis.register_script(SHIM + script_source("commit_plan"))
    rid, lease = await pending_with_lease(store)
    keys_, args = commit_args(lease, PLAN)
    await script(keys=keys_, args=args)
    trace = await fake_redis.lrange(TRACE_KEY, 0, -1)
    request_writes = [t for t in trace if t.startswith(f"HSET {keys.request(rid)} ")]
    assert len(request_writes) == 1
    fields = request_writes[0].split(" ")
    assert "plan" in fields and "status" in fields and "Running" in fields


# ------------------------------------------------------------------ finalize


async def test_finalize_writes_final_result_before_completed(fake_redis):
    store = StateStore(fake_redis)
    script = fake_redis.register_script(SHIM + script_source("finalize"))
    rid, lease = await running_with_lease(store)
    keys_, args = finalize_args(lease, FINAL)
    assert list(await script(keys=keys_, args=args))[0] == "OK"

    trace = await fake_redis.lrange(TRACE_KEY, 0, -1)
    req = keys.request(rid)
    result_idx = next(i for i, t in enumerate(trace) if t.startswith(f"HSET {req} final_result "))
    status_idx = next(i for i, t in enumerate(trace) if t == f"HSET {req} status Completed")
    assert result_idx < status_idx
    snap = await store.load_snapshot(rid)
    assert snap.status is RequestStatus.COMPLETED and snap.final_result == FINAL
    assert snap.lease is None


async def test_finalize_fault_at_any_write_never_exposes_completed_without_result(fake_redis):
    store = StateStore(fake_redis)
    script = fake_redis.register_script(SHIM + script_source("finalize"))
    rid, lease = await running_with_lease(store)
    keys_, args = finalize_args(lease, FINAL)
    await script(keys=keys_, args=args)
    total_writes = await fake_redis.llen(TRACE_KEY)

    for fail_at in range(1, total_writes + 1):
        rid, lease = await running_with_lease(store)
        keys_, args = finalize_args(lease, FINAL)
        await fake_redis.set("test:fail_at", fail_at)
        with pytest.raises(ResponseError, match="injected fault"):
            await script(keys=keys_, args=args)
        await fake_redis.delete("test:fail_at")
        snap = await store.load_snapshot(rid)
        if snap.status is RequestStatus.COMPLETED:
            assert snap.final_result == FINAL, f"fault at write {fail_at}"
        else:
            assert snap.status is RequestStatus.RUNNING, f"fault at write {fail_at}"


async def test_finalize_completed_without_result_writes_nothing(state_store: StateStore):
    rid, lease = await running_with_lease(state_store)
    before = await dump_state(state_store, rid)
    res = await state_store.finalize(lease, None)
    assert res.status is StoreStatus.INVALID_STATE
    assert await dump_state(state_store, rid) == before


@pytest.mark.parametrize("variant", ["wrong_owner", "stale_epoch", "released", "expired"])
async def test_finalize_without_valid_lease_writes_nothing(state_store: StateStore, variant):
    rid, lease = await pending_with_lease(state_store)
    if variant == "expired":
        assert (await state_store.release_lease(lease)).ok
        lease = (await state_store.acquire_lease(rid, "m1", 0.05)).lease
    assert (await state_store.commit_plan(lease, PLAN)).ok
    if variant == "wrong_owner":
        lease = _foreign(lease, owner="m2")
    elif variant == "stale_epoch":
        lease = _foreign(lease, epoch=lease.epoch - 1)
    elif variant == "released":
        assert (await state_store.release_lease(lease)).ok
    else:
        await asyncio.sleep(0.1)
    before = await dump_state(state_store, rid)

    res = await state_store.finalize(lease, FINAL)

    assert res.status is StoreStatus.LEASE_LOST
    assert await dump_state(state_store, rid) == before
    snap = await state_store.load_snapshot(rid)
    assert snap.status is RequestStatus.RUNNING and snap.final_result is None


async def test_concurrent_readers_never_see_completed_without_result(state_store: StateStore):
    rid, lease = await running_with_lease(state_store)
    r = state_store.client
    seen: list[tuple[str, str | None]] = []
    done = asyncio.Event()

    async def reader():
        while not done.is_set():
            status, final = await r.hmget(keys.request(rid), keys.F_STATUS, keys.F_FINAL_RESULT)
            seen.append((status, final))
            await asyncio.sleep(0)

    task = asyncio.create_task(reader())
    await asyncio.sleep(0)
    assert (await state_store.finalize(lease, FINAL)).ok
    await asyncio.sleep(0)
    done.set()
    await task
    seen.append(tuple(await r.hmget(keys.request(rid), keys.F_STATUS, keys.F_FINAL_RESULT)))
    assert seen[-1][0] == "Completed"
    assert all(final for status, final in seen if status == "Completed")
