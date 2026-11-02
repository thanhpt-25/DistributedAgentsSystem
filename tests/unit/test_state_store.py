"""Unit tests for the Redis State_Store Lua scripts and typed API (task 9.1)."""

from __future__ import annotations

import asyncio
import itertools
from datetime import UTC, datetime

import pytest

from das.models import (
    FinalResult,
    JobPlan,
    JobReport,
    JobResultPayload,
    JobState,
    PlannedJob,
    RequestStatus,
    TokenUsage,
)
from das.state import (
    CANCEL_REASON,
    INBOX_UNACKED_EXHAUSTED,
    StateStore,
    StoreStatus,
    Timer,
    TimerKey,
    TransitionBatch,
    keys,
)

PLAN = JobPlan(
    jobs=[
        PlannedJob(job_id="a", instruction="research x", capabilities=["research"]),
        PlannedJob(job_id="b", instruction="sum", capabilities=["summarize"], depends_on=["a"]),
    ]
)


def result(rid: str, job_id: str, attempt: int = 1, status: str = "Succeeded", content: str = "ok"):
    return JobResultPayload(
        request_id=rid,
        job_id=job_id,
        attempt=attempt,
        worker_id="w1",
        status=status,
        content=content,
        usage=TokenUsage(input_tokens=1, output_tokens=2, total_tokens=3),
        duration_ms=5,
    )


async def running_request(store: StateStore):
    rid = await store.create_request("hello")
    acq = await store.acquire_lease(rid, "m1", 30)
    assert (await store.commit_plan(acq.lease, PLAN)).ok
    return rid, acq.lease


# ------------------------------------------------------------------ create


async def test_create_request_is_pending_and_enqueued(state_store: StateStore):
    rid = await state_store.create_request("hello")
    snap = await state_store.load_snapshot(rid)
    assert snap.status is RequestStatus.PENDING
    assert snap.prompt == "hello"
    assert snap.jobs == {} and snap.plan is None and snap.lease is None
    assert rid in await state_store.claimable_request_ids()


async def test_create_request_regenerates_id_on_collision(fake_redis):
    ids = itertools.chain(["dup", "dup", "fresh"])
    store = StateStore(fake_redis, id_factory=lambda: next(ids))
    assert await store.create_request("p1") == "dup"
    assert await store.create_request("p2") == "fresh"
    assert (await store.load_snapshot("dup")).prompt == "p1"


async def test_unknown_request_snapshot_is_none(state_store: StateStore):
    assert await state_store.load_snapshot("nope") is None
    assert await state_store.get_status("nope") is None


# ------------------------------------------------------------------ leases


async def test_lease_single_winner_under_concurrency(state_store: StateStore):
    rid = await state_store.create_request("p")
    results = await asyncio.gather(
        *(state_store.acquire_lease(rid, f"m{i}", 30) for i in range(10))
    )
    winners = [r for r in results if r.ok]
    assert len(winners) == 1
    assert all(r.status is StoreStatus.HELD for r in results if not r.ok)
    lease = winners[0].lease
    assert lease.epoch == 1
    assert await state_store.get_lease(rid) == lease
    # Acquired Requests leave the pending set.
    assert rid not in await state_store.client.zrange(keys.REQUESTS_PENDING, 0, -1)


async def test_renew_extends_expiry_and_rejects_wrong_epoch(state_store: StateStore):
    rid = await state_store.create_request("p")
    lease = (await state_store.acquire_lease(rid, "m1", 30)).lease
    before = await state_store.server_time()
    renewed = await state_store.renew_lease(lease, 100)
    assert renewed.ok and renewed.expires_at >= before + 100
    stale = lease.model_copy(update={"epoch": lease.epoch + 1})
    assert (await state_store.renew_lease(stale, 100)).status is StoreStatus.LEASE_LOST


async def test_expired_lease_is_lost_and_takeover_bumps_epoch(state_store: StateStore):
    rid = await state_store.create_request("p")
    old = (await state_store.acquire_lease(rid, "m1", 0.05)).lease
    await asyncio.sleep(0.1)
    assert rid in await state_store.claimable_request_ids()
    new = (await state_store.acquire_lease(rid, "m2", 30)).lease
    assert new.epoch == old.epoch + 1
    assert (await state_store.renew_lease(old, 30)).status is StoreStatus.LEASE_LOST
    assert (await state_store.commit_plan(old, PLAN)).status is StoreStatus.LEASE_LOST
    assert (await state_store.load_snapshot(rid)).status is RequestStatus.PENDING
    assert (await state_store.commit_plan(new, PLAN)).ok


async def test_release_keeps_epoch_monotonic_and_makes_claimable(state_store: StateStore):
    rid = await state_store.create_request("p")
    first = (await state_store.acquire_lease(rid, "m1", 30)).lease
    assert (await state_store.release_lease(first)).ok
    assert await state_store.get_lease(rid) is None
    assert rid in await state_store.claimable_request_ids()
    second = (await state_store.acquire_lease(rid, "m1", 30)).lease
    assert second.epoch == 2
    assert (await state_store.release_lease(first)).status is StoreStatus.LEASE_LOST


async def test_acquire_on_terminal_or_missing_request(state_store: StateStore):
    assert (await state_store.acquire_lease("missing", "m1", 30)).status is StoreStatus.NOT_FOUND
    rid = await state_store.create_request("p")
    assert (await state_store.cancel_request(rid)).ok
    assert (await state_store.acquire_lease(rid, "m1", 30)).status is StoreStatus.REQUEST_TERMINAL


# ------------------------------------------------------------------ commit_plan


async def test_commit_plan_writes_jobs_status_and_transitions(state_store: StateStore):
    rid, lease = await running_request(state_store)
    snap = await state_store.load_snapshot(rid, include_transitions=True)
    assert snap.status is RequestStatus.RUNNING
    assert snap.plan == PLAN
    assert list(snap.jobs) == ["a", "b"]
    assert all(r.state is JobState.WAITING and r.attempt == 0 for r in snap.jobs.values())
    assert [(t.job_id, t.from_state, t.to_state) for t in snap.transitions] == [
        ("a", None, JobState.WAITING),
        ("b", None, JobState.WAITING),
    ]
    at = snap.transitions[0].at
    assert at.tzinfo is not None and at.microsecond % 1000 == 0
    assert abs((datetime.now(UTC) - at).total_seconds()) < 5
    # A second commit is rejected without changes.
    again = await state_store.commit_plan(lease, PLAN)
    assert again.status is StoreStatus.INVALID_STATE and again.detail == "Running"


# ------------------------------------------------------------------ apply_transition


async def test_apply_transition_updates_jobs_results_timers_and_cursor(state_store: StateStore):
    rid, lease = await running_request(state_store)
    snap = await state_store.load_snapshot(rid)
    a = snap.jobs["a"]
    dispatched = a.model_copy(update={"state": JobState.DISPATCHED, "attempt": 1})
    res = await state_store.apply_transition(
        lease,
        TransitionBatch(
            jobs={"a": dispatched},
            timers_add=[Timer("retry", "a", 1, 123.5)],
            inbox_cursor="5-0",
            request_fields={"note": "x"},
        ),
    )
    assert res.ok and res.transitions_written == 1

    first = result(rid, "a", content="first")
    succeeded = dispatched.model_copy(update={"state": JobState.SUCCEEDED, "worker_id": "w1"})
    res = await state_store.apply_transition(
        lease,
        TransitionBatch(
            jobs={"a": succeeded}, results=[first], timers_remove=[TimerKey("retry", "a", 1)]
        ),
    )
    assert res.ok
    # Duplicate result for the same (job, attempt) never replaces the first one (Req 2.8).
    dup = result(rid, "a", content="second")
    assert (await state_store.apply_transition(lease, TransitionBatch(results=[dup]))).ok

    snap = await state_store.load_snapshot(rid, include_transitions=True)
    assert snap.jobs["a"].state is JobState.SUCCEEDED
    assert snap.results[("a", 1)].content == "first"
    assert snap.timers == []
    assert snap.inbox_cursor == "5-0"
    tail = [(t.from_state, t.to_state, t.attempt, t.worker_id) for t in snap.transitions[2:]]
    assert tail == [
        (JobState.WAITING, JobState.DISPATCHED, 1, None),
        (JobState.DISPATCHED, JobState.SUCCEEDED, 1, "w1"),
    ]
    times = [t.at for t in snap.transitions]
    assert times == sorted(times)


async def test_apply_transition_without_state_change_logs_nothing(state_store: StateStore):
    rid, lease = await running_request(state_store)
    a = (await state_store.load_snapshot(rid)).jobs["a"]
    res = await state_store.apply_transition(
        lease, TransitionBatch(jobs={"a": a.model_copy(update={"publish_failures": 1})})
    )
    assert res.ok and res.transitions_written == 0
    assert len(await state_store.get_transitions(rid)) == 2


async def test_apply_transition_fenced_by_lease(state_store: StateStore):
    rid, lease = await running_request(state_store)
    before = await state_store.load_snapshot(rid, include_transitions=True)
    a = before.jobs["a"].model_copy(update={"state": JobState.READY})
    impostor = lease.model_copy(update={"owner": "m2"})
    res = await state_store.apply_transition(impostor, TransitionBatch(jobs={"a": a}))
    assert res.status is StoreStatus.LEASE_LOST
    assert await state_store.load_snapshot(rid, include_transitions=True) == before


async def test_apply_transition_on_cancelled_request(state_store: StateStore):
    rid, lease = await running_request(state_store)
    assert (await state_store.cancel_request(rid)).ok
    a = (await state_store.load_snapshot(rid)).jobs["a"]
    res = await state_store.apply_transition(
        lease, TransitionBatch(jobs={"a": a.model_copy(update={"state": JobState.READY})})
    )
    assert res.status is StoreStatus.REQUEST_TERMINAL
    assert (await state_store.renew_lease(lease, 30)).status is StoreStatus.REQUEST_TERMINAL


async def test_apply_transition_rejects_reserved_fields(state_store: StateStore):
    rid, lease = await running_request(state_store)
    with pytest.raises(ValueError):
        await state_store.apply_transition(lease, TransitionBatch(request_fields={"status": "x"}))


# ------------------------------------------------------------------ cancel


async def test_cancel_running_request_cancels_non_terminal_jobs(state_store: StateStore):
    rid, lease = await running_request(state_store)
    a = (await state_store.load_snapshot(rid)).jobs["a"]
    await state_store.apply_transition(
        lease, TransitionBatch(jobs={"a": a.model_copy(update={"state": JobState.SUCCEEDED})})
    )
    assert (await state_store.cancel_request(rid)).ok
    snap = await state_store.load_snapshot(rid, include_transitions=True)
    assert snap.status is RequestStatus.CANCELLED
    assert snap.jobs["a"].state is JobState.SUCCEEDED
    assert snap.jobs["b"].state is JobState.CANCELLED
    assert snap.jobs["b"].reason == CANCEL_REASON
    last = snap.transitions[-1]
    assert (last.job_id, last.from_state, last.to_state) == ("b", JobState.WAITING, JobState.CANCELLED)


async def test_cancel_terminal_and_missing(state_store: StateStore):
    assert (await state_store.cancel_request("missing")).status is StoreStatus.NOT_FOUND
    rid = await state_store.create_request("p")
    assert (await state_store.cancel_request(rid)).ok
    before = await state_store.load_snapshot(rid, include_transitions=True)
    again = await state_store.cancel_request(rid)
    assert again.status is StoreStatus.TERMINAL
    assert again.request_status is RequestStatus.CANCELLED
    assert await state_store.load_snapshot(rid, include_transitions=True) == before


# ------------------------------------------------------------------ finalize


def final_result() -> FinalResult:
    return FinalResult(
        summary="done",
        jobs=[JobReport(job_id="a", state=JobState.SUCCEEDED, worker_id="w1")],
    )


async def test_finalize_completed_writes_result_and_releases(state_store: StateStore):
    rid, lease = await running_request(state_store)
    assert (await state_store.finalize(lease, final_result())).ok
    snap = await state_store.load_snapshot(rid)
    assert snap.status is RequestStatus.COMPLETED
    assert snap.final_result == final_result()
    assert snap.lease is None
    assert rid not in await state_store.claimable_request_ids()
    assert (await state_store.finalize(lease, final_result())).status is StoreStatus.LEASE_LOST


async def test_finalize_completed_requires_running_and_result(state_store: StateStore):
    rid = await state_store.create_request("p")
    lease = (await state_store.acquire_lease(rid, "m1", 30)).lease
    res = await state_store.finalize(lease, final_result())
    assert res.status is StoreStatus.INVALID_STATE
    assert (await state_store.get_status(rid)) is RequestStatus.PENDING


async def test_fail_request_from_pending(state_store: StateStore):
    rid = await state_store.create_request("p")
    lease = (await state_store.acquire_lease(rid, "m1", 30)).lease
    assert (await state_store.fail_request(lease, "plan invalid: CYCLE")).ok
    snap = await state_store.load_snapshot(rid)
    assert snap.status is RequestStatus.FAILED
    assert snap.error == "plan invalid: CYCLE"
    assert snap.plan is None and snap.final_result is None and snap.lease is None


async def test_finalize_on_cancelled_request(state_store: StateStore):
    rid, lease = await running_request(state_store)
    await state_store.cancel_request(rid)
    res = await state_store.finalize(lease, final_result())
    assert res.status is StoreStatus.REQUEST_TERMINAL
    assert (await state_store.load_snapshot(rid)).final_result is None


# ------------------------------------------------------------------ inbox


async def test_inbox_append_and_read_after_cursor(state_store: StateStore):
    rid, lease = await running_request(state_store)
    r1 = result(rid, "a")
    id1 = await state_store.record_result_inbox(rid, r1)
    id2 = await state_store.record_inbox_event(rid, INBOX_UNACKED_EXHAUSTED, {"job_id": "a"})
    entries = await state_store.read_inbox(rid)
    assert [e.entry_id for e in entries] == [id1, id2]
    assert entries[0].job_result() == r1
    assert entries[1].payload() == {"job_id": "a"}
    await state_store.apply_transition(lease, TransitionBatch(inbox_cursor=id1))
    assert [e.entry_id for e in await state_store.read_inbox(rid)] == [id2]
    assert await state_store.read_inbox(rid, after=id2) == []


async def test_inbox_for_unknown_request_creates_nothing(state_store: StateStore):
    assert await state_store.record_result_inbox("missing", result("missing", "a")) is None
    assert await state_store.client.exists(keys.inbox("missing")) == 0


# ------------------------------------------------------------------ removal


async def test_transitions_survive_lease_handover_until_request_deleted(state_store: StateStore):
    rid, lease = await running_request(state_store)
    assert (await state_store.release_lease(lease)).ok
    lease2 = (await state_store.acquire_lease(rid, "m2", 30)).lease
    a = (await state_store.load_snapshot(rid)).jobs["a"]
    await state_store.apply_transition(
        lease2, TransitionBatch(jobs={"a": a.model_copy(update={"state": JobState.READY})})
    )
    assert len(await state_store.get_transitions(rid)) == 3
    await state_store.record_result_inbox(rid, result(rid, "a"))
    await state_store.delete_request(rid)
    assert await state_store.load_snapshot(rid) is None
    assert await state_store.client.exists(*keys.request_keys(rid)) == 0
    assert rid not in await state_store.claimable_request_ids()


# ------------------------------------------------------------------ keys


def test_key_layout():
    assert keys.request("r") == "das:req:r"
    assert keys.jobs("r") == "das:req:r:jobs"
    assert keys.results("r") == "das:req:r:results"
    assert keys.inbox("r") == "das:req:r:inbox"
    assert keys.timers("r") == "das:req:r:timers"
    assert keys.transitions("r") == "das:req:r:transitions"
    assert keys.lease("r") == "das:lease:r"
    assert keys.LEASES_EXPIRY == "das:leases:expiry"
    assert keys.REQUESTS_PENDING == "das:requests:pending"
    assert keys.agent("w") == "das:agent:w"
    assert keys.AGENTS == "das:agents"
    assert keys.parse_result_field(keys.result_field("job-1", 3)) == ("job-1", 3)
    assert keys.parse_timer_member(keys.timer_member("retry", "j", 2)) == ("retry", "j", 2)
    with pytest.raises(ValueError):
        keys.timer_member("a:b", "j", 1)


@pytest.mark.parametrize(
    "ms", [0, 951_782_400_123, 1_709_210_096_999, 4_102_444_799_000, 1_735_689_600_000]
)
async def test_lua_iso_ms_matches_python(fake_redis, ms):
    from das.state.scripts import _read

    script = fake_redis.register_script(_read("_common") + "\nreturn iso_ms(tonumber(ARGV[1]))")
    expected = datetime.fromtimestamp(ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms % 1000:03d}Z"
    assert await script(keys=[], args=[ms]) == expected
