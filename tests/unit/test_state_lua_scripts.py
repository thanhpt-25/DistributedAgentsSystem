"""Scenario unit tests for the Phase 2 Lua scripts, run through StateStore on
fakeredis[lua] with the real ``das/state/lua`` sources (das-observability-operations
task 9.8; Requirements 3.3, 13.2, 13.15, 14.2, 15.10).

Covered scenarios:
- ``finalize`` returns ``REPLAY_PENDING`` until the owner applies the Replay.
- ``cancel_request`` returns a Replay that was never applied to ``Pending``.
- A Failed Request is reopened by Replay with every effect of Req 13.2.
- ``acquire_lease`` reports ``initial`` / ``takeover`` / re-acquire by the same agent.
- ``index_request`` indexes Requests whose data was written by v1 scripts.
- ``finished_at`` is overwritten when a replayed Request becomes terminal again.
"""

from __future__ import annotations

from datetime import UTC, datetime

from das.models import FinalResult, JobRecord, JobReport, JobState, RequestStatus
from das.models.dlq import DeadLetterFields
from das.state import StateStore, StoreStatus, TransitionBatch, keys
from das.state.store import LEASE_KIND_INITIAL
from tests.unit.test_state_phase2_dlq import everything
from tests.unit.test_state_store import PLAN

DL1 = "c" * 32
T0 = 1_700_000_000_000

FAILED_FINAL = FinalResult(
    summary=None,
    jobs=[JobReport(job_id="a", state=JobState.FAILED, worker_id="w1")],
    error="job a failed",
)
DONE_FINAL = FinalResult(
    summary="done",
    jobs=[JobReport(job_id="a", state=JobState.SUCCEEDED, worker_id="w1")],
)


def dl_fields(rid: str, dlid: str = DL1, attempt: int = 1) -> DeadLetterFields:
    return DeadLetterFields(
        dead_letter_id=dlid, reason="retries_exhausted", request_id=rid, job_id="a",
        attempt=attempt, capabilities=["research"], last_error="boom", message_id="m-1",
    )


async def running_with_failed_job(store: StateStore, manager: str = "m1"):
    """A Running Request whose Job a is Failed (attempt 1) and Job b Skipped."""
    rid = await store.create_request("hello")
    lease = (await store.acquire_lease(rid, manager, 30)).lease
    assert (await store.commit_plan(lease, PLAN)).ok
    batch = TransitionBatch(jobs={
        "a": JobRecord(job=PLAN.jobs[0], state=JobState.FAILED, attempt=1, reason="x"),
        "b": JobRecord(job=PLAN.jobs[1], state=JobState.SKIPPED, reason="dep failed"),
    })
    assert (await store.apply_transition(lease, batch)).ok
    return rid, lease


def apply_replay_batch(dlid: str) -> TransitionBatch:
    """What the owner writes when it applies a Replay: reset Jobs, tag, drop the gate."""
    return TransitionBatch(
        jobs={
            "a": JobRecord(job=PLAN.jobs[0], state=JobState.READY, attempt=1),
            "b": JobRecord(job=PLAN.jobs[1], state=JobState.WAITING),
        },
        tag=dlid,
        replays_applied=[dlid],
    )


def ms_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, UTC)


# ------------------------------------------------------------------ finalize gate


async def test_finalize_returns_replay_pending_until_replay_applied(fake_redis_time, state_store):
    store = state_store
    fake_redis_time.set_ms(T0)
    rid, lease = await running_with_failed_job(store)
    await store.create_dead_letter(dl_fields(rid))
    assert (await store.replay_dead_letter(DL1)).accepted

    before = await everything(store)
    for status, final, err in (
        (RequestStatus.COMPLETED, DONE_FINAL, None),
        (RequestStatus.FAILED, FAILED_FINAL, "job a failed"),
    ):
        res = await store.finalize(lease, final, status, err)
        assert res.status is StoreStatus.REPLAY_PENDING
    assert await everything(store) == before  # the gate writes nothing

    # Once the owner applies the Replay (SREM pending_replays) finalize goes through.
    assert (await store.apply_transition(lease, apply_replay_batch(DL1))).ok
    assert await store.client.scard(keys.pending_replays(rid)) == 0
    fake_redis_time.set_ms(T0 + 700)
    res = await store.finalize(lease, DONE_FINAL)
    assert res.ok and res.finished_at_ms == T0 + 700
    assert await store.get_status(rid) is RequestStatus.COMPLETED


# ------------------------------------------------------------------ cancel + unapplied replay


async def test_cancel_returns_unapplied_replay_to_pending(fake_redis_time, state_store):
    store = state_store
    r = store.client
    fake_redis_time.set_ms(T0)
    rid, _ = await running_with_failed_job(store)
    await store.create_dead_letter(dl_fields(rid))
    fake_redis_time.set_ms(T0 + 1000)
    assert (await store.replay_dead_letter(DL1)).accepted
    assert await r.zscore(keys.dlq_by_status("Replayed"), DL1) == T0

    fake_redis_time.set_ms(T0 + 3000)
    res = await store.cancel_request(rid)
    assert res.ok and res.finished_at_ms == T0 + 3000

    rec = await store.get_dead_letter(DL1)
    assert rec.status == "Pending" and rec.status_changed_at == ms_dt(T0 + 3000)
    assert await r.zscore(keys.dlq_by_status("Pending"), DL1) == T0  # dead_lettered_at
    assert await r.zscore(keys.dlq_by_status("Replayed"), DL1) is None
    assert await r.zscore(keys.DLQ_TERMINAL, DL1) is None
    assert not await r.exists(keys.pending_replays(rid))

    snap = await store.load_snapshot(rid)
    assert snap.status is RequestStatus.CANCELLED and snap.finished_at == T0 + 3000
    assert await r.zscore(keys.requests_by_status("Cancelled"), rid) is not None
    assert await r.zscore(keys.requests_by_status("Running"), rid) is None

    # The record is actionable again: Replay is rejected on the Cancelled Request,
    # Discard is accepted.
    replay = await store.replay_dead_letter(DL1)
    assert (replay.outcome, replay.current_state) == ("request_state", "Cancelled")
    assert (await store.discard_dead_letter(DL1)).outcome == "accepted"


# ------------------------------------------------------------------ Failed reopen (Req 13.2)


async def test_replay_reopens_failed_request_only_without_unexpired_lease(
    fake_redis_time, state_store
):
    store = state_store
    r = store.client
    fake_redis_time.set_ms(T0)
    rid, lease = await running_with_failed_job(store)
    created = await r.zscore(keys.REQUESTS_INDEX, rid)
    fake_redis_time.set_ms(T0 + 2000)
    assert (await store.fail_request(lease, "job a failed", FAILED_FINAL)).ok
    await store.create_dead_letter(dl_fields(rid))

    # An unexpired Lease on the Failed Request blocks the reopen and writes nothing.
    now = await store.server_time()
    await r.hset(keys.lease(rid), mapping={"owner": "m9", "expires_at": f"{now + 10:.6f}"})
    before = await everything(store)
    held = await store.replay_dead_letter(DL1)
    assert (held.outcome, held.current_state) == ("lease_held", "Failed")
    assert await everything(store) == before

    # After the Lease expires the same Replay is accepted.
    fake_redis_time.set_ms(T0 + 20_000)
    res = await store.replay_dead_letter(DL1)
    assert res.accepted and res.request_status_before is RequestStatus.FAILED

    snap = await store.load_snapshot(rid, include_transitions=True)
    # Request back to Pending; error and Final_Result moved into the replay history.
    assert snap.status is RequestStatus.PENDING
    assert snap.error is None and snap.final_result is None
    [hist] = snap.replay_history
    assert hist.dead_letter_id == DL1 and hist.error == "job a failed"
    assert hist.final_result == FAILED_FINAL and hist.finished_at == ms_dt(T0 + 2000)
    # Failed -> Pending transition entry carrying the Dead_Letter_ID.
    [entry] = snap.request_transitions
    assert (entry.from_status, entry.to_status) == (RequestStatus.FAILED, RequestStatus.PENDING)
    assert entry.dead_letter_id == DL1
    # Finished_At cleared (hash and retention sets).
    assert snap.finished_at is None
    assert await r.hget(keys.request(rid), keys.F_FINISHED_AT) is None
    assert await r.zscore(keys.REQUESTS_FINISHED, rid) is None
    assert await r.zscore(keys.REQUESTS_INBOX_PURGE, rid) is None
    # Request_Index status is Pending.
    assert await r.zscore(keys.requests_by_status("Pending"), rid) == created
    assert await r.zscore(keys.requests_by_status("Failed"), rid) is None
    page = await store.list_requests(statuses=[RequestStatus.PENDING])
    assert [i.request_id for i in page.items] == [rid]
    # Queued for Lease acquisition, record Replayed.
    assert rid in await store.claimable_request_ids()
    assert (await store.get_dead_letter(DL1)).status == "Replayed"
    assert (await store.acquire_lease(rid, "m2", 30)).ok


# ------------------------------------------------------------------ acquire_lease kind (Req 3.3)


async def test_acquire_lease_kind_initial_takeover_and_reacquire(fake_redis_time, state_store):
    store = state_store
    fake_redis_time.set_ms(T0)
    rid = await store.create_request("p")

    first = await store.acquire_lease(rid, "m1", 10)
    assert first.ok and first.kind == LEASE_KIND_INITIAL == "initial" and first.lease.epoch == 1
    assert (await store.acquire_lease(rid, "m2", 10)).status is StoreStatus.HELD

    # Same agent after expiry: initial.
    fake_redis_time.advance(11)
    again = await store.acquire_lease(rid, "m1", 10)
    assert (again.kind, again.lease.epoch) == ("initial", 2)

    # Same agent after its own release: initial.
    assert (await store.release_lease(again.lease)).ok
    after_release = await store.acquire_lease(rid, "m1", 10)
    assert (after_release.kind, after_release.lease.epoch) == ("initial", 3)

    # Another agent after expiry: takeover.
    fake_redis_time.advance(11)
    taken = await store.acquire_lease(rid, "m2", 10)
    assert (taken.kind, taken.lease.epoch, taken.lease.owner) == ("takeover", 4, "m2")

    # Another agent after a release: still takeover (last_owner survives release).
    assert (await store.release_lease(taken.lease)).ok
    assert await store.client.hget(keys.lease(rid), keys.F_LAST_OWNER) == "m2"
    back = await store.acquire_lease(rid, "m1", 10)
    assert (back.kind, back.lease.epoch) == ("takeover", 5)


# ------------------------------------------------------------------ index_request on v1 data


async def strip_phase2(store: StateStore, rid: str) -> None:
    """Remove everything Phase 2 scripts add, leaving the layout v1 scripts wrote."""
    r = store.client
    await r.hdel(keys.request(rid), keys.F_FINISHED_AT, keys.F_JOB_COUNT, keys.F_TRACE_CONTEXT,
                 keys.F_USAGE_INPUT, keys.F_USAGE_OUTPUT, keys.F_USAGE_TOTAL)
    await r.hdel(keys.lease(rid), keys.F_LAST_OWNER)
    await r.delete(keys.usage(rid), keys.pending_replays(rid))
    for k in [keys.REQUESTS_INDEX, keys.REQUESTS_FINISHED, keys.REQUESTS_INBOX_PURGE,
              *(keys.requests_by_status(s.value) for s in RequestStatus)]:
        await r.zrem(k, rid)


async def test_index_request_on_v1_written_requests(fake_redis_time, state_store):
    store = state_store
    r = store.client
    fake_redis_time.set_ms(T0)
    pending_rid = await store.create_request("p")
    fake_redis_time.set_ms(T0 + 10)
    running_rid, _ = await running_with_failed_job(store)
    fake_redis_time.set_ms(T0 + 20)
    done_rid, lease = await running_with_failed_job(store)
    assert (await store.fail_request(lease, "job a failed", FAILED_FINAL)).ok
    rids = {pending_rid: RequestStatus.PENDING, running_rid: RequestStatus.RUNNING,
            done_rid: RequestStatus.FAILED}
    for rid in rids:
        await strip_phase2(store, rid)
    assert await r.zcard(keys.REQUESTS_INDEX) == 0

    fake_redis_time.set_ms(T0 + 50_000)
    for rid in rids:
        assert await store.index_request(rid)
    await store.set_index_ready()

    for rid, status in rids.items():
        snap = await store.load_snapshot(rid)
        created_ms = round(snap.created_at * 1000)
        assert await r.zscore(keys.REQUESTS_INDEX, rid) == created_ms
        assert await r.zscore(keys.requests_by_status(status.value), rid) == created_ms
    # Only the terminal v1 Request is stamped with the script's TIME.
    assert (await store.load_snapshot(done_rid)).finished_at == T0 + 50_000
    assert await r.zscore(keys.REQUESTS_FINISHED, done_rid) == T0 + 50_000
    assert await r.zscore(keys.REQUESTS_INBOX_PURGE, done_rid) == T0 + 50_000
    for rid in (pending_rid, running_rid):
        assert await r.hget(keys.request(rid), keys.F_FINISHED_AT) is None

    page = await store.list_requests()
    assert [(i.request_id, i.status) for i in page.items] == [
        (done_rid, RequestStatus.FAILED),
        (running_rid, RequestStatus.RUNNING),
        (pending_rid, RequestStatus.PENDING),
    ]
    assert page.items[0].finished_at_ms == T0 + 50_000
    failed = await store.list_requests(statuses=[RequestStatus.FAILED])
    assert [i.request_id for i in failed.items] == [done_rid]

    # Re-running the backfill is a no-op.
    before = await everything(store)
    fake_redis_time.advance(5)
    for rid in rids:
        assert await store.index_request(rid)
    assert await everything(store) == before


# ------------------------------------------------------------------ finished_at overwrite (Req 14.2)


async def test_finished_at_overwritten_after_replay(fake_redis_time, state_store):
    store = state_store
    r = store.client
    fake_redis_time.set_ms(T0)
    rid, lease = await running_with_failed_job(store)
    created = await r.zscore(keys.REQUESTS_INDEX, rid)
    fake_redis_time.set_ms(T0 + 1000)
    first = await store.fail_request(lease, "job a failed", FAILED_FINAL)
    assert first.finished_at_ms == T0 + 1000
    assert (await store.load_snapshot(rid)).finished_at == T0 + 1000

    await store.create_dead_letter(dl_fields(rid))
    fake_redis_time.set_ms(T0 + 2000)
    assert (await store.replay_dead_letter(DL1)).accepted

    fake_redis_time.set_ms(T0 + 3000)
    acq = await store.acquire_lease(rid, "m2", 30)
    assert acq.kind == "takeover"
    assert (await store.reopen_request(acq.lease)).ok
    assert (await store.apply_transition(acq.lease, apply_replay_batch(DL1))).ok

    fake_redis_time.set_ms(T0 + 9000)
    second = await store.finalize(acq.lease, DONE_FINAL)
    assert second.ok and second.finished_at_ms == T0 + 9000
    assert second.created_at_ms == created

    snap = await store.load_snapshot(rid)
    assert snap.status is RequestStatus.COMPLETED and snap.finished_at == T0 + 9000
    assert snap.replay_history[0].finished_at == ms_dt(T0 + 1000)
    assert await r.zscore(keys.REQUESTS_FINISHED, rid) == T0 + 9000
    assert await r.zscore(keys.REQUESTS_INBOX_PURGE, rid) == T0 + 9000
    assert await r.zscore(keys.requests_by_status("Completed"), rid) == created
    for s in ("Pending", "Running", "Failed"):
        assert await r.zscore(keys.requests_by_status(s), rid) is None
    [item] = (await store.list_requests()).items
    assert item.finished_at_ms == T0 + 9000
