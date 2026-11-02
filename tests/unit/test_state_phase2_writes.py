"""Unit tests for the Phase 2 write scripts: commit_plan, apply_transition, finalize,
cancel_request and record_inbox (das-observability-operations task 9.2)."""

from __future__ import annotations

import json

import pytest

from das.models import (
    FinalResult,
    JobReport,
    JobState,
    RequestStatus,
    TokenUsage,
)
from das.models.usage import UsageRecord
from das.state import INBOX_JOB_RESULT, StateStore, StoreStatus, TransitionBatch, keys
from das.state.scripts import script_source
from das.state.store import apply_transition_keys, cancel_request_keys
from tests.unit.test_state_store import PLAN, result

DL1 = "a" * 32
DL2 = "b" * 32
FINAL = FinalResult(
    summary="done", jobs=[JobReport(job_id="a", state=JobState.SUCCEEDED, worker_id="w1")]
)


def usage(op: str = "plan", i: int = 1, o: int = 2, job_id: str | None = None,
          attempt: int | None = None) -> UsageRecord:
    return UsageRecord(
        operation=op, provider="openai", model="gpt", job_id=job_id, attempt=attempt,
        usage=TokenUsage(input_tokens=i, output_tokens=o, total_tokens=i + o),
    )


def by_status(s: str) -> str:
    return keys.requests_by_status(s)


async def pending(store: StateStore):
    rid = await store.create_request("hello")
    return rid, (await store.acquire_lease(rid, "m1", 30)).lease


async def running(store: StateStore, plan_usage=()):
    rid, lease = await pending(store)
    assert (await store.commit_plan(lease, PLAN, plan_usage)).ok
    return rid, lease


async def replayed_record(store: StateStore, dlid: str, rid: str, at_ms: int = 1000) -> None:
    r = store.client
    await r.hset(keys.dlq(dlid), mapping={"status": "Replayed", "request_id": rid,
                                          "dead_lettered_at": at_ms})
    await r.zadd(keys.dlq_by_status("Replayed"), {dlid: at_ms})
    await r.zadd(keys.DLQ_TERMINAL, {dlid: at_ms + 1})


async def dump(store: StateStore, rid: str) -> dict:
    r = store.client
    out = {}
    for k in keys.request_keys(rid):
        t = await r.type(k)
        if t == "hash":
            out[k] = await r.hgetall(k)
        elif t == "list":
            out[k] = await r.lrange(k, 0, -1)
        elif t == "set":
            out[k] = await r.smembers(k)
        elif t == "zset":
            out[k] = await r.zrange(k, 0, -1, withscores=True)
    for k in [keys.REQUESTS_INDEX, by_status("Pending"), by_status("Running")]:
        out[k] = await r.zrange(k, 0, -1, withscores=True)
    return out


# ------------------------------------------------------------------ commit_plan


async def test_commit_plan_stores_usage_job_count_and_moves_index(state_store: StateStore):
    rid, lease = await pending(state_store)
    created = await state_store.client.zscore(keys.REQUESTS_INDEX, rid)
    recs = [usage(i=3, o=4), usage(i=1, o=1)]
    assert (await state_store.commit_plan(lease, PLAN, recs)).ok

    snap = await state_store.load_snapshot(rid, include_usage=True)
    assert snap.usage == recs
    assert await state_store.load_usage(rid) == recs
    assert snap.usage_totals == TokenUsage(input_tokens=4, output_tokens=5, total_tokens=9)
    assert snap.job_count == 2
    assert await state_store.client.zscore(by_status("Pending"), rid) is None
    assert await state_store.client.zscore(by_status("Running"), rid) == created


async def test_commit_plan_without_usage_keeps_v1_behaviour(state_store: StateStore):
    rid, _ = await running(state_store)
    assert await state_store.load_usage(rid) == []
    snap = await state_store.load_snapshot(rid)
    assert snap.status is RequestStatus.RUNNING and snap.usage is None


async def test_commit_plan_malformed_usage_writes_nothing(state_store: StateStore):
    from das.state.store import commit_plan_keys

    rid, lease = await pending(state_store)
    script = state_store.client.register_script(script_source("commit_plan"))
    args = [rid, lease.owner, lease.epoch, PLAN.model_dump_json(), '["{bad json"]', ""]
    for job in PLAN.jobs:
        args += [job.job_id, json.dumps({"job": job.model_dump(), "state": "Waiting"})]
    before = await dump(state_store, rid)
    with pytest.raises(Exception):  # noqa: B017
        await script(keys=commit_plan_keys(rid), args=args)
    assert await dump(state_store, rid) == before


# ------------------------------------------------------------------ apply_transition


async def test_usage_appended_only_for_new_results(state_store: StateStore):
    rid, lease = await running(state_store)
    res = result(rid, "a")
    u = usage("job", i=5, o=6, job_id="a", attempt=1)
    batch = TransitionBatch(results=[res], usage=[u])
    assert (await state_store.apply_transition(lease, batch)).ok
    # The same Job_Result again (duplicate): HSETNX keeps the first, no new usage.
    assert (await state_store.apply_transition(lease, batch)).ok
    assert await state_store.load_usage(rid) == [u]
    snap = await state_store.load_snapshot(rid)
    assert snap.usage_totals == TokenUsage(input_tokens=5, output_tokens=6, total_tokens=11)


async def test_usage_without_matching_result_is_rejected(state_store: StateStore):
    rid, lease = await running(state_store)
    with pytest.raises(ValueError):
        await state_store.apply_transition(
            lease, TransitionBatch(usage=[usage("job", job_id="a", attempt=1)])
        )
    with pytest.raises(ValueError):
        await state_store.apply_transition(
            lease, TransitionBatch(results=[result(rid, "a")], usage=[usage("plan")])
        )


async def test_tag_job_count_and_replays_applied(state_store: StateStore):
    rid, lease = await running(state_store)
    await state_store.client.sadd(keys.pending_replays(rid), DL1, DL2)
    snap = await state_store.load_snapshot(rid)
    rec = snap.jobs["a"].model_copy(update={"state": JobState.READY, "attempt": 1})
    batch = TransitionBatch(jobs={"a": rec}, tag=DL1, replays_applied=[DL1], job_count=2)
    res = await state_store.apply_transition(lease, batch)
    assert res.ok and res.transitions_written == 1
    entry = (await state_store.get_transitions(rid))[-1]
    assert entry.job_id == "a" and entry.dead_letter_id == DL1
    assert await state_store.client.smembers(keys.pending_replays(rid)) == {DL2}
    assert (await state_store.load_snapshot(rid)).job_count == 2


async def test_dead_letter_reverts_replayed_record_to_pending(fake_redis_time, state_store):
    fake_redis_time.set_ms(1_700_000_000_000)
    rid, lease = await running(state_store)
    await replayed_record(state_store, DL1, rid, at_ms=1000)
    await state_store.client.sadd(keys.pending_replays(rid), DL1)
    batch = TransitionBatch(replays_applied=[DL1], dead_letter_reverts=[DL1])
    assert (await state_store.apply_transition(lease, batch)).ok
    r = state_store.client
    rec = await r.hgetall(keys.dlq(DL1))
    assert rec["status"] == "Pending" and rec["status_changed_at"] == "1700000000000"
    assert await r.zscore(keys.dlq_by_status("Pending"), DL1) == 1000
    assert await r.zscore(keys.dlq_by_status("Replayed"), DL1) is None
    assert await r.zscore(keys.DLQ_TERMINAL, DL1) is None
    assert await r.smembers(keys.pending_replays(rid)) == set()


async def test_dead_letter_reverts_compare_and_set(state_store: StateStore):
    rid, lease = await running(state_store)
    r = state_store.client
    # Missing record, record of another Request, and a Discarded record stay untouched.
    await replayed_record(state_store, DL1, "other-request")
    await r.hset(keys.dlq(DL2), mapping={"status": "Discarded", "request_id": rid})
    missing = "c" * 32
    batch = TransitionBatch(dead_letter_reverts=[DL1, DL2, missing])
    assert (await state_store.apply_transition(lease, batch)).ok
    assert await r.hget(keys.dlq(DL1), "status") == "Replayed"
    assert await r.hget(keys.dlq(DL2), "status") == "Discarded"
    assert not await r.exists(keys.dlq(missing))
    assert await r.zcard(keys.dlq_by_status("Pending")) == 0


async def test_apply_transition_malformed_input_writes_nothing(state_store: StateStore):
    rid, lease = await running(state_store)
    await state_store.client.sadd(keys.pending_replays(rid), DL1)
    snap = await state_store.load_snapshot(rid)
    good = snap.jobs["a"].model_copy(update={"state": JobState.READY, "attempt": 1})
    script = state_store.client.register_script(script_source("apply_transition"))
    batches = [
        # The last job record does not decode.
        {"jobs": [["a", good.model_dump_json()], ["b", "{not json"]], "replays_applied": [DL1]},
        # A usage entry does not decode.
        {"jobs": [["a", good.model_dump_json()]],
         "results": [["a:1", result(rid, "a").model_dump_json()]],
         "usage": [["a:1", "{bad"]]},
        # A timer score is not a number.
        {"jobs": [["a", good.model_dump_json()]], "timers_add": [["retry:a:1", "soon"]]},
    ]
    for batch in batches:
        before = await dump(state_store, rid)
        with pytest.raises(Exception):  # noqa: B017
            await script(
                keys=apply_transition_keys(rid),
                args=[rid, lease.owner, lease.epoch, json.dumps(batch)],
            )
        assert await dump(state_store, rid) == before


# ------------------------------------------------------------------ finalize


async def test_finalize_returns_ms_marks_finished_and_moves_index(fake_redis_time, state_store):
    fake_redis_time.set_ms(1_700_000_000_250)
    rid, lease = await running(state_store, [usage("plan", 1, 1)])
    fake_redis_time.set_ms(1_700_000_002_000)
    summ = usage("summarize", 2, 3)
    res = await state_store.finalize(lease, FINAL, usage=[summ])
    assert res.ok
    assert (res.finished_at_ms, res.created_at_ms) == (1_700_000_002_000, 1_700_000_000_250)
    r = state_store.client
    snap = await state_store.load_snapshot(rid, include_usage=True)
    assert snap.status is RequestStatus.COMPLETED and snap.finished_at == 1_700_000_002_000
    assert [u.operation for u in snap.usage] == ["plan", "summarize"]
    assert snap.usage_totals.total_tokens == 7
    assert await r.zscore(keys.REQUESTS_FINISHED, rid) == 1_700_000_002_000
    assert await r.zscore(keys.REQUESTS_INBOX_PURGE, rid) == 1_700_000_002_000
    assert await r.zscore(by_status("Running"), rid) is None
    assert await r.zscore(by_status("Completed"), rid) == 1_700_000_000_250


async def test_fail_from_pending_moves_pending_index_and_keeps_plan_usage(state_store):
    rid, lease = await pending(state_store)
    recs = [usage("plan", 1, 1), usage("plan", 2, 2)]
    res = await state_store.fail_request(lease, "planning failed", usage=recs)
    assert res.ok and res.created_at_ms is not None
    r = state_store.client
    assert await r.zscore(by_status("Pending"), rid) is None
    assert await r.zscore(by_status("Failed"), rid) == res.created_at_ms
    assert await state_store.load_usage(rid) == recs


async def test_finalize_replay_pending_writes_nothing(state_store: StateStore):
    rid, lease = await running(state_store)
    await state_store.client.sadd(keys.pending_replays(rid), DL1)
    before = await dump(state_store, rid)
    res = await state_store.finalize(lease, FINAL, usage=[usage("summarize")])
    assert res.status is StoreStatus.REPLAY_PENDING
    assert await dump(state_store, rid) == before
    assert (await state_store.load_snapshot(rid)).lease == lease


async def test_finalize_rejected_writes_no_usage(state_store: StateStore):
    rid, lease = await running(state_store)
    stale = lease.model_copy(update={"epoch": lease.epoch + 1})
    res = await state_store.finalize(stale, FINAL, usage=[usage("summarize")])
    assert res.status is StoreStatus.LEASE_LOST
    assert await state_store.load_usage(rid) == []


async def test_created_ms_falls_back_to_created_at_for_v1_request(state_store: StateStore):
    rid, lease = await running(state_store)
    r = state_store.client
    await r.zrem(keys.REQUESTS_INDEX, rid)  # as if written by a v1 Gateway
    await r.hset(keys.request(rid), "created_at", "1700000000.123456")
    res = await state_store.finalize(lease, FINAL)
    assert res.created_at_ms == 1_700_000_000_123


# ------------------------------------------------------------------ cancel_request


async def test_cancel_stamps_finished_and_moves_index(fake_redis_time, state_store):
    fake_redis_time.set_ms(1_700_000_000_000)
    rid, _ = await running(state_store)
    fake_redis_time.set_ms(1_700_000_001_500)
    res = await state_store.cancel_request(rid)
    assert res.ok
    assert (res.finished_at_ms, res.created_at_ms) == (1_700_000_001_500, 1_700_000_000_000)
    assert res.at is not None
    r = state_store.client
    snap = await state_store.load_snapshot(rid)
    assert snap.status is RequestStatus.CANCELLED and snap.finished_at == 1_700_000_001_500
    assert await r.zscore(by_status("Running"), rid) is None
    assert await r.zscore(by_status("Cancelled"), rid) == 1_700_000_000_000
    assert await r.zscore(keys.REQUESTS_FINISHED, rid) == 1_700_000_001_500


async def test_cancel_returns_unapplied_replays_to_pending(state_store: StateStore):
    rid, _ = await running(state_store)
    await replayed_record(state_store, DL1, rid, at_ms=42)
    await state_store.client.sadd(keys.pending_replays(rid), DL1)
    assert (await state_store.cancel_request(rid)).ok
    r = state_store.client
    assert await r.hget(keys.dlq(DL1), "status") == "Pending"
    assert await r.zscore(keys.dlq_by_status("Pending"), DL1) == 42
    assert not await r.exists(keys.pending_replays(rid))


async def test_cancel_conflicts_when_pending_replays_changed(state_store: StateStore):
    rid, _ = await running(state_store)
    await state_store.client.sadd(keys.pending_replays(rid), DL1, DL2)
    script = state_store.client.register_script(script_source("cancel_request"))
    snap = await state_store.load_snapshot(rid)
    args = [rid, json.dumps([DL1]), ""]
    for jid, rec in snap.jobs.items():
        new = rec.model_copy(update={"state": JobState.CANCELLED})
        args += [jid, rec.model_dump_json(), new.model_dump_json()]
    reply = await script(keys=cancel_request_keys(rid, [DL1]), args=args)
    assert list(reply) == ["CONFLICT"]
    assert (await state_store.get_status(rid)) is RequestStatus.RUNNING


async def test_cancel_pending_request_moves_pending_index(state_store: StateStore):
    rid = await state_store.create_request("p")
    assert (await state_store.cancel_request(rid)).ok
    assert await state_store.client.zscore(by_status("Pending"), rid) is None
    assert await state_store.client.zscore(by_status("Cancelled"), rid) is not None


# ------------------------------------------------------------------ record_inbox


async def test_inbox_meta_round_trip(state_store: StateStore):
    rid, _ = await running(state_store)
    meta = {"provider": "openai", "model": "gpt"}
    await state_store.record_result_inbox(rid, result(rid, "a"), meta=meta)
    await state_store.record_result_inbox(rid, result(rid, "b"))
    first, second = await state_store.read_inbox(rid)
    assert first.kind == INBOX_JOB_RESULT and first.meta_payload() == meta
    assert second.meta is None and second.meta_payload() is None
