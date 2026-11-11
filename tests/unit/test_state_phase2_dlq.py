"""Unit tests for the dead-letter, Replay, Discard and reopen scripts
(das-observability-operations task 9.3)."""

from __future__ import annotations

import json
from datetime import UTC, datetime

from das.models import FinalResult, JobRecord, JobReport, JobState, RequestStatus
from das.models.dlq import DeadLetterFields
from das.state import INBOX_REPLAY, StateStore, StoreStatus, TransitionBatch, keys
from tests.unit.test_state_store import PLAN

DL1 = "1" * 32
DL2 = "2" * 32
DL3 = "3" * 32
T0 = 1_700_000_000_000

FINAL = FinalResult(
    summary=None,
    jobs=[JobReport(job_id="a", state=JobState.FAILED, worker_id="w1")],
    error="job a failed",
)


def fields(dlid: str = DL1, rid: str | None = None, **kw) -> DeadLetterFields:
    base = {
        "dead_letter_id": dlid,
        "reason": "retries_exhausted",
        "request_id": rid,
        "job_id": "a",
        "attempt": 1,
        "capabilities": ["research"],
        "last_error": "boom",
        "message_id": f"m-{dlid[:4]}",
    }
    base.update(kw)
    return DeadLetterFields(**base)


async def running_with_failed_job(store: StateStore, attempt: int = 1):
    rid = await store.create_request("hello")
    lease = (await store.acquire_lease(rid, "m1", 30)).lease
    assert (await store.commit_plan(lease, PLAN)).ok
    failed = JobRecord(job=PLAN.jobs[0], state=JobState.FAILED, attempt=attempt, reason="x")
    skipped = JobRecord(job=PLAN.jobs[1], state=JobState.SKIPPED, reason="dep failed")
    batch = TransitionBatch(jobs={"a": failed, "b": skipped})
    assert (await store.apply_transition(lease, batch)).ok
    return rid, lease


async def failed_request(store: StateStore):
    rid, lease = await running_with_failed_job(store)
    assert (await store.fail_request(lease, "job a failed", FINAL)).ok
    return rid


async def everything(store: StateStore) -> dict:
    """Every key in Redis with its content (to assert that nothing changed).

    Rate-limit buckets are left out: every authenticated Gateway call takes a token
    (Phase 4 Req 2.1), so a rejected call still changes its caller's bucket.
    """
    r = store.client
    out = {}
    for k in sorted(await r.keys("*")):
        if k.startswith("das:client:") and k.endswith(":bucket"):
            continue
        t = await r.type(k)
        if t == "hash":
            out[k] = await r.hgetall(k)
        elif t == "list":
            out[k] = await r.lrange(k, 0, -1)
        elif t == "set":
            out[k] = await r.smembers(k)
        elif t == "zset":
            out[k] = await r.zrange(k, 0, -1, withscores=True)
        elif t == "stream":
            out[k] = await r.xrange(k)
        else:
            out[k] = await r.get(k)
    return out


# ------------------------------------------------------------------ create / get


async def test_create_dead_letter_once(fake_redis_time, state_store: StateStore):
    fake_redis_time.set_ms(T0 + 123)
    res = await state_store.create_dead_letter(fields(rid="r1"))
    assert res.created and res.dead_lettered_at_ms == T0 + 123
    r = state_store.client
    for k in (keys.DLQ_INDEX, keys.dlq_by_status("Pending"),
              keys.dlq_by_reason("retries_exhausted")):
        assert await r.zscore(k, DL1) == T0 + 123
    rec = await state_store.get_dead_letter(DL1)
    assert rec.status == "Pending" and rec.request_id == "r1" and rec.attempt == 1
    assert rec.capabilities == ["research"] and rec.last_error == "boom"
    assert rec.dead_lettered_at == datetime(2023, 11, 14, 22, 13, 20, 123000, tzinfo=UTC)
    assert rec.status_changed_at is None and rec.truncated is False

    # A redelivery (same Dead_Letter_ID) writes nothing (Req 12.2).
    before = await everything(state_store)
    fake_redis_time.advance(5)
    again = await state_store.create_dead_letter(fields(rid="r1", last_error="other"))
    assert not again.created
    assert await everything(state_store) == before


async def test_malformed_record_round_trip(state_store: StateStore):
    f = DeadLetterFields(
        dead_letter_id=DL2, reason="malformed", last_error="bad json", parse_reason="bad json",
        parse_field=None, original_size=10, raw_b64="YWJj", truncated=True,
    )
    assert (await state_store.create_dead_letter(f)).created
    rec = await state_store.get_dead_letter(DL2)
    assert rec.reason == "malformed" and rec.truncated is True and rec.original_size == 10
    assert rec.request_id is None and rec.job_id is None and rec.parse_field is None
    assert rec.raw_b64 == "YWJj"
    assert await state_store.get_dead_letter("f" * 32) is None


# ------------------------------------------------------------------ list


async def test_list_dead_letters_order_filters_and_pages(fake_redis_time, state_store):
    store = state_store
    fake_redis_time.set_ms(T0)
    specs = [
        ("0" * 31 + "a", "retries_exhausted", "r1", ["research"]),
        ("0" * 31 + "b", "delivery_limit", "r2", ["review", "research"]),
        ("0" * 31 + "c", "retries_exhausted", "r1", ["review"]),
    ]
    for dlid, reason, rid, caps in specs:  # a and b share the same dead_lettered_at
        await store.create_dead_letter(fields(dlid, rid, reason=reason, capabilities=caps))
    fake_redis_time.advance(1)
    await store.create_dead_letter(fields(DL3, "r3", reason="malformed", job_id=None,
                                          attempt=None, capabilities=None))
    await store.discard_dead_letter("0" * 31 + "c")

    page = await store.list_dead_letters()
    ids = [r.dead_letter_id for r in page.records]
    # dead_lettered_at desc, then Dead_Letter_ID desc.
    assert ids == [DL3, "0" * 31 + "c", "0" * 31 + "b", "0" * 31 + "a"]
    assert page.next_after is None

    async def ids_of(**kw):
        return [r.dead_letter_id for r in (await store.list_dead_letters(**kw)).records]

    assert await ids_of(status="Pending") == [DL3, "0" * 31 + "b", "0" * 31 + "a"]
    assert await ids_of(reason="retries_exhausted") == ["0" * 31 + "c", "0" * 31 + "a"]
    assert await ids_of(request_id="r1", status="Pending") == ["0" * 31 + "a"]
    assert await ids_of(capability="research") == ["0" * 31 + "b", "0" * 31 + "a"]
    assert await ids_of(capability="research", reason="delivery_limit") == ["0" * 31 + "b"]
    assert await ids_of(request_id="nobody") == []

    # Following next_after with limit 1 yields every record exactly once, in order.
    seen, after = [], None
    while True:
        p = await store.list_dead_letters(limit=1, after=after)
        seen += [r.dead_letter_id for r in p.records]
        if p.next_after is None:
            break
        after = p.next_after
    assert seen == ids


# ------------------------------------------------------------------ discard


async def test_discard_then_idempotent_then_replay_rejected(fake_redis_time, state_store):
    fake_redis_time.set_ms(T0)
    await state_store.create_dead_letter(fields(rid="r1"))
    fake_redis_time.set_ms(T0 + 500)
    res = await state_store.discard_dead_letter(DL1)
    assert res.outcome == "accepted" and res.status == "Discarded"
    assert res.discarded_at == datetime.fromtimestamp((T0 + 500) / 1000, UTC)
    r = state_store.client
    assert await r.zscore(keys.dlq_by_status("Pending"), DL1) is None
    assert await r.zscore(keys.dlq_by_status("Discarded"), DL1) == T0
    assert await r.zscore(keys.DLQ_TERMINAL, DL1) == T0 + 500
    rec = await state_store.get_dead_letter(DL1)
    assert rec.status == "Discarded" and rec.discarded_at == res.discarded_at

    before = await everything(state_store)
    fake_redis_time.advance(3)
    again = await state_store.discard_dead_letter(DL1)
    assert again.outcome == "already" and again.discarded_at == res.discarded_at
    replay = await state_store.replay_dead_letter(DL1)
    assert replay.outcome == "record_not_pending" and replay.current_state == "Discarded"
    assert await everything(state_store) == before

    assert (await state_store.discard_dead_letter(DL2)).outcome == "not_found"


async def test_discard_replayed_record_is_rejected(state_store: StateStore):
    rid, _ = await running_with_failed_job(state_store)
    await state_store.create_dead_letter(fields(rid=rid))
    assert (await state_store.replay_dead_letter(DL1)).accepted
    before = await everything(state_store)
    res = await state_store.discard_dead_letter(DL1)
    assert res.outcome == "record_not_pending" and res.status == "Replayed"
    assert await everything(state_store) == before


# ------------------------------------------------------------------ replay rejections


async def test_replay_rejections_follow_check_order_and_write_nothing(state_store):
    store = state_store
    r = store.client
    rid, lease = await running_with_failed_job(store, attempt=2)

    async def rejected(dlid: str, outcome: str, state: str | None) -> None:
        before = await everything(store)
        res = await store.replay_dead_letter(dlid)
        assert (res.outcome, res.current_state) == (outcome, state)
        assert await everything(store) == before

    await rejected(DL1, "not_found", None)
    # malformed beats request_gone (no request_id).
    await store.create_dead_letter(fields(DL1, None, reason="malformed"))
    await rejected(DL1, "not_replayable", "malformed")
    await store.create_dead_letter(fields(DL2, "gone"))
    await rejected(DL2, "request_gone", None)
    # stale attempt (record attempt 1, Job attempt 2).
    await store.create_dead_letter(fields(DL3, rid, attempt=1))
    await rejected(DL3, "stale_attempt", "2")
    # job_state: Job b is Skipped.
    dl4 = "4" * 32
    await store.create_dead_letter(fields(dl4, rid, job_id="b", attempt=2))
    await rejected(dl4, "job_state", "Skipped")
    dl5 = "5" * 32
    await store.create_dead_letter(fields(dl5, rid, job_id="zz", attempt=2))
    await rejected(dl5, "job_state", None)
    # request_state: a Pending Request.
    pending_rid = await store.create_request("p")
    dl6 = "6" * 32
    await store.create_dead_letter(fields(dl6, pending_rid))
    await rejected(dl6, "request_state", "Pending")
    # lease_held: Failed Request with an unexpired Lease.
    frid = await failed_request(store)
    dl7 = "7" * 32
    await store.create_dead_letter(fields(dl7, frid))
    now = await store.server_time()
    await r.hset(keys.lease(frid), mapping={"owner": "m9", "expires_at": f"{now + 60:.6f}"})
    await rejected(dl7, "lease_held", "Failed")
    assert lease  # Running Request's Lease does not block its replay (Req 13.3)


# ------------------------------------------------------------------ replay accepted


async def test_replay_running_request(fake_redis_time, state_store: StateStore):
    fake_redis_time.set_ms(T0)
    rid, lease = await running_with_failed_job(state_store)
    await state_store.create_dead_letter(fields(rid=rid))
    fake_redis_time.set_ms(T0 + 1000)
    res = await state_store.replay_dead_letter(DL1)
    assert res.accepted and res.new_attempt == 2
    assert res.request_status_before is RequestStatus.RUNNING
    assert (res.request_id, res.job_id) == (rid, "a")
    r = state_store.client
    entries = await state_store.read_inbox(rid)
    assert [e.kind for e in entries] == [INBOX_REPLAY]
    assert entries[0].payload() == {"dead_letter_id": DL1, "job_id": "a", "attempt": 1}
    assert await r.smembers(keys.pending_replays(rid)) == {DL1}
    rec = await state_store.get_dead_letter(DL1)
    assert rec.status == "Replayed"
    assert rec.status_changed_at == datetime.fromtimestamp((T0 + 1000) / 1000, UTC)
    assert await r.zscore(keys.dlq_by_status("Pending"), DL1) is None
    assert await r.zscore(keys.dlq_by_status("Replayed"), DL1) == T0
    assert await r.zscore(keys.DLQ_TERMINAL, DL1) == T0 + 1000
    snap = await state_store.load_snapshot(rid, include_transitions=True)
    assert snap.status is RequestStatus.RUNNING and snap.lease == lease
    assert snap.request_transitions == []
    # The finalize gate holds until the owner applies the replay (D8).
    assert (await state_store.finalize(lease, FINAL)).status is StoreStatus.REPLAY_PENDING
    # Second replay of the same record: record_not_pending.
    again = await state_store.replay_dead_letter(DL1)
    assert again.outcome == "record_not_pending" and again.current_state == "Replayed"


async def test_replay_failed_request_reopens_it(fake_redis_time, state_store: StateStore):
    fake_redis_time.set_ms(T0)
    rid, _ = await running_with_failed_job(state_store)
    created = await state_store.client.zscore(keys.REQUESTS_INDEX, rid)
    fake_redis_time.set_ms(T0 + 2000)
    assert (await state_store.fail_request(
        (await state_store.load_snapshot(rid)).lease, "job a failed", FINAL)).ok
    await state_store.create_dead_letter(fields(rid=rid))
    fake_redis_time.set_ms(T0 + 5000)
    res = await state_store.replay_dead_letter(DL1)
    assert res.accepted and res.request_status_before is RequestStatus.FAILED
    assert res.new_attempt == 2

    r = state_store.client
    snap = await state_store.load_snapshot(rid, include_transitions=True)
    assert snap.status is RequestStatus.PENDING
    assert snap.error is None and snap.final_result is None and snap.finished_at is None
    assert snap.plan == PLAN and snap.jobs["a"].state is JobState.FAILED
    [hist] = snap.replay_history
    assert hist.dead_letter_id == DL1 and hist.error == "job a failed"
    assert hist.final_result == FINAL
    assert hist.finished_at == datetime.fromtimestamp((T0 + 2000) / 1000, UTC)
    assert hist.at == datetime.fromtimestamp((T0 + 5000) / 1000, UTC)
    [entry] = snap.request_transitions
    assert (entry.from_status, entry.to_status) == (RequestStatus.FAILED, RequestStatus.PENDING)
    assert entry.dead_letter_id == DL1
    assert await r.zscore(keys.requests_by_status("Failed"), rid) is None
    assert await r.zscore(keys.requests_by_status("Pending"), rid) == created
    assert await r.zscore(keys.REQUESTS_FINISHED, rid) is None
    assert await r.zscore(keys.REQUESTS_INBOX_PURGE, rid) is None
    assert rid in await state_store.claimable_request_ids()
    assert await r.smembers(keys.pending_replays(rid)) == {DL1}
    assert [e.kind for e in await state_store.read_inbox(rid)] == [INBOX_REPLAY]
    assert (await state_store.get_dead_letter(DL1)).status == "Replayed"

    # A new owner reopens the Request Pending -> Running (D7).
    lease = (await state_store.acquire_lease(rid, "m2", 30)).lease
    assert lease is not None
    bad = lease.model_copy(update={"epoch": lease.epoch + 1})
    assert (await state_store.reopen_request(bad)).status is StoreStatus.LEASE_LOST
    res2 = await state_store.reopen_request(lease)
    assert res2.ok
    snap = await state_store.load_snapshot(rid, include_transitions=True)
    assert snap.status is RequestStatus.RUNNING
    assert [(e.from_status, e.to_status) for e in snap.request_transitions] == [
        (RequestStatus.FAILED, RequestStatus.PENDING),
        (RequestStatus.PENDING, RequestStatus.RUNNING),
    ]
    assert await r.zscore(keys.requests_by_status("Running"), rid) == created
    assert await r.zscore(keys.requests_by_status("Pending"), rid) is None
    again = await state_store.reopen_request(lease)
    assert (again.status, again.detail) == (StoreStatus.INVALID_STATE, "Running")


async def test_replay_history_appends(state_store: StateStore):
    rid = await failed_request(state_store)
    await state_store.client.hset(
        keys.request(rid), keys.F_REPLAY_HISTORY,
        json.dumps([{"dead_letter_id": DL2, "at": "2023-01-01T00:00:00.000Z"}]),
    )
    await state_store.create_dead_letter(fields(rid=rid))
    assert (await state_store.replay_dead_letter(DL1)).accepted
    hist = (await state_store.load_snapshot(rid)).replay_history
    assert [h.dead_letter_id for h in hist] == [DL2, DL1]


async def test_reopen_requires_a_plan(state_store: StateStore):
    rid = await state_store.create_request("p")
    lease = (await state_store.acquire_lease(rid, "m1", 30)).lease
    before = await everything(state_store)
    res = await state_store.reopen_request(lease)
    assert (res.status, res.detail) == (StoreStatus.INVALID_STATE, "Pending")
    assert await everything(state_store) == before
