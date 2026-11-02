"""Unit tests for Active_Set release and re-add (das-client-api task 4.5, Phase 4
Req 3.3, 3.4, 3.5, 3.6, 3.7): finalize, cancel, Replay of a Failed Request,
retention purge and ``delete_request`` keep ``das:client:{fp}:active`` in sync."""

from __future__ import annotations

from das.models import JobRecord, JobState, Lease, RequestStatus
from das.state import PURGE_DELETED, ClientRequestSpec, StateStore, TransitionBatch, keys
from tests.unit.test_state_phase2_dlq import FINAL, fields
from tests.unit.test_state_store import PLAN

FP = "cbabecbd"
OTHER_FP = "0f0f0f0f"
DLID = "e" * 32
TTL_MS = 60_000


async def submitted(store: StateStore, fp: str = FP, max_active: int = 0) -> str:
    res = await store.submit_request("p", ClientRequestSpec(fingerprint=fp, max_active_requests=max_active))
    assert res.outcome == "created" and res.request_id is not None
    return res.request_id


async def leased(store: StateStore, rid: str) -> Lease:
    lease = (await store.acquire_lease(rid, "m1", 30)).lease
    assert lease is not None
    return lease


async def running(store: StateStore, fp: str = FP) -> tuple[str, Lease]:
    rid = await submitted(store, fp)
    lease = await leased(store, rid)
    assert (await store.commit_plan(lease, PLAN)).ok
    return rid, lease


async def failed_with_dead_letter(store: StateStore) -> str:
    """A Failed Request with a Failed Job, a Pending dead letter for it and no Lease."""
    rid, lease = await running(store)
    failed = JobRecord(job=PLAN.jobs[0], state=JobState.FAILED, attempt=1, reason="x")
    skipped = JobRecord(job=PLAN.jobs[1], state=JobState.SKIPPED, reason="dep failed")
    assert (await store.apply_transition(lease, TransitionBatch(jobs={"a": failed, "b": skipped}))).ok
    assert (await store.fail_request(lease, "job a failed", FINAL)).ok
    await store.client.delete(keys.lease(rid))
    assert (await store.create_dead_letter(fields(DLID, rid))).created
    return rid


async def active(store: StateStore, fp: str = FP) -> set[str]:
    return set(await store.client.smembers(keys.client_active(fp)))


async def test_finalize_completed_removes_member(state_store: StateStore):
    rid, lease = await running(state_store)
    other = await submitted(state_store)
    assert await active(state_store) == {rid, other}
    assert (await state_store.finalize(lease, FINAL)).ok
    assert await active(state_store) == {other}


async def test_finalize_failed_removes_member(state_store: StateStore):
    rid = await submitted(state_store)
    lease = await leased(state_store, rid)
    assert (await state_store.fail_request(lease, "boom")).ok
    assert await active(state_store) == set()


async def test_rejected_finalize_keeps_member(state_store: StateStore):
    rid = await submitted(state_store)
    lease = await leased(state_store, rid)
    stale = Lease(request_id=rid, owner="m1", epoch=lease.epoch + 1, expires_at=lease.expires_at)
    assert not (await state_store.fail_request(stale, "boom")).ok
    assert await active(state_store) == {rid}


async def test_cancel_removes_member(state_store: StateStore):
    pending = await submitted(state_store)
    rid, _ = await running(state_store)
    assert (await state_store.cancel_request(rid)).ok
    assert await active(state_store) == {pending}
    assert (await state_store.cancel_request(pending)).ok
    assert await active(state_store) == set()


async def test_replay_of_failed_request_re_adds_member(state_store: StateStore):
    rid = await failed_with_dead_letter(state_store)
    assert await active(state_store) == set()
    assert (await state_store.replay_dead_letter(DLID)).outcome == "accepted"
    assert await state_store.get_status(rid) is RequestStatus.PENDING
    assert await active(state_store) == {rid}


async def test_replay_re_adds_even_when_set_is_full(state_store: StateStore):
    rid = await failed_with_dead_letter(state_store)
    others = {await submitted(state_store, max_active=2) for _ in range(2)}
    assert (await state_store.submit_request(
        "p", ClientRequestSpec(fingerprint=FP, max_active_requests=2))).outcome == "quota_exceeded"
    assert (await state_store.replay_dead_letter(DLID)).outcome == "accepted"
    assert await active(state_store) == others | {rid}


async def test_replay_of_running_request_leaves_set_alone(state_store: StateStore):
    rid, lease = await running(state_store)
    failed = JobRecord(job=PLAN.jobs[0], state=JobState.FAILED, attempt=1, reason="x")
    assert (await state_store.apply_transition(lease, TransitionBatch(jobs={"a": failed}))).ok
    assert (await state_store.create_dead_letter(fields(DLID, rid))).created
    assert (await state_store.replay_dead_letter(DLID)).outcome == "accepted"
    assert await active(state_store) == {rid}


async def test_purge_of_expired_request_leaves_no_member(fake_redis_time, state_store: StateStore):
    fake_redis_time.set_ms(1_700_000_000_000)
    rid = await submitted(state_store)
    keep = await submitted(state_store)
    lease = await leased(state_store, rid)
    assert (await state_store.fail_request(lease, "boom")).ok
    # Simulate a dangling member (e.g. written before the release existed): purge removes it.
    await state_store.client.sadd(keys.client_active(FP), rid)
    fake_redis_time.advance(TTL_MS / 1000 + 1)
    assert await state_store.purge_request(rid, TTL_MS) == PURGE_DELETED
    assert await active(state_store) == {keep}


async def test_delete_request_removes_member(state_store: StateStore):
    rid = await submitted(state_store)
    keep = await submitted(state_store)
    await state_store.delete_request(rid)
    assert await active(state_store) == {keep}
    await state_store.delete_request("missing")
    assert await active(state_store) == {keep}


async def test_requests_without_client_fp_touch_no_active_set(fake_redis_time, state_store: StateStore):
    fake_redis_time.set_ms(1_700_000_000_000)
    other = await submitted(state_store, OTHER_FP)
    rid = await state_store.create_request("p")
    lease = await leased(state_store, rid)
    assert (await state_store.fail_request(lease, "boom")).ok
    cancelled = await state_store.create_request("p")
    assert (await state_store.cancel_request(cancelled)).ok
    fake_redis_time.advance(TTL_MS / 1000 + 1)
    assert await state_store.purge_request(rid, TTL_MS) == PURGE_DELETED
    await state_store.delete_request(cancelled)
    assert await state_store.client.keys(keys.client_active("*")) == [keys.client_active(OTHER_FP)]
    assert await active(state_store, OTHER_FP) == {other}
