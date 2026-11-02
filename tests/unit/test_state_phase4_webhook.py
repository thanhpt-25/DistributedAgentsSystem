"""Unit tests for Webhook_Delivery claim and completion (das-client-api task 4.8;
Phase 4 Req 8.3, 8.5, 8.6, 8.7)."""

from __future__ import annotations

import pytest
import redis.exceptions

from das.models import JobRecord, JobState
from das.state import (
    ClientRequestSpec,
    StateStore,
    TransitionBatch,
    WebhookClaim,
    WebhookState,
    keys,
)
from tests.unit.test_state_phase2_dlq import fields as dlq_fields
from tests.unit.test_state_store import PLAN

FP = "cbabecbd"
URL = "https://hooks.example.test/das"
SECRET_NAME = "hook-secret"
LEASE = 30.0


async def enqueued(store: StateStore) -> str:
    """A Cancelled Request with a webhook target, so one delivery is due now."""
    spec = ClientRequestSpec(fingerprint=FP, webhook_url=URL, webhook_secret_name=SECRET_NAME)
    res = await store.submit_request("p", spec)
    assert res.request_id is not None
    assert (await store.cancel_request(res.request_id)).ok
    return res.request_id


async def due_score(store: StateStore, rid: str) -> float | None:
    return await store.client.zscore(keys.WEBHOOKS_DUE, rid)


async def test_nothing_due_returns_none(fake_redis_time, state_store: StateStore):
    assert await state_store.claim_webhook(LEASE) is None


async def test_due_delivery_is_claimed_once(fake_redis_time, state_store: StateStore):
    rid = await enqueued(state_store)
    h = await state_store.client.hgetall(keys.webhook(rid))
    c = await state_store.claim_webhook(LEASE)
    assert c is not None
    assert c == WebhookClaim(
        request_id=rid, claim=1, attempts=0, url=URL, secret_name=SECRET_NAME,
        event_id=h["event_id"], event_type="request.cancelled", data=c.data,
        claimed_at=c.claimed_at,
    )
    assert c.data["status"] == "Cancelled"
    assert await due_score(state_store, rid) == pytest.approx(fake_redis_time.now() + LEASE)
    assert await state_store.claim_webhook(LEASE) is None


async def test_expired_claim_is_reclaimed_and_old_claim_is_stale(fake_redis_time, state_store: StateStore):
    rid = await enqueued(state_store)
    first = await state_store.claim_webhook(LEASE)
    assert first is not None
    fake_redis_time.advance(LEASE + 1)
    second = await state_store.claim_webhook(LEASE)
    assert second is not None and second.request_id == rid and second.claim == 2
    before = await state_store.client.hgetall(keys.webhook(rid))
    score = await due_score(state_store, rid)
    assert await state_store.complete_webhook(first, "delivered", "200") is False
    assert await state_store.client.hgetall(keys.webhook(rid)) == before
    assert await due_score(state_store, rid) == score


async def test_delivered(fake_redis_time, state_store: StateStore):
    rid = await enqueued(state_store)
    c = await state_store.claim_webhook(LEASE)
    assert c is not None
    assert await state_store.complete_webhook(c, "delivered", "204") is True
    assert await state_store.get_webhook(rid) == WebhookState("delivered", 1, "204")
    assert await due_score(state_store, rid) is None
    assert await state_store.complete_webhook(c, "delivered", "204") is False  # no longer pending


async def test_retry_keeps_pending_due_at_next_due(fake_redis_time, state_store: StateStore):
    rid = await enqueued(state_store)
    c = await state_store.claim_webhook(LEASE)
    assert c is not None
    nd = fake_redis_time.now() + 5.5
    assert await state_store.complete_webhook(c, "retry", "503", next_due=nd) is True
    assert await state_store.get_webhook(rid) == WebhookState("pending", 1, "503")
    assert await due_score(state_store, rid) == pytest.approx(nd)
    assert await state_store.claim_webhook(LEASE) is None
    fake_redis_time.advance(5.5)
    again = await state_store.claim_webhook(LEASE)
    assert again is not None and again.claim == 2 and again.attempts == 1


async def test_failed_removes_from_outbox(fake_redis_time, state_store: StateStore):
    rid = await enqueued(state_store)
    c = await state_store.claim_webhook(LEASE)
    assert c is not None
    assert await state_store.complete_webhook(c, "failed", "timeout") is True
    assert await state_store.get_webhook(rid) == WebhookState("failed", 1, "timeout")
    assert await due_score(state_store, rid) is None


async def test_entry_without_hash_is_dropped(fake_redis_time, state_store: StateStore):
    rid = await enqueued(state_store)
    await state_store.client.delete(keys.webhook(rid))
    assert await state_store.claim_webhook(LEASE) is None
    assert await due_score(state_store, rid) is None


async def test_retry_without_next_due_raises_before_redis(fake_redis_time, state_store: StateStore):
    rid = await enqueued(state_store)
    c = await state_store.claim_webhook(LEASE)
    assert c is not None
    before = await state_store.client.hgetall(keys.webhook(rid))
    with pytest.raises(ValueError):
        await state_store.complete_webhook(c, "retry", "503")
    assert await state_store.client.hgetall(keys.webhook(rid)) == before


async def failed_with_target(store: StateStore) -> str:
    """A Failed Request with a webhook target whose Job ``a`` is Failed (attempt 1),
    so a Dead_Letter_Record for it can be replayed."""
    spec = ClientRequestSpec(fingerprint=FP, webhook_url=URL, webhook_secret_name=SECRET_NAME)
    res = await store.submit_request("p", spec)
    assert res.request_id is not None
    rid = res.request_id
    lease = (await store.acquire_lease(rid, "m1", 3600.0)).lease
    assert lease is not None
    assert (await store.commit_plan(lease, PLAN)).ok
    failed = JobRecord(job=PLAN.jobs[0], state=JobState.FAILED, attempt=1, reason="x")
    assert (await store.apply_transition(lease, TransitionBatch(jobs={"a": failed}))).ok
    assert (await store.fail_request(lease, "boom")).ok
    return rid


async def test_claim_from_before_a_delivery_reset_is_stale(fake_redis_time, state_store: StateStore):
    """Req 8.3: a later terminal write resets the delivery (claim back to 0, new
    event_id); a claim taken before the reset must not complete the new delivery."""
    store = state_store
    rid = await failed_with_target(store)
    a = await store.claim_webhook(LEASE)
    assert a is not None and a.request_id == rid and a.claim == 1
    # Replay reopens the Request; it is applied and the Request fails again.
    dlid = "1" * 32
    assert (await store.create_dead_letter(dlq_fields(dlid, rid, attempt=1))).created
    assert (await store.replay_dead_letter(dlid)).outcome == "accepted"
    lease = (await store.acquire_lease(rid, "m1", 3600.0)).lease
    assert lease is not None
    assert (await store.reopen_request(lease)).ok
    failed = JobRecord(job=PLAN.jobs[0], state=JobState.FAILED, attempt=2, reason="x")
    batch = TransitionBatch(jobs={"a": failed}, replays_applied=(dlid,))
    assert (await store.apply_transition(lease, batch)).ok
    assert (await store.fail_request(lease, "boom again")).ok
    b = await store.claim_webhook(LEASE)
    assert b is not None and b.request_id == rid and b.claim == 1
    assert b.event_id != a.event_id
    before = await store.client.hgetall(keys.webhook(rid))
    score = await due_score(store, rid)
    assert await store.complete_webhook(a, "delivered", "200") is False
    assert await store.client.hgetall(keys.webhook(rid)) == before
    assert await due_score(store, rid) == score
    assert await store.complete_webhook(b, "delivered", "204") is True
    assert await store.get_webhook(rid) == WebhookState("delivered", 1, "204")


@pytest.mark.parametrize(
    ("outcome", "next_due", "event_id"),
    [
        ("sent", "", "E"),  # unknown outcome
        ("retry", "soon", "E"),  # retry with non-numeric next_due
        ("delivered", "", ""),  # empty event_id
    ],
)
async def test_lua_rejects_bad_arguments_without_writing(
    fake_redis_time, state_store: StateStore, outcome: str, next_due: str, event_id: str
):
    rid = await enqueued(state_store)
    c = await state_store.claim_webhook(LEASE)
    assert c is not None
    eid = c.event_id if event_id == "E" else event_id
    before = await state_store.client.hgetall(keys.webhook(rid))
    score = await due_score(state_store, rid)
    with pytest.raises(redis.exceptions.ResponseError):
        await state_store._run(
            "complete_webhook",
            [keys.WEBHOOKS_DUE, keys.webhook(rid)],
            [rid, c.claim, outcome, "200", next_due, eid],
        )
    assert await state_store.client.hgetall(keys.webhook(rid)) == before
    assert await due_score(state_store, rid) == score


async def test_claimed_at_is_redis_time_of_the_claim(fake_redis_time, state_store: StateStore):
    await enqueued(state_store)
    fake_redis_time.advance(2.25)
    c = await state_store.claim_webhook(LEASE)
    assert c is not None
    assert c.claimed_at == fake_redis_time.now()


async def test_next_webhook_due(fake_redis_time, state_store: StateStore):
    assert await state_store.next_webhook_due() is None
    fake_redis_time.set_ms(1_700_000_012_345)
    await enqueued(state_store)
    assert await state_store.next_webhook_due() == pytest.approx(1_700_000_012.345)
    fake_redis_time.advance(1.5)
    c = await state_store.claim_webhook(LEASE)
    assert c is not None
    assert await state_store.next_webhook_due() == pytest.approx(c.claimed_at + LEASE)
    assert await state_store.complete_webhook(c, "delivered", "200") is True
    assert await state_store.next_webhook_due() is None
