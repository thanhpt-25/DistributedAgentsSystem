"""Unit tests for idempotent, quota-checked Request creation and ``request.created``
(das-client-api task 4.3, Phase 4 Req 3.1, 3.2, 4.2-4.7, 5.4, 6.1, 8.1)."""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from das.client.budget import Budget
from das.client.events import created_event_data
from das.state import DEFAULT_EVENTS_MAX_LEN, ClientRequestSpec, CreateResult, StateStore, keys

FP = "cbabecbd"


async def _request_hashes(client) -> list[str]:
    prefix = keys.request("")
    return [k for k in await client.keys(f"{prefix}*") if ":" not in k[len(prefix):]]


async def test_create_request_writes_created_event_and_no_client_fields(state_store: StateStore):
    rid = await state_store.create_request("p")
    r = state_store.client
    assert await r.hget(keys.request(rid), keys.F_CLIENT_FP) is None
    assert await r.hget(keys.request(rid), keys.F_EVENTS_MAX_LEN) is None
    entries = await r.xrange(keys.events(rid))
    assert len(entries) == 1
    _, fields = entries[0]
    assert fields["type"] == "request.created"
    assert json.loads(fields["data"]) == created_event_data()


async def test_submit_request_created_stores_client_fields(state_store: StateStore):
    budget = Budget(max_total_tokens=500, max_cost=Decimal("1.50"))
    res = await state_store.submit_request("p", ClientRequestSpec(fingerprint=FP, budget=budget, events_max_len=7))
    assert res.outcome == "created"
    assert res.request_id is not None
    rid = res.request_id
    r = state_store.client
    h = await r.hgetall(keys.request(rid))
    assert h[keys.F_CLIENT_FP] == FP
    assert json.loads(h[keys.F_BUDGET]) == budget.to_json_obj()
    assert h[keys.F_EVENTS_MAX_LEN] == "7"
    assert h[keys.F_STATUS] == "Pending"
    assert await r.smembers(keys.client_active(FP)) == {rid}
    entries = await r.xrange(keys.events(rid))
    assert [f["type"] for _, f in entries] == ["request.created"]


async def test_submit_request_default_events_max_len(state_store: StateStore):
    res = await state_store.submit_request("p", ClientRequestSpec(fingerprint=FP))
    assert res.request_id is not None
    stored = await state_store.client.hget(keys.request(res.request_id), keys.F_EVENTS_MAX_LEN)
    assert stored == str(DEFAULT_EVENTS_MAX_LEN)
    assert await state_store.client.hget(keys.request(res.request_id), keys.F_BUDGET) is None


async def test_replay_returns_same_request(state_store: StateStore):
    spec = ClientRequestSpec(fingerprint=FP, idempotency_key_hash="k1", body_hash="b1")
    first = await state_store.submit_request("p", spec)
    second = await state_store.submit_request("p", spec)
    assert first.outcome == "created"
    assert second == CreateResult("replayed", request_id=first.request_id)
    r = state_store.client
    assert len(await _request_hashes(r)) == 1
    assert await r.scard(keys.client_active(FP)) == 1
    assert first.request_id is not None
    assert len(await r.xrange(keys.events(first.request_id))) == 1


async def test_conflict_writes_nothing(state_store: StateStore):
    await state_store.submit_request("p", ClientRequestSpec(fingerprint=FP, idempotency_key_hash="k1", body_hash="b1"))
    r = state_store.client
    before = sorted(await r.keys("*"))
    res = await state_store.submit_request(
        "other", ClientRequestSpec(fingerprint=FP, idempotency_key_hash="k1", body_hash="b2")
    )
    assert res == CreateResult("idempotency_conflict")
    assert sorted(await r.keys("*")) == before
    assert len(await _request_hashes(r)) == 1


async def test_same_key_under_two_fingerprints_creates_two(state_store: StateStore):
    def spec(fp: str) -> ClientRequestSpec:
        return ClientRequestSpec(fingerprint=fp, idempotency_key_hash="k", body_hash="b")

    a = await state_store.submit_request("p", spec("fa"))
    b = await state_store.submit_request("p", spec("fb"))
    assert a.outcome == b.outcome == "created"
    assert a.request_id != b.request_id
    assert len(await _request_hashes(state_store.client)) == 2


async def test_idempotency_record_ttl(state_store: StateStore):
    spec = ClientRequestSpec(fingerprint=FP, idempotency_key_hash="k1", body_hash="b1", idempotency_ttl=300)
    res = await state_store.submit_request("p", spec)
    r = state_store.client
    idem = keys.client_idem(FP, "k1")
    ttl = await r.ttl(idem)
    assert 1 <= ttl <= 300
    assert await r.hgetall(idem) == {"rid": res.request_id, "body_hash": "b1"}


async def test_quota_exceeded_and_replay_still_allowed(state_store: StateStore):
    def spec(k: str) -> ClientRequestSpec:
        return ClientRequestSpec(fingerprint=FP, idempotency_key_hash=k, body_hash="b", max_active_requests=2)

    first = await state_store.submit_request("p", spec("k1"))
    second = await state_store.submit_request("p", spec("k2"))
    third = await state_store.submit_request("p", spec("k3"))
    assert first.outcome == second.outcome == "created"
    assert third == CreateResult("quota_exceeded", limit=2)
    assert await state_store.submit_request("p", spec("k1")) == CreateResult("replayed", request_id=first.request_id)
    assert await state_store.client.exists(keys.client_idem(FP, "k3")) == 0
    assert len(await _request_hashes(state_store.client)) == 2


async def test_zero_quota_never_limits(state_store: StateStore):
    for _ in range(5):
        res = await state_store.submit_request("p", ClientRequestSpec(fingerprint=FP, max_active_requests=0))
        assert res.outcome == "created"
    assert await state_store.client.scard(keys.client_active(FP)) == 5


@pytest.mark.parametrize(
    ("url", "secret", "stored"),
    [
        ("https://hooks.example/x", "hook1", True),
        ("https://hooks.example/x", None, False),
        (None, "hook1", False),
        (None, None, False),
    ],
)
async def test_webhook_target_stored_only_when_complete(state_store: StateStore, url, secret, stored):
    res = await state_store.submit_request(
        "p", ClientRequestSpec(fingerprint=FP, webhook_url=url, webhook_secret_name=secret)
    )
    assert res.request_id is not None
    got = await state_store.client.hmget(
        keys.request(res.request_id), [keys.F_WEBHOOK_URL, keys.F_WEBHOOK_SECRET_NAME]
    )
    assert got == ([url, secret] if stored else [None, None])


async def test_idempotency_key_without_body_hash_is_rejected(state_store: StateStore):
    with pytest.raises(ValueError, match="body_hash"):
        await state_store.submit_request("p", ClientRequestSpec(fingerprint=FP, idempotency_key_hash="k1"))
    assert await state_store.client.keys("*") == []


@pytest.mark.parametrize("body_hash", ["b1", "b2"])
async def test_record_naming_a_deleted_request_counts_as_absent(state_store: StateStore, body_hash: str):
    """Req 4.8: a record whose Request is gone is replaced, for the same body and a different one."""
    first = await state_store.submit_request(
        "p", ClientRequestSpec(fingerprint=FP, idempotency_key_hash="k1", body_hash="b1", idempotency_ttl=300)
    )
    assert first.request_id is not None
    await state_store.delete_request(first.request_id)
    again = await state_store.submit_request(
        "p", ClientRequestSpec(fingerprint=FP, idempotency_key_hash="k1", body_hash=body_hash, idempotency_ttl=300)
    )
    assert again.outcome == "created"
    assert again.request_id is not None and again.request_id != first.request_id
    r = state_store.client
    idem = keys.client_idem(FP, "k1")
    assert await r.hgetall(idem) == {"rid": again.request_id, "body_hash": body_hash}
    assert 1 <= await r.ttl(idem) <= 300
    assert await r.hget(keys.request(again.request_id), keys.F_STATUS) == "Pending"
    # The replaced record now replays the new Request.
    replay = await state_store.submit_request(
        "p", ClientRequestSpec(fingerprint=FP, idempotency_key_hash="k1", body_hash=body_hash)
    )
    assert replay == CreateResult("replayed", request_id=again.request_id)


async def test_unknown_create_reply_raises_store_error(state_store: StateStore):
    from das.state import StoreError

    calls = 0

    async def fake_run(name, keys_, args):
        nonlocal calls
        calls += 1
        return ["SURPRISE"]

    state_store._run = fake_run  # type: ignore[method-assign]
    with pytest.raises(StoreError, match="SURPRISE"):
        await state_store.create_request("p")
    assert calls == 1  # only EXISTS draws a new Request_ID
