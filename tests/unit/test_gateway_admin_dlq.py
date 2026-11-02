"""Unit tests for the Admin_API DLQ endpoints (task 13.4).

Requirements 3.9, 12.4, 12.5, 12.7, 12.8, 12.9, 12.12, 13.1, 13.2, 13.3, 13.6, 13.7,
13.8, 13.11, 13.12, 13.13, 13.16, 14.14.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

import httpx
import pytest

from das.gateway.admin import InvalidQuery, parse_dlq_query
from das.gateway.app import create_app
from das.gateway.cursor_key import CursorKeyProvider
from das.models import RequestStatus
from das.ops.cursor import PageCursor, encode_cursor
from das.registry import AgentRegistry
from das.state import StateStore, keys
from tests.unit.test_state_phase2_dlq import (
    DL1,
    DL2,
    DL3,
    T0,
    everything,
    failed_request,
    fields,
    running_with_failed_job,
)
from tests.unit.test_gateway_v1_extensions import Recorders

API_KEY = "client-api-key-0001"
ADMIN_KEY = "operator-admin-key-0001"
ADMIN = {"X-API-Key": ADMIN_KEY}


def _client(app: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw")


def build(store: StateStore, recorders: Recorders | None = None, **kw: Any) -> Any:
    return create_app(
        store,
        AgentRegistry(store.client),
        None,
        [API_KEY],
        10,
        admin_api_keys=[ADMIN_KEY],
        metrics_factory=recorders,
        **kw,
    )


@pytest.fixture
def recorders() -> Recorders:
    return Recorders()


@pytest.fixture
async def client(state_store: StateStore, recorders: Recorders):
    async with _client(build(state_store, recorders)) as c:
        yield c


def action(recorders: Recorders, act: str, outcome: str) -> float:
    return recorders.value("das_dlq_admin_actions_total", action=act, outcome=outcome)


# --------------------------------------------------------------------------- query parsing


def q(**kw: str | Sequence[str]) -> dict[str, list[str]]:
    return {k: [v] if isinstance(v, str) else list(v) for k, v in kw.items()}


def test_parse_dlq_query_defaults_and_filters() -> None:
    parsed = parse_dlq_query({})
    assert parsed.limit == 20 and parsed.filters == {} and parsed.cursor is None
    parsed = parse_dlq_query(
        q(status="Pending", reason="malformed", request_id="r1", capability="research", limit="100")
    )
    assert parsed.limit == 100
    assert parsed.filters == {
        "status": "Pending", "reason": "malformed", "request_id": "r1", "capability": "research",
    }


@pytest.mark.parametrize(
    ("params", "parameter"),
    [
        (q(status="pending"), "status"),
        (q(status=["Pending", "Replayed"]), "status"),
        (q(reason="other"), "reason"),
        (q(request_id=""), "request_id"),
        (q(request_id="x" * 65), "request_id"),
        (q(capability="bad cap"), "capability"),
        (q(capability="x" * 65), "capability"),
        (q(limit="0"), "limit"),
        (q(limit="101"), "limit"),
        (q(limit="abc"), "limit"),
        (q(limit="+5"), "limit"),
        (q(status="bad", limit="0"), "status"),  # first invalid parameter wins
    ],
)
def test_parse_dlq_query_rejections(params: dict[str, list[str]], parameter: str) -> None:
    with pytest.raises(InvalidQuery) as info:
        parse_dlq_query(params)
    assert info.value.parameter == parameter


# --------------------------------------------------------------------------- list


async def _seed(fake_redis_time: Any, store: StateStore) -> list[str]:
    fake_redis_time.set_ms(T0)
    a, b, c = "0" * 31 + "a", "0" * 31 + "b", "0" * 31 + "c"
    await store.create_dead_letter(fields(a, "r1", capabilities=["research"]))
    await store.create_dead_letter(
        fields(b, "r2", reason="delivery_limit", capabilities=["review", "research"])
    )
    await store.create_dead_letter(fields(c, "r1", capabilities=["review"]))
    fake_redis_time.advance(1)
    await store.create_dead_letter(
        fields(DL3, None, reason="malformed", job_id=None, attempt=None, capabilities=None,
               raw_b64="YWJj", truncated=True, original_size=3)
    )
    await store.discard_dead_letter(c)
    return [DL3, c, b, a]


async def test_list_orders_filters_and_hides_payload(fake_redis_time, state_store, client):
    order = await _seed(fake_redis_time, state_store)
    r = await client.get("/v1/admin/dlq", headers=ADMIN)
    assert r.status_code == 200
    body = r.json()
    assert [i["dead_letter_id"] for i in body["items"]] == order
    assert "next_cursor" not in body
    first = body["items"][0]
    assert "raw_b64" not in first and "envelope_json" not in first
    assert first["dead_lettered_at"] == "2023-11-14T22:13:21.000Z"

    async def ids(**params: str) -> list[str]:
        resp = await client.get("/v1/admin/dlq", params=params, headers=ADMIN)
        assert resp.status_code == 200
        return [i["dead_letter_id"] for i in resp.json()["items"]]

    assert await ids(status="Pending") == [DL3, order[2], order[3]]
    assert await ids(reason="retries_exhausted") == [order[1], order[3]]
    assert await ids(status="Pending", request_id="r1") == [order[3]]
    assert await ids(capability="research", reason="delivery_limit") == [order[2]]
    assert await ids(request_id="nobody") == []


async def test_list_pages_follow_next_cursor(fake_redis_time, state_store, client):
    order = await _seed(fake_redis_time, state_store)
    seen: list[str] = []
    params: dict[str, str] = {"limit": "1"}
    while True:
        body = (await client.get("/v1/admin/dlq", params=params, headers=ADMIN)).json()
        assert len(body["items"]) <= 1
        seen += [i["dead_letter_id"] for i in body["items"]]
        if "next_cursor" not in body:
            break
        params = {"limit": "1", "cursor": body["next_cursor"]}
    assert seen == order
    # The generated key is shared through Redis (design D10).
    assert await state_store.client.get(keys.GATEWAY_CURSOR_KEY)


async def test_list_rejects_invalid_query_and_foreign_cursor(fake_redis_time, state_store, client):
    await _seed(fake_redis_time, state_store)
    r = await client.get("/v1/admin/dlq", params={"limit": "500"}, headers=ADMIN)
    assert (r.status_code, r.json()) == (400, {"error": "invalid_query", "parameter": "limit"})
    body = (await client.get("/v1/admin/dlq", params={"limit": "1", "status": "Pending"},
                             headers=ADMIN)).json()
    cursor = body["next_cursor"]
    key = await state_store.client.get(keys.GATEWAY_CURSOR_KEY)
    bad = [
        ({"cursor": cursor, "status": "Discarded"}),  # other filters
        ({"cursor": cursor}),  # filter dropped
        ({"cursor": cursor[:-1] + ("A" if cursor[-1] != "A" else "B"), "status": "Pending"}),
        ({"cursor": "x" * 600, "status": "Pending"}),
        ({"cursor": "", "status": "Pending"}),
        ({"cursor": encode_cursor(PageCursor(e="requests", f={"status": "Pending"}, s=1, m="x"),
                                  key.encode()), "status": "Pending"}),
    ]
    for params in bad:
        r = await client.get("/v1/admin/dlq", params=params, headers=ADMIN)
        assert (r.status_code, r.json()) == (400, {"error": "invalid_query", "parameter": "cursor"})


async def test_cursor_secret_is_used_when_configured(fake_redis_time, state_store):
    await _seed(fake_redis_time, state_store)
    async with _client(build(state_store, cursor_secret="s" * 32)) as c:
        body = (await c.get("/v1/admin/dlq", params={"limit": "2"}, headers=ADMIN)).json()
        nxt = await c.get("/v1/admin/dlq", params={"limit": "2", "cursor": body["next_cursor"]},
                          headers=ADMIN)
    assert nxt.status_code == 200 and len(nxt.json()["items"]) == 2
    assert await state_store.client.get(keys.GATEWAY_CURSOR_KEY) is None
    # A replica with another key rejects the cursor.
    async with _client(build(state_store, cursor_secret="t" * 32)) as c:
        r = await c.get("/v1/admin/dlq", params={"limit": "2", "cursor": body["next_cursor"]},
                        headers=ADMIN)
    assert r.status_code == 400


async def test_cursor_key_provider_shares_one_key(fake_redis) -> None:
    k1 = await CursorKeyProvider(lambda: fake_redis).get()
    k2 = await CursorKeyProvider(lambda: fake_redis).get()
    assert k1 == k2 and len(k1) == 64
    assert await CursorKeyProvider(lambda: fake_redis, "abc").get() == b"abc"


# --------------------------------------------------------------------------- detail


async def test_detail_with_envelope_request_and_job(state_store, client):
    rid, _ = await running_with_failed_job(state_store, attempt=2)
    envelope = {"schema_version": 1, "type": "job_dispatch", "payload": {"job_id": "a"}}
    await state_store.create_dead_letter(
        fields(rid=rid, attempt=2, envelope_json=json.dumps(envelope))
    )
    r = await client.get(f"/v1/admin/dlq/{DL1}", headers=ADMIN)
    assert r.status_code == 200
    body = r.json()
    assert body["message"] == envelope
    assert "raw_b64" not in body and "envelope_json" not in body
    assert (body["status"], body["reason"], body["request_id"], body["job_id"]) == (
        "Pending", "retries_exhausted", rid, "a"
    )
    assert (body["request_status"], body["job_state"], body["job_attempt"]) == (
        "Running", "Failed", 2
    )
    assert body["request_exists"] is True


async def test_detail_malformed_and_missing_request(state_store, client):
    await state_store.create_dead_letter(
        fields(DL2, None, reason="malformed", job_id=None, attempt=None,
               raw_b64="YWJj", truncated=True, original_size=3)
    )
    body = (await client.get(f"/v1/admin/dlq/{DL2}", headers=ADMIN)).json()
    assert (body["raw_b64"], body["truncated"]) == ("YWJj", True)
    assert "message" not in body
    assert (body["request_status"], body["job_state"], body["job_attempt"],
            body["request_exists"]) == (None, None, None, None)
    # Request deleted (e.g. by retention): request_exists false, the rest null (Req 14.14).
    await state_store.create_dead_letter(fields(DL3, "purged-rid"))
    body = (await client.get(f"/v1/admin/dlq/{DL3}", headers=ADMIN)).json()
    assert (body["request_status"], body["job_state"], body["job_attempt"],
            body["request_exists"]) == (None, None, None, False)


@pytest.mark.parametrize("dlid", ["f" * 32, "not-a-dlid"])
async def test_detail_not_found(client, dlid):
    r = await client.get(f"/v1/admin/dlq/{dlid}", headers=ADMIN)
    assert (r.status_code, r.json()) == (404, {"error": "not_found"})


# --------------------------------------------------------------------------- replay


async def test_replay_running_request_accepted(state_store, client, recorders):
    rid, _ = await running_with_failed_job(state_store)
    await state_store.create_dead_letter(fields(rid=rid))
    r = await client.post(f"/v1/admin/dlq/{DL1}/replay", headers=ADMIN)
    assert r.status_code == 202
    assert r.json() == {"dead_letter_id": DL1, "request_id": rid, "job_id": "a", "attempt": 2}
    assert (await state_store.get_dead_letter(DL1)).status == "Replayed"
    assert action(recorders, "replay", "accepted") == 1
    # Second call: record_not_pending with the current Record_Status (Req 13.6).
    r = await client.post(f"/v1/admin/dlq/{DL1}/replay", headers=ADMIN)
    assert (r.status_code, r.json()) == (
        409, {"error": "record_not_pending", "current_state": "Replayed"}
    )
    assert action(recorders, "replay", "record_not_pending") == 1


async def test_replay_failed_request_reopens(state_store, client):
    rid = await failed_request(state_store)
    await state_store.create_dead_letter(fields(rid=rid))
    r = await client.post(f"/v1/admin/dlq/{DL1}/replay", headers=ADMIN)
    assert r.status_code == 202 and r.json()["attempt"] == 2
    assert await state_store.get_status(rid) is RequestStatus.PENDING  # Req 13.2


async def test_replay_rejections_write_nothing(state_store, client, recorders):
    rid, _ = await running_with_failed_job(state_store, attempt=2)
    await state_store.create_dead_letter(fields(DL1, None, reason="malformed"))
    await state_store.create_dead_letter(fields(DL2, "gone"))
    await state_store.create_dead_letter(fields(DL3, rid, attempt=1))
    cases = [
        (DL1, "not_replayable", "malformed"),
        (DL2, "request_gone", None),
        (DL3, "stale_attempt", "2"),
    ]
    for dlid, code, state in cases:
        before = await everything(state_store)
        r = await client.post(f"/v1/admin/dlq/{dlid}/replay", headers=ADMIN)
        assert (r.status_code, r.json()) == (409, {"error": code, "current_state": state})
        assert await everything(state_store) == before
        assert action(recorders, "replay", code) == 1
    r = await client.post(f"/v1/admin/dlq/{'e' * 32}/replay", headers=ADMIN)
    assert (r.status_code, r.json()) == (404, {"error": "not_found"})
    assert action(recorders, "replay", "not_found") == 1


async def test_replay_lease_held(state_store, client, recorders):
    rid = await failed_request(state_store)
    await state_store.create_dead_letter(fields(rid=rid))
    now = await state_store.server_time()
    await state_store.client.hset(
        keys.lease(rid), mapping={"owner": "m9", "expires_at": f"{now + 60:.6f}"}
    )
    r = await client.post(f"/v1/admin/dlq/{DL1}/replay", headers=ADMIN)
    assert (r.status_code, r.json()) == (409, {"error": "lease_held", "current_state": "Failed"})
    assert action(recorders, "replay", "lease_held") == 1


# --------------------------------------------------------------------------- discard


async def test_discard_then_idempotent_then_conflict(fake_redis_time, state_store, client, recorders):
    fake_redis_time.set_ms(T0)
    await state_store.create_dead_letter(fields(rid="r1"))
    fake_redis_time.set_ms(T0 + 500)
    r = await client.post(f"/v1/admin/dlq/{DL1}/discard", headers=ADMIN)
    expected = {"dead_letter_id": DL1, "status": "Discarded",
                "discarded_at": "2023-11-14T22:13:20.500Z"}
    assert (r.status_code, r.json()) == (200, expected)
    fake_redis_time.advance(3)
    r = await client.post(f"/v1/admin/dlq/{DL1}/discard", headers=ADMIN)
    assert (r.status_code, r.json()) == (200, expected)  # original time (Req 13.12)
    assert action(recorders, "discard", "accepted") == 2
    r = await client.post(f"/v1/admin/dlq/{DL1}/replay", headers=ADMIN)
    assert r.json() == {"error": "record_not_pending", "current_state": "Discarded"}
    r = await client.post(f"/v1/admin/dlq/{DL2}/discard", headers=ADMIN)
    assert (r.status_code, r.json()) == (404, {"error": "not_found"})
    assert action(recorders, "discard", "not_found") == 1


async def test_discard_replayed_is_conflict(state_store, client, recorders):
    rid, _ = await running_with_failed_job(state_store)
    await state_store.create_dead_letter(fields(rid=rid))
    assert (await state_store.replay_dead_letter(DL1)).accepted
    before = await everything(state_store)
    r = await client.post(f"/v1/admin/dlq/{DL1}/discard", headers=ADMIN)
    assert (r.status_code, r.json()) == (
        409, {"error": "record_not_pending", "current_state": "Replayed"}
    )
    assert await everything(state_store) == before
    assert action(recorders, "discard", "record_not_pending") == 1


# --------------------------------------------------------------------------- auth / 503


async def test_unauthorized_calls_do_not_count(state_store, client, recorders):
    await state_store.create_dead_letter(fields(rid="r1"))
    for headers in ({}, {"X-API-Key": API_KEY}):
        for verb in ("replay", "discard"):
            r = await client.post(f"/v1/admin/dlq/{DL1}/{verb}", headers=headers)
            assert r.status_code in (401, 403)
    body, _ = recorders.recorder.render()  # type: ignore[union-attr]
    assert "das_dlq_admin_actions_total{" not in body.decode()
    assert (await state_store.get_dead_letter(DL1)).status == "Pending"


class DownStore(StateStore):
    async def _down(self, *_: Any, **__: Any) -> Any:
        raise ConnectionError("down")

    list_dead_letters = _down  # type: ignore[assignment]
    get_dead_letter = _down  # type: ignore[assignment]
    replay_dead_letter = _down  # type: ignore[assignment]
    discard_dead_letter = _down  # type: ignore[assignment]


async def test_store_unavailable_is_503(fake_redis, recorders):
    async with _client(build(DownStore(fake_redis), recorders)) as c:
        calls = [
            c.get("/v1/admin/dlq", headers=ADMIN),
            c.get(f"/v1/admin/dlq/{DL1}", headers=ADMIN),
            c.post(f"/v1/admin/dlq/{DL1}/replay", headers=ADMIN),
            c.post(f"/v1/admin/dlq/{DL1}/discard", headers=ADMIN),
        ]
        for call in calls:
            r = await call
            assert (r.status_code, r.json()) == (503, {"error": "service_unavailable"})
    assert action(recorders, "replay", "service_unavailable") == 1
    assert action(recorders, "discard", "service_unavailable") == 1


def test_dlq_route_templates_registered(state_store) -> None:
    app = build(state_store)
    assert {
        "/v1/admin/dlq",
        "/v1/admin/dlq/{dead_letter_id}",
        "/v1/admin/dlq/{dead_letter_id}/replay",
        "/v1/admin/dlq/{dead_letter_id}/discard",
    } <= set(app.state.route_templates)

