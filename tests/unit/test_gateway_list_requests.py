"""Unit tests for ``GET /v1/requests`` (das-observability-operations task 13.3).

Requirements 14.11, 15.1, 15.2, 15.3, 15.4, 15.7, 15.8, 15.9, 15.10, 15.12, 15.13,
19.8, 19.9, 20.1.

Superseded boundary (das-multi-tenant task 8.1; Phase 5 Req 3.3, 9.6): v2 Req 20.1, that a listing is not filtered by
the calling key, no longer holds as a rule. A key of a configured tenant lists only its tenant's Requests
(``tests/unit/test_gateway_isolation.py``, ``tests/property/test_tenant_isolation.py``). It still holds for what these
tests use: there are no ``tenants`` here, so every API key is a Default_Tenant key, and a Default_Tenant listing before
the Tenant_Backfill reads the global index (Req 9.3). The Operator, here the Admin_API_Key, lists every tenant.
"""

from __future__ import annotations

import socket
from collections.abc import Sequence
from typing import Any

import httpx
import pytest

from das.gateway.app import build_redis_client, create_app
from das.gateway.listing import (
    InvalidQuery,
    decode_list_cursor,
    encode_list_cursor,
    parse_list_query,
    parse_timestamp_ms,
)
from das.models import RequestStatus
from das.ops.cursor import PageCursor, encode_cursor
from das.registry import AgentRegistry
from das.state import StateStore, keys
from tests.unit.test_state_phase2_retention import T0, counter_ids

API_KEY = "client-api-key-0001"
ADMIN_KEY = "operator-admin-key-0001"
AUTH = {"X-API-Key": API_KEY}
SECRET = "cursor-secret-for-tests"


def _client(app: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw")


def build(store: StateStore, **kw: Any) -> Any:
    return create_app(
        store, AgentRegistry(store.client), None, [API_KEY], 10, admin_api_keys=[ADMIN_KEY], **kw
    )


def q(**kw: str | Sequence[str]) -> dict[str, list[str]]:
    return {k: [v] if isinstance(v, str) else list(v) for k, v in kw.items()}


# --------------------------------------------------------------------------- parsing


def test_parse_defaults() -> None:
    parsed = parse_list_query({})
    assert parsed.limit == 20
    assert parsed.statuses is None and parsed.cursor is None and parsed.filters == {}


def test_parse_normalizes_filters() -> None:
    parsed = parse_list_query(
        q(
            status="Failed,Pending,Failed",
            created_after="2023-11-14T22:13:20.0009+00:00",
            created_before="2023-11-15T08:13:20+09:00",
            limit="100",
        )
    )
    assert parsed.statuses == (RequestStatus.PENDING, RequestStatus.FAILED)
    assert parsed.created_after_ms == T0  # sub-millisecond part floored
    assert parsed.created_before_ms == T0 + 3_600_000
    assert parsed.limit == 100
    assert parsed.filters == {
        "status": ["Pending", "Failed"],
        "created_after": T0,
        "created_before": T0 + 3_600_000,
    }
    # Equivalent status lists produce equal filters (cursor stays valid).
    assert parse_list_query(q(status="Pending,Failed")).filters == parse_list_query(
        q(status="Failed,Pending")
    ).filters


def test_parse_timestamp_ms_accepts_z_suffix() -> None:
    assert parse_timestamp_ms("2023-11-14T22:13:20.123Z") == T0 + 123


@pytest.mark.parametrize(
    ("params", "parameter"),
    [
        (q(status=""), "status"),
        (q(status="pending"), "status"),
        (q(status="Pending,"), "status"),
        (q(status="Pending,Unknown"), "status"),
        (q(status=["Pending", "Failed"]), "status"),
        (q(created_after="2023-11-14T22:13:20"), "created_after"),  # no time zone
        (q(created_after="2023-11-14"), "created_after"),
        (q(created_after="yesterday"), "created_after"),
        (q(created_before="2023-13-01T00:00:00Z"), "created_before"),
        (
            q(created_after="2023-11-14T22:13:20Z", created_before="2023-11-14T22:13:20Z"),
            "created_after",
        ),
        (
            q(created_after="2023-11-14T23:00:00Z", created_before="2023-11-14T22:00:00Z"),
            "created_after",
        ),
        (q(limit="0"), "limit"),
        (q(limit="101"), "limit"),
        (q(limit="-1"), "limit"),
        (q(limit="1.5"), "limit"),
        (q(limit=["1", "2"]), "limit"),
        (q(cursor=["a", "b"]), "cursor"),
        (q(status="bad", limit="0"), "status"),  # first invalid parameter wins
    ],
)
def test_parse_rejections(params: dict[str, list[str]], parameter: str) -> None:
    with pytest.raises(InvalidQuery) as info:
        parse_list_query(params)
    assert info.value.parameter == parameter
    assert info.value.body() == {"error": "invalid_query", "parameter": parameter}


def test_cursor_bound_to_endpoint_and_filters() -> None:
    key = b"k" * 32
    query = parse_list_query(q(status="Failed"))
    token = encode_list_cursor(query, (T0, "r001"), key)
    assert decode_list_cursor(parse_list_query(q(status="Failed", cursor=token)), key) == (
        T0,
        "r001",
    )
    for other in (q(cursor=token), q(status="Pending", cursor=token)):
        with pytest.raises(InvalidQuery) as info:
            decode_list_cursor(parse_list_query(other), key)
        assert info.value.parameter == "cursor"
    with pytest.raises(InvalidQuery):
        decode_list_cursor(parse_list_query(q(status="Failed", cursor=token)), b"x" * 32)
    dlq = encode_cursor(PageCursor(e="dlq", f={"status": ["Failed"]}, s=T0, m="r001"), key)
    with pytest.raises(InvalidQuery):
        decode_list_cursor(parse_list_query(q(status="Failed", cursor=dlq)), key)


# --------------------------------------------------------------------------- endpoint


async def _populate(fake_redis_time: Any, store: StateStore) -> list[str]:
    """7 Requests over 3 ms values (ties), every third one Cancelled; desc order."""
    out = []
    for i in range(7):
        fake_redis_time.set_ms(T0 + i // 3)
        rid = await store.create_request(f"secret prompt {i}")
        out.append((T0 + i // 3, rid))
        if i % 3 == 1:
            await store.cancel_request(rid)
    await store.set_index_ready()
    return [rid for _, rid in sorted(out, reverse=True)]


async def _walk(c: httpx.AsyncClient, params: dict[str, str]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    cursor = None
    for _ in range(50):
        p = dict(params) if cursor is None else {**params, "cursor": cursor}
        r = await c.get("/v1/requests", params=p, headers=AUTH)
        assert r.status_code == 200, r.text
        body = r.json()
        assert len(body["items"]) <= int(params.get("limit", 20))
        items += body["items"]
        cursor = body.get("next_cursor")
        if cursor is None:
            return items
    raise AssertionError("pagination did not end")


@pytest.fixture
async def store(state_store: StateStore) -> StateStore:
    return StateStore(state_store.client, id_factory=counter_ids())


async def test_list_pages_filters_and_item_shape(fake_redis_time, store: StateStore) -> None:
    order = await _populate(fake_redis_time, store)
    async with _client(build(store, cursor_secret=SECRET)) as c:
        for limit in ("1", "2", "3", "7", "100"):
            assert [i["request_id"] for i in await _walk(c, {"limit": limit})] == order

        everything = await _walk(c, {})
        cancelled = [i["request_id"] for i in everything if i["status"] == "Cancelled"]
        assert len(cancelled) == 2
        got = await _walk(c, {"status": "Cancelled,Cancelled", "limit": "1"})
        assert [i["request_id"] for i in got] == cancelled
        assert await _walk(c, {"status": "Running"}) == []

        window = await _walk(
            c,
            {
                "created_after": "2023-11-14T22:13:20.001Z",
                "created_before": "2023-11-14T22:13:20.002Z",
                "limit": "1",
            },
        )
        assert [i["created_at"] for i in window] == ["2023-11-14T22:13:20.001Z"] * 3

        r = await c.get("/v1/requests", params={"limit": "7"}, headers=AUTH)
        assert "next_cursor" not in r.json()  # nothing after the last item (Req 15.4)

    first = everything[0]
    assert set(first) == {
        "request_id", "status", "created_at", "finished_at", "job_count", "token_usage",
    }
    assert first["job_count"] == 0
    assert first["token_usage"] == {"input": 0, "output": 0, "total": 0}
    for item in everything:
        assert (item["finished_at"] is None) == (item["status"] != "Cancelled")
    assert "secret prompt" not in str(everything)


async def test_admin_key_lists_too_and_bad_key_is_401(fake_redis_time, store) -> None:
    # v2 Req 20.1 (no filtering by key) is superseded by das-multi-tenant Req 3.3: this holds because the Admin_API_Key
    # is an Operator, who lists every tenant, and the client key is a Default_Tenant key with no tenants configured.
    order = await _populate(fake_redis_time, store)
    async with _client(build(store)) as c:
        r = await c.get("/v1/requests", headers={"X-API-Key": ADMIN_KEY})
        assert r.status_code == 200
        assert [i["request_id"] for i in r.json()["items"]] == order
        assert (await c.get("/v1/requests")).json() == {"error": "unauthorized"}
        r = await c.get("/v1/requests", headers={"X-API-Key": "wrong-key-000000"})
        assert r.status_code == 401 and r.json() == {"error": "unauthorized"}


async def test_cursor_skips_deleted_position_and_new_requests(fake_redis_time, store) -> None:
    order = await _populate(fake_redis_time, store)
    async with _client(build(store)) as c:  # generated key at das:gateway:cursor_key
        r = await c.get("/v1/requests", params={"limit": "3"}, headers=AUTH)
        body = r.json()
        assert [i["request_id"] for i in body["items"]] == order[:3]
        await store.delete_request(order[2])  # cursor position removed (Req 15.12)
        fake_redis_time.set_ms(T0 + 1000)
        await store.create_request("late")  # created after the walk began (Req 15.7)
        r = await c.get(
            "/v1/requests", params={"limit": "50", "cursor": body["next_cursor"]}, headers=AUTH
        )
        assert r.status_code == 200
        assert [i["request_id"] for i in r.json()["items"]] == order[3:]
        assert "next_cursor" not in r.json()
    assert await store.client.get(keys.GATEWAY_CURSOR_KEY) is not None


async def test_cursor_valid_across_replicas_sharing_key(fake_redis_time, store) -> None:
    order = await _populate(fake_redis_time, store)
    async with _client(build(store)) as a, _client(build(store)) as b:
        first = (await a.get("/v1/requests", params={"limit": "4"}, headers=AUTH)).json()
        rest = await b.get(
            "/v1/requests", params={"limit": "4", "cursor": first["next_cursor"]}, headers=AUTH
        )
        assert rest.status_code == 200
        assert [i["request_id"] for i in first["items"] + rest.json()["items"]] == order


@pytest.mark.parametrize(
    "params",
    [
        {"cursor": "not-a-cursor"},
        {"cursor": "a" * 600},
        {"cursor": "abc%def"},
    ],
)
async def test_invalid_cursor_is_400(fake_redis_time, store, params) -> None:
    await _populate(fake_redis_time, store)
    async with _client(build(store, cursor_secret=SECRET)) as c:
        r = await c.get("/v1/requests", params=params, headers=AUTH)
    assert r.status_code == 400
    assert r.json() == {"error": "invalid_query", "parameter": "cursor"}


async def test_cursor_with_changed_filters_is_400(fake_redis_time, store) -> None:
    await _populate(fake_redis_time, store)
    async with _client(build(store, cursor_secret=SECRET)) as c:
        first = (await c.get("/v1/requests", params={"limit": "1"}, headers=AUTH)).json()
        token = first["next_cursor"]
        for extra in (
            {"status": "Pending"},
            {"created_after": "2000-01-01T00:00:00Z"},
            {"created_before": "2100-01-01T00:00:00Z"},
        ):
            r = await c.get("/v1/requests", params={"cursor": token, **extra}, headers=AUTH)
            assert r.status_code == 400
            assert r.json() == {"error": "invalid_query", "parameter": "cursor"}
        # A different limit keeps the cursor valid (limit is not a filter).
        r = await c.get("/v1/requests", params={"cursor": token, "limit": "5"}, headers=AUTH)
        assert r.status_code == 200
    async with _client(build(store, cursor_secret="another-secret")) as c:
        r = await c.get("/v1/requests", params={"cursor": token}, headers=AUTH)
        assert r.status_code == 400


async def test_invalid_query_parameter_is_400(fake_redis_time, store) -> None:
    await _populate(fake_redis_time, store)
    async with _client(build(store)) as c:
        r = await c.get("/v1/requests", params={"limit": "0"}, headers=AUTH)
    assert r.status_code == 400
    assert r.json() == {"error": "invalid_query", "parameter": "limit"}


async def test_index_not_ready_is_503(store) -> None:
    await store.create_request("p")
    async with _client(build(store)) as c:
        r = await c.get("/v1/requests", headers=AUTH)
    assert r.status_code == 503
    assert r.json() == {"error": "index_not_ready"}


async def test_empty_index_is_empty_page(store) -> None:
    await store.set_index_ready()
    async with _client(build(store)) as c:
        r = await c.get("/v1/requests", headers=AUTH)
    assert r.status_code == 200
    assert r.json() == {"items": []}


def _closed_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def test_unreachable_store_is_503() -> None:
    client = build_redis_client(f"redis://127.0.0.1:{_closed_port()}/0")
    store = StateStore(client)
    try:
        async with _client(build(store)) as c:
            r = await c.get("/v1/requests", headers=AUTH)
            assert r.status_code == 503
            assert r.json() == {"error": "service_unavailable"}
    finally:
        await client.aclose()


def test_route_registered(state_store) -> None:
    app = build(state_store)
    assert "/v1/requests" in app.state.route_templates
