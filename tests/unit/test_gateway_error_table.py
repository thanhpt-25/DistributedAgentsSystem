"""Every row of the design's Error Handling table, through the real Gateway (das-multi-tenant task 5.8).

Phase 5 Req 2.3, 3.2, 3.3, 3.4, 3.7, 5.3, 6.3, 7.1, 7.2. The two rows about the Helm chart (grace period, values
schema) are tests of the chart: ``tests/unit/test_chart_skeleton.py``. Each test here is named for its row; most
behaviours also have a closer test next to the code, and this file is the one place where the whole table is read."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from fastapi import Depends

from das.config.schema import ClientLimitsConfig, ClientOverride, ClientSettingsConfig, TenantConfig
from das.gateway.app import create_app
from das.gateway.auth import api_key_auth
from das.gateway.authz import authorize_route
from das.ops.keys import key_fingerprint
from das.registry import AgentRegistry
from das.state import StateStore
from tests.unit.test_state_phase2_dlq import DL1, everything, fields
from tests.unit.test_state_tenant_scripts import running_with_failed_job

A, A_READ, A_SUBMIT, B, DEFAULT, OP = (
    "team-a-key-000000001", "team-a-read-0000001", "team-a-submit-00001",
    "team-b-key-000000001", "default-key-00000001", "root-key-0000000001",
)  # fmt: skip
NOT_FOUND = {"error": "not_found"}
UNAVAILABLE = {"error": "service_unavailable"}


def fp(key: str) -> str:
    return key_fingerprint(key.encode())


def h(key: str) -> dict[str, str]:
    return {"X-API-Key": key}


def tenants(a_quota: int = 0) -> list[TenantConfig]:
    full, read, submit = ["submit", "read", "admin"], ["read"], ["submit"]
    team_a = {
        "id": "team-a", "max_active_requests": a_quota,
        "keys": [{"fingerprint": fp(A), "scopes": full}, {"fingerprint": fp(A_READ), "scopes": read},
                 {"fingerprint": fp(A_SUBMIT), "scopes": submit}],
    }  # fmt: skip
    team_b = {"id": "team-b", "keys": [{"fingerprint": fp(B), "scopes": full}]}
    return [TenantConfig.model_validate(team_a), TenantConfig.model_validate(team_b)]


def build(store: StateStore, redis: Any, **kw: Any) -> httpx.AsyncClient:
    kw.setdefault("admin_api_keys", [OP])
    kw.setdefault("tenants", tenants())
    app = create_app(store, AgentRegistry(redis), None, [A, A_READ, A_SUBMIT, B, DEFAULT], 1000, **kw)
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw")


@pytest.fixture
async def gw(state_store: StateStore, fake_redis: Any, fake_redis_time: Any) -> AsyncIterator[httpx.AsyncClient]:
    async with build(state_store, fake_redis) as client:
        yield client


async def submit(gw: httpx.AsyncClient, key: str, **body: Any) -> httpx.Response:
    return await gw.post("/v1/requests", json={"prompt": "p", **body}, headers=h(key))


# --------------------------------------------------------------------------- 401: unknown or missing key


@pytest.mark.parametrize("path", ["/v1/requests", "/v1/usage", "/v1/admin/dlq", "/v1/admin/usage"])
async def test_row_unknown_or_missing_key_is_401(gw: httpx.AsyncClient, path: str) -> None:
    for headers in ({}, h("nobody-key-0000000001")):
        reply = await gw.get(path, headers=headers)
        assert (reply.status_code, reply.json()) == (401, {"error": "unauthorized"})


# --------------------------------------------------------------------------- 403: scope or role missing


@pytest.mark.parametrize(
    ("method", "path", "key", "scope"),
    [
        ("POST", "/v1/requests", A_READ, "submit"),
        ("GET", "/v1/requests", A_SUBMIT, "read"),
        ("GET", "/v1/usage", A_SUBMIT, "read"),
        ("GET", "/v1/admin/dlq", A_READ, "admin"),
        ("GET", "/v1/admin/usage", A, "operator"),
        ("POST", "/v1/admin/agents/w1/drain", A, "operator"),
    ],
)
async def test_row_a_missing_scope_or_role_is_403_naming_it(
    gw: httpx.AsyncClient, method: str, path: str, key: str, scope: str
) -> None:
    reply = await gw.request(method, path, headers=h(key), json={"prompt": "p"} if method == "POST" else None)
    assert (reply.status_code, reply.json()) == (403, {"error": "forbidden", "scope": scope})


async def test_row_forbidden_comes_before_any_state_store_access() -> None:
    """With Redis unreachable a refused call is still 403 (nothing was asked of the store); an allowed one is 503."""
    server = FakeServer()
    redis = FakeRedis(server=server, decode_responses=True)
    async with build(StateStore(redis), redis) as client:
        server.connected = False
        refused = await client.get("/v1/requests", headers=h(A_SUBMIT))
        allowed = await client.get("/v1/requests", headers=h(A_READ))
    assert (refused.status_code, refused.json()["error"]) == (403, "forbidden")
    assert (allowed.status_code, allowed.json()) == (503, UNAVAILABLE)


async def test_row_forbidden_comes_before_the_rate_limit(
    state_store: StateStore, fake_redis: Any, fake_redis_time: Any
) -> None:
    limits = ClientLimitsConfig(default=ClientSettingsConfig(rate=1.0, burst=1))  # one token, frozen clock
    async with build(state_store, fake_redis, client_limits=limits) as client:
        for _ in range(3):
            assert (await client.get("/v1/requests", headers=h(A_SUBMIT))).status_code == 403  # never 429
        assert (await client.get("/v1/agents", headers=h(A_READ))).status_code == 200  # the token was still there
        assert (await client.get("/v1/agents", headers=h(A_READ))).status_code == 429


# --------------------------------------------------------------------------- 403: route missing from ROUTE_SCOPES


async def test_row_a_route_missing_from_the_table_fails_closed(state_store: StateStore, fake_redis: Any) -> None:
    app = create_app(state_store, AgentRegistry(fake_redis), None, [A], 1000, admin_api_keys=[OP], tenants=tenants())
    guards = [Depends(api_key_auth([A], [OP], tenants=tenants())), Depends(authorize_route())]
    app.add_api_route("/v1/not-in-the-table", lambda: {"ok": True}, methods=["GET"], dependencies=guards)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
        for key in (A, OP):  # an Operator included
            reply = await client.get("/v1/not-in-the-table", headers=h(key))
            assert (reply.status_code, reply.json()) == (403, {"error": "forbidden"})


# --------------------------------------------------------------------------- 404: another tenant's resource


async def test_row_another_tenants_request_record_or_stream_is_the_404_of_a_missing_one(
    gw: httpx.AsyncClient, state_store: StateStore
) -> None:
    rid = (await submit(gw, B)).json()["request_id"]
    request_b, missing = f"/v1/requests/{rid}", f"/v1/requests/{uuid.uuid4().hex}"
    for suffix, method in (("", "GET"), ("/cancel", "POST"), ("/events", "GET")):
        foreign = await gw.request(method, request_b + suffix, headers=h(A))
        absent = await gw.request(method, missing + suffix, headers=h(A))
        assert (foreign.status_code, foreign.json()) == (absent.status_code, absent.json()) == (404, NOT_FOUND)
    rid_b, _ = await running_with_failed_job(state_store, "team-b")
    await state_store.create_dead_letter(fields(DL1, rid=rid_b))
    unknown = "f" * 32
    for method, suffix in (("GET", ""), ("POST", "/replay"), ("POST", "/discard")):
        foreign = await gw.request(method, f"/v1/admin/dlq/{DL1}{suffix}", headers=h(A))
        absent = await gw.request(method, f"/v1/admin/dlq/{unknown}{suffix}", headers=h(A))
        assert (foreign.status_code, foreign.json()) == (absent.status_code, absent.json()) == (404, NOT_FOUND)


# --------------------------------------------------------------------------- 400: tenant parameter, cursor


@pytest.mark.parametrize("key", [A, A_READ, B, DEFAULT])
async def test_row_the_tenant_parameter_from_a_non_operator_is_invalid(gw: httpx.AsyncClient, key: str) -> None:
    for path in ("/v1/requests", "/v1/admin/dlq"):
        if path.startswith("/v1/admin") and key in (A_READ, DEFAULT):
            continue  # without the admin Scope they never reach the parameter
        reply = await gw.get(path, params={"tenant": "team-b"}, headers=h(key))
        assert (reply.status_code, reply.json()) == (400, {"error": "invalid_query", "parameter": "tenant"})


async def test_row_an_operator_must_send_a_valid_tenant(gw: httpx.AsyncClient) -> None:
    reply = await gw.get("/v1/requests", params={"tenant": "Not A Tenant"}, headers=h(OP))
    assert (reply.status_code, reply.json()) == (400, {"error": "invalid_query", "parameter": "tenant"})


async def test_row_a_cursor_from_another_tenant_is_invalid(gw: httpx.AsyncClient, state_store: StateStore) -> None:
    await state_store.set_index_ready()
    await state_store.set_tenant_index_ready()
    for _ in range(3):
        await submit(gw, A)
    cursor = (await gw.get("/v1/requests", params={"limit": 1}, headers=h(A))).json()["next_cursor"]
    for key in (B, DEFAULT, OP):
        reply = await gw.get("/v1/requests", params={"limit": 1, "cursor": cursor}, headers=h(key))
        assert (reply.status_code, reply.json()) == (400, {"error": "invalid_query", "parameter": "cursor"}), key
    assert (await gw.get("/v1/requests", params={"limit": 1, "cursor": cursor}, headers=h(A))).status_code == 200


# --------------------------------------------------------------------------- 429: Tenant_Quota


async def test_row_the_tenant_quota_answers_429_with_its_scope_and_writes_nothing(
    state_store: StateStore, fake_redis: Any, fake_redis_time: Any
) -> None:
    async with build(state_store, fake_redis, tenants=tenants(a_quota=1)) as client:
        assert (await submit(client, A)).status_code == 202
        before = await everything(state_store)
        reply = await submit(client, A_SUBMIT)
        assert (reply.status_code, reply.json()) == (
            429, {"error": "quota_exceeded", "limit": 1, "limit_scope": "tenant"},
        )  # fmt: skip
        assert await everything(state_store) == before  # refused before any write


async def test_row_the_per_key_quota_is_checked_first_and_keeps_its_phase_4_body(
    state_store: StateStore, fake_redis: Any, fake_redis_time: Any
) -> None:
    limits = ClientLimitsConfig(overrides=[ClientOverride(fingerprint=fp(A), max_active_requests=1)])
    async with build(state_store, fake_redis, tenants=tenants(a_quota=1), client_limits=limits) as client:
        assert (await submit(client, A)).status_code == 202
        reply = await submit(client, A)  # both limits are reached: the key's is the one reported
        assert (reply.status_code, reply.json()) == (429, {"error": "quota_exceeded", "limit": 1})


# --------------------------------------------------------------------------- 400: priority


async def test_row_priority_invalid_or_above_the_limit(gw: httpx.AsyncClient, state_store: StateStore) -> None:
    before = await everything(state_store)
    invalid = await submit(gw, A, priority="urgent")
    assert (invalid.status_code, invalid.json()) == (400, {"error": "invalid_request", "parameter": "priority"})
    above = await submit(gw, A, priority="high")  # team-a has the default max_priority: normal
    assert (above.status_code, above.json()) == (400, {"error": "priority_above_limit", "limit": "normal"})
    assert await everything(state_store) == before  # nothing was created


# --------------------------------------------------------------------------- 400: usage range


@pytest.mark.parametrize(
    ("query", "parameter"),
    [
        ("from=2026-13-01", "from"),  # malformed
        ("from=2026-02-01&to=2026-01-01", "from"),  # inverted
        ("to=12/11/2026", "to"),  # malformed
        ("from=2020-01-01&to=2020-12-31", "to"),  # longer than 92 days
    ],
)
async def test_row_a_bad_usage_range_names_from_or_to(gw: httpx.AsyncClient, query: str, parameter: str) -> None:
    reply = await gw.get(f"/v1/usage?{query}", headers=h(A))
    assert (reply.status_code, reply.json()) == (400, {"error": "invalid_query", "parameter": parameter})


# --------------------------------------------------------------------------- 503: index not ready


async def test_row_a_non_default_listing_before_the_backfill_is_503(
    gw: httpx.AsyncClient, state_store: StateStore
) -> None:
    await state_store.set_index_ready()  # the Phase 3 index is done; the Tenant_Backfill is not
    reply = await gw.get("/v1/requests", headers=h(A))
    assert (reply.status_code, reply.json()) == (503, {"error": "index_not_ready"})
    assert (await gw.get("/v1/requests", headers=h(DEFAULT))).status_code == 200  # the default tenant still reads
    await state_store.set_tenant_index_ready()
    assert (await gw.get("/v1/requests", headers=h(A))).status_code == 200


# --------------------------------------------------------------------------- 503: Redis unreachable


@pytest.mark.parametrize(
    ("method", "path", "key"),
    [
        ("POST", "/v1/requests", A),
        ("GET", "/v1/requests/abc", A),
        ("POST", "/v1/requests/abc/cancel", A),
        ("GET", "/v1/requests/abc/events", A),
        ("GET", "/v1/requests", A),
        ("GET", "/v1/usage", A),
        ("GET", "/v1/admin/dlq", OP),
        ("GET", "/v1/admin/usage", OP),
    ],
)
async def test_row_redis_unreachable_is_503_as_today(method: str, path: str, key: str) -> None:
    server = FakeServer()
    redis = FakeRedis(server=server, decode_responses=True)
    async with build(StateStore(redis), redis) as client:
        server.connected = False
        reply = await client.request(method, path, headers=h(key), json={"prompt": "p"} if method == "POST" else None)
    assert (reply.status_code, reply.json()) == (503, UNAVAILABLE)  # fail closed, with no detail


# --------------------------------------------------------------------------- admin_disabled compatibility


async def test_admin_disabled_when_nothing_can_administer(state_store: StateStore, fake_redis: Any) -> None:
    """Phase 4: no Admin_API_Key and no key with the admin Scope: an admin route says so instead of ``forbidden``."""
    async with build(state_store, fake_redis, admin_api_keys=[], tenants=[]) as client:
        for path in ("/v1/admin/dlq", "/v1/admin/usage"):
            reply = await client.get(path, headers=h(DEFAULT))
            assert (reply.status_code, reply.json()) == (403, {"error": "admin_disabled"}), path


async def test_a_tenant_key_with_the_admin_scope_means_admin_is_not_disabled(
    state_store: StateStore, fake_redis: Any
) -> None:
    await state_store.set_index_ready()
    await state_store.set_tenant_index_ready()  # team-a's Dead_Letter list is a tenant read: it needs the twins
    async with build(state_store, fake_redis, admin_api_keys=[]) as client:  # team-a's key A holds `admin`
        reply = await client.get("/v1/admin/usage", headers=h(DEFAULT))
        assert (reply.status_code, reply.json()) == (403, {"error": "forbidden", "scope": "operator"})
        assert (await client.get("/v1/admin/dlq", headers=h(A))).status_code == 200
