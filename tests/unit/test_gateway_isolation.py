"""Tenants cannot see each other's Requests (das-multi-tenant tasks 5.1, 5.2, 5.3; Phase 5 Req 3.1 to 3.4, 3.7, 4.1,
4.4, 4.6, 9.3).

Driven through the real Gateway application with two tenants, the default tenant and an Operator. A Request of another
tenant answers exactly like a Request that does not exist, and a refused call writes nothing."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from das.config.schema import ClientLimitsConfig, ClientSettingsConfig, EventsConfig, TenantConfig
from das.gateway.app import create_app
from das.ops.keys import key_fingerprint
from das.registry import AgentRegistry
from das.state import StateStore, keys

A1, A2, A_READ, B1, DEFAULT, OP = (
    "team-a-key-000000001", "team-a-key-000000002", "team-a-read-0000001",
    "team-b-key-000000001", "default-key-00000001", "root-key-0000000001",
)  # fmt: skip
SECRET = "webhook-secret-0123456789"
NOT_FOUND = {"error": "not_found"}


def fp(key: str) -> str:
    return key_fingerprint(key.encode())


def h(key: str) -> dict[str, str]:
    return {"X-API-Key": key}


def tenants(a_quota: int = 0, a_settings: dict[str, Any] | None = None) -> list[TenantConfig]:
    return [
        TenantConfig.model_validate(
            {
                "id": "team-a",
                "keys": [
                    {"fingerprint": fp(A1), "scopes": ["submit", "read"]},
                    {"fingerprint": fp(A2), "scopes": ["submit", "read"]},
                    {"fingerprint": fp(A_READ), "scopes": ["read"]},
                ],
                "max_active_requests": a_quota,
                "settings": a_settings or {},
            }
        ),
        TenantConfig.model_validate({"id": "team-b", "keys": [{"fingerprint": fp(B1), "scopes": ["submit", "read"]}]}),
    ]


async def build(
    state_store: StateStore,
    fake_redis: Any,
    *,
    a_quota: int = 0,
    a_settings: dict[str, Any] | None = None,
    limits: ClientLimitsConfig | None = None,
    events: EventsConfig | None = None,
) -> httpx.AsyncClient:
    app = create_app(
        state_store,
        AgentRegistry(fake_redis),
        None,
        [A1, A2, A_READ, B1, DEFAULT],
        1000,
        admin_api_keys=[OP],
        tenants=tenants(a_quota, a_settings),
        client_limits=limits,
        events=events or EventsConfig(keepalive=0.05),  # a short keepalive: a terminal stream ends quickly
        webhook_secrets={"partner": SECRET},  # type: ignore[arg-type]
    )
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw")


@pytest.fixture
async def gw(state_store: StateStore, fake_redis: Any, fake_redis_time: Any) -> AsyncIterator[httpx.AsyncClient]:
    await state_store.set_index_ready()
    await state_store.set_tenant_index_ready()
    async with await build(state_store, fake_redis) as client:
        yield client


async def submit(gw: httpx.AsyncClient, key: str, prompt: str = "hello", **headers: str) -> str:
    reply = await gw.post("/v1/requests", json={"prompt": prompt}, headers={**h(key), **headers})
    assert reply.status_code == 202, reply.text
    return str(reply.json()["request_id"])


# =========================================================================== 5.1: get, cancel, events


async def test_a_tenant_reads_its_own_requests_whichever_of_its_keys_made_them(gw: httpx.AsyncClient) -> None:
    rid = await submit(gw, A1)
    for key in (A1, A2, A_READ):  # the tenant owns the Request, not the key
        assert (await gw.get(f"/v1/requests/{rid}", headers=h(key))).status_code == 200, key


async def test_another_tenant_the_default_tenant_and_nobody_else_get_the_body_of_a_missing_request(
    gw: httpx.AsyncClient,
) -> None:
    rid = await submit(gw, A1)
    missing = await gw.get("/v1/requests/no-such-request", headers=h(B1))
    for key in (B1, DEFAULT):
        reply = await gw.get(f"/v1/requests/{rid}", headers=h(key))
        assert (reply.status_code, reply.json()) == (404, NOT_FOUND), key
        assert (reply.status_code, reply.json(), reply.headers["content-length"]) == (
            missing.status_code,
            missing.json(),
            missing.headers["content-length"],
        )  # the same answer as for a Request that was never created


async def test_an_operator_reads_every_tenants_requests(gw: httpx.AsyncClient) -> None:
    for key in (A1, B1, DEFAULT):
        rid = await submit(gw, key)
        assert (await gw.get(f"/v1/requests/{rid}", headers=h(OP))).status_code == 200


async def test_a_foreign_cancel_is_a_404_that_writes_nothing(gw: httpx.AsyncClient, state_store: StateStore) -> None:
    rid = await submit(gw, A1)
    before = {k: await state_store.client.hgetall(k) for k in (keys.request(rid),)}
    events_before = await state_store.client.xlen(keys.events(rid))
    reply = await gw.post(f"/v1/requests/{rid}/cancel", headers=h(B1))
    assert (reply.status_code, reply.json()) == (404, NOT_FOUND)
    assert {k: await state_store.client.hgetall(k) for k in before} == before  # still Pending, nothing touched
    assert await state_store.client.xlen(keys.events(rid)) == events_before
    assert (await gw.get(f"/v1/requests/{rid}", headers=h(A1))).json()["status"] == "Pending"


async def test_the_owner_and_an_operator_can_cancel(gw: httpx.AsyncClient) -> None:
    own, other = await submit(gw, A1), await submit(gw, B1)
    assert (await gw.post(f"/v1/requests/{own}/cancel", headers=h(A2))).status_code == 202
    assert (await gw.post(f"/v1/requests/{other}/cancel", headers=h(OP))).status_code == 202


async def test_a_foreign_events_stream_is_a_404_that_takes_no_slot(
    gw: httpx.AsyncClient, state_store: StateStore
) -> None:
    rid = await submit(gw, A1)
    await gw.post(f"/v1/requests/{rid}/cancel", headers=h(A1))  # finished: a wrongly allowed stream would end
    reply = await gw.get(f"/v1/requests/{rid}/events", headers=h(B1))
    assert (reply.status_code, reply.json()) == (404, NOT_FOUND)
    assert await state_store.client.keys("das:client:*:streams") == []  # no stream slot was taken


async def test_the_owner_streams_a_finished_requests_events(
    state_store: StateStore, fake_redis: Any, fake_redis_time: Any
) -> None:
    async with await build(state_store, fake_redis, events=EventsConfig(keepalive=0.05)) as gw:
        rid = await submit(gw, A1)
        await gw.post(f"/v1/requests/{rid}/cancel", headers=h(A1))
        reply = await gw.get(f"/v1/requests/{rid}/events", headers=h(A2))
        assert reply.status_code == 200 and "snapshot" in reply.text


# =========================================================================== 5.2: the listing


async def ids(gw: httpx.AsyncClient, key: str, query: str = "") -> list[str]:
    reply = await gw.get(f"/v1/requests{query}", headers=h(key))
    assert reply.status_code == 200, reply.text
    return [item["request_id"] for item in reply.json()["items"]]


async def test_a_tenant_lists_only_its_own_requests_and_an_operator_lists_them_all(gw: httpx.AsyncClient) -> None:
    a = [await submit(gw, A1), await submit(gw, A2)]
    b = [await submit(gw, B1)]
    d = [await submit(gw, DEFAULT)]
    assert sorted(await ids(gw, A_READ)) == sorted(a)
    assert await ids(gw, B1) == b
    assert await ids(gw, DEFAULT) == d
    assert sorted(await ids(gw, OP)) == sorted(a + b + d)


async def test_an_operator_may_filter_by_tenant_and_nobody_else_may(gw: httpx.AsyncClient) -> None:
    a, b = await submit(gw, A1), await submit(gw, B1)
    assert await ids(gw, OP, "?tenant=team-a") == [a]
    assert await ids(gw, OP, "?tenant=team-b") == [b]
    assert await ids(gw, OP, "?tenant=nobody") == []
    for key in (A1, DEFAULT):
        reply = await gw.get("/v1/requests?tenant=team-a", headers=h(key))
        assert (reply.status_code, reply.json()) == (400, {"error": "invalid_query", "parameter": "tenant"}), key
    bad = await gw.get("/v1/requests?tenant=Not_A_Tenant", headers=h(OP))
    assert (bad.status_code, bad.json()) == (400, {"error": "invalid_query", "parameter": "tenant"})


async def test_the_filters_of_the_phase_4_listing_still_apply_inside_a_tenant(gw: httpx.AsyncClient) -> None:
    first, second = await submit(gw, A1), await submit(gw, A1)
    await gw.post(f"/v1/requests/{first}/cancel", headers=h(A1))
    assert await ids(gw, A1, "?status=Cancelled") == [first]
    assert await ids(gw, A1, "?status=Pending") == [second]
    assert await ids(gw, B1, "?status=Cancelled") == []


async def test_paging_stays_inside_the_tenant_without_gaps(gw: httpx.AsyncClient) -> None:
    mine = [await submit(gw, A1) for _ in range(5)]
    for _ in range(4):
        await submit(gw, B1)
    seen: list[str] = []
    query = "?limit=2"
    for _ in range(5):
        page = (await gw.get(f"/v1/requests{query}", headers=h(A_READ))).json()
        seen += [i["request_id"] for i in page["items"]]
        if "next_cursor" not in page:
            break
        query = f"?limit=2&cursor={page['next_cursor']}"
    assert sorted(seen) == sorted(mine)


async def test_a_cursor_issued_to_one_tenant_is_rejected_for_another(gw: httpx.AsyncClient) -> None:
    for _ in range(3):
        await submit(gw, A1)
        await submit(gw, B1)
    cursor = (await gw.get("/v1/requests?limit=1", headers=h(A1))).json()["next_cursor"]
    refused = await gw.get(f"/v1/requests?limit=1&cursor={cursor}", headers=h(B1))
    assert (refused.status_code, refused.json()) == (400, {"error": "invalid_query", "parameter": "cursor"})
    assert (await gw.get(f"/v1/requests?limit=1&cursor={cursor}", headers=h(A2))).status_code == 200  # same tenant


async def test_an_operators_cursor_is_bound_to_the_tenant_it_listed(gw: httpx.AsyncClient) -> None:
    for _ in range(3):
        await submit(gw, A1)
        await submit(gw, B1)
    everything = (await gw.get("/v1/requests?limit=1", headers=h(OP))).json()["next_cursor"]
    only_a = (await gw.get("/v1/requests?limit=1&tenant=team-a", headers=h(OP))).json()["next_cursor"]
    for reused, query in ((everything, "&tenant=team-a"), (only_a, "")):
        reply = await gw.get(f"/v1/requests?limit=1&cursor={reused}{query}", headers=h(OP))
        assert (reply.status_code, reply.json()["parameter"]) == (400, "cursor")


async def test_a_non_default_tenant_is_refused_until_the_backfill_has_run(
    state_store: StateStore, fake_redis: Any, fake_redis_time: Any
) -> None:
    await state_store.set_index_ready()  # the Phase 2 index is done; the Tenant_Backfill is not
    async with await build(state_store, fake_redis) as gw:
        rid = await submit(gw, DEFAULT)
        reply = await gw.get("/v1/requests", headers=h(A1))
        assert (reply.status_code, reply.json()) == (503, {"error": "index_not_ready"})
        assert await ids(gw, DEFAULT) == [rid]  # the default tenant reads the global index meanwhile (Req 9.3)
        assert await ids(gw, OP) == [rid]


# =========================================================================== 5.3: submit


async def test_a_submit_stores_the_tenant_of_the_principal(gw: httpx.AsyncClient, state_store: StateStore) -> None:
    for key, tenant in ((A1, "team-a"), (B1, "team-b"), (DEFAULT, "default"), (OP, "default")):
        rid = await submit(gw, key)
        snap = await state_store.load_snapshot(rid)
        assert snap is not None and snap.tenant == tenant, key
        assert await state_store.client.zscore(keys.tenant_requests(tenant), rid) is not None, key


async def test_the_tenant_quota_counts_every_key_of_the_tenant_and_answers_with_its_scope(
    state_store: StateStore, fake_redis: Any, fake_redis_time: Any
) -> None:
    await state_store.set_index_ready()
    async with await build(state_store, fake_redis, a_quota=2) as gw:
        await submit(gw, A1)
        await submit(gw, A2)  # a different key of the same tenant
        refused = await gw.post("/v1/requests", json={"prompt": "x"}, headers=h(A1))
        assert (refused.status_code, refused.json()) == (
            429, {"error": "quota_exceeded", "limit": 2, "limit_scope": "tenant"},
        )  # fmt: skip
        assert (await gw.post("/v1/requests", json={"prompt": "x"}, headers=h(A2))).status_code == 429
        await submit(gw, B1)  # another tenant is not affected
        assert (await state_store.client.scard(keys.tenant_active("team-a"))) == 2  # nothing was created


async def test_the_per_key_quota_keeps_its_phase_4_body_and_both_quotas_apply(
    state_store: StateStore, fake_redis: Any, fake_redis_time: Any
) -> None:
    limits = ClientLimitsConfig(default=ClientSettingsConfig(max_active_requests=1))
    async with await build(state_store, fake_redis, a_quota=5, limits=limits) as gw:
        await submit(gw, A1)
        refused = await gw.post("/v1/requests", json={"prompt": "x"}, headers=h(A1))
        assert (refused.status_code, refused.json()) == (429, {"error": "quota_exceeded", "limit": 1})  # no limit_scope
        await submit(gw, A2)  # its own key has room and the tenant has room


async def test_the_tenants_default_budget_caps_a_client_budget(
    state_store: StateStore, fake_redis: Any, fake_redis_time: Any
) -> None:
    async with await build(state_store, fake_redis, a_settings={"default_budget": {"max_total_tokens": 1000}}) as gw:
        over = await gw.post("/v1/requests", json={"prompt": "x", "budget": {"max_total_tokens": 5000}}, headers=h(A1))
        assert (over.status_code, over.json()["error"], over.json()["limit"]) == (400, "budget_above_limit", 1000)
        rid = await submit(gw, A1)  # no client budget: the tenant default applies
        snap = await state_store.load_snapshot(rid)
        assert snap is not None and snap.budget is not None and snap.budget.max_total_tokens == 1000
        other = await submit(gw, B1)  # another tenant has no default budget
        snap_b = await state_store.load_snapshot(other)
        assert snap_b is not None and snap_b.budget is None


async def test_the_webhook_target_is_resolved_when_the_request_is_created(
    state_store: StateStore, fake_redis: Any, fake_redis_time: Any
) -> None:
    settings = {"webhook_url": "https://hooks.example.com/a", "webhook_secret_name": "partner"}
    async with await build(state_store, fake_redis, a_settings=settings) as gw:
        rid = await submit(gw, A1)
        other = await submit(gw, B1)
    r = state_store.client
    assert await r.hget(keys.request(rid), keys.F_WEBHOOK_URL) == "https://hooks.example.com/a"
    assert await r.hget(keys.request(rid), keys.F_WEBHOOK_SECRET_NAME) == "partner"
    assert await r.hget(keys.request(other), keys.F_WEBHOOK_URL) is None


async def test_an_idempotency_key_stays_per_api_key(gw: httpx.AsyncClient) -> None:
    first = await submit(gw, A1, Idempotency_Key="same")  # type: ignore[arg-type]
    del first
    one = await gw.post("/v1/requests", json={"prompt": "p"}, headers={**h(A1), "Idempotency-Key": "k1"})
    two = await gw.post("/v1/requests", json={"prompt": "p"}, headers={**h(A2), "Idempotency-Key": "k1"})
    replay = await gw.post("/v1/requests", json={"prompt": "p"}, headers={**h(A1), "Idempotency-Key": "k1"})
    assert (one.status_code, two.status_code, replay.status_code) == (202, 202, 200)
    assert one.json() == replay.json() and one.json() != two.json()  # a tenant's other key starts its own record
