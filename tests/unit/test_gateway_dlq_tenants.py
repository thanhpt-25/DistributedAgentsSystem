"""Dead-letter and admin routes are tenant-scoped (das-multi-tenant task 5.4; Phase 5 Req 5.3, 5.4, 5.6, 2.2).

A tenant's ``admin`` key lists, reads, replays and discards only the records of its own tenant, and gets the body of a
missing record for any other; an Operator sees every record and may filter by tenant; draining agents is for Operators
only. A configuration with nothing that can administer still answers ``admin_disabled``."""

from __future__ import annotations

import io
import json
from collections.abc import AsyncIterator, Iterator
from typing import Any

import httpx
import pytest

from das.config.schema import TenantConfig
from das.gateway.app import create_app
from das.models import JobRecord, JobState
from das.observability.logging import configure_logging
from das.ops.keys import key_fingerprint
from das.registry import AgentInfo, AgentRegistry
from das.state import ClientRequestSpec, StateStore, TransitionBatch, keys
from tests.unit.test_state_phase2_dlq import fields
from tests.unit.test_state_store import PLAN

A_ADMIN, A_PLAIN, B_ADMIN = "team-a-admin-0000001", "team-a-plain-0000001", "team-b-admin-0000001"
DEFAULT, OP = "default-key-00000001", "root-key-0000000001"  # not named "operator": see the log redactor
DL_A, DL_B, DL_NONE, DL_A2 = "a" * 32, "b" * 32, "c" * 32, "d" * 32
NOT_FOUND = {"error": "not_found"}


def fp(key: str) -> str:
    return key_fingerprint(key.encode())


def h(key: str) -> dict[str, str]:
    return {"X-API-Key": key}


TENANTS = [
    TenantConfig.model_validate(
        {
            "id": "team-a",
            "keys": [
                {"fingerprint": fp(A_ADMIN), "scopes": ["read", "admin"]},
                {"fingerprint": fp(A_PLAIN), "scopes": ["submit", "read"]},
            ],
        }
    ),
    TenantConfig.model_validate({"id": "team-b", "keys": [{"fingerprint": fp(B_ADMIN), "scopes": ["admin"]}]}),
]


async def request_with_failed_job(store: StateStore, tenant: str | None) -> str:
    """A Running Request of ``tenant`` whose Job failed, so a dead letter of it can be replayed."""
    spec = ClientRequestSpec(fingerprint="cbabecbd", tenant=tenant)
    rid = (await store.submit_request("p", spec)).request_id
    assert rid is not None
    lease = (await store.acquire_lease(rid, "m1", 30)).lease
    assert lease is not None and (await store.commit_plan(lease, PLAN)).ok
    failed = JobRecord(job=PLAN.jobs[0], state=JobState.FAILED, attempt=1, reason="x")
    skipped = JobRecord(job=PLAN.jobs[1], state=JobState.SKIPPED, reason="dep failed")
    assert (await store.apply_transition(lease, TransitionBatch(jobs={"a": failed, "b": skipped}))).ok
    return rid


@pytest.fixture
async def gw(state_store: StateStore, fake_redis: Any, fake_redis_time: Any) -> AsyncIterator[httpx.AsyncClient]:
    for dlid, tenant in ((DL_A, "team-a"), (DL_A2, "team-a"), (DL_B, "team-b")):
        rid = await request_with_failed_job(state_store, tenant)
        assert (await state_store.create_dead_letter(fields(dlid, rid=rid))).created
    assert (await state_store.create_dead_letter(fields(DL_NONE, rid=None))).created  # no Request: no tenant
    await state_store.set_index_ready()
    await state_store.set_tenant_index_ready()
    registry = AgentRegistry(fake_redis)
    await registry.register(AgentInfo(agent_id="worker-1", role="worker", host="h"))
    app = create_app(
        state_store, registry, None, [A_ADMIN, A_PLAIN, B_ADMIN, DEFAULT], 100, admin_api_keys=[OP], tenants=TENANTS
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
        yield client


async def listed(gw: httpx.AsyncClient, key: str, query: str = "") -> list[str]:
    reply = await gw.get(f"/v1/admin/dlq{query}", headers=h(key))
    assert reply.status_code == 200, reply.text
    return [item["dead_letter_id"] for item in reply.json()["items"]]


# =========================================================================== who may use the routes


async def test_a_tenant_admin_key_reaches_the_dlq_routes_and_a_plain_key_does_not(gw: httpx.AsyncClient) -> None:
    assert (await gw.get("/v1/admin/dlq", headers=h(A_ADMIN))).status_code == 200
    for key in (A_PLAIN, DEFAULT):
        reply = await gw.get("/v1/admin/dlq", headers=h(key))
        assert (reply.status_code, reply.json()) == (403, {"error": "forbidden", "scope": "admin"}), key


async def test_drain_and_undrain_are_for_operators_only(gw: httpx.AsyncClient) -> None:
    for action in ("drain", "undrain"):
        for key in (A_ADMIN, B_ADMIN, A_PLAIN, DEFAULT):
            reply = await gw.post(f"/v1/admin/agents/worker-1/{action}", headers=h(key))
            assert (reply.status_code, reply.json()) == (403, {"error": "forbidden", "scope": "operator"}), (
                action,
                key,
            )
    # An Operator passes the check: the agent has no control transport here, so it answers 503, not 403.
    assert (await gw.post("/v1/admin/agents/worker-1/drain", headers=h(OP))).status_code == 503


# =========================================================================== list


async def test_a_tenant_admin_lists_only_its_own_tenants_records(gw: httpx.AsyncClient) -> None:
    assert sorted(await listed(gw, A_ADMIN)) == sorted([DL_A, DL_A2])
    assert await listed(gw, B_ADMIN) == [DL_B]


async def test_an_operator_lists_everything_including_records_of_no_tenant(gw: httpx.AsyncClient) -> None:
    assert sorted(await listed(gw, OP)) == sorted([DL_A, DL_A2, DL_B, DL_NONE])


async def test_an_operator_may_filter_by_tenant_and_nobody_else_may(gw: httpx.AsyncClient) -> None:
    assert sorted(await listed(gw, OP, "?tenant=team-a")) == sorted([DL_A, DL_A2])
    assert await listed(gw, OP, "?tenant=team-b") == [DL_B]
    assert await listed(gw, OP, "?tenant=nobody") == []
    for key, query in ((A_ADMIN, "?tenant=team-a"), (B_ADMIN, "?tenant=team-a")):
        reply = await gw.get(f"/v1/admin/dlq{query}", headers=h(key))
        assert (reply.status_code, reply.json()) == (400, {"error": "invalid_query", "parameter": "tenant"})
    bad = await gw.get("/v1/admin/dlq?tenant=Bad_ID", headers=h(OP))
    assert (bad.status_code, bad.json()) == (400, {"error": "invalid_query", "parameter": "tenant"})


async def test_the_filters_of_the_phase_2_listing_still_apply_inside_a_tenant(gw: httpx.AsyncClient) -> None:
    assert sorted(await listed(gw, A_ADMIN, "?status=Pending")) == sorted([DL_A, DL_A2])
    assert await listed(gw, A_ADMIN, "?status=Discarded") == []
    assert await listed(gw, A_ADMIN, f"?request_id={'x' * 5}") == []


async def test_paging_stays_inside_the_tenant_and_the_cursor_is_bound_to_it(gw: httpx.AsyncClient) -> None:
    first = (await gw.get("/v1/admin/dlq?limit=1", headers=h(A_ADMIN))).json()
    assert len(first["items"]) == 1 and "next_cursor" in first
    second = (await gw.get(f"/v1/admin/dlq?limit=1&cursor={first['next_cursor']}", headers=h(A_ADMIN))).json()
    assert sorted([first["items"][0]["dead_letter_id"], second["items"][0]["dead_letter_id"]]) == sorted([DL_A, DL_A2])
    refused = await gw.get(f"/v1/admin/dlq?limit=1&cursor={first['next_cursor']}", headers=h(B_ADMIN))
    assert (refused.status_code, refused.json()) == (400, {"error": "invalid_query", "parameter": "cursor"})
    everything = (await gw.get("/v1/admin/dlq?limit=1", headers=h(OP))).json()["next_cursor"]
    reused = await gw.get(f"/v1/admin/dlq?limit=1&tenant=team-a&cursor={everything}", headers=h(OP))
    assert (reused.status_code, reused.json()["parameter"]) == (400, "cursor")


async def test_a_non_default_tenant_is_refused_until_the_backfill_has_run(
    state_store: StateStore, fake_redis: Any, fake_redis_time: Any
) -> None:
    await state_store.set_index_ready()
    app = create_app(state_store, AgentRegistry(fake_redis), None, [A_ADMIN], 100, admin_api_keys=[OP], tenants=TENANTS)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
        reply = await client.get("/v1/admin/dlq", headers=h(A_ADMIN))
        assert (reply.status_code, reply.json()) == (503, {"error": "index_not_ready"})
        assert (await client.get("/v1/admin/dlq", headers=h(OP))).status_code == 200


# =========================================================================== get, replay, discard


async def test_get_answers_404_for_another_tenants_record_and_for_a_record_of_no_tenant(gw: httpx.AsyncClient) -> None:
    assert (await gw.get(f"/v1/admin/dlq/{DL_A}", headers=h(A_ADMIN))).status_code == 200
    missing = await gw.get(f"/v1/admin/dlq/{'e' * 32}", headers=h(A_ADMIN))
    for dlid, key in ((DL_B, A_ADMIN), (DL_A, B_ADMIN), (DL_NONE, A_ADMIN), (DL_NONE, B_ADMIN)):
        reply = await gw.get(f"/v1/admin/dlq/{dlid}", headers=h(key))
        assert (reply.status_code, reply.json()) == (404, NOT_FOUND) == (missing.status_code, missing.json()), (
            dlid,
            key,
        )
    for dlid in (DL_A, DL_B, DL_NONE):  # an Operator reads all of them
        assert (await gw.get(f"/v1/admin/dlq/{dlid}", headers=h(OP))).status_code == 200


async def test_a_foreign_replay_or_discard_is_a_404_that_writes_nothing(
    gw: httpx.AsyncClient, state_store: StateStore
) -> None:
    before = {k: await state_store.client.hgetall(k) for k in (keys.dlq(DL_B), keys.dlq(DL_NONE))}
    for action in ("replay", "discard"):
        for dlid, key in ((DL_B, A_ADMIN), (DL_NONE, A_ADMIN), (DL_A, B_ADMIN)):
            reply = await gw.post(f"/v1/admin/dlq/{dlid}/{action}", headers=h(key))
            assert (reply.status_code, reply.json()) == (404, NOT_FOUND), (action, dlid, key)
    assert {k: await state_store.client.hgetall(k) for k in before} == before
    assert await state_store.client.zscore(keys.dlq_by_status("Pending"), DL_A) is not None  # DL_A untouched too


async def test_a_tenant_replays_and_discards_its_own_records(gw: httpx.AsyncClient) -> None:
    replay = await gw.post(f"/v1/admin/dlq/{DL_A}/replay", headers=h(A_ADMIN))
    assert replay.status_code == 202 and replay.json()["dead_letter_id"] == DL_A
    discard = await gw.post(f"/v1/admin/dlq/{DL_A2}/discard", headers=h(A_ADMIN))
    assert (discard.status_code, discard.json()["status"]) == (200, "Discarded")
    assert await listed(gw, A_ADMIN, "?status=Discarded") == [DL_A2]
    assert await listed(gw, B_ADMIN, "?status=Discarded") == []  # nothing leaked into the other tenant's view


async def test_an_operator_may_replay_and_discard_any_record(gw: httpx.AsyncClient) -> None:
    assert (await gw.post(f"/v1/admin/dlq/{DL_B}/replay", headers=h(OP))).status_code == 202
    assert (await gw.post(f"/v1/admin/dlq/{DL_NONE}/discard", headers=h(OP))).status_code == 200


# =========================================================================== compatibility and audit


async def test_a_phase_4_configuration_still_answers_admin_disabled(
    state_store: StateStore, fake_redis: Any, fake_redis_time: Any
) -> None:
    app = create_app(state_store, AgentRegistry(fake_redis), None, [DEFAULT], 100)  # no admin keys, no tenants
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
        for method, path in (("GET", "/v1/admin/dlq"), ("POST", "/v1/admin/agents/x/drain")):
            reply = await client.request(method, path, headers=h(DEFAULT))
            assert (reply.status_code, reply.json()) == (403, {"error": "admin_disabled"}), path


async def test_with_an_operator_configured_a_plain_key_is_forbidden_not_disabled(gw: httpx.AsyncClient) -> None:
    reply = await gw.get("/v1/admin/dlq", headers=h(DEFAULT))
    assert reply.json()["error"] == "forbidden"


@pytest.fixture
def stream() -> Iterator[io.StringIO]:
    out = io.StringIO()
    configure_logging("gw-test", secrets=[A_ADMIN, B_ADMIN, OP], stream=out)
    yield out
    configure_logging("test", stream=io.StringIO())


async def test_the_audit_entry_of_a_tenant_admin_call_names_its_tenant(
    gw: httpx.AsyncClient, stream: io.StringIO
) -> None:
    await gw.get(f"/v1/admin/dlq/{DL_B}", headers=h(A_ADMIN))  # refused as a 404
    await gw.get("/v1/admin/dlq", headers=h(A_ADMIN))
    entries = [json.loads(line) for line in stream.getvalue().splitlines() if '"audit"' in line]
    assert [(e["tenant"], e["operator"], e["http_status"]) for e in entries] == [
        ("team-a", False, 404),
        ("team-a", False, 200),
    ]
    assert all(A_ADMIN not in json.dumps(e) for e in entries)
