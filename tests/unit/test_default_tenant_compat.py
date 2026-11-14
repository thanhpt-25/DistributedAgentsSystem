"""A Default_Tenant client sees Phase 4 (das-multi-tenant task 8.3; Phase 5 Req 9.5, 9.6).

With no ``tenants`` configured every API key is a Default_Tenant key. Three things must hold:

- the wire-format baseline is byte for byte what Phase 4 had (the codec corpus), and the contract baseline only grew;
- the Phase 4 flows (submit, replay, get, list, cancel, errors, quota) answer what Phase 4 answered, apart from the
  added ``priority`` field of the Request view, the added fields of the Audit_Entry and the ``scope`` of a 403;
- none of that needed the Tenant_Backfill: a Phase 4 fleet upgraded in place works before the flag is set.

The Phase 4 test suites that run beside this one (``tests/contract``, the Gateway unit tests) pass unchanged except the
tests listed in the plan under task 8.1."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from das.config.schema import ClientLimitsConfig, ClientSettingsConfig
from das.gateway.app import create_app
from das.registry import AgentRegistry
from das.state import StateStore
from tests.fixtures.phase3_baseline import CODEC_CORPUS, CONTRACT

PHASE_4_CONTRACT = Path(__file__).parent.parent / "fixtures" / "phase4_contract.json"
#: SHA-256 of ``codec_corpus.jsonl`` as committed in Phase 4 (``039d0bc``).
PHASE_4_CORPUS_SHA256 = "a1e51e5df147fc9836b2d16c9a2fb22be5cc48a3e9060d22a7cfb87b9c4a867f"

KEY, ADMIN = "client-key-0000000001", "operator-admin-key-0001"
AUTH, ADMIN_AUTH = {"X-API-Key": KEY}, {"X-API-Key": ADMIN}

# --------------------------------------------------------------------------- the baselines


def test_the_wire_format_baseline_is_byte_identical_to_phase_4() -> None:
    """The file is the Phase 4 one: nobody regenerated it. That the current codec still writes every one of its
    messages byte for byte is ``tests/property/test_codec_baseline_bytes.py``; together they say the codec output did
    not change (Req 7.5, 9.6)."""
    assert hashlib.sha256(CODEC_CORPUS.read_bytes()).hexdigest() == PHASE_4_CORPUS_SHA256


def only_grew(old: Any, new: Any, path: str = "$") -> list[str]:
    """What of the Phase 4 contract is missing or changed in the current one. Additions are not reported."""
    if isinstance(old, dict):
        if not isinstance(new, dict):
            return [f"{path}: no longer an object"]
        problems: list[str] = []
        for key, value in old.items():
            if key not in new:
                problems.append(f"{path}.{key}: removed")
            elif key == "required":  # a new required field of an existing section would break a Phase 4 configuration
                if set(value) != set(new[key]):
                    problems.append(f"{path}.required: {sorted(set(value) ^ set(new[key]))}")
            else:
                problems += only_grew(value, new[key], f"{path}.{key}")
        return problems
    if isinstance(old, list):
        if not isinstance(new, list):
            return [f"{path}: no longer a list"]
        if old and all(isinstance(item, dict) and "name" in item for item in old):  # the metric catalog, by name
            by_name = {item["name"]: item for item in new}
            return [
                problem
                for item in old
                for problem in (
                    only_grew(item, by_name[item["name"]], f"{path}[{item['name']}]")
                    if item["name"] in by_name
                    else [f"{path}[{item['name']}]: removed"]
                )
            ]
        return [f"{path}: lost {json.dumps(item)[:60]}" for item in old if item not in new]
    return [] if old == new else [f"{path}: {old!r} became {new!r}"]


#: Phase 7 made ``redis.url`` optional in the schema, because Sentinel mode has none (Phase 7 Req 1.2); standalone mode
#: still requires it in ``cross_field_errors``, so every Phase 4 configuration is still valid. These two baseline paths
#: are the one thing allowed to be more than an addition (see the Compatibility section of the Phase 7 design).
ALLOWED_RELAXATIONS = frozenset({
    "$.config_schema.$defs.RedisConfig.properties.url.type",
    "$.config_schema.$defs.RedisConfig.required",
})


def test_the_contract_baseline_only_grew_since_phase_4() -> None:
    """Every route, metric, label, reason, environment variable, exit code, configuration field and default of
    Phase 4 is still there, unchanged (Req 9.6): Phase 5 adds, it does not take away."""
    phase_4 = json.loads(PHASE_4_CONTRACT.read_text(encoding="utf-8"))
    current = json.loads(CONTRACT.read_text(encoding="utf-8"))
    problems = [p for p in only_grew(phase_4, current) if p.split(":")[0] not in ALLOWED_RELAXATIONS]
    assert problems == []


def test_the_check_notices_what_it_is_for() -> None:
    old = {"routes": [["GET", "/a"], ["GET", "/b"]], "metrics": [{"name": "m", "labels": ["x"]}], "required": ["a"]}
    assert only_grew(old, {**old, "routes": old["routes"] + [["GET", "/c"]]}) == []  # an addition is fine
    assert only_grew(old, {**old, "metrics": [{"name": "m", "labels": ["x", "tenant"]}]}) == []
    assert only_grew(old, {**old, "routes": [["GET", "/a"]]}) != []  # a removed route
    assert only_grew(old, {**old, "metrics": [{"name": "m", "labels": []}]}) != []  # a removed label
    assert only_grew(old, {**old, "metrics": []}) != []  # a removed metric
    assert only_grew(old, {**old, "required": ["a", "b"]}) != []  # a new required field


# --------------------------------------------------------------------------- the flows


@pytest.fixture
async def gw(state_store: StateStore, fake_redis: Any, fake_redis_time: Any) -> AsyncIterator[httpx.AsyncClient]:
    await state_store.set_index_ready()  # a Phase 4 fleet has this; the Tenant_Backfill flag is NOT set
    limits = ClientLimitsConfig(default=ClientSettingsConfig(max_active_requests=2))
    app = create_app(
        state_store, AgentRegistry(fake_redis), None, [KEY], 1000, admin_api_keys=[ADMIN], client_limits=limits
    )  # no tenants
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
        yield client


PHASE_4_VIEW = {
    "request_id", "status", "jobs", "transitions", "token_usage", "estimated_cost", "replay_history", "budget",
    "webhook",
}  # fmt: skip
PHASE_4_LIST_ITEM = {"request_id", "status", "created_at", "finished_at", "job_count", "token_usage"}


async def submit(gw: httpx.AsyncClient, **headers: str) -> httpx.Response:
    return await gw.post("/v1/requests", json={"prompt": "hello"}, headers={**AUTH, **headers})


async def test_submit_and_the_idempotent_replay_answer_as_in_phase_4(gw: httpx.AsyncClient) -> None:
    first = await submit(gw, **{"Idempotency-Key": "k-1"})
    assert first.status_code == 202 and set(first.json()) == {"request_id"}
    again = await submit(gw, **{"Idempotency-Key": "k-1"})
    assert (again.status_code, again.json()) == (200, first.json())
    assert again.headers["Idempotent-Replayed"] == "true" and "Idempotent-Replayed" not in first.headers
    other = await gw.post("/v1/requests", json={"prompt": "different"}, headers={**AUTH, "Idempotency-Key": "k-1"})
    assert (other.status_code, other.json()) == (422, {"error": "idempotency_key_reused"})


async def test_the_request_view_is_the_phase_4_one_plus_priority_and_replans(gw: httpx.AsyncClient) -> None:
    rid = (await submit(gw)).json()["request_id"]
    body = (await gw.get(f"/v1/requests/{rid}", headers=AUTH)).json()
    assert set(body) - {"priority", "replans"} == PHASE_4_VIEW  # Phase 5 added priority, Phase 6 added replans
    assert body["priority"] == "normal" and body["replans"] == 0  # the added fields, at their defaults
    assert (body["status"], body["jobs"], body["budget"], body["webhook"]) == ("Pending", [], None, None)


async def test_the_listing_works_before_the_backfill_and_has_the_phase_4_shape(
    gw: httpx.AsyncClient, state_store: StateStore
) -> None:
    rid = (await submit(gw)).json()["request_id"]
    for twin in await state_store.client.keys("das:tenant:*"):  # make it Phase 4 data: no tenant index anywhere
        await state_store.client.delete(twin)
    reply = await gw.get("/v1/requests", headers=AUTH)  # the tenant flag is not set: the global index answers
    assert reply.status_code == 200 and set(reply.json()) == {"items"}  # no next_cursor on a last page
    [item] = reply.json()["items"]
    assert item["request_id"] == rid and set(item) == PHASE_4_LIST_ITEM


async def test_cancel_and_the_errors_answer_as_in_phase_4(gw: httpx.AsyncClient) -> None:
    rid = (await submit(gw)).json()["request_id"]
    assert (await gw.post(f"/v1/requests/{rid}/cancel", headers=AUTH)).json() == {"status": "Cancelled"}
    again = await gw.post(f"/v1/requests/{rid}/cancel", headers=AUTH)
    assert (again.status_code, again.json()) == (409, {"error": "request_terminal", "status": "Cancelled"})
    for method, path in (("GET", "/v1/requests/nope"), ("POST", "/v1/requests/nope/cancel"),
                         ("GET", "/v1/requests/nope/events")):  # fmt: skip
        reply = await gw.request(method, path, headers=AUTH)
        assert (reply.status_code, reply.json()) == (404, {"error": "not_found"}), path
    bad = await gw.post("/v1/requests", json={"prompt": "   "}, headers=AUTH)
    assert (bad.status_code, bad.json()) == (400, {"error": "empty_prompt"})
    budget = await gw.post("/v1/requests", json={"prompt": "p", "budget": {"max_total_tokens": 0}}, headers=AUTH)
    assert (budget.status_code, budget.json()) == (
        400, {"error": "invalid_request", "parameter": "budget.max_total_tokens"},
    )  # fmt: skip
    for headers in ({}, {"X-API-Key": "wrong-key-000000000"}):
        reply = await gw.get("/v1/requests", headers=headers)
        assert (reply.status_code, reply.json()) == (401, {"error": "unauthorized"})


async def test_the_per_key_quota_keeps_its_phase_4_body(gw: httpx.AsyncClient) -> None:
    for _ in range(2):
        assert (await submit(gw)).status_code == 202
    over = await submit(gw)
    assert (over.status_code, over.json()) == (429, {"error": "quota_exceeded", "limit": 2})  # no limit_scope


async def test_admin_routes_answer_as_in_phase_4_apart_from_the_scope_of_a_403(gw: httpx.AsyncClient) -> None:
    listed = await gw.get("/v1/admin/dlq", headers=ADMIN_AUTH)  # an Admin_API_Key still works everywhere
    assert (listed.status_code, listed.json()) == (200, {"items": []})
    assert (await gw.get("/v1/agents", headers=ADMIN_AUTH)).status_code == 200
    refused = await gw.get("/v1/admin/dlq", headers=AUTH)  # a normal key on /v1/admin
    assert refused.status_code == 403
    assert {"error": "forbidden"}.items() <= refused.json().items()  # Phase 4's body, plus the field naming the Scope
    assert set(refused.json()) == {"error", "scope"}
    assert (await gw.get("/v1/admin/dlq")).json() == {"error": "unauthorized"}


async def test_an_admin_key_lists_every_request_as_in_phase_4(gw: httpx.AsyncClient) -> None:
    ids = {(await submit(gw)).json()["request_id"] for _ in range(2)}
    reply = await gw.get("/v1/requests", headers=ADMIN_AUTH)
    assert reply.status_code == 200 and {i["request_id"] for i in reply.json()["items"]} == ids
