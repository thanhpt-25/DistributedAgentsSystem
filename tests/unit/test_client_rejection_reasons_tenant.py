"""The Phase 5 rejection reasons of ``das_client_rejections_total`` (das-multi-tenant task 7.2; Phase 5 Req 8.2).

``forbidden`` counts a 403 ``forbidden`` (a Principal without the Scope or role its route needs),
``tenant_quota_exceeded`` the tenant's 429, ``priority_above_limit`` the 400 of that name. The reason is one of an
enumerated set: never a fingerprint, tenant or key."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from das.config.schema import TenantConfig
from das.gateway.app import create_app
from das.observability.metrics import CLIENT_REJECTION_REASONS
from das.ops.keys import key_fingerprint
from das.registry import AgentRegistry
from das.state import StateStore
from tests.unit.test_gateway_v1_extensions import Recorders

READER, SUBMITTER, FULL = "reader-key-00000001", "submit-key-0000001", "full-key-000000001"
LOW, DEFAULT, OP = "low-key-000000001", "default-key-00000001", "root-key-0000000001"
METRIC = "das_client_rejections_total"


def fp(key: str) -> str:
    return key_fingerprint(key.encode())


def tenant(tid: str, scopes_by_key: dict[str, list[str]], **extra: Any) -> TenantConfig:
    keys = [{"fingerprint": fp(k), "scopes": s} for k, s in scopes_by_key.items()]
    return TenantConfig.model_validate({"id": tid, "keys": keys, **extra})


def h(key: str) -> dict[str, str]:
    return {"X-API-Key": key}


def test_the_catalog_knows_the_phase_5_reasons() -> None:
    assert {"forbidden", "tenant_quota_exceeded", "priority_above_limit"} <= CLIENT_REJECTION_REASONS
    assert {"rate_limited", "quota_exceeded", "too_many_streams", "budget_above_limit"} <= CLIENT_REJECTION_REASONS


@pytest.fixture
def recorders() -> Recorders:
    return Recorders()


@pytest.fixture
async def gw(state_store: StateStore, fake_redis: Any, recorders: Recorders) -> AsyncIterator[httpx.AsyncClient]:
    team_a = tenant(
        "team-a", {READER: ["read"], SUBMITTER: ["submit"], FULL: ["submit", "read", "admin"]}, max_active_requests=1
    )
    team_low = tenant("team-low", {LOW: ["submit", "read"]})  # max_priority is normal
    app = create_app(
        state_store, AgentRegistry(fake_redis), None, [READER, SUBMITTER, FULL, LOW, DEFAULT], 1000,
        admin_api_keys=[OP], tenants=[team_a, team_low], metrics_factory=recorders,
    )  # fmt: skip
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
        yield client


# --------------------------------------------------------------------------- forbidden


async def test_a_missing_scope_on_a_v1_route_is_counted(gw: httpx.AsyncClient, recorders: Recorders) -> None:
    reply = await gw.post("/v1/requests", json={"prompt": "p"}, headers=h(READER))  # read only: no submit
    assert (reply.status_code, reply.json()) == (403, {"error": "forbidden", "scope": "submit"})
    assert recorders.value(METRIC, reason="forbidden") == 1
    assert (await gw.get("/v1/agents", headers=h(SUBMITTER))).status_code == 403  # submit only: no read
    assert recorders.value(METRIC, reason="forbidden") == 2


async def test_a_missing_scope_or_role_on_an_admin_route_is_counted(
    gw: httpx.AsyncClient, recorders: Recorders
) -> None:
    assert (await gw.get("/v1/admin/dlq", headers=h(READER))).status_code == 403  # no admin Scope
    assert (await gw.get("/v1/admin/usage", headers=h(FULL))).status_code == 403  # admin Scope is not the Operator role
    assert recorders.value(METRIC, reason="forbidden") == 2


async def test_allowed_and_unauthenticated_calls_are_not_counted(gw: httpx.AsyncClient, recorders: Recorders) -> None:
    assert (await gw.get("/v1/agents", headers=h(READER))).status_code == 200
    assert (await gw.get("/v1/admin/dlq", headers=h(OP))).status_code == 200
    assert (await gw.get("/v1/agents", headers=h("nobody-key-0000000001"))).status_code == 401
    assert recorders.value(METRIC, reason="forbidden") == 0


async def test_admin_disabled_is_not_counted_as_forbidden(state_store: StateStore, fake_redis: Any) -> None:
    """With nothing able to administer, a refused admin route answers ``admin_disabled``: another answer."""
    recorder = Recorders()
    app = create_app(state_store, AgentRegistry(fake_redis), None, [DEFAULT], 1000, metrics_factory=recorder)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
        reply = await c.get("/v1/admin/dlq", headers=h(DEFAULT))
    assert (reply.status_code, reply.json()) == (403, {"error": "admin_disabled"})
    assert recorder.value(METRIC, reason="forbidden") == 0


# --------------------------------------------------------------------------- tenant quota


async def test_the_tenant_quota_has_its_own_reason(gw: httpx.AsyncClient, recorders: Recorders) -> None:
    assert (await gw.post("/v1/requests", json={"prompt": "p"}, headers=h(FULL))).status_code == 202
    reply = await gw.post("/v1/requests", json={"prompt": "p"}, headers=h(SUBMITTER))  # the tenant's quota is 1
    assert (reply.status_code, reply.json()["limit_scope"]) == (429, "tenant")
    assert recorders.value(METRIC, reason="tenant_quota_exceeded") == 1
    assert recorders.value(METRIC, reason="quota_exceeded") == 0  # not also counted as the per-key reason


# --------------------------------------------------------------------------- priority


async def test_a_priority_above_the_limit_is_counted_and_an_invalid_one_is_not(
    gw: httpx.AsyncClient, recorders: Recorders
) -> None:
    refused = await gw.post("/v1/requests", json={"prompt": "p", "priority": "high"}, headers=h(LOW))
    assert (refused.status_code, refused.json()["error"]) == (400, "priority_above_limit")
    assert recorders.value(METRIC, reason="priority_above_limit") == 1
    invalid = await gw.post("/v1/requests", json={"prompt": "p", "priority": "urgent"}, headers=h(LOW))
    assert invalid.json()["error"] == "invalid_request"
    assert recorders.value(METRIC, reason="priority_above_limit") == 1  # a malformed value is not a limit


async def test_no_reason_carries_a_key_a_fingerprint_or_a_tenant(gw: httpx.AsyncClient, recorders: Recorders) -> None:
    await gw.post("/v1/requests", json={"prompt": "p"}, headers=h(READER))
    await gw.post("/v1/requests", json={"prompt": "p", "priority": "high"}, headers=h(LOW))
    assert recorders.recorder is not None
    text = recorders.recorder.render()[0].decode()
    rejected = [line for line in text.splitlines() if line.startswith(METRIC)]
    assert rejected
    for line in rejected:
        for secret in (READER, LOW, fp(READER), fp(LOW), "team-a", "team-low"):
            assert secret not in line
