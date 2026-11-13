"""Request priority at the Gateway (das-multi-tenant task 6.1; Phase 5 Req 7.1, 7.2, 7.3).

``priority`` is ``normal`` or ``high``. A tenant has a ``default_priority`` (used when the body names none) and a
``max_priority`` (a body above it is refused, nothing created); the Default_Tenant may use ``high``. The State_Store
keeps it only when ``high`` and the Request view always shows it."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from das.client.settings import resolve_settings
from das.config.schema import ClientLimitsConfig, TenantConfig
from das.gateway.app import create_app
from das.ops.keys import key_fingerprint
from das.registry import AgentRegistry
from das.state import StateStore, keys

BATCH, LIVE, PLAIN = "batch-key-0000000001", "live-key-00000000001", "plain-key-000000001"
DEFAULT, OP = "default-key-00000001", "root-key-0000000001"


def fp(key: str) -> str:
    return key_fingerprint(key.encode())


def h(key: str) -> dict[str, str]:
    return {"X-API-Key": key}


def tenant(tid: str, key: str, **extra: Any) -> TenantConfig:
    return TenantConfig.model_validate(
        {"id": tid, "keys": [{"fingerprint": fp(key), "scopes": ["submit", "read"]}], **extra}
    )


TENANTS = {
    "batch": tenant("batch", BATCH),  # the defaults: normal, and may not go above normal
    "live": tenant("live", LIVE, default_priority="high", max_priority="high"),
    "plain": tenant("plain", PLAIN, max_priority="high"),  # may use high, but does not by default
}


@pytest.fixture
async def gw(state_store: StateStore, fake_redis: Any) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(
        state_store, AgentRegistry(fake_redis), None, [BATCH, LIVE, PLAIN, DEFAULT], 1000,
        admin_api_keys=[OP], tenants=list(TENANTS.values()),
    )  # fmt: skip
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as client:
        yield client


async def submit(gw: httpx.AsyncClient, key: str, **body: Any) -> httpx.Response:
    return await gw.post("/v1/requests", json={"prompt": "hello", **body}, headers=h(key))


async def stored(state_store: StateStore, reply: httpx.Response) -> str | None:
    return await state_store.client.hget(keys.request(reply.json()["request_id"]), keys.F_PRIORITY)


# --------------------------------------------------------------------------- the settings


def test_a_tenant_carries_its_priorities_and_the_default_tenant_may_use_high() -> None:
    limits = ClientLimitsConfig()
    live = resolve_settings(fp(LIVE), limits, TENANTS["live"])
    assert (live.default_priority, live.max_priority) == ("high", "high")
    batch = resolve_settings(fp(BATCH), limits, TENANTS["batch"])
    assert (batch.default_priority, batch.max_priority) == ("normal", "normal")
    default = resolve_settings(fp(DEFAULT), limits)
    assert (default.default_priority, default.max_priority) == ("normal", "high")  # Req 7.2


# --------------------------------------------------------------------------- the body


async def test_a_body_priority_within_the_limit_is_stored_only_when_high(
    gw: httpx.AsyncClient, state_store: StateStore
) -> None:
    high = await submit(gw, PLAIN, priority="high")
    normal = await submit(gw, PLAIN, priority="normal")
    assert (high.status_code, normal.status_code) == (202, 202)
    assert await stored(state_store, high) == "high"
    assert await stored(state_store, normal) is None  # normal is the absence of the field (Req 7.3)


async def test_an_absent_priority_is_the_tenants_default(gw: httpx.AsyncClient, state_store: StateStore) -> None:
    assert await stored(state_store, await submit(gw, LIVE)) == "high"
    assert await stored(state_store, await submit(gw, PLAIN)) is None
    assert await stored(state_store, await submit(gw, BATCH)) is None
    assert await stored(state_store, await submit(gw, LIVE, priority="normal")) is None  # lower is always allowed


async def test_the_default_tenant_may_submit_high(gw: httpx.AsyncClient, state_store: StateStore) -> None:
    assert await stored(state_store, await submit(gw, DEFAULT, priority="high")) == "high"


@pytest.mark.parametrize("value", ["urgent", "HIGH", "", 1, True, ["high"], {"a": 1}])
async def test_an_invalid_priority_is_a_400_naming_it_and_creates_nothing(
    gw: httpx.AsyncClient, state_store: StateStore, value: Any
) -> None:
    reply = await submit(gw, PLAIN, priority=value)
    assert (reply.status_code, reply.json()) == (400, {"error": "invalid_request", "parameter": "priority"})
    assert await state_store.client.keys("das:request:*") == []


async def test_an_explicit_null_priority_is_the_default(gw: httpx.AsyncClient, state_store: StateStore) -> None:
    assert await stored(state_store, await submit(gw, LIVE, priority=None)) == "high"


async def test_a_priority_above_the_tenants_limit_is_refused_with_the_limit_and_creates_nothing(
    gw: httpx.AsyncClient, state_store: StateStore
) -> None:
    reply = await submit(gw, BATCH, priority="high")
    assert (reply.status_code, reply.json()) == (400, {"error": "priority_above_limit", "limit": "normal"})
    assert await state_store.client.keys("das:request:*") == []
    assert await state_store.client.keys("das:tenant:batch:*") == []  # no index, no usage: nothing was written


async def test_an_invalid_value_is_reported_as_such_and_a_bad_prompt_before_either(
    gw: httpx.AsyncClient,
) -> None:
    bad_prompt = await gw.post("/v1/requests", json={"prompt": "", "priority": "high"}, headers=h(BATCH))
    assert bad_prompt.status_code == 400 and bad_prompt.json()["error"] != "priority_above_limit"
    assert (await submit(gw, BATCH, priority="nope")).json()["error"] == "invalid_request"


async def test_a_refused_priority_does_not_spend_an_idempotency_key(gw: httpx.AsyncClient) -> None:
    headers = {**h(BATCH), "Idempotency-Key": "key-1"}
    refused = await gw.post("/v1/requests", json={"prompt": "p", "priority": "high"}, headers=headers)
    assert refused.status_code == 400
    ok = await gw.post("/v1/requests", json={"prompt": "p"}, headers=headers)
    assert ok.status_code == 202  # the key was never recorded by the refused call


# --------------------------------------------------------------------------- the view


async def test_the_request_view_shows_the_priority(gw: httpx.AsyncClient) -> None:
    high = (await submit(gw, PLAIN, priority="high")).json()["request_id"]
    normal = (await submit(gw, PLAIN)).json()["request_id"]
    assert (await gw.get(f"/v1/requests/{high}", headers=h(PLAIN))).json()["priority"] == "high"
    assert (await gw.get(f"/v1/requests/{normal}", headers=h(PLAIN))).json()["priority"] == "normal"


async def test_a_request_written_before_the_upgrade_is_normal_in_the_view(
    gw: httpx.AsyncClient, state_store: StateStore
) -> None:
    rid = await state_store.create_request("old")
    assert (await gw.get(f"/v1/requests/{rid}", headers=h(DEFAULT))).json()["priority"] == "normal"
