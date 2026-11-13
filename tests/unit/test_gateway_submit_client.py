"""``POST /v1/requests`` with Idempotency-Key, quota and budget (Phase 4 Req 3.2, 4.1-4.5, 5.1, 5.2, 5.4)."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

import httpx
import pytest

from das.config.schema import BudgetConfig, ClientLimitsConfig, ClientSettingsConfig, EventsConfig
from das.gateway.app import create_app
from das.registry import AgentRegistry
from das.state import StateStore, keys
from tests.unit.test_gateway_limits import Recorders, fp

KEY = "client-key-0001"
OTHER_KEY = "client-key-0002"
AUTH = {"X-API-Key": KEY}
OTHER_AUTH = {"X-API-Key": OTHER_KEY}
IDEM = "Idempotency-Key"


def settings(**fields: Any) -> ClientLimitsConfig:
    base: dict[str, Any] = {"rate": 0.0, "max_active_requests": 0}
    return ClientLimitsConfig(default=ClientSettingsConfig(**{**base, **fields}))


async def make_client(
    state_store: StateStore,
    fake_redis,
    recorders: Recorders,
    client_limits: ClientLimitsConfig | None = None,
    events: EventsConfig | None = None,
) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(
        state_store,
        AgentRegistry(fake_redis),
        None,
        [KEY, OTHER_KEY],
        50,
        client_limits=client_limits or settings(),
        events=events,
        metrics_factory=recorders,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw") as c:
        yield c


@pytest.fixture
def recorders() -> Recorders:
    return Recorders()


@pytest.fixture
async def client(state_store, fake_redis, fake_redis_time, recorders):
    async for c in make_client(state_store, fake_redis, recorders):
        yield c


async def submit(c: httpx.AsyncClient, prompt: str = "hello", headers: dict | None = None, **body: Any):
    return await c.post("/v1/requests", json={"prompt": prompt, **body}, headers={**AUTH, **(headers or {})})


async def active(fake_redis, key: str = KEY) -> set[str]:
    return {m.decode() if isinstance(m, bytes) else m for m in await fake_redis.smembers(keys.client_active(fp(key)))}


# --------------------------------------------------------------------------- created


async def test_submit_creates_a_request_owned_by_the_key(client, fake_redis, recorders):
    r = await submit(client)
    assert r.status_code == 202
    rid = r.json()["request_id"]
    assert await active(fake_redis) == {rid}  # Req 3.1
    stored = await fake_redis.hget(keys.request(rid), keys.F_CLIENT_FP)
    assert (stored.decode() if isinstance(stored, bytes) else stored) == fp(KEY)
    assert recorders.value("das_requests_submitted_total", tenant="default") == 1


# --------------------------------------------------------------------------- idempotency


async def test_replay_returns_the_original_request_without_creating_another(client, fake_redis, recorders):
    first = await submit(client, headers={IDEM: "k-1"})
    again = await submit(client, headers={IDEM: "k-1"})
    assert first.status_code == 202
    assert again.status_code == 200
    assert again.json() == first.json()
    assert again.headers["Idempotent-Replayed"] == "true"
    assert "Idempotent-Replayed" not in first.headers
    assert len(await active(fake_redis)) == 1
    assert recorders.value("das_requests_submitted_total", tenant="default") == 1  # Req 4.3: a replay is not a submit
    assert recorders.value("das_idempotent_replays_total") == 1


async def test_same_key_with_another_prompt_is_422(client, fake_redis, recorders):
    await submit(client, "one", headers={IDEM: "k-1"})
    r = await submit(client, "two", headers={IDEM: "k-1"})
    assert (r.status_code, r.json()) == (422, {"error": "idempotency_key_reused"})
    assert len(await active(fake_redis)) == 1
    assert recorders.value("das_client_rejections_total", reason="idempotency_key_reused") == 1


async def test_same_key_with_another_budget_is_422(client):
    await submit(client, headers={IDEM: "k-1"}, budget={"max_total_tokens": 10})
    r = await submit(client, headers={IDEM: "k-1"}, budget={"max_total_tokens": 11})
    assert (r.status_code, r.json()) == (422, {"error": "idempotency_key_reused"})


async def test_equal_budgets_in_other_notation_replay(client):
    """Body_Hash uses canonical decimal text, so ``1.50`` and ``1.5`` are one body (Req 4.3)."""
    first = await submit(client, headers={IDEM: "k-1"}, budget={"max_cost": "1.50"})
    again = await submit(client, headers={IDEM: "k-1"}, budget={"max_cost": 1.5})
    assert again.status_code == 200 and again.json() == first.json()


async def test_keys_are_scoped_per_client(client, fake_redis):
    a = await submit(client, headers={IDEM: "shared"})
    b = await client.post(
        "/v1/requests", json={"prompt": "hello"}, headers={**OTHER_AUTH, IDEM: "shared"}
    )
    assert (a.status_code, b.status_code) == (202, 202)
    assert a.json() != b.json()  # Req 4.7


async def test_without_a_key_every_submit_creates_a_request(client, fake_redis):
    a, b = await submit(client), await submit(client)
    assert (a.status_code, b.status_code) == (202, 202)
    assert len(await active(fake_redis)) == 2


@pytest.mark.parametrize("value", ["", "x" * 129, "has space", "tab\there", "\x7f", "é"])
async def test_invalid_idempotency_key_is_400(client, fake_redis, value):
    r = await submit(client, headers={IDEM: value.encode("latin-1")})
    assert (r.status_code, r.json()) == (400, {"error": "invalid_request", "parameter": "Idempotency-Key"})
    assert await active(fake_redis) == set()


async def test_key_of_128_printable_characters_is_accepted(client):
    r = await submit(client, headers={IDEM: "!~" * 64})
    assert r.status_code == 202


# --------------------------------------------------------------------------- quota


async def test_quota_exceeded_is_429_with_the_limit(state_store, fake_redis, fake_redis_time, recorders):
    async for c in make_client(state_store, fake_redis, recorders, settings(max_active_requests=2)):
        assert (await submit(c)).status_code == 202
        assert (await submit(c)).status_code == 202
        r = await submit(c)
        assert (r.status_code, r.json()) == (429, {"error": "quota_exceeded", "limit": 2})
        assert len(await active(fake_redis)) == 2
        assert recorders.value("das_client_rejections_total", reason="quota_exceeded") == 1


async def test_replay_is_answered_when_the_quota_is_full(state_store, fake_redis, fake_redis_time, recorders):
    """Req 4.5: the idempotency record is evaluated before the quota."""
    async for c in make_client(state_store, fake_redis, recorders, settings(max_active_requests=1)):
        first = await submit(c, headers={IDEM: "k-1"})
        assert first.status_code == 202
        assert (await submit(c, "other")).status_code == 429
        again = await submit(c, headers={IDEM: "k-1"})
        assert (again.status_code, again.json()) == (200, first.json())


# --------------------------------------------------------------------------- budget


async def stored_budget(fake_redis, rid: str) -> dict[str, Any] | None:
    raw = await fake_redis.hget(keys.request(rid), keys.F_BUDGET)
    text = raw.decode() if isinstance(raw, bytes) else raw
    return json.loads(text) if text else None


async def test_effective_budget_is_stored_per_field(state_store, fake_redis, fake_redis_time, recorders):
    default = BudgetConfig(max_total_tokens=1000, max_cost="2.5")
    async for c in make_client(state_store, fake_redis, recorders, settings(default_budget=default)):
        own = (await submit(c, budget={"max_total_tokens": 500})).json()["request_id"]
        inherited = (await submit(c)).json()["request_id"]
        empty = (await submit(c, budget={})).json()["request_id"]
    assert await stored_budget(fake_redis, own) == {"max_total_tokens": 500, "max_cost": "2.5"}
    assert await stored_budget(fake_redis, inherited) == {"max_total_tokens": 1000, "max_cost": "2.5"}
    assert await stored_budget(fake_redis, empty) == {"max_total_tokens": 1000, "max_cost": "2.5"}


async def test_no_budget_anywhere_stores_none(client, fake_redis):
    for body in ({}, {"budget": None}, {"budget": {}}):
        rid = (await submit(client, **body)).json()["request_id"]
        assert await stored_budget(fake_redis, rid) is None  # an empty Budget is no Budget


@pytest.mark.parametrize(
    ("budget", "parameter", "limit"),
    [({"max_total_tokens": 1001}, "budget.max_total_tokens", 1000), ({"max_cost": "2.51"}, "budget.max_cost", "2.5")],
)
async def test_budget_above_the_key_default_is_400(
    state_store, fake_redis, fake_redis_time, recorders, budget, parameter, limit
):
    default = BudgetConfig(max_total_tokens=1000, max_cost="2.5")
    async for c in make_client(state_store, fake_redis, recorders, settings(default_budget=default)):
        r = await submit(c, budget=budget)
        assert r.status_code == 400
        assert r.json() == {"error": "budget_above_limit", "parameter": parameter, "limit": limit}
        assert await active(fake_redis) == set()
        assert recorders.value("das_client_rejections_total", reason="budget_above_limit") == 1


@pytest.mark.parametrize(
    ("budget", "parameter"),
    [
        ({"max_total_tokens": 0}, "budget.max_total_tokens"),
        ({"max_total_tokens": -5}, "budget.max_total_tokens"),
        ({"max_total_tokens": True}, "budget.max_total_tokens"),
        ({"max_total_tokens": "5"}, "budget.max_total_tokens"),
        ({"max_total_tokens": 5.5}, "budget.max_total_tokens"),
        ({"max_cost": 0}, "budget.max_cost"),
        ({"max_cost": -1}, "budget.max_cost"),
        ({"max_cost": "abc"}, "budget.max_cost"),
        ({"max_cost": True}, "budget.max_cost"),
        ({"max_cost": "NaN"}, "budget.max_cost"),
        ({"max_cost": "0.0000001"}, "budget.max_cost"),
        ({"max_cost": "1" + "0" * 20}, "budget.max_cost"),
        ({"unknown": 1}, "budget.unknown"),
        (5, "budget"),
        ([], "budget"),
        ("x", "budget"),
    ],
)
async def test_invalid_budget_is_400_naming_the_parameter(client, fake_redis, budget, parameter):
    r = await submit(client, budget=budget)
    assert (r.status_code, r.json()) == (400, {"error": "invalid_request", "parameter": parameter})
    assert await active(fake_redis) == set()


async def test_prompt_validation_still_comes_first(client):
    r = await submit(client, "   ", headers={IDEM: ""}, budget={"max_cost": 0})
    assert (r.status_code, r.json()) == (400, {"error": "empty_prompt"})


async def test_missing_prompt_is_still_invalid_body(client):
    r = await client.post("/v1/requests", json={"text": "x"}, headers=AUTH)
    assert (r.status_code, r.json()) == (400, {"error": "invalid_body"})


# --------------------------------------------------------------------------- stored client fields


async def test_webhook_target_and_events_length_are_stored(state_store, fake_redis, fake_redis_time, recorders):
    limits = settings(webhook_url="https://hooks.example.com/das", webhook_secret_name="main")
    async for c in make_client(state_store, fake_redis, recorders, limits, EventsConfig(max_len=250)):
        rid = (await submit(c)).json()["request_id"]
    stored = {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in (await fake_redis.hgetall(keys.request(rid))).items()
    }
    assert stored[keys.F_WEBHOOK_URL] == "https://hooks.example.com/das"
    assert stored[keys.F_WEBHOOK_SECRET_NAME] == "main"
    assert stored[keys.F_EVENTS_MAX_LEN] == "250"
