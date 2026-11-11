"""``GET /v1/requests/{id}`` budget and webhook fields, and cancel with the Gateway's Price_Table
(das-client-api task 7.3; Phase 4 Req 5.9, 8.9, 11.1)."""

from __future__ import annotations

from decimal import Decimal

import httpx
import pytest

from das.client.budget import Budget
from das.config.schema import ClientLimitsConfig, ClientSettingsConfig
from das.gateway.app import create_app
from das.ops.cost import PriceTable
from das.registry import AgentRegistry
from das.state import StateStore
from tests.unit.test_state_phase4_cancel_budget import PRICES, cancelled_event
from tests.unit.test_state_phase4_events import PLAN, leased, running, submitted, usage_rec

KEY = "client-key-0001"
AUTH = {"X-API-Key": KEY}
NO_RATE_LIMIT = ClientLimitsConfig(default=ClientSettingsConfig(rate=0.0))


def client_for(
    state_store: StateStore, fake_redis, price_table: PriceTable | None = None
) -> httpx.AsyncClient:
    app = create_app(
        state_store,
        AgentRegistry(fake_redis),
        None,
        [KEY],
        50,
        client_limits=NO_RATE_LIMIT,
        price_table=price_table,
    )
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw")


async def budgeted(store: StateStore, budget: Budget) -> str:
    rid = await submitted(store, budget=budget)
    lease = await leased(store, rid)
    assert (await store.commit_plan(lease, PLAN, usage=[usage_rec(3, 4)])).ok
    return rid


async def view(c: httpx.AsyncClient, rid: str) -> dict:
    r = await c.get(f"/v1/requests/{rid}", headers=AUTH)
    assert r.status_code == 200
    return r.json()


# --------------------------------------------------------------------------- always present


async def test_budget_and_webhook_are_null_for_a_plain_request(state_store, fake_redis):
    """Req 11.1: the only change for a Phase 3 client is the two added fields."""
    rid = await state_store.create_request("hello")
    async with client_for(state_store, fake_redis) as c:
        body = await view(c, rid)
    assert body["budget"] is None and body["webhook"] is None
    assert {"request_id", "status", "jobs", "transitions", "token_usage", "estimated_cost"} <= set(body)


# --------------------------------------------------------------------------- budget


async def test_budget_view_reports_limits_used_and_exceeded(state_store, fake_redis):
    rid = await budgeted(state_store, Budget(max_total_tokens=10))
    async with client_for(state_store, fake_redis) as c:
        body = await view(c, rid)
    assert body["budget"] == {
        "limits": {"max_total_tokens": 10},
        "used": {"total_tokens": 7, "cost": None},
        "exceeded": False,
    }


async def test_budget_view_is_exceeded_once_usage_reaches_the_limit(state_store, fake_redis):
    rid = await budgeted(state_store, Budget(max_total_tokens=7))
    async with client_for(state_store, fake_redis) as c:
        assert (await view(c, rid))["budget"]["exceeded"] is True


async def test_budget_cost_uses_the_gateway_price_table(state_store, fake_redis):
    rid = await budgeted(state_store, Budget(max_cost=Decimal("100")))
    async with client_for(state_store, fake_redis, PRICES) as c:
        budget = (await view(c, rid))["budget"]
    assert budget["limits"] == {"max_cost": "100"}
    assert Decimal(budget["used"]["cost"]) == Decimal("7")
    assert budget["exceeded"] is False


async def test_max_cost_without_a_price_table_is_unenforceable(state_store, fake_redis):
    rid = await budgeted(state_store, Budget(max_cost=Decimal("100")))
    async with client_for(state_store, fake_redis) as c:
        budget = (await view(c, rid))["budget"]
    assert budget["used"]["cost"] is None and budget["exceeded"] is True


# --------------------------------------------------------------------------- webhook


async def test_webhook_is_null_until_a_delivery_exists_then_reports_its_state(state_store, fake_redis):
    rid = await submitted(state_store, webhook=True)
    async with client_for(state_store, fake_redis) as c:
        assert (await view(c, rid))["webhook"] is None
        cancel = await c.post(f"/v1/requests/{rid}/cancel", headers=AUTH)
        assert cancel.status_code == 202
        assert (await view(c, rid))["webhook"] == {"state": "pending", "attempts": 0, "last_status": None}


# --------------------------------------------------------------------------- cancel


async def test_cancel_reports_priced_budget_used_in_the_terminal_event(state_store, fake_redis):
    rid = await budgeted(state_store, Budget(max_cost=Decimal("100")))
    async with client_for(state_store, fake_redis, PRICES) as c:
        assert (await c.post(f"/v1/requests/{rid}/cancel", headers=AUTH)).status_code == 202
    budget = (await cancelled_event(state_store, rid))["budget"]
    assert Decimal(budget["used"]["cost"]) == Decimal("7")  # the Gateway's Price_Table reached the store


@pytest.mark.parametrize("with_budget", [True, False])
async def test_cancel_still_works_for_requests_with_and_without_a_budget(
    state_store, fake_redis, with_budget
):
    rid, _ = await running(state_store, budget=Budget(max_total_tokens=5) if with_budget else None)
    async with client_for(state_store, fake_redis) as c:
        r = await c.post(f"/v1/requests/{rid}/cancel", headers=AUTH)
    assert (r.status_code, r.json()) == (202, {"status": "Cancelled"})
