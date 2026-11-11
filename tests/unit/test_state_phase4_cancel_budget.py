"""``StateStore.cancel_request`` with a budget: priced Budget_Used, and corrupt stored data never
blocks a cancel (das-client-api task 7.3; Phase 4 Req 5.9, 6.1; Phase 1 Req 1.6)."""

from __future__ import annotations

import io
import json
from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import pytest

from das.client.budget import Budget
from das.models import RequestStatus
from das.observability.logging import configure_logging
from das.ops.cost import PriceEntry, PriceTable
from das.state import StateStore, keys
from tests.unit.test_state_phase4_events import PLAN, events, leased, running, submitted, usage_rec

#: 1 per token (the largest price a PriceEntry allows): usage_rec(3, 4) costs 7.
PRICES = PriceTable(
    currency="USD",
    models=[
        PriceEntry(
            provider="openai",
            model="gpt",
            input_per_million=Decimal("1000000"),
            output_per_million=Decimal("1000000"),
        )
    ],
)


@pytest.fixture
def stream() -> Iterator[io.StringIO]:
    out = io.StringIO()
    configure_logging("store-test", stream=out)
    yield out
    configure_logging("test", stream=io.StringIO())


def log_entries(stream: io.StringIO) -> list[dict[str, Any]]:
    return [json.loads(line) for line in stream.getvalue().splitlines() if line]


async def cancelled_event(store: StateStore, rid: str) -> dict[str, Any]:
    *_, (_, event_type, data) = await events(store, rid)
    assert event_type == "request.cancelled"
    return data


async def budgeted_with_usage(store: StateStore, budget: Budget) -> str:
    rid = await submitted(store, budget=budget)
    lease = await leased(store, rid)
    assert (await store.commit_plan(lease, PLAN, usage=[usage_rec(3, 4)])).ok
    return rid


# --------------------------------------------------------------------------- priced Budget_Used


async def test_cancel_with_prices_reports_cost_against_a_max_cost_budget(state_store: StateStore):
    rid = await budgeted_with_usage(state_store, Budget(max_cost=Decimal("100")))
    assert (await state_store.cancel_request(rid, prices=PRICES)).ok
    budget = (await cancelled_event(state_store, rid))["budget"]
    assert budget["limits"] == {"max_cost": "100"}
    assert budget["used"]["total_tokens"] == 7
    assert Decimal(budget["used"]["cost"]) == Decimal("7")
    assert budget["exceeded"] is False


async def test_cancel_without_prices_marks_a_max_cost_budget_unenforceable(state_store: StateStore):
    rid = await budgeted_with_usage(state_store, Budget(max_cost=Decimal("100")))
    assert (await state_store.cancel_request(rid)).ok
    budget = (await cancelled_event(state_store, rid))["budget"]
    assert budget["used"]["cost"] is None and budget["exceeded"] is True


# --------------------------------------------------------------------------- corrupt data


@pytest.mark.parametrize("stored", ["{not json", '{"bogus": 1}', '{"max_total_tokens": 0}', "[]"])
async def test_corrupt_stored_budget_does_not_block_cancel(state_store: StateStore, stored: str):
    rid, _ = await running(state_store, budget=Budget(max_total_tokens=5))
    await state_store.client.hset(keys.request(rid), keys.F_BUDGET, stored)
    assert (await state_store.cancel_request(rid, prices=PRICES)).ok
    assert await state_store.get_status(rid) is RequestStatus.CANCELLED
    assert "budget" not in await cancelled_event(state_store, rid)


async def test_corrupt_usage_record_does_not_block_cancel(state_store: StateStore):
    rid, _ = await running(state_store, budget=Budget(max_total_tokens=5))
    await state_store.client.rpush(keys.usage(rid), "not json")
    assert (await state_store.cancel_request(rid)).ok
    assert await state_store.get_status(rid) is RequestStatus.CANCELLED
    assert "budget" not in await cancelled_event(state_store, rid)


async def test_corrupt_budget_logs_one_warning_without_the_stored_value(
    state_store: StateStore, stream: io.StringIO
):
    rid, _ = await running(state_store, budget=Budget(max_total_tokens=5))
    await state_store.client.hset(keys.request(rid), keys.F_BUDGET, "SECRET-GARBAGE-{")
    assert (await state_store.cancel_request(rid)).ok
    warnings = [e for e in log_entries(stream) if e.get("level") == "warning"]
    assert len(warnings) == 1
    assert "budget" in warnings[0]["message"]
    assert "SECRET-GARBAGE" not in stream.getvalue()


async def test_valid_budget_still_logs_nothing(state_store: StateStore, stream: io.StringIO):
    rid, _ = await running(state_store, budget=Budget(max_total_tokens=5))
    assert (await state_store.cancel_request(rid)).ok
    assert [e for e in log_entries(stream) if e.get("level") == "warning"] == []
    assert "budget" in await cancelled_event(state_store, rid)
