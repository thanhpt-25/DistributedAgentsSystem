"""Budget resolution, status and view (Phase 4 Req 5)."""
from __future__ import annotations

from decimal import Decimal

import pytest

from das.client.budget import (
    Budget,
    BudgetError,
    BudgetStatus,
    budget_status,
    budget_view,
    resolve_budget,
)
from das.models.results import TokenUsage
from das.models.usage import UsageRecord
from das.ops.cost import PriceEntry, PriceTable

PRICES = PriceTable(
    currency="USD",
    models=[
        PriceEntry(
            provider="openai",
            model="m",
            input_per_million=Decimal("1"),
            output_per_million=Decimal("2"),
        )
    ],
)


def record(model: str = "m", input_tokens: int = 10, output_tokens: int = 5) -> UsageRecord:
    """One Job call; under PRICES model "m" costs 10*1/1e6 + 5*2/1e6 = 0.00002."""
    return UsageRecord(
        operation="job",
        provider="openai",
        model=model,
        usage=TokenUsage(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=input_tokens + output_tokens,
        ),
        job_id="j1",
        attempt=1,
    )


# ---- Budget


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_total_tokens": 0},
        {"max_total_tokens": -1},
        {"max_total_tokens": True},
        {"max_cost": Decimal("0")},
        {"max_cost": Decimal("-1")},
        {"max_cost": Decimal("NaN")},
        {"max_cost": Decimal("Infinity")},
    ],
)
def test_budget_rejects_out_of_range_values(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        Budget(**kwargs)  # type: ignore[arg-type]


def test_budget_json_round_trip_uses_canonical_decimal_text() -> None:
    b = Budget(max_total_tokens=5, max_cost=Decimal("1.50"))
    assert b.to_json_obj() == {"max_total_tokens": 5, "max_cost": "1.5"}
    assert Budget(max_cost=Decimal("100")).to_json_obj() == {"max_cost": "100"}
    assert Budget.from_json_obj(b.to_json_obj()) == b
    assert Budget().to_json_obj() == {}
    assert Budget().is_empty()


def test_budget_from_json_rejects_unknown_fields() -> None:
    with pytest.raises(ValueError):
        Budget.from_json_obj({"max_tokens": 5})


# ---- resolve_budget


def test_resolve_without_any_budget_is_none() -> None:
    assert resolve_budget(None, None) is None
    assert resolve_budget(Budget(), Budget()) is None


def test_resolve_takes_client_value_when_given_else_default() -> None:
    assert resolve_budget(Budget(max_total_tokens=100), None) == Budget(max_total_tokens=100)
    assert resolve_budget(None, Budget(max_total_tokens=50)) == Budget(max_total_tokens=50)
    assert resolve_budget(Budget(max_total_tokens=40), Budget(max_total_tokens=50)) == Budget(max_total_tokens=40)
    assert resolve_budget(Budget(max_cost=Decimal("1")), Budget(max_total_tokens=50)) == Budget(
        max_total_tokens=50, max_cost=Decimal("1")
    )


def test_resolve_rejects_client_value_above_default() -> None:
    with pytest.raises(BudgetError) as tokens:
        resolve_budget(Budget(max_total_tokens=60), Budget(max_total_tokens=50))
    assert tokens.value.parameter == "budget.max_total_tokens"
    assert tokens.value.limit == 50

    with pytest.raises(BudgetError) as cost:
        resolve_budget(Budget(max_cost=Decimal("2")), Budget(max_cost=Decimal("1.5")))
    assert cost.value.parameter == "budget.max_cost"
    assert cost.value.limit == Decimal("1.5")


# ---- budget_status


def test_tokens_below_limit_is_not_exceeded() -> None:
    s = budget_status([record()], Budget(max_total_tokens=16), PRICES)
    assert s == BudgetStatus(used_tokens=15, used_cost=None, exceeded=None)


def test_tokens_reaching_limit_is_exceeded() -> None:
    assert budget_status([record()], Budget(max_total_tokens=15), None).exceeded == "tokens"


def test_no_usage_is_not_exceeded() -> None:
    assert budget_status([], Budget(max_total_tokens=1), None) == BudgetStatus(0, None, None)


def test_cost_reaching_limit_is_exceeded() -> None:
    s = budget_status([record()], Budget(max_cost=Decimal("0.00002")), PRICES)
    assert s.used_cost == Decimal("0.00002")
    assert s.exceeded == "cost"


def test_cost_below_limit_reports_used_cost() -> None:
    s = budget_status([record()], Budget(max_cost=Decimal("0.001")), PRICES)
    assert s == BudgetStatus(used_tokens=15, used_cost=Decimal("0.00002"), exceeded=None)


def test_max_cost_without_price_table_is_unenforceable() -> None:
    s = budget_status([], Budget(max_cost=Decimal("1")), None)
    assert s == BudgetStatus(used_tokens=0, used_cost=None, exceeded="unenforceable")


def test_unpriced_model_is_unenforceable() -> None:
    s = budget_status([record(model="unknown")], Budget(max_cost=Decimal("1")), PRICES)
    assert s.exceeded == "unenforceable"
    assert s.used_cost is None


def test_unenforceable_wins_over_tokens_and_tokens_over_cost() -> None:
    both = Budget(max_total_tokens=1, max_cost=Decimal("1"))
    assert budget_status([record(model="unknown")], both, PRICES).exceeded == "unenforceable"
    tight = Budget(max_total_tokens=1, max_cost=Decimal("0.000001"))
    assert budget_status([record()], tight, PRICES).exceeded == "tokens"


def test_tokens_only_budget_ignores_missing_prices() -> None:
    s = budget_status([record(model="unknown")], Budget(max_total_tokens=100), None)
    assert s == BudgetStatus(used_tokens=15, used_cost=None, exceeded=None)


# ---- budget_view


def test_budget_view_shape() -> None:
    b = Budget(max_total_tokens=10, max_cost=Decimal("0.001"))
    view = budget_view(b, budget_status([record()], b, PRICES))
    assert view == {
        "limits": {"max_total_tokens": 10, "max_cost": "0.001"},
        "used": {"total_tokens": 15, "cost": "0.000020"},
        "exceeded": True,
    }


def test_budget_view_without_cost() -> None:
    b = Budget(max_total_tokens=100)
    assert budget_view(b, budget_status([record()], b, None)) == {
        "limits": {"max_total_tokens": 100},
        "used": {"total_tokens": 15, "cost": None},
        "exceeded": False,
    }


@pytest.mark.parametrize(
    "obj",
    [
        {"max_cost": "abc"},
        {"max_cost": 1.5},
        {"max_cost": "NaN"},
        {"max_cost": "0"},
        {"max_total_tokens": "5"},
        {"max_total_tokens": 1.5},
        {"max_total_tokens": True},
    ],
)
def test_budget_from_json_rejects_invalid_values(obj: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        Budget.from_json_obj(obj)
