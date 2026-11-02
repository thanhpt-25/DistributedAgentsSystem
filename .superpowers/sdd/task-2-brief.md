### Task 2: Budget resolution and status (spec tasks 1.2, 1.3)

**Files:**
- Create: `das/client/budget.py`
- Test: `tests/unit/test_client_budget.py`
- Test: `tests/property/test_budget_resolution.py`

**Interfaces:**
- Consumes: `das.models.usage.UsageRecord`; `das.ops.cost.PriceTable`, `unrounded_costs(records, table) -> (Decimal, dict, set)`, `format_amount(Decimal) -> str`.
- Produces:
  - `BUDGET_FIELDS: tuple[str, ...] = ("max_total_tokens", "max_cost")`
  - `ExceededKind = Literal["tokens", "cost", "unenforceable"]`
  - `decimal_text(value: Decimal) -> str`
  - `Budget(max_total_tokens: int | None = None, max_cost: Decimal | None = None)` with `is_empty() -> bool`, `to_json_obj() -> dict[str, Any]`, `Budget.from_json_obj(obj: Mapping[str, Any]) -> Budget`
  - `BudgetError(ValueError)` with attributes `parameter: str` (e.g. `"budget.max_cost"`) and `limit: int | Decimal`
  - `resolve_budget(client: Budget | None, default: Budget | None) -> Budget | None`
  - `BudgetStatus(used_tokens: int, used_cost: Decimal | None, exceeded: ExceededKind | None)`
  - `budget_status(usage: Sequence[UsageRecord], budget: Budget, prices: PriceTable | None) -> BudgetStatus`
  - `budget_view(budget: Budget, status: BudgetStatus) -> dict[str, Any]`

- [ ] **Step 1: Write the failing unit tests**

Create `tests/unit/test_client_budget.py`:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_client_budget.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'das.client.budget'`

- [ ] **Step 3: Write the implementation**

Create `das/client/budget.py`:

```python
"""Per-Request budgets (Phase 4 Req 5). Pure.

``resolve_budget`` merges the client's Budget with the key's default and never
raises a limit (Property 4). ``budget_status`` measures Budget_Used against a
Budget; the Manager's reducer stops dispatching once it reports ``exceeded``.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal

from das.models.usage import UsageRecord
from das.ops.cost import PriceTable, format_amount, unrounded_costs

BUDGET_FIELDS: tuple[str, ...] = ("max_total_tokens", "max_cost")

ExceededKind = Literal["tokens", "cost", "unenforceable"]


def decimal_text(value: Decimal) -> str:
    """Canonical text of a Decimal: no exponent, no trailing zeros (``1.50`` -> ``1.5``)."""
    return format(value.normalize(), "f")


@dataclass(frozen=True, slots=True)
class Budget:
    """Optional per-Request limits (Req 5.1). ``max_cost`` is in the Price_Table currency."""

    max_total_tokens: int | None = None
    max_cost: Decimal | None = None

    def __post_init__(self) -> None:
        tokens = self.max_total_tokens
        if tokens is not None and (isinstance(tokens, bool) or not isinstance(tokens, int) or tokens < 1):
            raise ValueError("max_total_tokens must be an int >= 1")
        cost = self.max_cost
        if cost is not None and (not isinstance(cost, Decimal) or not cost.is_finite() or cost <= 0):
            raise ValueError("max_cost must be a finite Decimal > 0")

    def is_empty(self) -> bool:
        return self.max_total_tokens is None and self.max_cost is None

    def to_json_obj(self) -> dict[str, Any]:
        """JSON object with only the set fields; ``max_cost`` as canonical decimal text."""
        obj: dict[str, Any] = {}
        if self.max_total_tokens is not None:
            obj["max_total_tokens"] = self.max_total_tokens
        if self.max_cost is not None:
            obj["max_cost"] = decimal_text(self.max_cost)
        return obj

    @classmethod
    def from_json_obj(cls, obj: Mapping[str, Any]) -> Budget:
        """Inverse of :meth:`to_json_obj` (the form stored on the Request record)."""
        unknown = set(obj) - set(BUDGET_FIELDS)
        if unknown:
            raise ValueError(f"unknown budget fields: {sorted(unknown)}")
        cost = obj.get("max_cost")
        return cls(
            max_total_tokens=obj.get("max_total_tokens"),
            max_cost=Decimal(cost) if cost is not None else None,
        )


class BudgetError(ValueError):
    """A client Budget field is above the key's default (Req 5.2)."""

    def __init__(self, parameter: str, limit: int | Decimal) -> None:
        super().__init__(f"{parameter} is above the limit {limit}")
        self.parameter = parameter
        self.limit = limit


def resolve_budget(client: Budget | None, default: Budget | None) -> Budget | None:
    """Effective_Budget: per field the client value, else the default (Req 5.2, 5.3).

    Raises :class:`BudgetError` for the first field, in ``BUDGET_FIELDS`` order, whose
    client value is above the default. Returns ``None`` when no field is set.
    """
    values: dict[str, Any] = {}
    for name in BUDGET_FIELDS:
        mine = getattr(client, name) if client is not None else None
        limit = getattr(default, name) if default is not None else None
        if mine is not None and limit is not None and mine > limit:
            raise BudgetError(f"budget.{name}", limit)
        values[name] = mine if mine is not None else limit
    budget = Budget(**values)
    return None if budget.is_empty() else budget


@dataclass(frozen=True, slots=True)
class BudgetStatus:
    used_tokens: int
    #: Unrounded Estimated_Cost; ``None`` without ``max_cost`` or when it cannot be priced.
    used_cost: Decimal | None
    exceeded: ExceededKind | None


def budget_status(usage: Sequence[UsageRecord], budget: Budget, prices: PriceTable | None) -> BudgetStatus:
    """Budget_Used against ``budget`` (Req 5.5-5.7).

    A limit is exceeded when usage reaches it. ``unenforceable`` (``max_cost`` without a
    Price_Table, or a record whose provider/model is not priced) wins over ``tokens``,
    which wins over ``cost``.
    """
    used_tokens = sum(r.usage.total_tokens for r in usage)
    used_cost: Decimal | None = None
    unenforceable = False
    if budget.max_cost is not None:
        if prices is None:
            unenforceable = True
        else:
            total, _, unpriced = unrounded_costs(usage, prices)
            if unpriced:
                unenforceable = True
            else:
                used_cost = total
    exceeded: ExceededKind | None = None
    if unenforceable:
        exceeded = "unenforceable"
    elif budget.max_total_tokens is not None and used_tokens >= budget.max_total_tokens:
        exceeded = "tokens"
    elif budget.max_cost is not None and used_cost is not None and used_cost >= budget.max_cost:
        exceeded = "cost"
    return BudgetStatus(used_tokens=used_tokens, used_cost=used_cost, exceeded=exceeded)


def budget_view(budget: Budget, status: BudgetStatus) -> dict[str, Any]:
    """``budget`` object of the Request view and the Terminal_Event (Req 5.9)."""
    return {
        "limits": budget.to_json_obj(),
        "used": {
            "total_tokens": status.used_tokens,
            "cost": format_amount(status.used_cost) if status.used_cost is not None else None,
        },
        "exceeded": status.exceeded is not None,
    }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_client_budget.py -v`
Expected: PASS (all tests)

- [ ] **Step 5: Write the property test (Property 4)**

Create `tests/property/test_budget_resolution.py`:

```python
# Feature: das-client-api, Property 4: Budget resolution never raises a limit
"""Property 4: Budget resolution never raises a limit.

For any client Budget and default Budget, ``resolve_budget`` either raises
``BudgetError`` for a field where the client value exceeds the default, or returns
a Budget whose every field is <= the default's when the default's is set, and
equals the client value when given.

**Validates: Requirements 5.2, 5.3**
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any

from hypothesis import given
from hypothesis import strategies as st

from das.client.budget import BUDGET_FIELDS, Budget, BudgetError, resolve_budget

budgets = st.builds(
    Budget,
    max_total_tokens=st.none() | st.integers(min_value=1, max_value=10**9),
    max_cost=st.none()
    | st.decimals(
        min_value=Decimal("0.000001"),
        max_value=Decimal("1000000"),
        places=6,
        allow_nan=False,
        allow_infinity=False,
    ),
)


def _field(budget: Budget | None, name: str) -> Any:
    return getattr(budget, name) if budget is not None else None


@given(client=st.none() | budgets, default=st.none() | budgets)
def test_resolution_never_raises_a_limit(client: Budget | None, default: Budget | None) -> None:
    try:
        resolved = resolve_budget(client, default)
    except BudgetError as err:
        name = err.parameter.removeprefix("budget.")
        assert name in BUDGET_FIELDS
        mine, limit = _field(client, name), _field(default, name)
        assert mine is not None and limit is not None and mine > limit
        assert err.limit == limit
        return

    for name in BUDGET_FIELDS:
        mine, limit = _field(client, name), _field(default, name)
        assert not (mine is not None and limit is not None and mine > limit)
        got = _field(resolved, name)
        assert got == (mine if mine is not None else limit)
        if limit is not None:
            assert got <= limit
    if resolved is not None:
        assert not resolved.is_empty()
```

- [ ] **Step 6: Run the property test**

Run: `uv run pytest tests/property/test_budget_resolution.py -v`
Expected: PASS

- [ ] **Step 7: Lint and type-check**

Run: `uv run ruff check das/client tests/unit/test_client_budget.py tests/property/test_budget_resolution.py && uv run mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 8: Tick the spec tasks**

In `.kiro/specs/das-client-api/tasks.md` tick `1.2 Implement budget resolution and status` and `1.3 Write property test for budget resolution`.

---

