# DAS Client API (Phase 4) Pure Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `das/client`, the pure core of Phase 4: token bucket, budget resolution and status, idempotency helpers, Event payloads, and webhook signing (spec task group 1 in `.kiro/specs/das-client-api/tasks.md`).

**Architecture:** Five small pure modules with no I/O, clock reads or randomness (time and random values are parameters). Later task groups call them from the Lua-backed State_Store, the Manager reducer, the Gateway and the client package. Spec: `.kiro/specs/das-client-api/requirements.md` and `design.md` §2.

**Tech Stack:** Python 3.12, pydantic models already in `das/models`, `das.ops.cost` for pricing, pytest 9.1.1, Hypothesis 6.170.8, uv.

## Global Constraints

- Python `>=3.12,<3.13`; run everything through `uv run`.
- No new dependencies.
- Modules in `das/client/` are pure: no I/O, no `time`/`asyncio.sleep`, no `random`. Pass time and random values in.
- `das/client/` must not import `redis`, `aio_pika`, `aiormq`, `pamqp` or `grpc` (it will be imported by `das.manager`, which is covered by the import-linter contract).
- Lint: `uv run ruff check` (rules E, F; line length 120). Types: `uv run mypy` (package `das/`).
- Property tests: in `tests/property/`, first line `# Feature: das-client-api, Property {n}: {title}`, docstring ending with `**Validates: Requirements …**`, default Hypothesis profile (≥ 100 examples).
- Module and function docstrings cite requirement numbers as `Phase 4 Req X.Y` or `Req X.Y`, matching the surrounding code style.
- The workspace is not a git repository. Instead of committing, each task ends by ticking its checkboxes in `.kiro/specs/das-client-api/tasks.md`.

---

### Task 1: Token bucket (spec task 1.1)

**Files:**
- Create: `das/client/__init__.py`
- Create: `das/client/bucket.py`
- Test: `tests/unit/test_client_bucket.py`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `BucketState(tokens: float, updated: float)` (frozen dataclass)
  - `TakeResult(allowed: bool, state: BucketState, retry_after: float)` (frozen dataclass)
  - `take(state: BucketState | None, now: float, rate: float, burst: int) -> TakeResult`
  - `retry_after_header(seconds: float) -> int`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_client_bucket.py`:

```python
"""Token bucket (Phase 4 Req 2.1-2.3)."""
from __future__ import annotations

import pytest

from das.client.bucket import BucketState, TakeResult, retry_after_header, take


def test_first_call_starts_from_a_full_bucket() -> None:
    r = take(None, 100.0, rate=1.0, burst=2)
    assert r == TakeResult(True, BucketState(1.0, 100.0), 0.0)


def test_burst_is_used_up_then_denied_with_retry_after() -> None:
    r1 = take(None, 100.0, rate=1.0, burst=2)
    r2 = take(r1.state, 100.0, rate=1.0, burst=2)
    r3 = take(r2.state, 100.0, rate=1.0, burst=2)
    assert (r1.allowed, r2.allowed, r3.allowed) == (True, True, False)
    assert r3.state == BucketState(0.0, 100.0)
    assert r3.retry_after == pytest.approx(1.0)


def test_refill_over_time() -> None:
    empty = BucketState(0.0, 100.0)
    half = take(empty, 100.5, rate=1.0, burst=2)
    assert not half.allowed
    assert half.retry_after == pytest.approx(0.5)
    full_token = take(half.state, 101.0, rate=1.0, burst=2)
    assert full_token.allowed
    assert full_token.state.tokens == pytest.approx(0.0)


def test_refill_is_capped_at_burst() -> None:
    r = take(BucketState(0.0, 0.0), 100.0, rate=10.0, burst=2)
    assert r.allowed
    assert r.state == BucketState(1.0, 100.0)


def test_clock_stepping_back_refills_nothing() -> None:
    r = take(BucketState(0.0, 100.0), 50.0, rate=1.0, burst=2)
    assert not r.allowed
    assert r.state == BucketState(0.0, 100.0)


def test_rate_zero_disables_the_limit() -> None:
    state = None
    for _ in range(1000):
        r = take(state, 5.0, rate=0.0, burst=1)
        assert r.allowed
        assert r.retry_after == 0.0
        state = r.state


@pytest.mark.parametrize(("rate", "burst"), [(-0.1, 1), (1.0, 0), (1.0, -5)])
def test_invalid_arguments(rate: float, burst: int) -> None:
    with pytest.raises(ValueError):
        take(None, 0.0, rate=rate, burst=burst)


@pytest.mark.parametrize(("seconds", "header"), [(0.0, 1), (0.2, 1), (1.0, 1), (1.01, 2), (59.5, 60)])
def test_retry_after_header_rounds_up_to_at_least_one(seconds: float, header: int) -> None:
    assert retry_after_header(seconds) == header
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_client_bucket.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'das.client'`

- [ ] **Step 3: Write minimal implementation**

Create `das/client/__init__.py`:

```python
"""Pure core for Phase 4 client API: limits, budgets, idempotency, events, webhooks.

Modules here are pure functions: no I/O, no clock reads, no randomness.
Time and random values are passed in by the I/O shell.
"""
```

Create `das/client/bucket.py`:

```python
"""Token bucket for per-key rate limits (Phase 4 Req 2.1-2.3). Pure.

``rate_check.lua`` implements the same arithmetic with Redis ``TIME``;
Property 1 checks that the two agree.
"""
from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BucketState:
    """Tokens left and the Redis time (seconds) they were computed at."""

    tokens: float
    updated: float


@dataclass(frozen=True, slots=True)
class TakeResult:
    allowed: bool
    state: BucketState
    #: Seconds until one token is available; 0.0 when ``allowed``.
    retry_after: float


def take(state: BucketState | None, now: float, rate: float, burst: int) -> TakeResult:
    """Refill the bucket up to ``burst`` at ``rate`` tokens per second, then take one token.

    ``state is None`` is a full bucket. ``rate == 0`` disables the limit (Req 1.2).
    A ``now`` earlier than ``state.updated`` (clock stepped back) refills nothing.
    """
    if rate < 0:
        raise ValueError(f"rate must be >= 0, got {rate}")
    if burst < 1:
        raise ValueError(f"burst must be >= 1, got {burst}")
    if state is None:
        tokens, updated = float(burst), now
    else:
        elapsed = max(0.0, now - state.updated)
        tokens = min(float(burst), state.tokens + rate * elapsed)
        updated = max(now, state.updated)
    if rate == 0:
        return TakeResult(True, BucketState(tokens, updated), 0.0)
    if tokens >= 1.0:
        return TakeResult(True, BucketState(tokens - 1.0, updated), 0.0)
    return TakeResult(False, BucketState(tokens, updated), (1.0 - tokens) / rate)


def retry_after_header(seconds: float) -> int:
    """``Retry-After`` value: whole seconds rounded up, at least 1 (Req 2.2)."""
    return max(1, math.ceil(seconds))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_client_bucket.py -v`
Expected: PASS (all tests)

- [ ] **Step 5: Lint and type-check**

Run: `uv run ruff check das/client tests/unit/test_client_bucket.py && uv run mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 6: Tick the spec task**

In `.kiro/specs/das-client-api/tasks.md` change `- [ ] 1.1 Implement the token bucket` to `- [x] 1.1 Implement the token bucket`.

---

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

### Task 3: Idempotency helpers and Event payloads (spec task 1.4)

**Files:**
- Create: `das/client/idempotency.py`
- Create: `das/client/events.py`
- Test: `tests/unit/test_client_idempotency.py`
- Test: `tests/unit/test_client_events.py`

**Interfaces:**
- Consumes: `Budget`, `budget_status`, `budget_view` (Task 2); `das.models.job.TransitionEntry`, `JobState`; `das.models.plan.JobPlan`, `PlannedJob`; `das.models.request.RequestStatus`; `das.models.usage.UsageRecord`; `das.ops.cost.PriceTable`.
- Produces:
  - `MAX_IDEMPOTENCY_KEY_LENGTH = 128`
  - `validate_idempotency_key(value: str) -> bool`
  - `body_hash(prompt: str, budget: Budget | None) -> str` (64 hex chars)
  - `idem_key_hash(value: str) -> str` (64 hex chars)
  - `EventType` (Literal of 7 names), `EVENT_TYPES`, `TERMINAL_EVENT_TYPES`
  - `terminal_event_type(status: RequestStatus) -> EventType`
  - `created_event_data() -> dict[str, Any]`
  - `planned_event_data(plan: JobPlan) -> dict[str, Any]`
  - `transition_event_data(entry: TransitionEntry) -> dict[str, Any]`
  - `reopened_event_data() -> dict[str, Any]`
  - `terminal_event_data(status: RequestStatus, error: str | None, usage: Sequence[UsageRecord], budget: Budget | None, prices: PriceTable | None) -> dict[str, Any]`

- [ ] **Step 1: Write the failing idempotency tests**

Create `tests/unit/test_client_idempotency.py`:

```python
"""Idempotency_Key validation and Body_Hash (Phase 4 Req 4)."""
from __future__ import annotations

import hashlib
from decimal import Decimal

import pytest

from das.client.budget import Budget
from das.client.idempotency import body_hash, idem_key_hash, validate_idempotency_key


@pytest.mark.parametrize("value", ["a", "abc-123_XYZ", "!" + "~" * 127, "a" * 128])
def test_valid_keys(value: str) -> None:
    assert validate_idempotency_key(value)


@pytest.mark.parametrize("value", ["", "a" * 129, "a b", "é", "a\n", "\n", "\x7f", "tab\tkey"])
def test_invalid_keys(value: str) -> None:
    assert not validate_idempotency_key(value)


def test_body_hash_known_value() -> None:
    expected = hashlib.sha256(b'{"budget":null,"prompt":"hi"}').hexdigest()
    assert body_hash("hi", None) == expected


def test_body_hash_ignores_decimal_spelling_and_empty_budget() -> None:
    assert body_hash("p", Budget(max_cost=Decimal("1.50"))) == body_hash("p", Budget(max_cost=Decimal("1.5")))
    assert body_hash("p", Budget()) == body_hash("p", None)


def test_body_hash_changes_with_prompt_and_budget() -> None:
    base = body_hash("p", Budget(max_total_tokens=10))
    assert body_hash("q", Budget(max_total_tokens=10)) != base
    assert body_hash("p", Budget(max_total_tokens=11)) != base
    assert body_hash("p", None) != base


def test_body_hash_accepts_lone_surrogates() -> None:
    # JSON "\ud800" decodes to a lone surrogate; hashing must not raise.
    assert len(body_hash("\ud800", None)) == 64


def test_idem_key_hash() -> None:
    assert idem_key_hash("k") == hashlib.sha256(b"k").hexdigest()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_client_idempotency.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'das.client.idempotency'`

- [ ] **Step 3: Write the idempotency module**

Create `das/client/idempotency.py`:

```python
"""Idempotent submit helpers (Phase 4 Req 4). Pure."""
from __future__ import annotations

import hashlib
import json
import re
from typing import Final

from das.client.budget import Budget

MAX_IDEMPOTENCY_KEY_LENGTH: Final = 128
_KEY: Final = re.compile(rf"[\x21-\x7e]{{1,{MAX_IDEMPOTENCY_KEY_LENGTH}}}")


def validate_idempotency_key(value: str) -> bool:
    """1 to 128 printable ASCII characters, 0x21-0x7E (Req 4.1)."""
    return _KEY.fullmatch(value) is not None


def body_hash(prompt: str, budget: Budget | None) -> str:
    """Body_Hash: sha256 of the canonical JSON of ``{prompt, budget}`` as submitted (Req 4.3, 4.4).

    An empty Budget counts as no Budget, and ``max_cost`` uses its canonical text, so
    ``1.50`` and ``1.5`` hash the same.
    """
    obj = {
        "prompt": prompt,
        "budget": None if budget is None or budget.is_empty() else budget.to_json_obj(),
    }
    text = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(text.encode("utf-8", "surrogatepass")).hexdigest()


def idem_key_hash(value: str) -> str:
    """Hex sha256 of an Idempotency_Key, used in the Idempotency_Record key name."""
    return hashlib.sha256(value.encode("ascii")).hexdigest()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_client_idempotency.py -v`
Expected: PASS (all tests)

- [ ] **Step 5: Write the failing events tests**

Create `tests/unit/test_client_events.py`:

```python
"""Event_Stream payloads (Phase 4 Req 6.1, 6.2)."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from das.client.budget import Budget
from das.client.events import (
    EVENT_TYPES,
    TERMINAL_EVENT_TYPES,
    created_event_data,
    planned_event_data,
    reopened_event_data,
    terminal_event_data,
    terminal_event_type,
    transition_event_data,
)
from das.models.job import JobState, TransitionEntry
from das.models.plan import JobPlan, PlannedJob
from das.models.request import RequestStatus
from das.models.results import TokenUsage
from das.models.usage import UsageRecord

USAGE = [
    UsageRecord(
        operation="job",
        provider="openai",
        model="m",
        usage=TokenUsage(input_tokens=10, output_tokens=5, total_tokens=15),
        job_id="j1",
        attempt=1,
    )
]


def test_terminal_types_are_event_types() -> None:
    assert TERMINAL_EVENT_TYPES <= set(EVENT_TYPES)
    assert len(EVENT_TYPES) == 7


@pytest.mark.parametrize(
    ("status", "event_type"),
    [
        (RequestStatus.COMPLETED, "request.completed"),
        (RequestStatus.FAILED, "request.failed"),
        (RequestStatus.CANCELLED, "request.cancelled"),
    ],
)
def test_terminal_event_type(status: RequestStatus, event_type: str) -> None:
    assert terminal_event_type(status) == event_type


@pytest.mark.parametrize("status", [RequestStatus.PENDING, RequestStatus.RUNNING])
def test_terminal_event_type_rejects_non_terminal(status: RequestStatus) -> None:
    with pytest.raises(ValueError):
        terminal_event_type(status)


def test_created_and_reopened() -> None:
    assert created_event_data() == {"status": "Pending"}
    assert reopened_event_data() == {"status": "Running"}


def test_planned_event_omits_instructions() -> None:
    plan = JobPlan(
        jobs=[
            PlannedJob(job_id="j1", instruction="secret instruction", capabilities=["research"]),
            PlannedJob(job_id="j2", instruction="other", capabilities=["summarize"], depends_on=["j1"]),
        ]
    )
    data = planned_event_data(plan)
    assert data == {
        "jobs": [
            {"job_id": "j1", "capabilities": ["research"], "depends_on": []},
            {"job_id": "j2", "capabilities": ["summarize"], "depends_on": ["j1"]},
        ]
    }
    assert "secret instruction" not in json.dumps(data)


def test_transition_event() -> None:
    entry = TransitionEntry(
        job_id="j1",
        from_state=JobState.READY,
        to_state=JobState.DISPATCHED,
        attempt=1,
        worker_id=None,
        at=datetime(2026, 10, 30, 1, 2, 3, 456000, tzinfo=UTC),
    )
    assert transition_event_data(entry) == {
        "job_id": "j1",
        "from": "Ready",
        "to": "Dispatched",
        "attempt": 1,
        "worker_id": None,
        "at": "2026-10-30T01:02:03.456Z",
    }


def test_transition_event_from_none() -> None:
    entry = TransitionEntry(
        job_id="j1",
        from_state=None,
        to_state=JobState.WAITING,
        attempt=0,
        worker_id="w1",
        at=datetime(2026, 10, 30, tzinfo=UTC),
    )
    data = transition_event_data(entry)
    assert data["from"] is None
    assert data["worker_id"] == "w1"
    assert data["at"] == "2026-10-30T00:00:00.000Z"


def test_terminal_event_without_budget() -> None:
    assert terminal_event_data(RequestStatus.COMPLETED, None, USAGE, None, None) == {
        "status": "Completed",
        "error": None,
        "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15},
    }


def test_terminal_event_with_budget() -> None:
    data = terminal_event_data(RequestStatus.FAILED, "budget_exceeded", USAGE, Budget(max_total_tokens=10), None)
    assert data["error"] == "budget_exceeded"
    assert data["budget"] == {
        "limits": {"max_total_tokens": 10},
        "used": {"total_tokens": 15, "cost": None},
        "exceeded": True,
    }


def test_terminal_event_with_no_usage() -> None:
    data = terminal_event_data(RequestStatus.CANCELLED, None, [], Budget(max_cost=Decimal("1")), None)
    assert data["usage"] == {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    assert data["budget"]["exceeded"] is True  # max_cost without a Price_Table is unenforceable


def test_terminal_event_rejects_non_terminal_status() -> None:
    with pytest.raises(ValueError):
        terminal_event_data(RequestStatus.RUNNING, None, [], None, None)
```

- [ ] **Step 6: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_client_events.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'das.client.events'`

- [ ] **Step 7: Write the events module**

Create `das/client/events.py`:

```python
"""Event_Stream payloads (Phase 4 Req 6.1, 6.2). Pure.

Payloads never carry the prompt, Job instructions, Job_Result content or the
Final_Result (Req 6.2). The I/O shell passes them to the Lua scripts, which append
them in the same atomic write as the state change they describe.
"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC
from typing import Any, Final, Literal

from das.client.budget import Budget, budget_status, budget_view
from das.models.job import TransitionEntry
from das.models.plan import JobPlan
from das.models.request import RequestStatus
from das.models.usage import UsageRecord
from das.ops.cost import PriceTable

EventType = Literal[
    "request.created",
    "request.planned",
    "job.transition",
    "request.reopened",
    "request.completed",
    "request.failed",
    "request.cancelled",
]
EVENT_TYPES: Final[tuple[EventType, ...]] = (
    "request.created",
    "request.planned",
    "job.transition",
    "request.reopened",
    "request.completed",
    "request.failed",
    "request.cancelled",
)
TERMINAL_EVENT_TYPES: Final[frozenset[EventType]] = frozenset(
    {"request.completed", "request.failed", "request.cancelled"}
)

_TERMINAL: Final[dict[RequestStatus, EventType]] = {
    RequestStatus.COMPLETED: "request.completed",
    RequestStatus.FAILED: "request.failed",
    RequestStatus.CANCELLED: "request.cancelled",
}


def terminal_event_type(status: RequestStatus) -> EventType:
    """Terminal_Event type for a terminal Request_Status."""
    try:
        return _TERMINAL[status]
    except KeyError:
        raise ValueError(f"not a terminal status: {status.value}") from None


def created_event_data() -> dict[str, Any]:
    return {"status": RequestStatus.PENDING.value}


def planned_event_data(plan: JobPlan) -> dict[str, Any]:
    """Job_IDs, Capabilities and dependencies; never instructions."""
    return {
        "jobs": [
            {"job_id": j.job_id, "capabilities": list(j.capabilities), "depends_on": list(j.depends_on)}
            for j in plan.jobs
        ]
    }


def transition_event_data(entry: TransitionEntry) -> dict[str, Any]:
    """One ``job.transition`` Event per transition-log entry."""
    return {
        "job_id": entry.job_id,
        "from": entry.from_state.value if entry.from_state is not None else None,
        "to": entry.to_state.value,
        "attempt": entry.attempt,
        "worker_id": entry.worker_id,
        "at": entry.at.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    }


def reopened_event_data() -> dict[str, Any]:
    return {"status": RequestStatus.RUNNING.value}


def _usage_total(usage: Sequence[UsageRecord]) -> dict[str, int]:
    return {
        "input_tokens": sum(r.usage.input_tokens for r in usage),
        "output_tokens": sum(r.usage.output_tokens for r in usage),
        "total_tokens": sum(r.usage.total_tokens for r in usage),
    }


def terminal_event_data(
    status: RequestStatus,
    error: str | None,
    usage: Sequence[UsageRecord],
    budget: Budget | None,
    prices: PriceTable | None,
) -> dict[str, Any]:
    """Terminal_Event data: status, stored error, token usage total, and Budget_Used when budgeted."""
    terminal_event_type(status)
    data: dict[str, Any] = {"status": status.value, "error": error, "usage": _usage_total(usage)}
    if budget is not None:
        data["budget"] = budget_view(budget, budget_status(usage, budget, prices))
    return data
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_client_idempotency.py tests/unit/test_client_events.py -v`
Expected: PASS (all tests)

- [ ] **Step 9: Lint and type-check**

Run: `uv run ruff check das/client tests/unit/test_client_idempotency.py tests/unit/test_client_events.py && uv run mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 10: Tick the spec task**

In `.kiro/specs/das-client-api/tasks.md` tick `1.4 Implement idempotency helpers and event payloads`.

---

### Task 4: Webhook signing and retry schedule (spec task 1.5)

**Files:**
- Create: `das/client/webhook.py`
- Create: `tests/fixtures/webhook_vectors.json`
- Test: `tests/unit/test_client_webhook.py`

**Interfaces:**
- Consumes: `das.manager.backoff.backoff(attempt: int, base: float, max_backoff: float) -> float`.
- Produces:
  - `SIGNATURE_PREFIX = "v1="`
  - `webhook_body(request_id: str, event_id: str, event_type: str, data: dict[str, Any]) -> bytes`
  - `sign(secret: bytes, timestamp: int, body: bytes) -> str`
  - `verify(secret: bytes, timestamp: str, body: bytes, signature: str, now: float, tolerance: int) -> bool`
  - `next_due(now: float, attempts: int, base: float, max_backoff: float, rand: float) -> float`
  - `tests/fixtures/webhook_vectors.json`, reused by the client package in spec task 9.5.

- [ ] **Step 1: Create the test vectors**

Create `tests/fixtures/webhook_vectors.json`. The signatures were computed as `"v1=" + hex(HMAC-SHA256(secret_utf8, f"{timestamp}." + body_utf8))`:

```json
{
  "description": "Webhook signature vectors (Phase 4 Req 8.4, 9.7). Shared by das.client.webhook and das_client tests.",
  "vectors": [
    {
      "secret": "whsec_test_0123456789",
      "timestamp": 1793000000,
      "body": "{\"data\":{\"status\":\"Completed\"},\"event_id\":\"1793000000000-0\",\"request_id\":\"r1\",\"type\":\"request.completed\"}",
      "signature": "v1=8847d8f3b0002096373ae63db4f4d3fc79ffa69566fb6d1d53baf77ed40095e7"
    },
    {
      "secret": "whsec_test_0123456789",
      "timestamp": 1793000000,
      "body": "",
      "signature": "v1=4de65a69d4ed28cb8f56a4fdec85f30f14bc6d4ff5599e5ba20cb9df88b0ffdd"
    },
    {
      "secret": "another-secret",
      "timestamp": 1793000000,
      "body": "{\"data\":{\"status\":\"Completed\"},\"event_id\":\"1793000000000-0\",\"request_id\":\"r1\",\"type\":\"request.completed\"}",
      "signature": "v1=f36ca3883cb4aa25abbeefd2dd01146187b8eade8349bf51ae10af684724e464"
    }
  ]
}
```

- [ ] **Step 2: Write the failing test**

Create `tests/unit/test_client_webhook.py`:

```python
"""Webhook signing, verification and retry schedule (Phase 4 Req 8.4, 8.6, 9.7)."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from das.client.webhook import next_due, sign, verify, webhook_body

VECTORS: list[dict[str, Any]] = json.loads(
    (Path(__file__).resolve().parents[1] / "fixtures" / "webhook_vectors.json").read_text()
)["vectors"]
SECRET = b"whsec_test_0123456789"
TS = 1793000000
BODY = b'{"data":{"status":"Completed"},"event_id":"1793000000000-0","request_id":"r1","type":"request.completed"}'


@pytest.mark.parametrize("v", VECTORS)
def test_sign_matches_vectors(v: dict[str, Any]) -> None:
    assert sign(v["secret"].encode(), v["timestamp"], v["body"].encode()) == v["signature"]


@pytest.mark.parametrize("v", VECTORS)
def test_verify_accepts_vectors(v: dict[str, Any]) -> None:
    ts = v["timestamp"]
    assert verify(v["secret"].encode(), str(ts), v["body"].encode(), v["signature"], now=ts, tolerance=300)


def test_webhook_body_is_canonical_json() -> None:
    assert webhook_body("r1", "1793000000000-0", "request.completed", {"status": "Completed"}) == BODY


def test_verify_rejects_changes() -> None:
    good = sign(SECRET, TS, BODY)
    assert verify(SECRET, str(TS), BODY, good, now=TS, tolerance=300)
    assert not verify(SECRET, str(TS), BODY + b" ", good, now=TS, tolerance=300)
    assert not verify(SECRET, str(TS + 1), BODY, good, now=TS, tolerance=300)
    assert not verify(b"wrong", str(TS), BODY, good, now=TS, tolerance=300)
    assert not verify(SECRET, str(TS), BODY, good.removeprefix("v1="), now=TS, tolerance=300)
    flipped = good[:-1] + ("1" if good[-1] == "0" else "0")
    assert not verify(SECRET, str(TS), BODY, flipped, now=TS, tolerance=300)


def test_verify_tolerance_window() -> None:
    good = sign(SECRET, TS, BODY)
    assert verify(SECRET, str(TS), BODY, good, now=TS + 300, tolerance=300)
    assert verify(SECRET, str(TS), BODY, good, now=TS - 300, tolerance=300)
    assert not verify(SECRET, str(TS), BODY, good, now=TS + 301, tolerance=300)
    assert not verify(SECRET, str(TS), BODY, good, now=TS - 301, tolerance=300)


@pytest.mark.parametrize("ts", ["", "12a", "+1793000000", " 1793000000", "1793000000 ", "1.5", "١٢٣"])
def test_verify_rejects_malformed_timestamps(ts: str) -> None:
    assert not verify(SECRET, ts, BODY, sign(SECRET, TS, BODY), now=TS, tolerance=300)


def test_verify_handles_non_ascii_signature() -> None:
    assert not verify(SECRET, str(TS), BODY, "v1=é", now=TS, tolerance=300)


@pytest.mark.parametrize(("rand", "delay"), [(0.0, 24.0), (0.5, 30.0), (1.0, 36.0)])
def test_next_due_jitter(rand: float, delay: float) -> None:
    assert next_due(1000.0, 1, 30.0, 3600.0, rand) == pytest.approx(1000.0 + delay)


def test_next_due_is_capped_by_max_backoff() -> None:
    assert next_due(1000.0, 10, 30.0, 3600.0, 1.0) == pytest.approx(1000.0 + 3600.0 * 1.2)


@pytest.mark.parametrize("rand", [-0.1, 1.1])
def test_next_due_rejects_rand_out_of_range(rand: float) -> None:
    with pytest.raises(ValueError):
        next_due(1000.0, 1, 30.0, 3600.0, rand)
```

- [ ] **Step 3: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_client_webhook.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'das.client.webhook'`

- [ ] **Step 4: Write the implementation**

Create `das/client/webhook.py`:

```python
"""Webhook body, signing and retry schedule (Phase 4 Req 8.4, 8.6, 9.7). Pure.

The Client_Package keeps a copy of :func:`sign` and :func:`verify`; both copies are
checked against ``tests/fixtures/webhook_vectors.json`` so they cannot drift.
"""
from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any, Final

from das.manager.backoff import backoff

SIGNATURE_PREFIX: Final = "v1="
#: Retry delays are ``backoff(attempts) * (JITTER_LOW + JITTER_SPAN * rand)``: 0.8x to 1.2x.
JITTER_LOW: Final = 0.8
JITTER_SPAN: Final = 0.4


def webhook_body(request_id: str, event_id: str, event_type: str, data: dict[str, Any]) -> bytes:
    """Canonical JSON body of a Webhook_Delivery (Req 8.4)."""
    obj = {"request_id": request_id, "event_id": event_id, "type": event_type, "data": data}
    text = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return text.encode("utf-8")


def sign(secret: bytes, timestamp: int, body: bytes) -> str:
    """``DAS-Webhook-Signature`` value: ``v1=`` + hex HMAC-SHA256 of ``"{timestamp}.{body}"``."""
    mac = hmac.new(secret, f"{timestamp}.".encode("ascii") + body, hashlib.sha256)
    return SIGNATURE_PREFIX + mac.hexdigest()


def verify(secret: bytes, timestamp: str, body: bytes, signature: str, now: float, tolerance: int) -> bool:
    """True iff ``signature`` signs ``body`` at ``timestamp``, and ``timestamp`` is near ``now``.

    "Near" means at most ``tolerance`` seconds away. ``timestamp`` is the header text and
    must be ASCII digits only. The comparison runs in constant time.
    """
    if not (timestamp.isascii() and timestamp.isdigit()):
        return False
    ts = int(timestamp)
    if abs(now - ts) > tolerance:
        return False
    expected = sign(secret, ts, body).encode("ascii")
    return hmac.compare_digest(expected, signature.encode("utf-8", "surrogatepass"))


def next_due(now: float, attempts: int, base: float, max_backoff: float, rand: float) -> float:
    """Next attempt time after ``attempts`` failed attempts (Req 8.6). ``rand`` is in [0, 1]."""
    if not 0.0 <= rand <= 1.0:
        raise ValueError(f"rand must be in [0, 1], got {rand}")
    return now + backoff(attempts, base, max_backoff) * (JITTER_LOW + JITTER_SPAN * rand)
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_client_webhook.py -v`
Expected: PASS (all tests)

- [ ] **Step 6: Lint and type-check**

Run: `uv run ruff check das/client tests/unit/test_client_webhook.py && uv run mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 7: Tick the spec task**

In `.kiro/specs/das-client-api/tasks.md` tick `1.5 Implement webhook signing` and the parent `1. Pure core das/client`.

---

### Task 5: Verify the task group

**Files:** none changed.

- [ ] **Step 1: Run the fast suite**

Run: `uv run pytest -m "not integration" -q -p no:cacheprovider`
Expected: all pass; the count is the previous 1,978 plus the new tests, `0 failed`.

- [ ] **Step 2: Run every static check**

Run: `uv run ruff check && uv run mypy && uv run lint-imports`
Expected: `All checks passed!`, `Success: no issues found`, `Contracts: 1 kept, 0 broken.`
