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
