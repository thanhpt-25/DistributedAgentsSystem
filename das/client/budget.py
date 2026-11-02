"""Per-Request budgets (Phase 4 Req 5). Pure.

``resolve_budget`` merges the client's Budget with the key's default and never
raises a limit (Property 4). ``budget_status`` measures Budget_Used against a
Budget; the Manager's reducer stops dispatching once it reports ``exceeded``.
"""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
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
        """Inverse of :meth:`to_json_obj` (the form stored on the Request record).

        Invalid stored values raise ``ValueError``.
        """
        unknown = set(obj) - set(BUDGET_FIELDS)
        if unknown:
            raise ValueError(f"unknown budget fields: {sorted(unknown)}")
        cost = obj.get("max_cost")
        if cost is not None and not isinstance(cost, str):
            raise ValueError("max_cost must be a decimal string")
        try:
            max_cost = Decimal(cost) if cost is not None else None
        except InvalidOperation as exc:
            raise ValueError("max_cost must be a decimal string") from exc
        return cls(max_total_tokens=obj.get("max_total_tokens"), max_cost=max_cost)


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
