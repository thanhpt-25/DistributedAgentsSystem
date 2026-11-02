"""Price_Table and Estimated_Cost computation (Req 10).

Pure module (Req 19.5): no I/O, no clock, no randomness.

- A Usage_Record is priced iff its ``provider`` and ``model`` equal, exactly and
  case-sensitively, those of a Price_Table entry (Req 10.5).
- Amounts are computed with exact decimal arithmetic in a 50-digit context. The
  reported total is the rounded unrounded total, never the sum of rounded per-pair
  amounts (Req 10.4).
- Only reported amounts are rounded: 6 decimal places, round-half-even, fixed
  notation without exponent or thousands separators (Req 10.4).
"""
from __future__ import annotations

import decimal
from collections.abc import Iterable
from decimal import ROUND_HALF_EVEN, Decimal
from typing import Protocol

from pydantic import Field, field_validator, model_validator

from das.models.base import DasModel
from das.ops.iso4217 import ISO_4217_CODES

_PRECISION = 50
_MILLION = Decimal(1_000_000)
_QUANTUM = Decimal("0.000001")
_MAX_PRICE = Decimal(1_000_000)
MAX_PRICE_ENTRIES = 200


class _Usage(Protocol):
    @property
    def input_tokens(self) -> int: ...

    @property
    def output_tokens(self) -> int: ...


class _UsageRecordLike(Protocol):
    """Structural view of ``das.models.usage.UsageRecord`` used by this module."""

    @property
    def provider(self) -> str: ...

    @property
    def model(self) -> str: ...

    @property
    def usage(self) -> _Usage: ...


class PriceEntry(DasModel):
    """Price per 1,000,000 tokens for one provider/model pair (Req 10.1)."""

    provider: str = Field(min_length=1, max_length=64)
    model: str = Field(min_length=1, max_length=128)
    input_per_million: Decimal = Field(ge=0, le=_MAX_PRICE, allow_inf_nan=False)
    output_per_million: Decimal = Field(ge=0, le=_MAX_PRICE, allow_inf_nan=False)

    @field_validator("input_per_million", "output_per_million")
    @classmethod
    def _at_most_six_places(cls, value: Decimal) -> Decimal:
        # Trailing zeros do not count: Decimal("1.5000000") has one decimal place.
        exponent = value.normalize().as_tuple().exponent
        if isinstance(exponent, int) and exponent < -6:
            raise ValueError("price must have at most 6 decimal places")
        return value


class PriceTable(DasModel):
    """Optional pricing configuration: ISO 4217 currency and up to 200 entries."""

    currency: str = "USD"
    models: list[PriceEntry] = Field(default_factory=list, max_length=MAX_PRICE_ENTRIES)

    @field_validator("currency")
    @classmethod
    def _iso_4217(cls, value: str) -> str:
        if value not in ISO_4217_CODES:
            raise ValueError("currency must be an ISO 4217 code")
        return value

    @model_validator(mode="after")
    def _unique_pairs(self) -> PriceTable:
        seen: set[tuple[str, str]] = set()
        for index, entry in enumerate(self.models):
            pair = (entry.provider, entry.model)
            if pair in seen:
                raise ValueError(f"models[{index}]: duplicate provider/model pair")
            seen.add(pair)
        return self

    def lookup(self, provider: str, model: str) -> PriceEntry | None:
        """Entry whose provider and model match exactly (case-sensitive), if any."""
        for entry in self.models:
            if entry.provider == provider and entry.model == model:
                return entry
        return None


class ProviderModel(DasModel):
    provider: str
    model: str


class ModelCost(DasModel):
    provider: str
    model: str
    amount: str


class CostReport(DasModel):
    """``estimated_cost`` object of the Request response (Req 10.3)."""

    amount: str
    currency: str
    complete: bool
    unpriced_models: list[ProviderModel]
    by_model: list[ModelCost]


def record_cost(r: _UsageRecordLike, entry: PriceEntry) -> Decimal:
    """Unrounded cost of one record: in × price_in / 10^6 + out × price_out / 10^6."""
    with decimal.localcontext(prec=_PRECISION):
        return (
            Decimal(r.usage.input_tokens) * entry.input_per_million / _MILLION
            + Decimal(r.usage.output_tokens) * entry.output_per_million / _MILLION
        )


def format_amount(x: Decimal) -> str:
    """Round to 6 places (half-even) and render in fixed notation, e.g. ``0.000000``."""
    with decimal.localcontext(prec=_PRECISION):
        return format(x.quantize(_QUANTUM, rounding=ROUND_HALF_EVEN), "f")


def unrounded_costs(
    records: Iterable[_UsageRecordLike], table: PriceTable
) -> tuple[Decimal, dict[tuple[str, str], Decimal], set[tuple[str, str]]]:
    """Unrounded total, unrounded amount per priced pair, and the set of unpriced pairs.

    The total equals the sum of the per-pair amounts (Req 10.8).
    """
    entries = {(e.provider, e.model): e for e in table.models}
    by_pair: dict[tuple[str, str], Decimal] = {}
    unpriced: set[tuple[str, str]] = set()
    with decimal.localcontext(prec=_PRECISION):
        for r in records:
            pair = (r.provider, r.model)
            entry = entries.get(pair)
            if entry is None:
                unpriced.add(pair)
                continue
            by_pair[pair] = by_pair.get(pair, Decimal(0)) + record_cost(r, entry)
        total = sum(by_pair.values(), Decimal(0))
    return total, by_pair, unpriced


def estimate_cost(records: Iterable[_UsageRecordLike], table: PriceTable) -> CostReport:
    """Estimated_Cost of a Request's Usage_Records under ``table`` (Req 10.3-10.7)."""
    total, by_pair, unpriced = unrounded_costs(records, table)
    return CostReport(
        amount=format_amount(total),
        currency=table.currency,
        complete=not unpriced,
        unpriced_models=[ProviderModel(provider=p, model=m) for p, m in sorted(unpriced)],
        by_model=[
            ModelCost(provider=p, model=m, amount=format_amount(by_pair[(p, m)]))
            for p, m in sorted(by_pair)
        ],
    )


__all__ = [
    "MAX_PRICE_ENTRIES",
    "CostReport",
    "ModelCost",
    "PriceEntry",
    "PriceTable",
    "ProviderModel",
    "estimate_cost",
    "format_amount",
    "record_cost",
    "unrounded_costs",
]
