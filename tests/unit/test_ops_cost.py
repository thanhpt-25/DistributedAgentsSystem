"""Unit tests for das.ops.cost and das.ops.iso4217 (Req 10.1, 10.3-10.7)."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import pytest
from hypothesis import given, settings
from pydantic import ValidationError

from das.models import TokenUsage
from das.ops.cost import (
    CostReport,
    PriceEntry,
    PriceTable,
    estimate_cost,
    format_amount,
    record_cost,
)
from das.ops.iso4217 import ISO_4217_CODES
from tests.strategies import CURRENCY_CODES, price_tables


@dataclass(frozen=True)
class _Rec:
    """Minimal Usage_Record shape (provider, model, usage)."""

    provider: str
    model: str
    usage: TokenUsage


def rec(provider: str, model: str, i: int, o: int) -> _Rec:
    return _Rec(provider, model, TokenUsage(input_tokens=i, output_tokens=o, total_tokens=i + o))


def entry(provider: str, model: str, pin: str, pout: str) -> dict[str, object]:
    return {
        "provider": provider,
        "model": model,
        "input_per_million": Decimal(pin),
        "output_per_million": Decimal(pout),
    }


TABLE = PriceTable.model_validate(
    {
        "currency": "USD",
        "models": [
            entry("openai", "gpt-4o", "2.5", "10"),
            entry("anthropic", "claude-sonnet", "3", "15"),
        ],
    }
)


# ------------------------------------------------------------------ iso4217
def test_iso_codes_shape_and_members() -> None:
    assert all(len(c) == 3 and c.isascii() and c.isupper() for c in ISO_4217_CODES)
    assert {"USD", "EUR", "JPY", "VND", "GBP"} <= ISO_4217_CODES
    assert set(CURRENCY_CODES) <= ISO_4217_CODES
    assert "usd" not in ISO_4217_CODES and "ABC" not in ISO_4217_CODES


# ------------------------------------------------------------------ models
def test_price_table_defaults() -> None:
    t = PriceTable()
    assert t.currency == "USD" and t.models == []


@pytest.mark.parametrize("currency", ["usd", "ABC", "US", ""])
def test_price_table_rejects_bad_currency(currency: str) -> None:
    with pytest.raises(ValidationError):
        PriceTable(currency=currency)


@pytest.mark.parametrize(
    "bad",
    [
        {"provider": ""},
        {"provider": "p" * 65},
        {"model": ""},
        {"model": "m" * 129},
        {"input_per_million": Decimal("-0.000001")},
        {"output_per_million": Decimal("1000000.000001")},
        {"input_per_million": Decimal("0.0000001")},
        {"output_per_million": Decimal("NaN")},
    ],
)
def test_price_entry_rejects_invalid(bad: dict[str, object]) -> None:
    value = entry("p", "m", "1", "1") | bad
    with pytest.raises(ValidationError):
        PriceEntry.model_validate(value)


def test_price_entry_accepts_bounds_and_trailing_zeros() -> None:
    PriceEntry.model_validate(entry("p" * 64, "m" * 128, "0", "1000000"))
    e = PriceEntry.model_validate(entry("p", "m", "0.123456", "1.50000000"))
    assert e.input_per_million == Decimal("0.123456")


def test_price_table_rejects_duplicate_pair_but_allows_case_variant() -> None:
    with pytest.raises(ValidationError):
        PriceTable.model_validate({"models": [entry("a", "b", "1", "1"), entry("a", "b", "2", "2")]})
    PriceTable.model_validate({"models": [entry("a", "b", "1", "1"), entry("A", "b", "2", "2")]})


def test_price_table_max_200_entries() -> None:
    models = [entry("p", f"m{i}", "1", "1") for i in range(201)]
    PriceTable.model_validate({"models": models[:200]})
    with pytest.raises(ValidationError):
        PriceTable.model_validate({"models": models})


# ------------------------------------------------------------------ format_amount
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal(0), "0.000000"),
        (Decimal("0.0000005"), "0.000000"),  # half-even rounds down to even
        (Decimal("0.0000015"), "0.000002"),  # half-even rounds up to even
        (Decimal("0.00000051"), "0.000001"),
        (Decimal("1E+7"), "10000000.000000"),  # no exponent, no separators
        (Decimal("1234567.1234565"), "1234567.123456"),
        (Decimal("1E-30"), "0.000000"),
    ],
)
def test_format_amount(value: Decimal, expected: str) -> None:
    assert format_amount(value) == expected


# ------------------------------------------------------------------ record_cost / estimate_cost
def test_record_cost_is_exact_and_unrounded() -> None:
    e = PriceEntry.model_validate(entry("p", "m", "0.000001", "0.000003"))
    assert record_cost(rec("p", "m", 1, 1), e) == Decimal("0.000000000004")


def test_estimate_cost_no_records() -> None:
    report = estimate_cost([], TABLE)
    assert report == CostReport(
        amount="0.000000", currency="USD", complete=True, unpriced_models=[], by_model=[]
    )


def test_estimate_cost_priced_records() -> None:
    records = [
        rec("openai", "gpt-4o", 1000, 500),  # 0.0025 + 0.005
        rec("openai", "gpt-4o", 1000, 0),  # 0.0025
        rec("anthropic", "claude-sonnet", 100, 100),  # 0.0003 + 0.0015
    ]
    report = estimate_cost(records, TABLE)
    assert report.amount == "0.011800"
    assert report.complete is True and report.unpriced_models == []
    assert [(m.provider, m.model, m.amount) for m in report.by_model] == [
        ("anthropic", "claude-sonnet", "0.001800"),
        ("openai", "gpt-4o", "0.010000"),
    ]


def test_total_rounds_unrounded_sum_not_sum_of_rounded() -> None:
    table = PriceTable.model_validate(
        {"models": [entry("a", "x", "0.6", "0"), entry("b", "y", "0.6", "0")]}
    )
    # Each pair costs 0.0000006 -> rounds to 0.000001; the sum 0.0000012 -> 0.000001.
    report = estimate_cost([rec("a", "x", 1, 0), rec("b", "y", 1, 0)], table)
    assert [m.amount for m in report.by_model] == ["0.000001", "0.000001"]
    assert report.amount == "0.000001"


def test_matching_is_case_sensitive_and_unpriced_sorted_unique() -> None:
    records = [
        rec("OpenAI", "gpt-4o", 10, 10),
        rec("openai", "GPT-4o", 10, 10),
        rec("unknown", "unknown", 5, 5),
        rec("OpenAI", "gpt-4o", 1, 1),
        rec("openai", "gpt-4o", 1_000_000, 0),
    ]
    report = estimate_cost(records, TABLE)
    assert report.complete is False
    assert [(u.provider, u.model) for u in report.unpriced_models] == [
        ("OpenAI", "gpt-4o"),
        ("openai", "GPT-4o"),
        ("unknown", "unknown"),
    ]
    assert report.amount == "2.500000"
    assert [(m.provider, m.model) for m in report.by_model] == [("openai", "gpt-4o")]


def test_empty_table_makes_every_pair_unpriced() -> None:
    report = estimate_cost([rec("openai", "gpt-4o", 10, 10)], PriceTable(currency="EUR"))
    assert report.currency == "EUR"
    assert report.amount == "0.000000" and report.by_model == []
    assert report.complete is False
    assert [(u.provider, u.model) for u in report.unpriced_models] == [("openai", "gpt-4o")]


def test_large_values_stay_exact() -> None:
    table = PriceTable.model_validate({"models": [entry("p", "m", "999999.999999", "1000000")]})
    records = [rec("p", "m", 10_000_000, 10_000_000)] * 50
    report = estimate_cost(records, table)
    # per record: 10 * 999999.999999 + 10 * 1000000 = 19999999.99999
    assert report.amount == "999999999.999500"


@settings(max_examples=50)
@given(price_tables(as_model=True))
def test_shared_strategy_builds_price_table(table: PriceTable) -> None:
    assert isinstance(table, PriceTable)
    assert estimate_cost([], table).amount == "0.000000"
