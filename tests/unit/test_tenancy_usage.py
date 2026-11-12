"""Usage report assembly (das-multi-tenant task 1.4; Phase 5 Req 6.3, 6.5).

Pure functions: the date range, the parsing of a Usage_Rollup hash, and the report with its cost computed at read
time."""

from __future__ import annotations

from datetime import date

import pytest

from das.ops.cost import PriceEntry, PriceTable
from das.tenancy.usage import (
    DEFAULT_RANGE_DAYS,
    MAX_RANGE_DAYS,
    UsageRangeError,
    build_report,
    day_key,
    parse_range,
    parse_rollup,
)

TODAY = date(2026, 11, 12)
PRICES = PriceTable(
    currency="USD",
    models=[PriceEntry(provider="openai", model="gpt-4o", input_per_million="2", output_per_million="4")],
)


# --------------------------------------------------------------------------- the date range


def test_the_default_range_is_the_last_seven_days_including_today() -> None:
    r = parse_range(None, None, TODAY)
    assert (r.start, r.end) == (date(2026, 11, 6), TODAY)
    assert len(r.days()) == DEFAULT_RANGE_DAYS == 7


def test_only_to_given_counts_seven_days_back_from_it() -> None:
    r = parse_range(None, "2026-11-10", TODAY)
    assert (r.start, r.end) == (date(2026, 11, 4), date(2026, 11, 10))


def test_only_from_given_runs_to_today() -> None:
    r = parse_range("2026-11-01", None, TODAY)
    assert (r.start, r.end) == (date(2026, 11, 1), TODAY)


def test_a_single_day_is_a_valid_range_and_days_are_ascending_and_inclusive() -> None:
    assert parse_range("2026-11-03", "2026-11-03", TODAY).days() == [date(2026, 11, 3)]
    assert parse_range("2026-11-01", "2026-11-03", TODAY).days() == [
        date(2026, 11, 1), date(2026, 11, 2), date(2026, 11, 3),
    ]  # fmt: skip


def test_the_longest_range_is_92_days_and_93_is_refused_naming_to() -> None:
    assert len(parse_range("2026-08-12", "2026-11-11", TODAY).days()) == MAX_RANGE_DAYS == 92
    with pytest.raises(UsageRangeError) as info:
        parse_range("2026-08-11", "2026-11-11", TODAY)
    assert info.value.parameter == "to"


def test_an_inverted_range_is_refused_naming_from() -> None:
    with pytest.raises(UsageRangeError) as info:
        parse_range("2026-11-05", "2026-11-04", TODAY)
    assert info.value.parameter == "from"


@pytest.mark.parametrize(
    "bad",
    [
        "2026-13-01", "2026-02-30", "2026-1-1", "20261101", "2026-11-01T00:00", "", " 2026-11-01", "yesterday",
        "٢٠٢٦-١١-٠١",  # digits of another script
    ],
)  # fmt: skip
def test_a_malformed_date_is_refused_naming_the_offending_parameter(bad: str) -> None:
    with pytest.raises(UsageRangeError) as info:
        parse_range(bad, None, TODAY)
    assert info.value.parameter == "from"
    with pytest.raises(UsageRangeError) as info:
        parse_range(None, bad, TODAY)
    assert info.value.parameter == "to"


def test_the_range_error_is_a_value_error() -> None:
    assert issubclass(UsageRangeError, ValueError)


def test_the_day_key_is_the_compact_utc_date() -> None:
    assert day_key(date(2026, 11, 2)) == "20261102"
    assert day_key(date(2027, 1, 31)) == "20270131"


# --------------------------------------------------------------------------- parsing one rollup hash


def test_an_empty_rollup_is_all_zero() -> None:
    u = parse_rollup({})
    assert (u.submitted, dict(u.finished), u.models) == (0, {"Completed": 0, "Failed": 0, "Cancelled": 0}, ())


def test_counters_and_tokens_are_parsed_and_models_merge_and_sort() -> None:
    u = parse_rollup(
        {
            "submitted": "5",
            "finished/Completed": "3",
            "finished/Failed": "1",
            "finished/Cancelled": "0",
            "tokens/in/openai/gpt-4o": "1200",
            "tokens/out/openai/gpt-4o": "800",
            "tokens/total/openai/gpt-4o": "2000",
            "tokens/in/anthropic/claude": "10",
            "tokens/out/anthropic/claude": "20",
            "tokens/total/anthropic/claude": "30",
        }
    )
    assert u.submitted == 5 and dict(u.finished) == {"Completed": 3, "Failed": 1, "Cancelled": 0}
    assert [(m.provider, m.model, m.input_tokens, m.output_tokens, m.total_tokens) for m in u.models] == [
        ("anthropic", "claude", 10, 20, 30),
        ("openai", "gpt-4o", 1200, 800, 2000),
    ]


def test_a_model_name_may_contain_slashes_and_colons() -> None:
    u = parse_rollup({"tokens/in/ollama/meta/llama3:8b": "7", "tokens/out/ollama/meta/llama3:8b": "3"})
    (m,) = u.models
    assert (m.provider, m.model, m.input_tokens, m.output_tokens) == ("ollama", "meta/llama3:8b", 7, 3)


def test_a_missing_total_falls_back_to_input_plus_output() -> None:
    (m,) = parse_rollup({"tokens/in/p/m": "4", "tokens/out/p/m": "6"}).models
    assert m.total_tokens == 10


def test_unknown_fields_and_non_integer_values_are_ignored() -> None:
    u = parse_rollup(
        {"submitted": "x", "surprise": "9", "tokens/in/p": "1", "finished/Weird": "4", "tokens/up/p/m": "3"}
    )
    assert (u.submitted, u.models) == (0, ())
    assert "Weird" not in dict(u.finished)


# --------------------------------------------------------------------------- the report


ROLLUPS = {
    date(2026, 11, 10): {
        "submitted": "4",
        "finished/Completed": "3",
        "finished/Failed": "1",
        "tokens/in/openai/gpt-4o": "1000000",
        "tokens/out/openai/gpt-4o": "500000",
        "tokens/total/openai/gpt-4o": "1500000",
    },
    date(2026, 11, 12): {
        "submitted": "2",
        "finished/Completed": "2",
        "tokens/in/openai/gpt-4o": "1000000",
        "tokens/out/openai/gpt-4o": "0",
        "tokens/total/openai/gpt-4o": "1000000",
        "tokens/in/other/unpriced": "5",
        "tokens/out/other/unpriced": "5",
        "tokens/total/other/unpriced": "10",
    },
    date(2026, 1, 1): {"submitted": "99"},  # outside the range: ignored
}


def report(prices: PriceTable | None) -> dict:
    return build_report("team-a", parse_range("2026-11-10", "2026-11-12", TODAY), ROLLUPS, prices)


def test_the_report_has_one_entry_per_day_even_without_data() -> None:
    r = report(None)
    assert (r["tenant"], r["from"], r["to"]) == ("team-a", "2026-11-10", "2026-11-12")
    assert [d["date"] for d in r["days"]] == ["2026-11-10", "2026-11-11", "2026-11-12"]
    zeros = {"Completed": 0, "Failed": 0, "Cancelled": 0}
    assert r["days"][1] == {"date": "2026-11-11", "requests": {"submitted": 0, "finished": zeros}, "models": []}


def test_without_a_price_table_every_cost_is_null() -> None:
    r = report(None)
    assert r["totals"]["estimated_cost"] is None
    assert all(m["estimated_cost"] is None for d in r["days"] for m in d["models"])


def test_totals_sum_tokens_and_requests_over_the_range_and_ignore_days_outside_it() -> None:
    t = report(None)["totals"]
    assert (t["input_tokens"], t["output_tokens"], t["total_tokens"]) == (2_000_005, 500_005, 2_500_010)
    assert t["requests"] == {"submitted": 6, "finished": {"Completed": 5, "Failed": 1, "Cancelled": 0}}


def test_cost_is_computed_at_read_time_per_model_and_in_total() -> None:
    r = report(PRICES)
    d10, _, d12 = r["days"]
    assert d10["models"] == [
        {
            "provider": "openai",
            "model": "gpt-4o",
            "input_tokens": 1_000_000,
            "output_tokens": 500_000,
            "total_tokens": 1_500_000,
            "estimated_cost": "4.000000",
        }
    ]
    by_model = {m["model"]: m for m in d12["models"]}
    assert by_model["gpt-4o"]["estimated_cost"] == "2.000000"
    assert by_model["unpriced"]["estimated_cost"] is None  # not priced by the table: null, as in v2 Req 10.7
    assert r["totals"]["estimated_cost"] == {"currency": "USD", "amount": "6.000000", "complete": False}


def test_the_total_is_complete_when_every_model_is_priced() -> None:
    only_priced = {date(2026, 11, 10): ROLLUPS[date(2026, 11, 10)]}
    r = build_report("t", parse_range("2026-11-10", "2026-11-10", TODAY), only_priced, PRICES)
    assert r["totals"]["estimated_cost"] == {"currency": "USD", "amount": "4.000000", "complete": True}


def test_changing_the_prices_re_prices_the_same_history() -> None:
    dearer = PriceTable(
        currency="EUR",
        models=[PriceEntry(provider="openai", model="gpt-4o", input_per_million="4", output_per_million="8")],
    )
    assert report(dearer)["totals"]["estimated_cost"]["amount"] == "12.000000"
    assert report(dearer)["totals"]["estimated_cost"]["currency"] == "EUR"
