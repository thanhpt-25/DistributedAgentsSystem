"""``loadtest-compare`` (das-scale-resilience task 12.1; Phase 7 Req 4.7): throughput fell or p99 rose by more than the
ratio is a regression; reports of different profiles are refused; other differences are notes."""

from __future__ import annotations

import copy
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from das.loadtest.compare import MAX_REGRESSION_LIMIT, CompareError, compare


def make(throughput: float = 10.0, p99: float | None = 1000.0, **context: Any) -> dict[str, Any]:
    return {
        "schema": 1, "profile": {"name": "steady"}, "transport": "rabbitmq", "settings": {"managers": "2"},
        "machine": {"cpu_count": 8}, "das_version": "1.0",
        "results": {"throughput_rps": throughput, "total_ms": {"count": 10, "p99": p99}},
        **context,
    }


# --------------------------------------------------------------------------- the verdict


def test_the_same_report_against_itself_has_not_regressed() -> None:
    verdict = compare(make(), make(), 0.0)
    assert not verdict.regressed and verdict.regressions == () and verdict.notes == ()


def test_a_throughput_fall_beyond_the_ratio_is_a_regression() -> None:
    verdict = compare(make(10.0), make(7.9), 0.2)
    assert verdict.regressed and "throughput fell from 10.00 to 7.90" in verdict.regressions[0]


def test_a_throughput_fall_inside_the_ratio_is_not() -> None:
    assert not compare(make(10.0), make(8.1), 0.2).regressed


def test_a_throughput_fall_of_exactly_the_ratio_is_not_more_than_it() -> None:
    assert not compare(make(10.0), make(9.0), 0.1).regressed
    assert compare(make(10.0), make(8.99), 0.1).regressed


def test_a_p99_rise_beyond_the_ratio_is_a_regression() -> None:
    verdict = compare(make(p99=1000.0), make(p99=1201.0), 0.2)
    assert verdict.regressed and "p99 rose from 1000 ms to 1201 ms" in verdict.regressions[0]


def test_a_p99_rise_inside_or_exactly_at_the_ratio_is_not() -> None:
    assert not compare(make(p99=1000.0), make(p99=1100.0), 0.2).regressed
    assert not compare(make(p99=1000.0), make(p99=1200.0), 0.2).regressed


def test_better_numbers_are_never_a_regression() -> None:
    assert not compare(make(10.0, 1000.0), make(50.0, 10.0), 0.0).regressed


def test_both_regressions_are_reported_together() -> None:
    assert len(compare(make(10.0, 1000.0), make(1.0, 9000.0), 0.1).regressions) == 2


def test_a_new_run_in_which_nothing_completed_is_a_regression() -> None:
    verdict = compare(make(10.0, 1000.0), make(0.0, None), 0.1)
    assert "no Request completed in the new run" in verdict.regressions


def test_no_base_p99_means_no_p99_comparison() -> None:
    assert not compare(make(10.0, None), make(10.0, 5000.0), 0.1).regressed


def test_a_zero_ratio_flags_any_worsening() -> None:
    assert compare(make(10.0, 1000.0), make(9.99, 1000.0), 0.0).regressed
    assert compare(make(10.0, 1000.0), make(10.0, 1000.1), 0.0).regressed


def test_the_verdict_dict_has_both_sides_of_each_number() -> None:
    d = compare(make(10.0, 1000.0), make(5.0, 2000.0), 0.1).to_dict()
    assert d["regressed"] is True and d["throughput_rps"] == {"base": 10.0, "new": 5.0}
    assert d["p99_ms"] == {"base": 1000.0, "new": 2000.0} and len(d["regressions"]) == 2 and d["notes"] == []


# --------------------------------------------------------------------------- what can be compared


def test_reports_of_different_profiles_are_refused() -> None:
    other = make()
    other["profile"] = {"name": "ramp"}
    with pytest.raises(CompareError, match="not comparable.*'steady'.*'ramp'"):
        compare(make(), other, 0.1)


@pytest.mark.parametrize("field", ["transport", "settings", "machine", "das_version"])
def test_a_difference_in_context_is_a_note_not_a_refusal(field: str) -> None:
    other = copy.deepcopy(make())
    other[field] = "changed" if field != "settings" else {"managers": "5"}
    verdict = compare(make(), other, 0.1)
    assert len(verdict.notes) == 1 and verdict.notes[0].startswith(f"{field} differs")


@pytest.mark.parametrize(
    "bad", [None, [], {}, {"schema": 2, "results": {}}, {"schema": 1}, {"schema": 1, "results": []}]
)
def test_a_file_that_is_not_a_capacity_report_is_refused_naming_which(bad: Any) -> None:
    with pytest.raises(CompareError, match="base report is not a Capacity_Report"):
        compare(bad, make(), 0.1)
    with pytest.raises(CompareError, match="new report is not a Capacity_Report"):
        compare(make(), bad, 0.1)


def test_a_report_without_throughput_or_total_time_is_refused() -> None:
    broken = make()
    del broken["results"]["throughput_rps"]
    with pytest.raises(CompareError, match="no throughput or total time"):
        compare(broken, make(), 0.1)


@pytest.mark.parametrize("ratio", [-0.01, MAX_REGRESSION_LIMIT + 0.01])
def test_a_ratio_outside_its_range_is_refused(ratio: float) -> None:
    with pytest.raises(CompareError, match="max_regression"):
        compare(make(), make(), ratio)


# --------------------------------------------------------------------------- properties of the verdict


numbers = st.floats(min_value=0.001, max_value=1e6, allow_nan=False)
ratios = st.floats(min_value=0, max_value=1, allow_nan=False)


@given(numbers, numbers, ratios)
def test_a_run_compared_with_itself_never_regresses(throughput: float, p99: float, ratio: float) -> None:
    assert not compare(make(throughput, p99), make(throughput, p99), ratio).regressed


@given(numbers, numbers, ratios, st.floats(min_value=1.0, max_value=5.0))
def test_a_run_that_is_no_worse_never_regresses(throughput: float, p99: float, ratio: float, better: float) -> None:
    assert not compare(make(throughput, p99), make(throughput * better, p99 / better), ratio).regressed


@given(numbers, numbers, st.floats(min_value=0, max_value=0.999), st.floats(min_value=1.01, max_value=5.0))
def test_a_throughput_fall_past_the_ratio_always_regresses(
    throughput: float, p99: float, ratio: float, factor: float
) -> None:
    worse = throughput * (1 - ratio) / factor  # a ratio of 1 is left out: no fall is more than 100 %
    assert compare(make(throughput, p99), make(worse, p99), ratio).regressed


@given(numbers, numbers, ratios, ratios)
def test_a_looser_ratio_never_turns_a_pass_into_a_regression(throughput: float, p99: float, a: float, b: float) -> None:
    tight, loose = sorted((a, b))
    new = make(throughput * 0.8, p99 * 1.2)
    if not compare(make(throughput, p99), new, tight).regressed:
        assert not compare(make(throughput, p99), new, loose).regressed
