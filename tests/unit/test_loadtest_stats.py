"""Load test statistics: examples (das-scale-resilience task 12.1; Phase 7 Req 4.3, 4.6). The property test is
``tests/property/test_loadtest_stats.py``."""

from __future__ import annotations

import pytest

from das.loadtest.stats import StepStats, percentile, saturation, summarize, within_limits

TEN = [float(n) for n in range(1, 11)]  # 1.0 .. 10.0


@pytest.mark.parametrize(
    ("p", "expected"), [(0, 1.0), (10, 1.0), (11, 2.0), (50, 5.0), (90, 9.0), (91, 10.0), (100, 10.0)]
)
def test_nearest_rank_on_ten_samples(p: float, expected: float) -> None:
    assert percentile(TEN, p) == expected


def test_the_order_of_the_samples_does_not_matter() -> None:
    assert percentile([9.0, 1.0, 5.0, 3.0, 7.0], 50) == percentile([1.0, 3.0, 5.0, 7.0, 9.0], 50) == 5.0


def test_one_sample_is_every_percentile() -> None:
    assert {percentile([42.0], p) for p in (0, 1, 50, 99, 100)} == {42.0}


def test_a_percentile_is_a_measured_value_not_an_interpolation() -> None:
    assert percentile([10.0, 20.0], 50) == 10.0 and percentile([10.0, 20.0], 51) == 20.0


def test_ninety_percent_of_ten_is_rank_nine_not_ten() -> None:
    assert percentile(TEN, 90) == 9.0


def test_a_percentile_with_a_decimal_part_is_exact_not_off_by_a_float_rounding_error() -> None:
    """64.4 % of 250 samples is exactly 161: in floating point 64.4 * 250 / 100 is 161.00000000000003, which a plain
    ceiling takes to rank 162."""
    assert percentile([float(n) for n in range(1, 251)], 64.4) == 161.0
    assert 64.4 * 250 / 100 > 161  # the trap is real


@pytest.mark.parametrize("p", [-0.1, 100.1, 1000])
def test_a_percentile_outside_0_to_100_is_refused(p: float) -> None:
    with pytest.raises(ValueError, match="between 0 and 100"):
        percentile(TEN, p)


def test_no_samples_have_no_percentile() -> None:
    with pytest.raises(ValueError, match="no samples"):
        percentile([], 50)


def test_summarize_reports_the_percentiles_the_mean_and_the_extremes() -> None:
    s = summarize(TEN)
    assert (s.count, s.min, s.max, s.mean, s.p50, s.p90, s.p99) == (10, 1.0, 10.0, 5.5, 5.0, 9.0, 10.0)
    assert s.to_dict()["p99"] == 10.0


def test_summarize_of_nothing_has_a_count_of_zero_and_no_numbers() -> None:
    s = summarize([])
    assert s.count == 0 and s.to_dict() == {"count": 0, **dict.fromkeys(("mean", "min", "max", "p50", "p90", "p99"))}


# --------------------------------------------------------------------------- saturation


def step(rate: float, offered: int = 100, errors: int = 0, p99: float | None = 100.0) -> StepStats:
    return StepStats(rate, offered, offered - errors if p99 is not None else 0, errors, p99)


LIMITS = {"max_error_rate": 0.05, "max_p99_ms": 1000.0}


def test_the_saturation_point_is_the_highest_rate_inside_both_limits() -> None:
    steps = [step(2), step(5), step(10, p99=900), step(20, p99=1500), step(40, errors=50, p99=5000)]
    assert saturation(steps, **LIMITS) == 10


def test_a_step_exactly_on_a_limit_is_inside() -> None:
    assert saturation([step(5, errors=5, p99=1000.0)], **LIMITS) == 5


def test_a_step_just_over_either_limit_is_outside() -> None:
    assert saturation([step(5, errors=6)], **LIMITS) is None
    assert saturation([step(5, p99=1000.1)], **LIMITS) is None


def test_a_later_step_that_is_inside_again_does_not_count() -> None:
    steps = [step(2), step(5, p99=3000), step(10)]
    assert saturation(steps, **LIMITS) == 2


def test_steps_are_taken_in_rate_order_whatever_order_they_are_given_in() -> None:
    steps = [step(10, p99=5000), step(2), step(5)]
    assert saturation(steps, **LIMITS) == 5


def test_none_when_even_the_lowest_rate_is_outside() -> None:
    assert saturation([step(2, p99=9999), step(5)], **LIMITS) is None


def test_none_without_steps() -> None:
    assert saturation([], **LIMITS) is None


def test_a_step_where_nothing_finished_has_no_p99_and_is_outside() -> None:
    nothing = step(5, errors=100, p99=None)
    assert nothing.error_rate == 1.0 and not within_limits(nothing, 1.0, 1e9)
    assert saturation([step(2), nothing], max_error_rate=1.0, max_p99_ms=1e9) == 2


def test_the_error_rate_of_a_step_that_offered_nothing_is_zero() -> None:
    assert StepStats(1.0, 0, 0, 0, None).error_rate == 0.0
