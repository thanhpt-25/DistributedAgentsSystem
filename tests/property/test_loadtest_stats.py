"""Property 4: load test percentiles are monotone in the percentile and lie within the samples (das-scale-resilience
task 12.1; Phase 7 Req 4.6, 6.4), checked against an oracle that works from the definition, not from the code's formula:
the nearest-rank percentile is the smallest sample that at least p percent of the samples are at or below."""

from __future__ import annotations

from fractions import Fraction

from hypothesis import given
from hypothesis import strategies as st

from das.loadtest.stats import StepStats, percentile, saturation, summarize, within_limits

samples = st.lists(
    st.floats(min_value=0, max_value=1e6, allow_nan=False, allow_infinity=False), min_size=1, max_size=200
)
percents = st.one_of(st.integers(0, 100), st.floats(min_value=0, max_value=100, allow_nan=False))


def oracle(values: list[float], p: float) -> float:
    """The smallest sample x with count(<= x) * 100 >= p * n, in exact arithmetic."""
    need = Fraction(repr(p)) * len(values)  # the decimal the caller wrote, not the nearest binary float
    return min(x for x in values if sum(1 for v in values if v <= x) * 100 >= need)


@given(samples, percents)
def test_a_percentile_is_the_nearest_rank_of_the_definition(values: list[float], p: float) -> None:
    assert percentile(values, p) == oracle(values, p)


@given(samples, percents)
def test_a_percentile_is_one_of_the_samples_and_inside_their_bounds(values: list[float], p: float) -> None:
    result = percentile(values, p)
    assert result in values and min(values) <= result <= max(values)


@given(samples, percents, percents)
def test_percentiles_never_decrease_as_the_percentile_grows(values: list[float], a: float, b: float) -> None:
    low, high = sorted((a, b))
    assert percentile(values, low) <= percentile(values, high)


@given(samples)
def test_the_ends_are_the_extremes(values: list[float]) -> None:
    assert percentile(values, 0) == min(values) and percentile(values, 100) == max(values)


@given(samples, st.randoms())
def test_the_order_of_the_samples_never_matters(values: list[float], rnd) -> None:  # noqa: ANN001
    shuffled = list(values)
    rnd.shuffle(shuffled)
    assert all(percentile(values, p) == percentile(shuffled, p) for p in (0, 25, 50, 90, 99, 100))


@given(samples)
def test_a_summary_is_ordered_and_its_mean_is_between_the_extremes(values: list[float]) -> None:
    s = summarize(values)
    assert s.count == len(values)
    assert s.min <= s.p50 <= s.p90 <= s.p99 <= s.max  # type: ignore[operator]
    assert s.min - 1e-6 <= s.mean <= s.max + 1e-6  # type: ignore[operator]


step_lists = st.lists(
    st.builds(
        StepStats,
        rate=st.integers(1, 100).map(float),
        offered=st.integers(1, 500),
        completed=st.integers(0, 500),
        errors=st.integers(0, 500),
        p99_ms=st.one_of(st.none(), st.floats(0, 1e5, allow_nan=False)),
    ),
    max_size=12,
    unique_by=lambda s: s.rate,
)
limits = st.tuples(st.floats(0, 1, allow_nan=False), st.floats(0, 1e5, allow_nan=False))


@given(step_lists, limits)
def test_the_saturation_point_is_a_rate_every_lower_step_stayed_inside_the_limits_for(
    steps: list[StepStats], bounds: tuple[float, float]
) -> None:
    max_error, max_p99 = bounds
    result = saturation(steps, max_error_rate=max_error, max_p99_ms=max_p99)
    ordered = sorted(steps, key=lambda s: s.rate)
    if result is None:
        assert not ordered or not within_limits(ordered[0], max_error, max_p99)
        return
    assert result in {s.rate for s in steps}
    assert all(within_limits(s, max_error, max_p99) for s in ordered if s.rate <= result)
    above = [s for s in ordered if s.rate > result]
    assert not above or not within_limits(above[0], max_error, max_p99), "the next step up was outside the limits"


@given(step_lists, limits, st.floats(0, 1e5, allow_nan=False))
def test_looser_limits_never_lower_the_saturation_point(
    steps: list[StepStats], bounds: tuple[float, float], extra: float
) -> None:
    max_error, max_p99 = bounds
    tight = saturation(steps, max_error_rate=max_error, max_p99_ms=max_p99)
    loose = saturation(steps, max_error_rate=min(1.0, max_error + 0.1), max_p99_ms=max_p99 + extra)
    assert tight is None or (loose is not None and loose >= tight)
