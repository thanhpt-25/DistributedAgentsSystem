"""Property 4: Không mất observation khi overflow.

For any sequence of observations with arbitrary label values (valid values, values outside
the allowed set, random strings, arbitrary ``status_code`` / HTTP method) and any label limit
in 1..500, the rendered and re-parsed output satisfies:

* for every counter, the sum over all label combinations (including ones with ``_other``)
  equals the sum of every applied increment;
* for every histogram, the sum of ``_count`` and of ``_sum`` over all label combinations
  equals the number of observations and the sum of observed values.

Amounts and values are non-negative multiples of 0.25 so float addition stays exact.

**Validates: Requirements 5.6**
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from prometheus_client.parser import text_string_to_metric_families

from das.clock import FakeClock
from das.config.schema import LabelLimitsConfig
from das.observability.metrics import (
    CATALOG,
    DYNAMIC_LABELS,
    UNMATCHED_ROUTE,
    MetricSpec,
    MetricsRecorder,
)

ROUTES = ("/v1/requests", "/v1/requests/{request_id}", "/v1/dlq/{record_id}/replay")
CURRENCY = "USD"

COUNTERS = tuple(s for s in CATALOG if s.kind == "counter")
HISTOGRAMS = tuple(s for s in CATALOG if s.kind == "histogram")
RECORDED = COUNTERS + HISTOGRAMS

# Small pool of dynamic values so repeats (kept) and new values beyond the limit (overflow) mix.
_DYNAMIC_POOL = tuple(f"v{i}" for i in range(12))
_random_text = st.text(max_size=12)


def _allowed(spec: MetricSpec, label: str) -> tuple[str, ...]:
    if label == "route":
        return ROUTES + (UNMATCHED_ROUTE,)
    if label == "currency":
        return (CURRENCY,)
    return tuple(sorted(spec.allowed.get(label, ())))


def _label_value(spec: MetricSpec, label: str) -> st.SearchStrategy[Any]:
    options: list[st.SearchStrategy[Any]] = [_random_text]
    if label in DYNAMIC_LABELS:
        options.append(st.sampled_from(_DYNAMIC_POOL))
    else:
        options.append(st.sampled_from(_allowed(spec, label)))
    if label == "status_code":
        options.append(st.integers(min_value=-1000, max_value=1000))
    if label == "method":
        options.append(st.sampled_from(("get", "BREW", "PROPFIND", "")))
    return st.one_of(*options)


_quarter = st.integers(min_value=0, max_value=4000).map(lambda n: n / 4)


@st.composite
def _observation(draw: st.DrawFn) -> tuple[MetricSpec, dict[str, Any], float]:
    spec = draw(st.sampled_from(RECORDED))
    labels = {label: draw(_label_value(spec, label)) for label in spec.labels}
    return spec, labels, draw(_quarter)


_limits = st.builds(
    LabelLimitsConfig,
    capability=st.one_of(st.integers(1, 4), st.integers(1, 500)),
    queue=st.one_of(st.integers(1, 4), st.integers(1, 500)),
    provider=st.one_of(st.integers(1, 4), st.integers(1, 500)),
    model=st.one_of(st.integers(1, 4), st.integers(1, 500)),
)


class _NullLog:
    def warning(self, *args: Any, **kwargs: Any) -> None:
        return None


def _parse(body: bytes) -> dict[str, float]:
    """Sum every sample value by sample name across all label combinations."""
    totals: dict[str, float] = defaultdict(float)
    for family in text_string_to_metric_families(body.decode("utf-8")):
        for sample in family.samples:
            totals[sample.name] += sample.value
    return totals


@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(limits=_limits, observations=st.lists(_observation(), max_size=120))
def test_no_observation_lost_on_overflow(
    limits: LabelLimitsConfig, observations: list[tuple[MetricSpec, dict[str, Any], float]]
) -> None:
    recorder = MetricsRecorder(
        CATALOG,
        label_limits=limits,
        clock=FakeClock(),
        log=_NullLog(),
        route_templates=ROUTES,
        currency=CURRENCY,
    )
    counter_sum: dict[str, float] = defaultdict(float)
    hist_count: dict[str, int] = defaultdict(int)
    hist_sum: dict[str, float] = defaultdict(float)

    for spec, labels, value in observations:
        if spec.kind == "counter":
            recorder.inc(spec.name, labels, value)
            counter_sum[spec.name] += value
        else:
            recorder.observe(spec.name, labels, value)
            hist_count[spec.name] += 1
            hist_sum[spec.name] += value

    body, _ = recorder.render()
    rendered = _parse(body)

    for spec in COUNTERS:
        assert rendered.get(spec.name, 0.0) == counter_sum[spec.name], spec.name
    for spec in HISTOGRAMS:
        assert rendered.get(f"{spec.name}_count", 0.0) == hist_count[spec.name], spec.name
        assert rendered.get(f"{spec.name}_sum", 0.0) == hist_sum[spec.name], spec.name
