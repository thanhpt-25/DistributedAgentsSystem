# Feature: das-observability-operations, Property 3: Giới hạn cardinality và tuân thủ catalog
"""Property 3: Label cardinality bound and catalog conformance.

*For any* sequence of observations with arbitrary label values (random strings,
valid values, out-of-set values, arbitrary ``status_code`` and HTTP method) and
any label limit from 1 to 500:

- the Label_Cardinality of every metric is at most the product of
  (|Bounded_Label_Value| + 1) over enumerated labels times (limit + 1) over
  dynamic labels;
- the bounded label values equal those of a ``ReferenceBounder`` model;
- every (metric, label) pair gets at most one overflow warning;
- ``render()`` only contains catalog metric names (prefix ``das_``), each with
  exactly one HELP and one TYPE line, and only catalog label names;
- the output contains no configured secret as a substring.

**Validates: Requirements 1.4, 1.5, 2.10, 5.1, 5.2, 5.3, 5.4, 5.5, 5.7**
"""

from __future__ import annotations

import math
from collections import Counter as Tally
from dataclasses import dataclass, field
from typing import Any

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from prometheus_client.parser import text_string_to_metric_families

from das.clock import FakeClock
from das.config.schema import LabelLimitsConfig
from das.observability.labels import OVERFLOW_VALUE
from das.observability.metrics import (
    ALLOWED_LABEL_NAMES,
    CATALOG,
    CATALOG_BY_NAME,
    DYNAMIC_LABELS,
    HTTP_METHODS,
    UNMATCHED_ROUTE,
    GatewayMetrics,
    MetricSpec,
    MetricsRecorder,
)

# Metrics whose gauge is computed at scrape time via ``set_function``; skip direct writes.
_FUNCTION_GAUGES = frozenset({"das_worker_jobs_in_flight", "das_leases_held"})
_WRITABLE = tuple(spec for spec in CATALOG if spec.name not in _FUNCTION_GAUGES)

# Label values use lowercase letters, punctuation, escapes and non-ASCII; secrets use
# only uppercase ASCII letters, so a secret can only reach the output if the recorder
# itself leaks it (catalog HELP texts have no run of 8 uppercase letters).
_VALUE_ALPHABET = st.sampled_from(
    list("abcdefghijklmnopqrstuvwxyz0123456789") + list("-_./:,{}=\"'\\ ") + ["\n", "\t", "é", "漢", "😀"]
)
_random_text = st.text(_VALUE_ALPHABET, max_size=12)
_secrets = st.lists(
    st.text(st.sampled_from("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), min_size=8, max_size=32),
    min_size=1,
    max_size=4,
)
# Small pool so dynamic values repeat and overflow is exercised.
_dynamic_pool = st.sampled_from(["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta"])
_limit = st.one_of(st.integers(1, 5), st.integers(1, 500))
_route_templates = st.lists(
    st.sampled_from(["/v1/requests", "/v1/requests/{id}", "/v1/agents", "/v1/admin/dlq"]),
    unique=True,
    max_size=4,
)


@dataclass
class ReferenceBounder:
    """Model: seen value kept, new value accepted while under limit, else ``_other``."""

    enumerated: dict[str, frozenset[str]]
    limits: dict[str, int]
    seen: dict[tuple[str, str], list[str]] = field(default_factory=dict)
    overflowed: set[tuple[str, str]] = field(default_factory=set)

    def bound(self, metric: str, labels: dict[str, str]) -> tuple[tuple[str, str], ...]:
        out: dict[str, str] = {}
        for name, value in labels.items():
            if name in self.enumerated:
                ok = value in self.enumerated[name]
            else:
                seen = self.seen.setdefault((metric, name), [])
                ok = value in seen or len(seen) < self.limits[name]
                if ok and value not in seen:
                    seen.append(value)
            out[name] = value if ok else OVERFLOW_VALUE
            if not ok:
                self.overflowed.add((metric, name))
        return tuple(sorted(out.items()))


class CaptureLog:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def warning(self, event: str, **fields: Any) -> None:
        self.events.append((event, fields))

    def __getattr__(self, name: str) -> Any:  # info/debug/error: record too
        return lambda event, **fields: self.events.append((event, fields))


def _enumerated_for(spec: MetricSpec, routes: tuple[str, ...], currency: str | None) -> dict[str, frozenset[str]]:
    out: dict[str, frozenset[str]] = {}
    for label in spec.labels:
        if label in DYNAMIC_LABELS:
            continue
        if label == "route":
            out[label] = frozenset(routes) | {UNMATCHED_ROUTE}
        elif label == "currency":
            out[label] = frozenset({currency}) if currency else frozenset()
        else:
            out[label] = spec.allowed[label]
    return out


def _label_value(data: st.DataObject, spec: MetricSpec, label: str, enumerated: dict[str, frozenset[str]]) -> str:
    if label == "status_code":
        return str(data.draw(st.one_of(st.integers(100, 599), st.integers(-1000, 2000))))
    if label in DYNAMIC_LABELS:
        return data.draw(st.one_of(_dynamic_pool, _random_text))
    allowed = sorted(enumerated[label])
    choices = [_random_text, st.sampled_from([OVERFLOW_VALUE, "ok", "GET"])]
    if allowed:
        choices.insert(0, st.sampled_from(allowed))
    return data.draw(st.one_of(*choices))


def _family_samples(text: str) -> dict[str, list[Any]]:
    """Catalog name -> parsed samples (counter family names get ``_total`` back)."""
    out: dict[str, list[Any]] = {}
    for family in text_string_to_metric_families(text):
        name = family.name + "_total" if family.type == "counter" else family.name
        assert name not in out, f"duplicate family {name}"
        out[name] = list(family.samples)
    return out


@settings(max_examples=150, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    data=st.data(),
    limits=st.builds(
        LabelLimitsConfig, capability=_limit, queue=_limit, provider=_limit, model=_limit
    ),
    routes=_route_templates.map(tuple),
    currency=st.one_of(st.none(), st.just("USD")),
    secrets=_secrets,
    n_obs=st.integers(0, 60),
)
def test_cardinality_bound_and_catalog_conformance(
    data: st.DataObject,
    limits: LabelLimitsConfig,
    routes: tuple[str, ...],
    currency: str | None,
    secrets: list[str],
    n_obs: int,
) -> None:
    log = CaptureLog()
    recorder = MetricsRecorder(
        CATALOG, label_limits=limits, clock=FakeClock(), log=log, route_templates=routes, currency=currency
    )
    gateway = GatewayMetrics(recorder)

    enumerated = {spec.name: _enumerated_for(spec, routes, currency) for spec in CATALOG}
    dynamic_limits = {label: int(getattr(limits, label)) for label in DYNAMIC_LABELS}
    reference = {
        spec.name: ReferenceBounder(enumerated[spec.name], dynamic_limits) for spec in CATALOG
    }
    expected: dict[str, set[tuple[tuple[str, str], ...]]] = {spec.name: set() for spec in CATALOG}

    for _ in range(n_obs):
        if data.draw(st.integers(0, 4)) == 0:
            # Facade path: arbitrary HTTP method and status code (Req 2.10).
            route = data.draw(st.one_of(st.none(), st.sampled_from(routes or ("/x",)), _random_text))
            method = data.draw(
                st.one_of(
                    st.sampled_from(sorted(HTTP_METHODS)),
                    st.sampled_from(["get", "Post", "TRACE"]),
                    _random_text,
                )
            )
            status_code = data.draw(st.one_of(st.integers(100, 599), st.integers(-1000, 2000)))
            gateway.http_request(route, method, status_code, data.draw(st.floats(-10, 100)))
            upper = method.upper()
            norm_method = upper if upper in HTTP_METHODS else "_other"
            base = {"route": route or UNMATCHED_ROUTE, "method": norm_method}
            name = "das_http_requests_total"
            expected[name].add(reference[name].bound(name, {**base, "status_code": str(status_code)}))
            name = "das_http_request_duration_seconds"
            expected[name].add(reference[name].bound(name, dict(base)))
            continue
        spec = data.draw(st.sampled_from(_WRITABLE))
        labels = {lbl: _label_value(data, spec, lbl, enumerated[spec.name]) for lbl in spec.labels}
        value = data.draw(st.floats(0, 1000, allow_nan=False))
        passed = labels or None
        if spec.kind == "counter":
            recorder.inc(spec.name, passed, value)
        elif spec.kind == "histogram":
            recorder.observe(spec.name, passed, value)
        else:
            recorder.set(spec.name, passed, value)
        if spec.labels:
            expected[spec.name].add(reference[spec.name].bound(spec.name, labels))

    body, _ = recorder.render()
    text = body.decode("utf-8")

    # No recording errors happened: every write was valid.
    assert not [e for e in log.events if e[0] == "metric_record_failed"]

    # HELP / TYPE: exactly one of each per catalog name, and nothing else.
    help_names = Tally(line.split(" ")[2] for line in text.splitlines() if line.startswith("# HELP "))
    type_names = Tally(line.split(" ")[2] for line in text.splitlines() if line.startswith("# TYPE "))
    catalog_names = {spec.name for spec in CATALOG}
    assert set(help_names) == catalog_names and all(c == 1 for c in help_names.values())
    assert set(type_names) == catalog_names and all(c == 1 for c in type_names.values())
    assert all(name.startswith("das_") for name in catalog_names)

    families = _family_samples(text)
    assert set(families) <= catalog_names
    for name, samples in families.items():
        spec = CATALOG_BY_NAME[name]
        if spec.kind == "histogram":
            allowed_samples = {f"{name}_bucket", f"{name}_count", f"{name}_sum"}
        else:
            allowed_samples = {name}
        series: set[tuple[tuple[str, str], ...]] = set()
        for sample in samples:
            assert sample.name in allowed_samples, sample.name
            extra = {"le"} if sample.name.endswith("_bucket") else set()
            assert set(sample.labels) == set(spec.labels) | extra
            assert set(sample.labels) - {"le"} <= ALLOWED_LABEL_NAMES
            series.add(tuple(sorted((k, v) for k, v in sample.labels.items() if k != "le")))

        if spec.labels:
            # Bounded values match the reference model exactly.
            assert series == expected[name], name
            # Label_Cardinality bound (Req 5.5).
            bound = math.prod(
                dynamic_limits[lbl] + 1 if lbl in DYNAMIC_LABELS else len(enumerated[name][lbl]) + 1
                for lbl in spec.labels
            )
            assert len(series) <= bound, (name, len(series), bound)
            for lbl in spec.labels:
                if lbl in DYNAMIC_LABELS:
                    assert len({dict(s)[lbl] for s in series} - {OVERFLOW_VALUE}) <= dynamic_limits[lbl]
                else:
                    assert {dict(s)[lbl] for s in series} <= enumerated[name][lbl] | {OVERFLOW_VALUE}
        else:
            assert series == {()}

    # Overflow warnings: exactly one per overflowed (metric, label) pair, none otherwise.
    warned = Tally(
        (fields["metric"], fields["label"]) for event, fields in log.events if event == "metric_label_overflow"
    )
    assert all(count == 1 for count in warned.values()), warned
    expected_overflow = set().union(*(ref.overflowed for ref in reference.values()))
    assert set(warned) == expected_overflow
    for event, fields in log.events:
        if event == "metric_label_overflow":
            lbl = fields["label"]
            want = dynamic_limits[lbl] if lbl in DYNAMIC_LABELS else len(enumerated[fields["metric"]][lbl])
            assert fields["limit"] == want
            assert fields["overflow_value"] == OVERFLOW_VALUE

    # No configured secret leaks into the output (Req 1.5).
    for secret in secrets:
        assert secret not in text
