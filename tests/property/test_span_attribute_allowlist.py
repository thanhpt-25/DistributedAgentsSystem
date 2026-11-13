"""Property 8: Allowlist attribute của Span.

*For any* dict attribute tùy ý (key ngẫu nhiên cộng key trong allowlist, value có
thể chứa prompt, instruction, API_Key hoặc Admin_API_Key đã cấu hình) truyền vào
``span(...)``, ``set_attributes`` và ``add_event``, mọi Span đã kết thúc (thu bằng
``InMemorySpanExporter``) chỉ có key thuộc ``ALLOWED_ATTRIBUTES``. Không attribute
value hay event attribute nào chứa secret đã cấu hình.

Superseded boundary (das-multi-tenant task 8.1; Phase 5 Req 8.3, 8.4): v2 Req 20.3 forbade any Span attribute carrying
a tenant identity. The allow-list now has exactly one more name, ``das.tenant`` (a tenant id), set on ``gateway.submit``
and ``manager.request``; the rest of 20.3 stands: no API_Key, Admin_API_Key or Key_Fingerprint in any attribute, which
this property still checks with the configured secrets and ``tests/unit/test_tracing_tenant.py`` checks end to end.

**Validates: Requirements 6.9**
"""
from __future__ import annotations

from typing import Any
from unittest import mock

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from das.clock import FakeClock
from das.config.schema import TracingConfig
from das.observability import logging as das_logging
from das.observability.redaction import Redactor
from das.observability.tracing import ALLOWED_ATTRIBUTES, OtelTracer, build_tracer

# Hex alphabet keeps generated secrets disjoint from fixed strings such as the
# default ``das.role``/``das.agent_id`` values, span names and event names.
_SECRET = st.text(alphabet="0123456789abcdef", min_size=16, max_size=40)

_CONTENT_KEYS = ("prompt", "instruction", "api_key", "admin_api_key", "output", "x-api-key")
_KEYS = st.one_of(
    st.sampled_from(sorted(ALLOWED_ATTRIBUTES)),
    st.sampled_from(_CONTENT_KEYS),
    st.text(min_size=1, max_size=20),
)


def _values(secrets: tuple[str, str]) -> st.SearchStrategy[Any]:
    """Values that may be prompts/instructions embedding a configured secret."""
    leaky = st.builds(
        lambda pre, s, post: f"{pre}{s}{post}",
        st.text(max_size=20),
        st.sampled_from(secrets),
        st.text(max_size=20),
    )
    return st.one_of(
        leaky,
        st.sampled_from(secrets),
        st.text(max_size=40),
        st.integers(),
        st.booleans(),
        st.floats(allow_nan=False),
        st.none(),
        st.lists(st.text(max_size=5), max_size=3),
    )


@st.composite
def _scenario(draw: st.DrawFn) -> dict[str, Any]:
    api_key = draw(_SECRET)
    admin_key = draw(_SECRET.filter(lambda s: s != api_key))
    values = _values((api_key, admin_key))
    attrs = st.dictionaries(_KEYS, values, max_size=10)
    return {
        "secrets": (api_key, admin_key),
        "start": draw(attrs),
        "sets": draw(st.lists(attrs, max_size=3)),
        "events": draw(st.lists(attrs, max_size=3)),
        "child": draw(st.booleans()),
    }


def _tracer(exporter: InMemorySpanExporter) -> OtelTracer:
    cfg = TracingConfig(enabled=True, otlp_endpoint="http://otel:4317")
    t = build_tracer(cfg, "manager", "m-1", FakeClock(), exporter=exporter, start_thread=False)
    assert isinstance(t, OtelTracer)
    return t


def _flush(t: OtelTracer) -> None:
    while t.processor.queued:
        assert t.processor.export_once()


@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(sc=_scenario())
def test_finished_spans_only_carry_allowlisted_secret_free_attributes(sc: dict[str, Any]) -> None:
    redactor = Redactor(sc["secrets"])
    exporter = InMemorySpanExporter()
    with mock.patch.object(das_logging, "_redactor", redactor):
        tracer = _tracer(exporter)
        with tracer.span("manager.request", attributes=sc["start"], root=True) as outer:
            for attrs in sc["sets"]:
                outer.set_attributes(attrs)
            for i, attrs in enumerate(sc["events"]):
                outer.add_event(f"evt{i}", attrs)
            if sc["child"]:
                with tracer.span("manager.job", attributes=sc["start"]) as inner:
                    for attrs in sc["sets"]:
                        inner.set_attributes(attrs)
                    for attrs in sc["events"]:
                        inner.add_event("child_evt", attrs)
        _flush(tracer)

    spans = exporter.get_finished_spans()
    assert len(spans) == (2 if sc["child"] else 1)

    def check(attributes: Any) -> None:
        for key, value in dict(attributes or {}).items():
            assert key in ALLOWED_ATTRIBUTES, key
            assert isinstance(value, str | int) and not isinstance(value, bool), (key, value)
            if isinstance(value, str):
                for secret in sc["secrets"]:
                    assert secret not in value, (key, value)
                assert not redactor.contains_secret(value), (key, value)

    for span in spans:
        check(span.attributes)
        for event in span.events:
            check(event.attributes)
