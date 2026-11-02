"""Unit tests for log-trace correlation and new secret redaction (task 6.7).

Requirements 8.9, 8.10, 8.11, 11.9, 19.3.
"""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest
import yaml
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, ALWAYS_ON
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags

import das.cli as cli
from das.config import load_config
from das.observability import configure_logging, get_logger
from das.observability.logging import add_trace_ids, build_processors, drop_content_fields
from das.observability.redaction import REDACTION_MARKER, Redactor

HEX = set("0123456789abcdef")


def sampled_tracer() -> trace.Tracer:
    return TracerProvider(sampler=ALWAYS_ON).get_tracer("test")


def unsampled_tracer() -> trace.Tracer:
    return TracerProvider(sampler=ALWAYS_OFF).get_tracer("test")


def run(event: dict) -> dict:
    return add_trace_ids(None, "info", event)


@pytest.fixture
def capture():
    stream = io.StringIO()
    configure_logging("agent-test", stream=stream)
    yield stream
    configure_logging("test", stream=io.StringIO())


def lines(stream: io.StringIO) -> list[dict]:
    return [json.loads(line) for line in stream.getvalue().splitlines()]


# --------------------------------------------------------------------------- processor (8.9-8.11)


def test_sampled_span_adds_lowercase_hex_ids() -> None:
    with sampled_tracer().start_as_current_span("s") as span:
        event = run({"message": "x"})
        ctx = span.get_span_context()
    assert event["trace_id"] == format(ctx.trace_id, "032x")
    assert event["span_id"] == format(ctx.span_id, "016x")
    assert len(event["trace_id"]) == 32 and set(event["trace_id"]) <= HEX
    assert len(event["span_id"]) == 16 and set(event["span_id"]) <= HEX


def test_sampled_span_overrides_caller_supplied_ids() -> None:
    with sampled_tracer().start_as_current_span("s") as span:
        event = run({"message": "x", "trace_id": "bogus", "span_id": "bogus"})
        ctx = span.get_span_context()
    assert event["trace_id"] == format(ctx.trace_id, "032x")
    assert event["span_id"] == format(ctx.span_id, "016x")


def test_nested_span_uses_innermost_span() -> None:
    tracer = sampled_tracer()
    with tracer.start_as_current_span("outer") as outer:
        with tracer.start_as_current_span("inner") as inner:
            inside = run({})
        after = run({})
    assert inside["span_id"] == format(inner.get_span_context().span_id, "016x")
    assert after["span_id"] == format(outer.get_span_context().span_id, "016x")
    assert inside["trace_id"] == after["trace_id"]


def test_no_active_span_removes_caller_ids() -> None:
    event = run({"message": "x", "trace_id": "a" * 32, "span_id": "b" * 16})
    assert "trace_id" not in event and "span_id" not in event


def test_unsampled_span_omits_ids() -> None:
    with unsampled_tracer().start_as_current_span("s") as span:
        assert span.get_span_context().is_valid
        event = run({"trace_id": "a" * 32, "span_id": "b" * 16})
    assert "trace_id" not in event and "span_id" not in event


def test_valid_but_unsampled_context_omits_ids() -> None:
    ctx = SpanContext(trace_id=1, span_id=2, is_remote=True, trace_flags=TraceFlags(0))
    with trace.use_span(NonRecordingSpan(ctx)):
        event = run({})
    assert "trace_id" not in event and "span_id" not in event


def test_noop_tracer_omits_ids() -> None:
    with trace.NoOpTracer().start_as_current_span("s"):
        event = run({"trace_id": "x"})
    assert "trace_id" not in event and "span_id" not in event


def test_processor_runs_before_drop_content_fields() -> None:
    pre_chain, _ = build_processors("a", Redactor())
    assert pre_chain.index(add_trace_ids) < pre_chain.index(drop_content_fields)


# --------------------------------------------------------------------------- end to end


def test_log_line_inside_sampled_span_carries_ids(capture: io.StringIO) -> None:
    log = get_logger("t")
    with sampled_tracer().start_as_current_span("s") as span:
        log.info("inside")
        ctx = span.get_span_context()
    log.info("outside", trace_id="forged")
    inside, outside = lines(capture)
    assert inside["trace_id"] == format(ctx.trace_id, "032x")
    assert inside["span_id"] == format(ctx.span_id, "016x")
    assert "trace_id" not in outside and "span_id" not in outside


def test_stdlib_record_inside_sampled_span_carries_ids(capture: io.StringIO) -> None:
    import logging

    with sampled_tracer().start_as_current_span("s") as span:
        logging.getLogger("third.party").warning("lib message")
        ctx = span.get_span_context()
    (entry,) = lines(capture)
    assert entry["trace_id"] == format(ctx.trace_id, "032x")
    assert entry["span_id"] == format(ctx.span_id, "016x")


# --------------------------------------------------------------------------- new secrets (11.9, 19.3)

API_KEY = "client-key-0123456789"
ADMIN_KEY = "admin-key-abcdef0123456789"
OTLP_TOKEN = "otlp-secret-token-42"
CURSOR_SECRET = "cursor-secret-0123456789abcdef"


def test_cli_redacts_admin_keys_otlp_headers_and_cursor_secret(tmp_path: Path) -> None:
    path = tmp_path / "config.yaml"
    path.write_text(
        yaml.safe_dump(
            {"role": "gateway", "agent_id": "gw-test", "redis": {"url": "redis://127.0.0.1:1/0"}}
        ),
        encoding="utf-8",
    )
    env = {
        "DAS__API_KEYS": API_KEY,
        "DAS__ADMIN_API_KEYS": ADMIN_KEY,
        "DAS__TRACING__OTLP_HEADERS": f"authorization={OTLP_TOKEN}",
        "DAS__GATEWAY__CURSOR_SECRET": CURSOR_SECRET,
    }
    cfg = load_config(path, env)
    secrets = cli.secrets_for_redaction(cfg)
    for value in (API_KEY, ADMIN_KEY, OTLP_TOKEN, CURSOR_SECRET):
        assert value in secrets

    stream = io.StringIO()
    try:
        configure_logging(str(cfg.agent_id), secrets=secrets, stream=stream)
        get_logger("t").info(f"{ADMIN_KEY} {OTLP_TOKEN} {CURSOR_SECRET}", detail=ADMIN_KEY)
    finally:
        configure_logging("test", stream=io.StringIO())
    out = stream.getvalue()
    for value in (ADMIN_KEY, OTLP_TOKEN, CURSOR_SECRET):
        assert value not in out
    assert REDACTION_MARKER in out
