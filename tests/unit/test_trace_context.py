"""Unit tests for the pure W3C TraceContext (Requirement 7.1)."""
from __future__ import annotations

import ast
from pathlib import Path

import pytest
from hypothesis import given

import das.observability.trace_context as trace_context_module
from das.observability.trace_context import (
    TraceContext,
    is_valid_traceparent,
    is_valid_tracestate,
)
from tests.strategies import trace_contexts

TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"
PARENT_ID = "00f067aa0ba902b7"
VALID = f"00-{TRACE_ID}-{PARENT_ID}-01"


def test_parse_valid_traceparent_and_tracestate() -> None:
    ctx = TraceContext.parse(VALID, "congo=t61rcWkgMzE,rojo=00f067aa0ba902b7")
    assert ctx == TraceContext(VALID, "congo=t61rcWkgMzE,rojo=00f067aa0ba902b7")
    assert ctx.trace_id == TRACE_ID
    assert ctx.parent_id == PARENT_ID
    assert ctx.sampled is True


@pytest.mark.parametrize(("flags", "sampled"), [("00", False), ("01", True), ("02", False), ("ff", True)])
def test_sampled_flag(flags: str, sampled: bool) -> None:
    ctx = TraceContext.parse(f"00-{TRACE_ID}-{PARENT_ID}-{flags}")
    assert ctx is not None and ctx.sampled is sampled


@pytest.mark.parametrize(
    "value",
    [
        None,
        55,
        b"00-" + TRACE_ID.encode(),
        "",
        f"ff-{TRACE_ID}-{PARENT_ID}-01",  # forbidden version
        f"00-{'0' * 32}-{PARENT_ID}-01",  # zero trace-id
        f"00-{TRACE_ID}-{'0' * 16}-01",  # zero parent-id
        VALID.upper(),  # uppercase hex
        f"00-{TRACE_ID.upper()}-{PARENT_ID}-01",
        VALID + "-extra",  # v00 must be exactly 55
        VALID[:-1],
        f"0g-{TRACE_ID}-{PARENT_ID}-01",
        f"00-{TRACE_ID}-{PARENT_ID}-0x",
        f"00_{TRACE_ID}-{PARENT_ID}-01",
        f"01-{TRACE_ID}-{PARENT_ID}-01x",  # future version: extra must start with '-'
    ],
)
def test_invalid_traceparent(value: object) -> None:
    assert not is_valid_traceparent(value)
    assert TraceContext.parse(value, "a=b") is None


def test_future_version_allows_extra_fields() -> None:
    assert is_valid_traceparent(f"cc-{TRACE_ID}-{PARENT_ID}-01")
    assert is_valid_traceparent(f"cc-{TRACE_ID}-{PARENT_ID}-01-what-the-future-holds")


@pytest.mark.parametrize(
    "value",
    [
        "a=b",
        "a=b , c=d",
        "a=b,,c=d",  # empty members allowed
        "tenant@sys=v",
        "0tenant@sys=v",
        "k=a b",  # inner space allowed
        "a=b ",  # trailing space is optional whitespace around the member
        ",".join(f"k{i}=v" for i in range(32)),
    ],
)
def test_valid_tracestate(value: str) -> None:
    assert is_valid_tracestate(value)
    ctx = TraceContext.parse(VALID, value)
    assert ctx is not None and ctx.tracestate == value


@pytest.mark.parametrize(
    "value",
    [
        1,
        ["a=b"],
        "",
        " , ",
        "a",  # no '='
        "A=b",  # uppercase key
        "1a=b",  # simple key must start with a letter
        "a=",  # empty value
        "a=b=c",
        "a=b,a=c",  # duplicate key
        "t@Sys=v",
        "a=\x7f",
        "a=é",
        ",".join(f"k{i}=v" for i in range(33)),  # 33 members
        "k=" + "v" * 511,  # > 512 chars
    ],
)
def test_invalid_tracestate_is_dropped(value: object) -> None:
    assert not is_valid_tracestate(value)
    ctx = TraceContext.parse(VALID, value)
    assert ctx == TraceContext(VALID, None)


def test_tracestate_length_limit_boundary() -> None:
    at_limit = "k=" + "v" * 254 + ",j=" + "v" * 253  # 2+254+3+253 = 512
    assert len(at_limit) == 512
    assert is_valid_tracestate(at_limit)
    assert not is_valid_tracestate(at_limit + "v")


def test_module_does_not_import_opentelemetry() -> None:
    tree = ast.parse(Path(trace_context_module.__file__).read_text())
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.append(node.module)
    assert not any(n.split(".")[0] == "opentelemetry" for n in names)


@given(trace_contexts())
def test_shared_strategy_always_parses(value: dict[str, str | None]) -> None:
    ctx = TraceContext.parse(value["traceparent"], value["tracestate"])
    assert ctx == TraceContext(value["traceparent"], value["tracestate"])
