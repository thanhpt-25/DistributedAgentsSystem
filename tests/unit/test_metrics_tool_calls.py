"""``das_tool_calls_total{tool, outcome}`` (das-smarter-agents task 5.5; Phase 6 Req 5.12): one count per tool call, by
the tool and by ``ok``, ``error`` or ``timeout``, with the ``tool`` label bounded by the tool names."""

from __future__ import annotations

from das.clock import FakeClock
from das.observability.metrics import (
    ALLOWED_LABEL_NAMES, CATALOG_BY_NAME, TOOL_NAMES, TOOL_OUTCOMES, MetricsRecorder, WorkerMetrics,
)


def make() -> tuple[MetricsRecorder, WorkerMetrics]:
    rec = MetricsRecorder(clock=FakeClock())
    return rec, WorkerMetrics(rec)


def count(rec: MetricsRecorder, tool: str, outcome: str) -> float | None:
    return rec.registry.get_sample_value("das_tool_calls_total", {"tool": tool, "outcome": outcome})


def test_the_metric_is_a_counter_with_exactly_the_tool_and_outcome_labels() -> None:
    spec = CATALOG_BY_NAME["das_tool_calls_total"]
    assert spec.kind == "counter" and spec.labels == ("tool", "outcome") and "tool" in ALLOWED_LABEL_NAMES


def test_the_label_values_are_the_tool_names_and_the_three_outcomes() -> None:
    assert TOOL_OUTCOMES == frozenset({"ok", "error", "timeout"})
    assert {"calculator", "http_fetch", "current_time", "run_python", "unknown"} == TOOL_NAMES


def test_each_call_is_counted_under_its_tool_and_outcome() -> None:
    rec, metrics = make()
    metrics.tool_call("calculator", "ok")
    metrics.tool_call("calculator", "ok")
    metrics.tool_call("calculator", "error")
    metrics.tool_call("http_fetch", "timeout")
    assert count(rec, "calculator", "ok") == 2
    assert count(rec, "calculator", "error") == 1
    assert count(rec, "http_fetch", "timeout") == 1
    assert count(rec, "current_time", "ok") is None


def test_a_name_outside_the_tool_names_cannot_grow_the_label() -> None:
    rec, metrics = make()
    for n in range(500):
        metrics.tool_call(f"tool-{n}", "ok")  # must not raise, must not make 500 series
    [family] = [f for f in rec.registry.collect() if f.name == "das_tool_calls"]
    series = [s for s in family.samples if s.name == "das_tool_calls_total"]
    assert len(series) <= len(TOOL_NAMES) * len(TOOL_OUTCOMES) + 1
