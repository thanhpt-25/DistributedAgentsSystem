"""Unit tests for LLM operation, observer, Span and usage normalization (Req 4.1-4.3, 4.7, 6.6, 9.6, 9.7)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import StatusCode

from das.clock import FakeClock
from das.config.schema import TracingConfig
from das.llm import retry as retry_mod
from das.llm.adapters._common import error_for_status, make_completion
from das.llm.adapters.bedrock import classify_bedrock_error
from das.llm.base import ChatMessage, Completion, ModelParams
from das.llm.errors import (
    LLMCallFailed,
    PermanentLLMError,
    RateLimitedLLMError,
    TransientLLMError,
)
from das.llm.retry import LLMCallObserver, LLMProvider, classify_outcome
from das.models.results import TokenUsage
from das.observability.tracing import OtelTracer, build_tracer
from tests.fakes.llm_adapter import HANG, FakeAdapter, completion

PARAMS = ModelParams(model="m-1", temperature=0.2, max_output_tokens=16)
MSGS = [ChatMessage(role="user", content="hello")]


class Recorder:
    def __init__(self, clock: FakeClock | None = None) -> None:
        self.clock = clock
        self.events: list[tuple[Any, ...]] = []

    def on_retry_scheduled(self, operation: str) -> None:
        # Record how many sleeps happened before this retry (must run before sleep).
        sleeps = len(self.clock.sleeps) if self.clock else None
        self.events.append(("retry", operation, sleeps))

    def on_finished(self, operation: str, outcome: str, duration: float,
                    usage: TokenUsage | None) -> None:
        self.events.append(("finished", operation, outcome, duration, usage))

    def finished(self) -> list[tuple[Any, ...]]:
        return [e for e in self.events if e[0] == "finished"]

    def retries(self) -> list[tuple[Any, ...]]:
        return [e for e in self.events if e[0] == "retry"]


def make(adapter: FakeAdapter, retry_count: int = 3, timeout: float = 60,
         **kw: Any) -> tuple[LLMProvider, FakeClock, Recorder]:
    clock = FakeClock()
    rec = Recorder(clock)
    llm = LLMProvider(adapter, PARAMS, retry_count=retry_count, timeout=timeout,
                      sleep=clock.sleep, clock=clock, observer=rec, **kw)
    return llm, clock, rec


def test_recorder_satisfies_protocol() -> None:
    assert isinstance(Recorder(), LLMCallObserver)


@pytest.mark.parametrize(
    ("exc", "outcome"),
    [
        (RateLimitedLLMError("429"), "rate_limited"),
        (TransientLLMError("reset"), "transient_error"),
        (TimeoutError(), "timeout"),
        (PermanentLLMError("bad key"), "non_transient_error"),
    ],
)
def test_classify_outcome(exc: BaseException, outcome: str) -> None:
    assert classify_outcome(exc) == outcome


async def test_success_reports_usage_and_duration_including_backoff() -> None:
    adapter = FakeAdapter([TransientLLMError("a"), RateLimitedLLMError("b"), completion("ok", 3, 4)])
    llm, clock, rec = make(adapter)
    out = await llm.complete("s", MSGS, operation="plan")
    assert out.content == "ok"
    # Retry hook runs before each backoff sleep.
    assert rec.retries() == [("retry", "plan", 0), ("retry", "plan", 1)]
    assert rec.finished() == [
        ("finished", "plan", "success", 3.0,
         TokenUsage(input_tokens=3, output_tokens=4, total_tokens=7))
    ]


async def test_exhausted_retries_use_last_attempt_error() -> None:
    adapter = FakeAdapter([TransientLLMError("a"), RateLimitedLLMError("b")])
    llm, clock, rec = make(adapter, retry_count=1)
    with pytest.raises(LLMCallFailed):
        await llm.complete("s", MSGS, operation="job")
    assert len(rec.retries()) == 1
    assert [e[2] for e in rec.finished()] == ["rate_limited"]
    assert rec.finished()[0][3] == 1.0


async def test_permanent_error_finishes_without_retry() -> None:
    llm, _, rec = make(FakeAdapter([PermanentLLMError("bad")]))
    with pytest.raises(LLMCallFailed):
        await llm.complete("s", MSGS, operation="summarize")
    assert rec.retries() == []
    assert [(e[1], e[2], e[4]) for e in rec.finished()] == [
        ("summarize", "non_transient_error", None)
    ]


async def test_timeout_outcome() -> None:
    llm, _, rec = make(FakeAdapter([HANG]), retry_count=0, timeout=1)
    with pytest.raises(LLMCallFailed):
        await llm.complete("s", MSGS, operation="job")
    assert [e[2] for e in rec.finished()] == ["timeout"]


async def test_cancel_keeps_retries_and_skips_finished() -> None:
    adapter = FakeAdapter([TransientLLMError("a"), HANG])
    llm, _, rec = make(adapter)
    task = asyncio.create_task(llm.complete("s", MSGS, operation="job"))
    while len(adapter.calls) < 2:
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(rec.retries()) == 1
    assert rec.finished() == []


async def test_unreported_usage_is_passed_as_none() -> None:
    reply = Completion("ok", TokenUsage(), usage_reported=False)
    llm, _, rec = make(FakeAdapter([reply]))
    assert (await llm.complete("s", MSGS, operation="job")) is reply
    assert rec.finished()[0][4] is None


async def test_invalid_input_is_not_observed() -> None:
    llm, _, rec = make(FakeAdapter(["x"]))
    with pytest.raises(ValueError):
        await llm.complete("s", [], operation="job")
    assert rec.events == []


async def test_failing_observer_does_not_break_call() -> None:
    class Broken:
        def on_retry_scheduled(self, operation: str) -> None:
            raise RuntimeError("boom")

        def on_finished(self, *a: Any) -> None:
            raise RuntimeError("boom")

    clock = FakeClock()
    llm = LLMProvider(FakeAdapter([TransientLLMError("a"), "ok"]), PARAMS, sleep=clock.sleep,
                      clock=clock, observer=Broken())
    assert (await llm.complete("s", MSGS, operation="job")).content == "ok"


async def test_usage_problems_log_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    logged: list[tuple[str, dict[str, Any]]] = []

    class Log:
        def warning(self, event: str, **kw: Any) -> None:
            logged.append((event, kw))

    monkeypatch.setattr(retry_mod, "log", Log())
    reply = make_completion("ok", 5, -1)
    llm, _, rec = make(FakeAdapter([reply]))
    await llm.complete("s", MSGS, operation="plan")
    assert logged == [("llm_usage_invalid", {
        "operation": "plan", "provider": "fake", "model": "m-1", "fields": ["output_tokens"],
    })]
    assert rec.finished()[0][4] == TokenUsage(input_tokens=5, output_tokens=0, total_tokens=5)


async def test_llm_completion_span_with_retry_events() -> None:
    exporter = InMemorySpanExporter()
    tracer = build_tracer(TracingConfig(enabled=True, otlp_endpoint="http://otel:4317"),
                          "worker", "w-1", FakeClock(), exporter=exporter, start_thread=False)
    assert isinstance(tracer, OtelTracer)
    adapter = FakeAdapter([TransientLLMError("a"), RateLimitedLLMError("b"), completion("ok", 2, 3)])
    llm, _, _ = make(adapter, tracer=tracer)
    await llm.complete("s", MSGS, operation="job")
    fail_llm, _, _ = make(FakeAdapter([PermanentLLMError("no")]), tracer=tracer)
    with pytest.raises(LLMCallFailed):
        await fail_llm.complete("s", MSGS, operation="plan")
    tracer.processor.export_once()
    ok, failed = exporter.get_finished_spans()
    assert ok.name == "llm.completion"
    assert ok.attributes["llm.operation"] == "job"
    assert ok.attributes["llm.provider"] == "fake"
    assert ok.attributes["llm.model"] == "m-1"
    assert ok.attributes["llm.input_tokens"] == 2
    assert ok.attributes["llm.output_tokens"] == 3
    assert [(e.name, dict(e.attributes)) for e in ok.events] == [
        ("retry", {"das.attempt": 1, "error.type": "TransientLLMError"}),
        ("retry", {"das.attempt": 2, "error.type": "RateLimitedLLMError"}),
    ]
    assert ok.status.status_code != StatusCode.ERROR
    assert failed.status.status_code == StatusCode.ERROR
    assert failed.events == ()


def test_make_completion_normalizes_usage() -> None:
    assert make_completion("x", 3, 4) == Completion(
        "x", TokenUsage(input_tokens=3, output_tokens=4, total_tokens=7)
    )
    missing = make_completion("x", None, None, None)
    assert missing.usage_reported is False and missing.usage == TokenUsage()
    assert missing.usage_problems == ()
    bad = make_completion("x", 2, "7", None)
    assert bad.usage_reported is True
    assert bad.usage == TokenUsage(input_tokens=2, output_tokens=0, total_tokens=2)
    assert bad.usage_problems == ("output_tokens",)


def test_rate_limit_errors_are_classified() -> None:
    import botocore.exceptions as bexc

    assert type(error_for_status("openai", 429, "")) is RateLimitedLLMError
    assert type(error_for_status("openai", 503, "")) is TransientLLMError
    throttled = bexc.ClientError({"Error": {"Code": "ThrottlingException"}}, "Converse")
    assert type(classify_bedrock_error(throttled)) is RateLimitedLLMError
    unavailable = bexc.ClientError({"Error": {"Code": "ServiceUnavailableException"}}, "Converse")
    assert type(classify_bedrock_error(unavailable)) is TransientLLMError
