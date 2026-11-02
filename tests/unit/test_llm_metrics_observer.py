"""Unit tests for LLMMetricsObserver (Req 4.1-4.6)."""
from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from das.clock import FakeClock
from das.llm.retry import LLMCallObserver
from das.models.results import TokenUsage
from das.observability.metrics import LLMMetrics, LLMMetricsObserver, MetricsRecorder
from das.ops.cost import PriceEntry, PriceTable

P, M = "openai", "gpt-x"


class ListLog:
    def __init__(self) -> None:
        self.entries: list[tuple[str, dict[str, Any]]] = []

    def warning(self, event: str, **fields: Any) -> None:
        self.entries.append((event, fields))


def table(provider: str = P, model: str = M) -> PriceTable:
    return PriceTable(
        currency="EUR",
        models=[
            PriceEntry(
                provider=provider,
                model=model,
                input_per_million=Decimal("1.5"),
                output_per_million=Decimal("3.000001"),
            )
        ],
    )


def make(price_table: PriceTable | None = None) -> tuple[MetricsRecorder, LLMMetricsObserver]:
    currency = price_table.currency if price_table is not None else None
    rec = MetricsRecorder(clock=FakeClock(), log=ListLog(), currency=currency)
    return rec, LLMMetricsObserver(LLMMetrics(rec), P, M, price_table)


def value(rec: MetricsRecorder, name: str, **labels: str) -> float | None:
    return rec.registry.get_sample_value(name, {"provider": P, "model": M, **labels})


def test_satisfies_llm_call_observer_protocol() -> None:
    _, obs = make()
    assert isinstance(obs, LLMCallObserver)


def test_success_with_usage_records_calls_duration_tokens_and_cost() -> None:
    rec, obs = make(table())
    obs.on_finished("plan", "success", 2.5, TokenUsage(input_tokens=1000, output_tokens=7, total_tokens=1007))

    assert value(rec, "das_llm_calls_total", operation="plan", outcome="success") == 1
    assert value(rec, "das_llm_call_duration_seconds_count", operation="plan") == 1
    assert value(rec, "das_llm_call_duration_seconds_sum", operation="plan") == 2.5
    assert value(rec, "das_llm_tokens_total", operation="plan", direction="input") == 1000
    assert value(rec, "das_llm_tokens_total", operation="plan", direction="output") == 7
    # Unrounded: 1000*1.5/1e6 + 7*3.000001/1e6 = 0.001521000007
    expected = float(Decimal("0.0015") + Decimal(7) * Decimal("3.000001") / Decimal(1_000_000))
    assert value(rec, "das_llm_estimated_cost_total", currency="EUR") == pytest.approx(expected, rel=1e-12)


def test_retry_increments_retries_only() -> None:
    rec, obs = make()
    obs.on_retry_scheduled("job")
    obs.on_retry_scheduled("job")
    assert value(rec, "das_llm_retries_total", operation="job") == 2
    assert value(rec, "das_llm_calls_total", operation="job", outcome="success") is None


@pytest.mark.parametrize(
    "outcome", ["rate_limited", "transient_error", "timeout", "non_transient_error"]
)
def test_failure_records_call_and_duration_but_no_tokens_or_cost(outcome: Any) -> None:
    rec, obs = make(table())
    # Even if usage were passed, non-success must not count tokens (Req 4.5).
    obs.on_finished("summarize", outcome, 1.0, TokenUsage(input_tokens=5, output_tokens=5))
    assert value(rec, "das_llm_calls_total", operation="summarize", outcome=outcome) == 1
    assert value(rec, "das_llm_call_duration_seconds_count", operation="summarize") == 1
    assert value(rec, "das_llm_tokens_total", operation="summarize", direction="input") is None
    assert value(rec, "das_llm_estimated_cost_total", currency="EUR") is None


def test_success_without_usage_records_no_tokens_or_cost() -> None:
    rec, obs = make(table())
    obs.on_finished("job", "success", 0.2, None)
    assert value(rec, "das_llm_calls_total", operation="job", outcome="success") == 1
    assert value(rec, "das_llm_tokens_total", operation="job", direction="output") is None
    assert value(rec, "das_llm_estimated_cost_total", currency="EUR") is None


@pytest.mark.parametrize("price_table", [None, table(provider="other"), table(model="GPT-X")])
def test_unpriced_model_records_tokens_but_no_cost(price_table: PriceTable | None) -> None:
    rec, obs = make(price_table)
    obs.on_finished("plan", "success", 0.1, TokenUsage(input_tokens=3, output_tokens=4))
    assert value(rec, "das_llm_tokens_total", operation="plan", direction="input") == 3
    assert value(rec, "das_llm_tokens_total", operation="plan", direction="output") == 4
    cur = price_table.currency if price_table is not None else "USD"
    assert value(rec, "das_llm_estimated_cost_total", currency=cur) is None


def test_observer_never_raises_when_facade_fails() -> None:
    class Boom(LLMMetrics):
        def __init__(self) -> None:
            pass

        def call_finished(self, *a: Any, **k: Any) -> None:
            raise RuntimeError("boom")

        def retry_scheduled(self, *a: Any, **k: Any) -> None:
            raise RuntimeError("boom")

    obs = LLMMetricsObserver(Boom(), P, M, table())
    obs.on_retry_scheduled("plan")
    obs.on_finished("plan", "success", 1.0, TokenUsage(input_tokens=1, output_tokens=1))
