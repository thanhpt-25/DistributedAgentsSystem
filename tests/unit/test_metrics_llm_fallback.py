"""LLM metrics for a model per call and for the fallback (das-smarter-agents task 2.4; Req 2.5, 2.6).

Without this an observer bound to one model would label a Capability_Model's calls, tokens and cost with the wrong
model, or (worse) raise on the new ``model`` argument and lose them: the provider swallows observer errors."""

from __future__ import annotations

from decimal import Decimal

from das.clock import FakeClock
from das.llm.base import ChatMessage, ModelParams
from das.llm.errors import PermanentLLMError
from das.llm.retry import LLMProvider
from das.models.results import TokenUsage
from das.observability.metrics import LLMMetrics, LLMMetricsObserver, MetricsRecorder
from das.ops.cost import PriceEntry, PriceTable
from tests.fakes.llm_adapter import FakeAdapter, completion

P = "fake"
USAGE = TokenUsage(input_tokens=1000, output_tokens=10, total_tokens=1010)


def prices() -> PriceTable:
    def entry(model: str, rate: str) -> PriceEntry:
        return PriceEntry(provider=P, model=model, input_per_million=Decimal(rate), output_per_million=Decimal(rate))

    return PriceTable(currency="EUR", models=[entry("big", "10"), entry("small", "1")])


def make(price_table: PriceTable | None = None) -> tuple[MetricsRecorder, LLMMetricsObserver]:
    rec = MetricsRecorder(clock=FakeClock(), currency=price_table.currency if price_table else None)
    return rec, LLMMetricsObserver(LLMMetrics(rec), P, "big", price_table)


def sample(rec: MetricsRecorder, name: str, **labels: str) -> float | None:
    return rec.registry.get_sample_value(name, {"provider": P, **labels})


# --------------------------------------------------------------------------- a model per call


def test_a_call_on_another_model_is_counted_under_that_model() -> None:
    rec, obs = make()
    obs.on_finished("job", "success", 1.0, USAGE, model="small")
    assert sample(rec, "das_llm_calls_total", model="small", operation="job", outcome="success") == 1
    assert sample(rec, "das_llm_calls_total", model="big", operation="job", outcome="success") is None
    assert sample(rec, "das_llm_tokens_total", model="small", operation="job", direction="input") == 1000


def test_a_call_without_a_model_is_counted_under_the_bound_model_as_before() -> None:
    rec, obs = make()
    obs.on_finished("job", "success", 1.0, USAGE)
    assert sample(rec, "das_llm_calls_total", model="big", operation="job", outcome="success") == 1


def test_a_retry_on_another_model_is_counted_under_that_model() -> None:
    rec, obs = make()
    obs.on_retry_scheduled("job", model="small")
    obs.on_retry_scheduled("job")
    assert sample(rec, "das_llm_retries_total", model="small", operation="job") == 1
    assert sample(rec, "das_llm_retries_total", model="big", operation="job") == 1


def test_the_estimated_cost_is_priced_by_the_model_that_was_used() -> None:
    """Req 2.5: priced by the model really used, not the configured one."""
    rec, obs = make(prices())
    obs.on_finished("job", "success", 1.0, USAGE, model="small")
    obs.on_finished("job", "success", 1.0, USAGE)
    small = sample(rec, "das_llm_estimated_cost_total", model="small", currency="EUR")
    big = sample(rec, "das_llm_estimated_cost_total", model="big", currency="EUR")
    assert small == float(Decimal("1") * Decimal(1010) / Decimal(1_000_000))
    assert big == float(Decimal("10") * Decimal(1010) / Decimal(1_000_000))


def test_a_model_missing_from_the_price_table_has_no_cost_but_still_has_calls_and_tokens() -> None:
    rec, obs = make(prices())
    obs.on_finished("job", "success", 1.0, USAGE, model="unpriced")
    assert sample(rec, "das_llm_calls_total", model="unpriced", operation="job", outcome="success") == 1
    assert sample(rec, "das_llm_tokens_total", model="unpriced", operation="job", direction="output") == 10
    assert sample(rec, "das_llm_estimated_cost_total", model="unpriced", currency="EUR") is None


# --------------------------------------------------------------------------- das_llm_fallbacks_total (Req 2.6)


def test_a_fallback_is_counted_under_the_model_that_failed() -> None:
    rec, obs = make()
    obs.on_fallback("job", "small")
    obs.on_fallback("job", "small")
    obs.on_fallback("plan", "big")
    assert sample(rec, "das_llm_fallbacks_total", model="small") == 2
    assert sample(rec, "das_llm_fallbacks_total", model="big") == 1


def test_the_fallback_counter_has_exactly_provider_and_model_labels() -> None:
    rec, obs = make()
    obs.on_fallback("job", "small")
    [family] = [f for f in rec.registry.collect() if f.name == "das_llm_fallbacks"]
    [metric] = [s for s in family.samples if s.name == "das_llm_fallbacks_total"]
    assert set(metric.labels) == {"provider", "model"}


def test_the_model_label_of_the_fallback_counter_is_bounded_like_the_others() -> None:
    rec = MetricsRecorder(clock=FakeClock())
    obs = LLMMetricsObserver(LLMMetrics(rec), P, "big")
    for n in range(1000):
        obs.on_fallback("job", f"model-{n}")
    [family] = [f for f in rec.registry.collect() if f.name == "das_llm_fallbacks"]
    samples = [s for s in family.samples if s.name == "das_llm_fallbacks_total"]
    assert len(samples) <= 101  # the default model limit (100) plus the overflow bucket


def test_an_observer_that_cannot_record_never_raises() -> None:
    class Broken(LLMMetrics):
        def fallback(self, provider: str, model: str) -> None:
            raise RuntimeError("boom")

    rec = MetricsRecorder(clock=FakeClock())
    LLMMetricsObserver(Broken(rec), P, "big").on_fallback("job", "small")  # must not raise


# --------------------------------------------------------------------------- end to end through the provider


async def test_a_real_provider_reports_the_failed_call_the_fallback_and_the_rescue_each_under_its_model() -> None:
    rec = MetricsRecorder(clock=FakeClock(), currency="EUR")
    observer = LLMMetricsObserver(LLMMetrics(rec), P, "big", prices())
    adapter = FakeAdapter([PermanentLLMError("gone"), completion("ok", 1000, 10)])
    provider = LLMProvider(
        adapter, ModelParams(model="big", temperature=0.0, max_output_tokens=64), retry_count=0, timeout=1,
        observer=observer, fallback_model="small",
    )
    await provider.complete("s", [ChatMessage(role="user", content="hi")], operation="job")
    assert sample(rec, "das_llm_calls_total", model="big", operation="job", outcome="non_transient_error") == 1
    assert sample(rec, "das_llm_fallbacks_total", model="big") == 1
    assert sample(rec, "das_llm_calls_total", model="small", operation="job", outcome="success") == 1
    assert sample(rec, "das_llm_tokens_total", model="small", operation="job", direction="input") == 1000
    assert sample(rec, "das_llm_estimated_cost_total", model="small", currency="EUR") is not None
