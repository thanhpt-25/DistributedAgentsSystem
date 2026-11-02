# Feature: das-observability-operations, Property 2: Nhất quán metrics LLM
"""Property 2: Nhất quán metrics LLM.

For any sequence of ``LLMProvider.complete`` calls on a scripted ``FakeAdapter``
(per-attempt outcome success / rate limited / transient / timeout / permanent, up to
``retry_count`` retries, with or without token usage, with or without a Price_Table,
and cancellation at an arbitrary point: during an attempt or during a backoff sleep,
driven by ``FakeClock``), on every ``{provider, model, operation}`` combination:

* sum of ``das_llm_calls_total`` over ``outcome`` = observation count of
  ``das_llm_call_duration_seconds`` = number of calls that were not cancelled;
* ``outcome`` is the classification of the last attempt;
* ``das_llm_call_duration_seconds`` sum = FakeClock time from the first attempt start to
  the last attempt end, backoff included;
* ``das_llm_retries_total`` = number of scheduled retries, including those of cancelled calls;
* ``das_llm_tokens_total{direction}`` = tokens of ``success`` calls with usage;
* ``das_llm_estimated_cost_total`` = sum of unrounded ``record_cost`` of priced ``success`` calls.

Timeouts are modelled by the adapter raising ``TimeoutError`` (what ``asyncio.timeout``
raises), so no example waits on real time.

**Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8**
"""

from __future__ import annotations

import asyncio
from collections import Counter, defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, ClassVar

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from das.clock import FakeClock
from das.llm.base import ChatMessage, Completion, ModelParams
from das.llm.errors import (
    LLMCallFailed,
    PermanentLLMError,
    RateLimitedLLMError,
    TransientLLMError,
)
from das.llm.retry import LLMProvider, retry_delay
from das.models.results import TokenUsage
from das.observability.metrics import LLMMetrics, LLMMetricsObserver, MetricsRecorder
from das.ops.cost import PriceEntry, PriceTable, record_cost
from tests.fakes.llm_adapter import FakeAdapter

MSGS = (ChatMessage(role="user", content="hello"),)
OPERATIONS = ("plan", "summarize", "job")
FAILURE_OUTCOME = {
    "rate_limited": "rate_limited",
    "transient": "transient_error",
    "timeout": "timeout",
    "permanent": "non_transient_error",
}
RETRYABLE = ("rate_limited", "transient", "timeout")


class FakeAdapterB(FakeAdapter):
    provider: ClassVar[str] = "fake-b"


# (adapter class, model); "m-a" is reused across providers on purpose.
PAIRS: tuple[tuple[type[FakeAdapter], str], ...] = (
    (FakeAdapter, "m-a"),
    (FakeAdapter, "m-b"),
    (FakeAdapterB, "m-a"),
)


class ListLog:
    def warning(self, event: str, **fields: Any) -> None:
        pass

    def error(self, event: str, **fields: Any) -> None:
        pass


@dataclass(frozen=True)
class Attempt:
    kind: str  # "ok" | "rate_limited" | "transient" | "timeout" | "permanent"
    latency: int  # FakeClock seconds spent inside the attempt
    usage: tuple[int, int] | None  # (input, output) for "ok"; None = usage not reported


@dataclass(frozen=True)
class Call:
    pair: int
    operation: str
    retry_count: int
    attempts: tuple[Attempt, ...]  # length retry_count + 1
    cancel: tuple[str, int] | None  # ("attempt", k) or ("sleep", k), 1-based


_attempt = st.builds(
    Attempt,
    kind=st.sampled_from(("ok", "rate_limited", "transient", "timeout", "permanent")),
    latency=st.integers(min_value=0, max_value=5),
    usage=st.none() | st.tuples(st.integers(0, 10_000), st.integers(0, 10_000)),
)


@st.composite
def _call(draw: st.DrawFn) -> Call:
    retry_count = draw(st.integers(min_value=0, max_value=3))
    attempts = tuple(draw(st.lists(_attempt, min_size=retry_count + 1, max_size=retry_count + 1)))
    cancel = draw(
        st.none()
        | st.tuples(st.sampled_from(("attempt", "sleep")), st.integers(1, retry_count + 1))
    )
    return Call(
        pair=draw(st.integers(0, len(PAIRS) - 1)),
        operation=draw(st.sampled_from(OPERATIONS)),
        retry_count=retry_count,
        attempts=attempts,
        cancel=cancel,
    )


_price = st.decimals(min_value=0, max_value=100, places=6, allow_nan=False, allow_infinity=False)
_entries = st.lists(
    st.builds(lambda i, a, b: (i, a, b), st.integers(0, len(PAIRS) - 1), _price, _price),
    max_size=len(PAIRS),
    unique_by=lambda e: e[0],
)


@dataclass
class Expected:
    """Reference model of one call's effect."""

    retries: int
    outcome: str | None  # None = cancelled
    duration: float
    usage: tuple[int, int] | None


def _expect(call: Call) -> Expected:
    elapsed = 0.0
    retries = 0
    for i, att in enumerate(call.attempts, start=1):
        if call.cancel == ("attempt", i):
            return Expected(retries, None, 0.0, None)
        elapsed += att.latency
        if att.kind == "ok":
            return Expected(retries, "success", elapsed, att.usage)
        if att.kind == "permanent" or i > call.retry_count:
            return Expected(retries, FAILURE_OUTCOME[att.kind], elapsed, None)
        retries += 1
        if call.cancel == ("sleep", i):
            return Expected(retries, None, 0.0, None)
        elapsed += retry_delay(i)
    raise AssertionError("script shorter than retry_count + 1")


def _responder(att: Attempt, clock: FakeClock, parked: list[bool], hang: bool):
    async def respond(system: str, messages: Any, params: ModelParams) -> Completion:
        if hang:
            parked[0] = True
            await asyncio.Event().wait()
        clock.advance(att.latency)
        if att.kind == "ok":
            if att.usage is None:
                return Completion("ok", TokenUsage(), usage_reported=False)
            i, o = att.usage
            return Completion("ok", TokenUsage(input_tokens=i, output_tokens=o, total_tokens=i + o))
        if att.kind == "rate_limited":
            raise RateLimitedLLMError("429")
        if att.kind == "transient":
            raise TransientLLMError("reset")
        if att.kind == "timeout":
            raise TimeoutError()
        raise PermanentLLMError("bad request")

    return respond


async def _run_call(call: Call, clock: FakeClock, observer: LLMMetricsObserver) -> bool:
    """Run one call; return True if it was cancelled."""
    adapter_cls, model = PAIRS[call.pair]
    parked = [False]
    hang_at = call.cancel[1] if call.cancel and call.cancel[0] == "attempt" else None
    sleep_at = call.cancel[1] if call.cancel and call.cancel[0] == "sleep" else None
    adapter = adapter_cls(
        [_responder(a, clock, parked, i == hang_at) for i, a in enumerate(call.attempts, start=1)]
    )
    sleeps = 0

    async def sleep(seconds: float) -> None:
        nonlocal sleeps
        sleeps += 1
        if sleeps == sleep_at:
            parked[0] = True
            await asyncio.Event().wait()
        await clock.sleep(seconds)

    llm = LLMProvider(
        adapter,
        ModelParams(model=model, temperature=0.0, max_output_tokens=16),
        retry_count=call.retry_count,
        timeout=60,
        sleep=sleep,
        clock=clock,
        observer=observer,
    )
    task = asyncio.create_task(llm.complete("sys", MSGS, operation=call.operation))  # type: ignore[arg-type]
    while not task.done() and not parked[0]:
        await asyncio.sleep(0)
    if parked[0]:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return True
    try:
        await task
    except LLMCallFailed:
        pass
    return False


def _samples(rec: MetricsRecorder) -> dict[str, list[tuple[dict[str, str], float]]]:
    out: dict[str, list[tuple[dict[str, str], float]]] = defaultdict(list)
    for family in rec.registry.collect():
        for s in family.samples:
            out[s.name].append((dict(s.labels), s.value))
    return out


@settings(max_examples=200, deadline=None)
@given(
    calls=st.lists(_call(), min_size=0, max_size=12),
    priced=st.none() | _entries,
)
def test_llm_metrics_consistency(calls: list[Call], priced: list[tuple[int, Decimal, Decimal]] | None) -> None:
    price_table = None
    if priced is not None:
        price_table = PriceTable(
            currency="EUR",
            models=[
                PriceEntry(
                    provider=PAIRS[i][0].provider,
                    model=PAIRS[i][1],
                    input_per_million=a,
                    output_per_million=b,
                )
                for i, a, b in priced
            ],
        )
    clock = FakeClock()
    rec = MetricsRecorder(
        clock=clock,
        log=ListLog(),
        currency=price_table.currency if price_table is not None else None,
    )
    llm_metrics = LLMMetrics(rec)
    observers = [
        LLMMetricsObserver(llm_metrics, cls.provider, model, price_table) for cls, model in PAIRS
    ]

    async def main() -> list[bool]:
        return [await _run_call(c, clock, observers[c.pair]) for c in calls]

    cancelled = asyncio.run(main())

    # Reference model, keyed by (provider, model, operation).
    exp_outcomes: dict[tuple[str, str, str], Counter[str]] = defaultdict(Counter)
    exp_retries: Counter[tuple[str, str, str]] = Counter()
    exp_duration: dict[tuple[str, str, str], float] = defaultdict(float)
    exp_finished: Counter[tuple[str, str, str]] = Counter()
    exp_tokens: Counter[tuple[str, str, str, str]] = Counter()
    exp_cost: dict[tuple[str, str], Decimal] = defaultdict(Decimal)
    for call, was_cancelled in zip(calls, cancelled, strict=True):
        cls, model = PAIRS[call.pair]
        key = (cls.provider, model, call.operation)
        e = _expect(call)
        assert was_cancelled == (e.outcome is None)
        exp_retries[key] += e.retries
        if e.outcome is None:
            continue
        exp_finished[key] += 1
        exp_outcomes[key][e.outcome] += 1
        exp_duration[key] += e.duration
        if e.outcome == "success" and e.usage is not None:
            exp_tokens[(*key, "input")] += e.usage[0]
            exp_tokens[(*key, "output")] += e.usage[1]
            entry = price_table.lookup(cls.provider, model) if price_table else None
            if entry is not None:
                usage = TokenUsage(input_tokens=e.usage[0], output_tokens=e.usage[1])
                exp_cost[(cls.provider, model)] += record_cost(
                    _Rec(cls.provider, model, usage), entry
                )

    samples = _samples(rec)

    def k3(labels: dict[str, str]) -> tuple[str, str, str]:
        return (labels["provider"], labels["model"], labels["operation"])

    got_outcomes: dict[tuple[str, str, str], Counter[str]] = defaultdict(Counter)
    for labels, v in samples["das_llm_calls_total"]:
        got_outcomes[k3(labels)][labels["outcome"]] += int(v)
    got_count = {k3(lb): int(v) for lb, v in samples["das_llm_call_duration_seconds_count"]}
    got_sum = {k3(lb): v for lb, v in samples["das_llm_call_duration_seconds_sum"]}
    got_retries = {k3(lb): int(v) for lb, v in samples["das_llm_retries_total"]}
    got_tokens = {(*k3(lb), lb["direction"]): int(v) for lb, v in samples["das_llm_tokens_total"]}
    got_cost = {
        (lb["provider"], lb["model"]): v for lb, v in samples["das_llm_estimated_cost_total"]
    }
    for lb, _ in samples["das_llm_estimated_cost_total"]:
        assert price_table is not None and lb["currency"] == price_table.currency

    keys = set(exp_finished) | set(got_count) | set(got_outcomes)
    for key in keys:
        # Req 4.1, 4.2, 4.7, 4.8: calls == duration observations == non-cancelled calls.
        assert sum(got_outcomes[key].values()) == got_count.get(key, 0) == exp_finished[key]
        # Req 4.1: outcome = classification of the last attempt.
        assert +got_outcomes[key] == exp_outcomes[key]
        # Req 4.2: duration spans every attempt and backoff (exact: integer seconds).
        assert got_sum.get(key, 0.0) == exp_duration.get(key, 0.0)
    # Req 4.3, 4.7, 4.8: retries counted, including those of cancelled calls.
    assert {k: v for k, v in got_retries.items() if v} == {k: v for k, v in exp_retries.items() if v}
    # Req 4.4, 4.5: tokens only for success calls with usage.
    assert {k: v for k, v in got_tokens.items() if v} == {k: v for k, v in exp_tokens.items() if v}
    # Req 4.6: unrounded Estimated_Cost only for priced success calls.
    assert set(got_cost) <= set(exp_cost)
    for pm, amount in exp_cost.items():
        assert got_cost.get(pm, 0.0) == pytest.approx(float(amount), rel=1e-9, abs=1e-12)


@dataclass(frozen=True)
class _Rec:
    provider: str
    model: str
    usage: TokenUsage
