"""Unit tests for the LLM_Provider retry wrapper (Requirements 10.1, 10.4, 10.5, 10.6)."""
from __future__ import annotations

import pytest

from das.clock import FakeClock
from das.llm.base import ChatMessage, Completion, ModelParams, ProviderAdapter
from das.llm.errors import LLMCallFailed, LLMInputError, PermanentLLMError, TransientLLMError
from das.llm.retry import LLMProvider, retry_delay
from das.models.results import TokenUsage
from tests.fakes.llm_adapter import HANG, FakeAdapter, completion

PARAMS = ModelParams(model="m-1", temperature=0.2, max_output_tokens=16)
MSGS = [ChatMessage(role="user", content="hello")]


def make(adapter: FakeAdapter, retry_count: int = 3, timeout: float = 60,
         clock: FakeClock | None = None) -> tuple[LLMProvider, FakeClock]:
    clock = clock or FakeClock()
    return LLMProvider(adapter, PARAMS, retry_count=retry_count, timeout=timeout,
                       sleep=clock.sleep), clock


def test_fake_adapter_satisfies_protocol() -> None:
    assert isinstance(FakeAdapter(), ProviderAdapter)


def test_completion_uses_shared_token_usage() -> None:
    c = completion("x", 2, 3)
    assert isinstance(c.usage, TokenUsage)
    assert c.usage.total_tokens == 5


@pytest.mark.parametrize(("k", "expected"), [(1, 1), (2, 2), (3, 4), (5, 16), (6, 30), (7, 30), (50, 30)])
def test_retry_delay(k: int, expected: float) -> None:
    assert retry_delay(k) == expected


async def test_success_first_call_passes_inputs_through() -> None:
    adapter = FakeAdapter([completion("hi", 3, 4)])
    llm, clock = make(adapter)
    result = await llm.complete("sys", MSGS, operation="job")
    assert result == Completion("hi", TokenUsage(input_tokens=3, output_tokens=4, total_tokens=7))
    assert len(adapter.calls) == 1
    call = adapter.calls[0]
    assert (call.system, call.messages, call.params, call.timeout) == ("sys", tuple(MSGS), PARAMS, 60)
    assert clock.sleeps == []


async def test_transient_errors_retry_with_backoff_then_succeed() -> None:
    adapter = FakeAdapter([TransientLLMError("rate limit"), TransientLLMError("reset"), "done"])
    llm, clock = make(adapter)
    assert (await llm.complete("s", MSGS, operation="job")).content == "done"
    assert len(adapter.calls) == 3
    assert clock.sleeps == [1, 2]


async def test_exhausted_retries_raise_with_details_and_no_content() -> None:
    adapter = FakeAdapter(default=TransientLLMError("503 unavailable"))
    llm, clock = make(adapter, retry_count=3)
    with pytest.raises(LLMCallFailed) as ei:
        await llm.complete("s", MSGS, operation="job")
    err = ei.value
    assert (err.provider, err.model, err.attempts) == ("fake", "m-1", 4)
    assert "503 unavailable" in err.reason
    assert not hasattr(err, "content")
    assert clock.sleeps == [1, 2, 4]


async def test_zero_retries_makes_one_call() -> None:
    adapter = FakeAdapter(default=TransientLLMError("x"))
    llm, clock = make(adapter, retry_count=0)
    with pytest.raises(LLMCallFailed) as ei:
        await llm.complete("s", MSGS, operation="job")
    assert ei.value.attempts == 1
    assert len(adapter.calls) == 1 and clock.sleeps == []


async def test_backoff_caps_at_30_seconds() -> None:
    adapter = FakeAdapter(default=TransientLLMError("x"))
    llm, clock = make(adapter, retry_count=10)
    with pytest.raises(LLMCallFailed):
        await llm.complete("s", MSGS, operation="job")
    assert clock.sleeps == [1, 2, 4, 8, 16, 30, 30, 30, 30, 30]


async def test_permanent_error_is_not_retried() -> None:
    adapter = FakeAdapter([PermanentLLMError("invalid api key"), "never"])
    llm, clock = make(adapter)
    with pytest.raises(LLMCallFailed) as ei:
        await llm.complete("s", MSGS, operation="job")
    assert ei.value.attempts == 1
    assert "invalid api key" in ei.value.reason
    assert len(adapter.calls) == 1 and clock.sleeps == []


async def test_timeout_counts_as_transient() -> None:
    adapter = FakeAdapter([HANG, "late ok"])
    llm, clock = make(adapter, retry_count=1, timeout=1)
    assert (await llm.complete("s", MSGS, operation="job")).content == "late ok"
    assert len(adapter.calls) == 2
    assert clock.sleeps == [1]


@pytest.mark.parametrize("n", [0, 101])
async def test_message_count_out_of_range_rejected(n: int) -> None:
    adapter = FakeAdapter(default="x")
    llm, _ = make(adapter)
    with pytest.raises(LLMInputError):
        await llm.complete("s", [ChatMessage("user", "m")] * n, operation="job")
    assert adapter.calls == []


async def test_message_count_bounds_accepted() -> None:
    adapter = FakeAdapter(default="x")
    llm, _ = make(adapter)
    await llm.complete("s", [ChatMessage("user", "m")] * 100, operation="job")
    assert len(adapter.calls) == 1


async def test_invalid_role_rejected() -> None:
    llm, _ = make(FakeAdapter(default="x"))
    with pytest.raises(LLMInputError):
        await llm.complete("s", [ChatMessage("tool", "m")], operation="job")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "params",
    [
        ModelParams("m", -0.1, 10),
        ModelParams("m", 2.01, 10),
        ModelParams("m", 0.5, 0),
        ModelParams("", 0.5, 10),
    ],
)
def test_invalid_model_params_rejected(params: ModelParams) -> None:
    with pytest.raises(LLMInputError):
        LLMProvider(FakeAdapter(), params)


@pytest.mark.parametrize(("retry_count", "timeout"), [(-1, 60), (11, 60), (3, 0.5), (3, 601)])
def test_out_of_range_retry_or_timeout_rejected(retry_count: int, timeout: float) -> None:
    with pytest.raises(LLMInputError):
        LLMProvider(FakeAdapter(), PARAMS, retry_count=retry_count, timeout=timeout)


def test_temperature_bounds_accepted() -> None:
    LLMProvider(FakeAdapter(), ModelParams("m", 0.0, 1))
    LLMProvider(FakeAdapter(), ModelParams("m", 2.0, 1))
