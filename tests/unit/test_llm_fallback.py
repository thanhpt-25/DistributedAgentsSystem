"""LLMProvider: a model per call, and one Fallback_Model after a permanent error
(das-smarter-agents task 2.3; Req 2.2, 2.3, 2.4, 2.5, 2.7)."""

from __future__ import annotations

import pytest

from das.llm.base import ChatMessage, ModelParams
from das.llm.errors import AuthLLMError, LLMCallFailed, PermanentLLMError, TransientLLMError
from das.llm.retry import LLMProvider
from das.models.results import TokenUsage
from tests.fakes.llm_adapter import FakeAdapter, completion

PARAMS = ModelParams(model="big", temperature=0.0, max_output_tokens=256)
MESSAGES = [ChatMessage(role="user", content="hi")]


async def no_sleep(_: float) -> None:
    return None


def provider(adapter: FakeAdapter, fallback: str | None = "backup", retry_count: int = 0, observer=None) -> LLMProvider:
    return LLMProvider(
        adapter, PARAMS, retry_count=retry_count, timeout=1, sleep=no_sleep, observer=observer, fallback_model=fallback
    )


def models(adapter: FakeAdapter) -> list[str]:
    return [call.params.model for call in adapter.calls]


class Recorder:
    """An observer that hears the new model argument and the fallback."""

    def __init__(self) -> None:
        self.finished: list[tuple[str, str | None]] = []
        self.retries: list[str | None] = []
        self.fallbacks: list[tuple[str, str]] = []

    def on_retry_scheduled(self, operation: str, model: str | None = None) -> None:
        self.retries.append(model)

    def on_finished(self, operation, outcome, duration, usage, model: str | None = None) -> None:  # noqa: ANN001
        self.finished.append((outcome, model))

    def on_fallback(self, operation: str, model: str) -> None:
        self.fallbacks.append((operation, model))


# --------------------------------------------------------------------------- a model per call (Req 2.2)


async def test_by_default_the_configured_model_is_used_and_the_completion_names_none() -> None:
    adapter = FakeAdapter(["ok"])
    result = await provider(adapter).complete("sys", MESSAGES, operation="job")
    assert models(adapter) == ["big"] and result.model is None  # None means "the provider's own model"


async def test_a_model_given_for_one_call_is_used_for_that_call_only() -> None:
    adapter = FakeAdapter(["a", "b"])
    p = provider(adapter)
    first = await p.complete("sys", MESSAGES, operation="job", model="small")
    second = await p.complete("sys", MESSAGES, operation="job")
    assert models(adapter) == ["small", "big"]
    assert first.model == "small" and second.model is None
    assert p.params.model == "big"  # the provider's default never changes


async def test_the_other_sampling_parameters_are_kept_when_the_model_changes() -> None:
    adapter = FakeAdapter(["a"])
    await provider(adapter).complete("sys", MESSAGES, operation="job", model="small")
    call = adapter.calls[0].params
    assert (call.temperature, call.max_output_tokens) == (0.0, 256)


# --------------------------------------------------------------------------- the fallback (Req 2.3)


async def test_a_permanent_error_is_retried_once_on_the_fallback_model() -> None:
    adapter = FakeAdapter([PermanentLLMError("no such model"), completion("saved", 3, 4)])
    result = await provider(adapter).complete("sys", MESSAGES, operation="job")
    assert models(adapter) == ["big", "backup"]
    assert result.content == "saved" and result.model == "backup"  # Req 2.5: the model that answered
    assert result.usage == TokenUsage(input_tokens=3, output_tokens=4, total_tokens=7)


async def test_no_fallback_is_configured_so_the_failure_stands() -> None:
    adapter = FakeAdapter([PermanentLLMError("no such model")])
    with pytest.raises(LLMCallFailed) as info:
        await provider(adapter, fallback=None).complete("sys", MESSAGES, operation="job")
    assert models(adapter) == ["big"]
    assert info.value.permanent is True and info.value.auth is False


async def test_the_fallback_is_used_after_a_permanent_error_without_retrying_it_first() -> None:
    adapter = FakeAdapter([PermanentLLMError("bad request"), "ok"])
    await provider(adapter, retry_count=3).complete("sys", MESSAGES, operation="job")
    assert models(adapter) == ["big", "backup"]  # a permanent error is never retried on the same model


async def test_a_fallback_that_equals_the_model_used_is_not_called() -> None:
    adapter = FakeAdapter([PermanentLLMError("bad")])
    with pytest.raises(LLMCallFailed):
        await provider(adapter, fallback="big").complete("sys", MESSAGES, operation="job")
    assert models(adapter) == ["big"]


async def test_the_fallback_follows_a_failed_capability_model() -> None:
    adapter = FakeAdapter([PermanentLLMError("bad"), "ok"])
    result = await provider(adapter).complete("sys", MESSAGES, operation="job", model="small")
    assert models(adapter) == ["small", "backup"] and result.model == "backup"


async def test_the_default_model_can_be_the_fallback_of_a_capability_model() -> None:
    adapter = FakeAdapter([PermanentLLMError("bad"), "ok"])
    result = await provider(adapter, fallback="big").complete("sys", MESSAGES, operation="job", model="small")
    assert models(adapter) == ["small", "big"] and result.model is None  # it answered as the default model


async def test_exactly_one_further_call_is_made_when_the_fallback_fails_too() -> None:
    adapter = FakeAdapter([PermanentLLMError("first"), PermanentLLMError("second")])
    with pytest.raises(LLMCallFailed) as info:
        await provider(adapter).complete("sys", MESSAGES, operation="job")
    assert models(adapter) == ["big", "backup"]  # never a third call
    assert info.value.model == "backup" and "second" in info.value.reason


async def test_a_fallback_model_given_with_stray_space_is_used_trimmed() -> None:
    adapter = FakeAdapter([PermanentLLMError("bad"), "ok"])
    p = provider(adapter, fallback="  backup  ")
    assert p.fallback_model == "backup"
    await p.complete("sys", MESSAGES, operation="job")
    assert models(adapter) == ["big", "backup"]


@pytest.mark.parametrize("blank", ["", "   "])
def test_a_blank_fallback_model_is_refused_at_construction(blank: str) -> None:
    from das.llm.errors import LLMInputError

    with pytest.raises(LLMInputError, match="fallback_model"):
        provider(FakeAdapter(), fallback=blank)


# --------------------------------------------------------------------------- what does not fall back (Req 2.3, 2.4)


async def test_an_authentication_error_never_falls_back() -> None:
    adapter = FakeAdapter([AuthLLMError("bad key")])
    with pytest.raises(LLMCallFailed) as info:
        await provider(adapter).complete("sys", MESSAGES, operation="job")
    assert models(adapter) == ["big"]
    assert info.value.auth is True and info.value.permanent is True


async def test_transient_errors_that_run_out_of_retries_do_not_fall_back() -> None:
    adapter = FakeAdapter(default=TransientLLMError("overloaded"))
    with pytest.raises(LLMCallFailed) as info:
        await provider(adapter, retry_count=2).complete("sys", MESSAGES, operation="job")
    assert models(adapter) == ["big", "big", "big"]
    assert info.value.permanent is False


async def test_a_success_never_touches_the_fallback() -> None:
    adapter = FakeAdapter(["ok"])
    await provider(adapter).complete("sys", MESSAGES, operation="job")
    assert models(adapter) == ["big"]


# --------------------------------------------------------------------------- the observer


async def test_the_observer_hears_the_fallback_with_the_model_that_failed() -> None:
    adapter = FakeAdapter([PermanentLLMError("bad"), "ok"])
    rec = Recorder()
    await provider(adapter, observer=rec).complete("sys", MESSAGES, operation="job", model="small")
    assert rec.fallbacks == [("job", "small")]
    assert rec.finished == [("non_transient_error", "small"), ("success", "backup")]  # each call under its own model


async def test_an_observer_without_a_model_argument_still_works_when_the_default_model_is_used() -> None:
    class OldStyle:
        def __init__(self) -> None:
            self.seen: list[str] = []

        def on_retry_scheduled(self, operation: str) -> None:
            self.seen.append("retry")

        def on_finished(self, operation, outcome, duration, usage) -> None:  # noqa: ANN001
            self.seen.append(outcome)

    old = OldStyle()
    await provider(FakeAdapter(["ok"]), observer=old).complete("sys", MESSAGES, operation="job")
    assert old.seen == ["success"]


async def test_a_failing_observer_never_breaks_the_fallback() -> None:
    class Broken:
        def on_retry_scheduled(self, operation: str, model: str | None = None) -> None:
            raise RuntimeError("boom")

        def on_finished(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("boom")

        def on_fallback(self, operation: str, model: str) -> None:
            raise RuntimeError("boom")

    adapter = FakeAdapter([PermanentLLMError("bad"), "ok"])
    result = await provider(adapter, observer=Broken()).complete("sys", MESSAGES, operation="job")
    assert result.content == "ok"
