"""``LLMProvider.complete_with_tools``: the same retries, fallback and observation as ``complete``
(das-smarter-agents task 5.1; Phase 6 Req 5.1, 5.6)."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import ClassVar

import pytest

from das.llm.base import ChatMessage, ModelParams
from das.llm.errors import AuthLLMError, LLMCallFailed, LLMInputError, PermanentLLMError, TransientLLMError
from das.llm.retry import LLMProvider
from das.models.results import TokenUsage
from das.tools.types import AssistantTurn, ToolCall, ToolResult, ToolResults, ToolSpec, ToolTurn, Turn
from tests.fakes.llm_adapter import FakeAdapter

PARAMS = ModelParams(model="big", temperature=0.0, max_output_tokens=64)
TOOLS = [ToolSpec("calculator", "Do sums.", {"type": "object"})]
CALL = ToolCall("c1", "calculator", {"expression": "1+1"})
TURNS: list[Turn] = [ChatMessage(role="user", content="1+1?")]


def turn(text: str = "", calls: tuple[ToolCall, ...] = (), tokens: int = 3) -> ToolTurn:
    return ToolTurn(text, calls, TokenUsage(input_tokens=tokens, output_tokens=1, total_tokens=tokens + 1))


class ToolAdapter:
    """Answers ``complete_with_tools_once`` from a script of ToolTurns and exceptions, and records the calls."""

    provider: ClassVar[str] = "fake"

    def __init__(self, *outcomes: object) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[str, tuple[Turn, ...], tuple[ToolSpec, ...], ModelParams, float]] = []

    async def complete_with_tools_once(
        self, system: str, turns: Sequence[Turn], tools: Sequence[ToolSpec], params: ModelParams, timeout: float
    ) -> ToolTurn:
        self.calls.append((system, tuple(turns), tuple(tools), params, timeout))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome  # type: ignore[return-value]

    async def complete_once(self, *args: object, **kwargs: object) -> None:  # pragma: no cover - not used here
        raise AssertionError("complete_once must not be called")

    async def ping(self) -> bool:
        return True


async def no_sleep(_: float) -> None:
    return None


class Recorder:
    def __init__(self) -> None:
        self.finished: list[tuple[str, str, TokenUsage | None, str | None]] = []
        self.retries = 0
        self.fallbacks: list[str] = []

    def on_retry_scheduled(self, operation: str, model: str | None = None) -> None:
        self.retries += 1

    def on_finished(self, operation: str, outcome: str, duration: float, usage: TokenUsage | None,
                    model: str | None = None) -> None:
        self.finished.append((operation, outcome, usage, model))

    def on_fallback(self, operation: str, model: str) -> None:
        self.fallbacks.append(model)


def provider(adapter: object, *, retries: int = 0, fallback: str | None = None, observer: object = None) -> LLMProvider:
    return LLMProvider(adapter, PARAMS, retry_count=retries, timeout=2, sleep=no_sleep,  # type: ignore[arg-type]
                       fallback_model=fallback, observer=observer)  # type: ignore[arg-type]


async def ask(
    p: LLMProvider, turns: Sequence[Turn] = TURNS, tools: Sequence[ToolSpec] = TOOLS, **kw: object
) -> ToolTurn:
    return await p.complete_with_tools("sys", turns, tools, operation="job", **kw)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- support


def test_a_provider_knows_whether_its_adapter_can_call_tools() -> None:
    assert provider(ToolAdapter()).supports_tools is True
    assert provider(FakeAdapter()).supports_tools is False


async def test_asking_an_adapter_without_tool_support_is_an_input_error_and_nothing_is_called() -> None:
    adapter = FakeAdapter(["x"])
    with pytest.raises(LLMInputError, match="tools"):
        await ask(provider(adapter))
    assert adapter.calls == []


# --------------------------------------------------------------------------- the call


async def test_the_system_prompt_turns_tools_params_and_timeout_reach_the_adapter() -> None:
    adapter = ToolAdapter(turn("done"))
    result = await ask(provider(adapter))
    assert result.text == "done" and result.model is None
    system, turns, tools, params, timeout = adapter.calls[0]
    assert (system, turns, tools, params, timeout) == ("sys", tuple(TURNS), tuple(TOOLS), PARAMS, 2.0)


async def test_a_model_for_this_call_is_used_and_named() -> None:
    adapter = ToolAdapter(turn("done"))
    result = await ask(provider(adapter), model="small")
    assert adapter.calls[0][3].model == "small" and result.model == "small"


async def test_tool_calls_come_back_as_data() -> None:
    result = await ask(provider(ToolAdapter(turn("", (CALL,)))))
    assert result.calls == (CALL,) and result.text == ""


async def test_a_conversation_with_earlier_tool_turns_is_accepted() -> None:
    turns: list[Turn] = [*TURNS, AssistantTurn("", (CALL,)), ToolResults((ToolResult("c1", "2"),))]
    adapter = ToolAdapter(turn("2"))
    assert (await ask(provider(adapter), turns)).text == "2" and adapter.calls[0][1] == tuple(turns)


# --------------------------------------------------------------------------- retries and fallback


async def test_transient_errors_are_retried_up_to_the_retry_count() -> None:
    adapter = ToolAdapter(TransientLLMError("a"), TransientLLMError("b"), turn("ok"))
    assert (await ask(provider(adapter, retries=2))).text == "ok" and len(adapter.calls) == 3


async def test_transient_errors_past_the_retry_count_fail_the_call() -> None:
    adapter = ToolAdapter(TransientLLMError("a"), TransientLLMError("b"))
    with pytest.raises(LLMCallFailed) as info:
        await ask(provider(adapter, retries=1))
    assert len(adapter.calls) == 2 and info.value.permanent is False


async def test_a_permanent_error_falls_back_once_to_the_fallback_model() -> None:
    adapter = ToolAdapter(PermanentLLMError("gone"), turn("saved"))
    result = await ask(provider(adapter, fallback="backup"))
    assert [c[3].model for c in adapter.calls] == ["big", "backup"] and result.model == "backup"


async def test_an_authentication_error_never_falls_back() -> None:
    adapter = ToolAdapter(AuthLLMError("bad key"))
    with pytest.raises(LLMCallFailed) as info:
        await ask(provider(adapter, fallback="backup"))
    assert len(adapter.calls) == 1 and info.value.auth is True


async def test_a_failing_fallback_is_the_end() -> None:
    adapter = ToolAdapter(PermanentLLMError("one"), PermanentLLMError("two"))
    with pytest.raises(LLMCallFailed) as info:
        await ask(provider(adapter, fallback="backup"))
    assert len(adapter.calls) == 2 and info.value.model == "backup"


async def test_an_adapter_that_returns_the_wrong_thing_fails_the_call() -> None:
    adapter = ToolAdapter("not a ToolTurn")
    with pytest.raises(LLMCallFailed, match="ToolTurn"):
        await ask(provider(adapter))


# --------------------------------------------------------------------------- observation


async def test_the_observer_hears_the_call_its_tokens_and_the_fallback() -> None:
    rec = Recorder()
    adapter = ToolAdapter(PermanentLLMError("gone"), turn("saved", tokens=9))
    await ask(provider(adapter, fallback="backup", observer=rec))
    assert rec.fallbacks == ["big"]
    assert [(op, outcome, model) for op, outcome, _, model in rec.finished] == [
        ("job", "non_transient_error", None), ("job", "success", "backup")]
    assert rec.finished[1][2] == TokenUsage(input_tokens=9, output_tokens=1, total_tokens=10)


async def test_a_retry_is_reported_to_the_observer() -> None:
    rec = Recorder()
    await ask(provider(ToolAdapter(TransientLLMError("a"), turn("ok")), retries=1, observer=rec))
    assert rec.retries == 1 and rec.finished[-1][1] == "success"


# --------------------------------------------------------------------------- bad input


@pytest.mark.parametrize("bad", [[], [ChatMessage(role="user", content="x")] * 201])
async def test_a_conversation_outside_the_allowed_size_is_an_input_error(bad: list[Turn]) -> None:
    adapter = ToolAdapter(turn())
    with pytest.raises(LLMInputError):
        await ask(provider(adapter), bad)
    assert adapter.calls == []


async def test_a_message_with_an_unknown_role_is_an_input_error() -> None:
    bad = [ChatMessage(role="robot", content="x")]  # type: ignore[arg-type]
    with pytest.raises(LLMInputError, match="role"):
        await ask(provider(ToolAdapter(turn())), bad)


async def test_a_conversation_of_tool_turns_alone_is_not_refused_for_having_no_chat_message() -> None:
    only_tools: list[Turn] = [AssistantTurn("", (CALL,)), ToolResults((ToolResult("c1", "2"),))]
    assert (await ask(provider(ToolAdapter(turn("ok"))), only_tools)).text == "ok"


async def test_a_chat_message_whose_content_is_not_text_is_an_input_error() -> None:
    bad = [ChatMessage(role="user", content=3)]  # type: ignore[arg-type]
    with pytest.raises(LLMInputError, match="content"):
        await ask(provider(ToolAdapter(turn())), bad)


async def test_a_call_without_tools_is_an_input_error() -> None:
    with pytest.raises(LLMInputError, match="tool"):
        await ask(provider(ToolAdapter(turn())), tools=[])


async def test_cancelling_the_call_propagates() -> None:
    class Hang(ToolAdapter):
        async def complete_with_tools_once(self, *args: object, **kwargs: object) -> ToolTurn:  # type: ignore[override]
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    task = asyncio.create_task(ask(LLMProvider(Hang(), PARAMS, retry_count=0, timeout=5)))  # type: ignore[arg-type]
    await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
