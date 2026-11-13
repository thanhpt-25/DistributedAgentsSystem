"""The tool loop (das-smarter-agents task 5.5; Phase 6 Req 5.5 to 5.7, 5.11): complete, run the requested tools, append
the results, complete again, until the model answers in text or the rounds are used up."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, ClassVar

import pytest

from das.clock import FakeClock
from das.config.schema import ToolsConfig
from das.llm.base import ChatMessage, ModelParams
from das.llm.errors import LLMCallFailed
from das.llm.retry import LLMProvider
from das.models.results import TokenUsage
from das.tools.loop import ToolLoop, ToolLoopAborted, ToolRoundsExceeded
from das.tools.registry import Tool, ToolRegistry, build_registry
from das.tools.types import (
    AssistantTurn, ToolCall, ToolResult, ToolResults, ToolSpec, ToolTimeout, ToolTurn, Turn,
)

PARAMS = ModelParams(model="big", temperature=0.0, max_output_tokens=64)
SPEC = ToolSpec("calculator", "Do sums.", {"type": "object"})
USER: list[Turn] = [ChatMessage(role="user", content="what is 2+3?")]


def turn(text: str = "", *calls: ToolCall, tokens: int = 10, model: str | None = None) -> ToolTurn:
    usage = TokenUsage(input_tokens=tokens, output_tokens=1, total_tokens=tokens + 1)
    return ToolTurn(text, tuple(calls), usage, model=model)


def call(i: int = 1, name: str = "calculator", **arguments: Any) -> ToolCall:
    return ToolCall(f"c{i}", name, arguments or {"expression": "2+3"})


class Adapter:
    provider: ClassVar[str] = "fake"

    def __init__(self, *outcomes: ToolTurn | BaseException) -> None:
        self.outcomes = list(outcomes)
        self.seen: list[tuple[Turn, ...]] = []
        self.offered: list[tuple[str, ...]] = []
        self.models: list[str] = []

    async def complete_with_tools_once(
        self, system: str, turns: Sequence[Turn], tools: Sequence[ToolSpec], params: ModelParams, timeout: float
    ) -> ToolTurn:
        self.seen.append(tuple(turns))
        self.offered.append(tuple(t.name for t in tools))
        self.models.append(params.model)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def complete_once(self, *a: object, **k: object) -> None:  # pragma: no cover
        raise AssertionError

    async def ping(self) -> bool:
        return True


async def no_sleep(_: float) -> None:
    return None


class Seen:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def tool_call(self, tool: str, outcome: str) -> None:
        self.calls.append((tool, outcome))


def loop(
    adapter: Adapter,
    registry: ToolRegistry | None = None,
    *,
    rounds: int = 4,
    cap: int = 16_384,
    observer: Seen | None = None,
) -> ToolLoop:
    llm = LLMProvider(adapter, PARAMS, retry_count=0, timeout=2, sleep=no_sleep)  # type: ignore[arg-type]
    reg = registry or build_registry(ToolsConfig.model_validate({"enabled": ["calculator"]}), FakeClock())
    return ToolLoop(llm, reg, max_rounds=rounds, max_result_bytes=cap, observer=observer)


def custom(name: str, run: Any) -> ToolRegistry:
    return ToolRegistry([Tool(ToolSpec(name, "x", {"type": "object"}), run)])


def spec_of(name: str) -> ToolSpec:
    return ToolSpec(name, "x", {"type": "object"})


# --------------------------------------------------------------------------- the happy path (Req 5.6)


async def test_a_model_that_answers_in_text_ends_the_loop_after_one_completion() -> None:
    adapter = Adapter(turn("5"))
    out = await loop(adapter).run("sys", USER, [SPEC])
    assert out.text == "5" and len(adapter.seen) == 1 and out.usage.total_tokens == 11


async def test_a_requested_tool_runs_and_its_result_goes_back_to_the_model() -> None:
    adapter = Adapter(turn("", call()), turn("The answer is 5."))
    out = await loop(adapter).run("sys", USER, [SPEC])
    assert out.text == "The answer is 5."
    second = adapter.seen[1]
    assert second[-2] == AssistantTurn("", (call(),))
    assert second[-1] == ToolResults((ToolResult("c1", "5", False, "calculator"),))


async def test_several_calls_in_one_turn_are_all_run_in_order_and_answered_in_one_turn() -> None:
    adapter = Adapter(turn("", call(1, expression="1+1"), call(2, expression="2+2")), turn("done"))
    await loop(adapter).run("sys", USER, [SPEC])
    results = adapter.seen[1][-1]
    assert isinstance(results, ToolResults) and [r.content for r in results.results] == ["2", "4"]


async def test_the_tokens_of_every_round_are_summed() -> None:
    adapter = Adapter(turn("", call(), tokens=10), turn("", call(), tokens=20), turn("ok", tokens=30))
    out = await loop(adapter).run("sys", USER, [SPEC])
    assert (out.usage.input_tokens, out.usage.output_tokens, out.usage.total_tokens) == (60, 3, 63)


async def test_the_model_of_the_last_completion_is_reported() -> None:
    adapter = Adapter(turn("", call(), model="small"), turn("ok", model="small"))
    assert (await loop(adapter).run("sys", USER, [SPEC])).model == "small"
    assert (await loop(Adapter(turn("ok"))).run("sys", USER, [SPEC])).model is None


async def test_a_model_that_rescued_a_round_keeps_answering_the_later_rounds() -> None:
    """A Fallback_Model that answered once is not asked to fail again on the next round (Phase 6 Req 2.3)."""
    adapter = Adapter(turn("", call(), model="small"), turn("", call()), turn("ok"))
    await loop(adapter, rounds=3).run("sys", USER, [SPEC])
    assert adapter.models == ["big", "small", "small"]


async def test_the_offered_tools_are_exactly_the_given_specs_every_round() -> None:
    adapter = Adapter(turn("", call()), turn("ok"))
    await loop(adapter).run("sys", USER, [SPEC])
    assert adapter.offered == [("calculator",), ("calculator",)]


async def test_the_conversation_returned_ends_with_the_final_answer() -> None:
    out = await loop(Adapter(turn("", call()), turn("done"))).run("sys", USER, [SPEC])
    assert out.turns[-1] == AssistantTurn("done", ())
    assert out.turns[0] == USER[0]


# --------------------------------------------------------------------------- rounds (Req 5.6)


async def test_the_job_may_use_exactly_max_rounds_rounds_of_tools_and_then_must_answer() -> None:
    adapter = Adapter(turn("", call()), turn("", call()), turn("fine"))
    assert (await loop(adapter, rounds=2).run("sys", USER, [SPEC])).text == "fine"
    assert len(adapter.seen) == 3


async def test_a_model_still_asking_for_tools_after_max_rounds_fails_with_the_tokens_spent() -> None:
    adapter = Adapter(turn("", call(), tokens=10), turn("", call(), tokens=10), turn("", call(), tokens=10))
    with pytest.raises(ToolRoundsExceeded) as info:
        await loop(adapter, rounds=2).run("sys", USER, [SPEC])
    assert info.value.usage.total_tokens == 33 and len(adapter.seen) == 3


async def test_the_tools_of_the_refused_last_round_are_not_run() -> None:
    ran: list[dict[str, Any]] = []

    async def run(arguments: dict[str, Any]) -> str:
        ran.append(arguments)
        return "r"

    adapter = Adapter(turn("", call(1, name="t")), turn("", call(2, name="t")))
    with pytest.raises(ToolRoundsExceeded):
        await loop(adapter, custom("t", run), rounds=1).run("sys", USER, [spec_of("t")])
    assert len(ran) == 1


# --------------------------------------------------------------------------- errors are answers (Req 5.7)


async def test_a_tool_error_goes_back_to_the_model_as_the_result_text() -> None:
    adapter = Adapter(turn("", call(expression="1/0")), turn("I cannot divide by zero."))
    seen = Seen()
    out = await loop(adapter, observer=seen).run("sys", USER, [SPEC])
    result = adapter.seen[1][-1]
    assert isinstance(result, ToolResults) and result.results[0].is_error and "zero" in result.results[0].content
    assert out.text == "I cannot divide by zero." and seen.calls == [("calculator", "error")]


async def test_a_call_whose_arguments_could_not_be_read_is_answered_without_running_anything() -> None:
    adapter = Adapter(turn("", ToolCall("c1", "calculator", {}, error="arguments are not a JSON object")), turn("ok"))
    seen = Seen()
    await loop(adapter, observer=seen).run("sys", USER, [SPEC])
    result = adapter.seen[1][-1]
    assert isinstance(result, ToolResults) and result.results[0].content == "arguments are not a JSON object"
    assert result.results[0].is_error and seen.calls == [("calculator", "error")]


async def test_a_tool_that_is_enabled_but_not_offered_for_this_job_is_refused() -> None:
    registry = build_registry(ToolsConfig.model_validate({"enabled": ["calculator", "current_time"]}), FakeClock())
    adapter = Adapter(turn("", call(1, name="current_time")), turn("ok"))
    await loop(adapter, registry).run("sys", USER, [SPEC])  # only the calculator is offered
    result = adapter.seen[1][-1]
    assert isinstance(result, ToolResults) and result.results[0].is_error
    assert "unknown tool" in result.results[0].content


async def test_an_unknown_tool_is_refused_and_counted_as_an_error_under_a_bounded_name() -> None:
    seen = Seen()
    await loop(Adapter(turn("", call(1, name="rm_rf")), turn("ok")), observer=seen).run("sys", USER, [SPEC])
    assert seen.calls == [("unknown", "error")]  # the metric's tool label stays bounded


async def test_a_tool_that_times_out_is_an_answer_counted_as_a_timeout() -> None:
    async def slow(arguments: dict[str, Any]) -> str:
        raise ToolTimeout("the request timed out")

    seen = Seen()
    adapter = Adapter(turn("", call(1, name="t")), turn("ok"))
    await loop(adapter, custom("t", slow), observer=seen).run("sys", USER, [spec_of("t")])
    assert seen.calls == [("t", "timeout")]
    result = adapter.seen[1][-1]
    assert isinstance(result, ToolResults) and "timed out" in result.results[0].content


async def test_a_tool_that_crashes_is_an_answer_that_does_not_leak_the_exception() -> None:
    async def crash(arguments: dict[str, Any]) -> str:
        raise RuntimeError("secret internal detail 10.1.2.3")

    seen = Seen()
    adapter = Adapter(turn("", call(1, name="t")), turn("ok"))
    await loop(adapter, custom("t", crash), observer=seen).run("sys", USER, [spec_of("t")])
    result = adapter.seen[1][-1]
    assert isinstance(result, ToolResults) and result.results[0].is_error
    assert "10.1.2.3" not in result.results[0].content and "secret" not in result.results[0].content
    assert seen.calls == [("t", "error")]


async def test_a_successful_call_is_counted_ok() -> None:
    seen = Seen()
    await loop(Adapter(turn("", call()), turn("ok")), observer=seen).run("sys", USER, [SPEC])
    assert seen.calls == [("calculator", "ok")]


# --------------------------------------------------------------------------- the result cap (Req 5.7)


async def test_a_result_over_the_cap_is_cut_to_the_cap_and_says_so() -> None:
    async def big(arguments: dict[str, Any]) -> str:
        return "x" * 5000

    adapter = Adapter(turn("", call(1, name="t")), turn("ok"))
    await loop(adapter, custom("t", big), cap=300).run("sys", USER, [spec_of("t")])
    result = adapter.seen[1][-1]
    assert isinstance(result, ToolResults)
    content = result.results[0].content
    assert len(content.encode()) <= 300 and "truncated" in content and content.startswith("x" * 100)


async def test_a_result_at_the_cap_is_unchanged() -> None:
    async def exact(arguments: dict[str, Any]) -> str:
        return "y" * 300

    adapter = Adapter(turn("", call(1, name="t")), turn("ok"))
    await loop(adapter, custom("t", exact), cap=300).run("sys", USER, [spec_of("t")])
    result = adapter.seen[1][-1]
    assert isinstance(result, ToolResults) and result.results[0].content == "y" * 300


async def test_the_cap_is_in_bytes_and_never_splits_a_character() -> None:
    async def wide(arguments: dict[str, Any]) -> str:
        return "é" * 1000  # two bytes each

    adapter = Adapter(turn("", call(1, name="t")), turn("ok"))
    # the cut falls inside a two-byte character: 300 minus the 19-byte note leaves 281 bytes
    await loop(adapter, custom("t", wide), cap=300).run("sys", USER, [spec_of("t")])
    result = adapter.seen[1][-1]
    assert isinstance(result, ToolResults)
    content = result.results[0].content
    assert len(content.encode()) <= 300 and "�" not in content and content.startswith("é" * 100)


# --------------------------------------------------------------------------- the model failing mid-loop


async def test_a_failure_of_a_later_completion_carries_the_tokens_already_spent() -> None:
    failure = LLMCallFailed("fake", "big", "boom", 1)
    adapter = Adapter(turn("", call(), tokens=10), failure)
    with pytest.raises(ToolLoopAborted) as info:
        await loop(adapter).run("sys", USER, [SPEC])
    assert info.value.cause is failure and info.value.usage.total_tokens == 11


async def test_a_failure_of_the_first_completion_is_raised_as_it_is() -> None:
    failure = LLMCallFailed("fake", "big", "boom", 1)
    with pytest.raises(LLMCallFailed):
        await loop(Adapter(failure)).run("sys", USER, [SPEC])


# --------------------------------------------------------------------------- what is logged (Req 5.11)


class LogSpy:
    """Stands in for the module's logger, so the test does not depend on how structlog was configured elsewhere."""

    def __init__(self) -> None:
        self.entries: list[tuple[str, str, dict[str, Any]]] = []

    def __getattr__(self, level: str) -> Any:
        return lambda event, **fields: self.entries.append((level, event, fields))


async def test_tool_names_and_sizes_are_logged_but_never_arguments_or_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import das.tools.loop as loop_module

    spy = LogSpy()
    monkeypatch.setattr(loop_module, "log", spy)
    adapter = Adapter(turn("", call(expression="123456789*987654321")), turn("ok"))
    await loop(adapter).run("sys", USER, [SPEC])
    finished = [f for _, event, f in spy.entries if event == "tool call finished"]
    assert [(f["tool"], f["outcome"], f["result_bytes"]) for f in finished] == [("calculator", "ok", 18)]
    assert "123456789" not in repr(spy.entries) and "121932631112635269" not in repr(spy.entries)
