"""JobExecutor with tools (das-smarter-agents task 5.5; Phase 6 Req 5.5 to 5.7): a Job that requires ``tool-<name>``
Capabilities runs the tool loop, any other Job runs exactly as in Phase 5, and the Job timeout and a cancel stop the
loop during a tool call."""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any, ClassVar

from das.clock import FakeClock
from das.config.schema import ToolsConfig
from das.llm.base import Completion, ModelParams
from das.llm.errors import LLMCallFailed
from das.llm.retry import LLMProvider
from das.models.results import JobDispatchPayload, TokenUsage
from das.tools.registry import Tool, ToolRegistry, build_registry
from das.tools.types import ToolCall, ToolResults, ToolSpec, ToolTurn, Turn
from das.worker.executor import JobExecutor

PARAMS = ModelParams(model="big", temperature=0.0, max_output_tokens=64)
SCHEMA = {"type": "object", "properties": {"n": {"type": "integer"}}, "required": ["n"], "additionalProperties": False}


def usage(tokens: int) -> TokenUsage:
    return TokenUsage(input_tokens=tokens, output_tokens=1, total_tokens=tokens + 1)


def turn(text: str = "", *calls: ToolCall, tokens: int = 10, model: str | None = None) -> ToolTurn:
    return ToolTurn(text, tuple(calls), usage(tokens), model=model)


def calc(expression: str = "2+3", i: int = 1) -> ToolCall:
    return ToolCall(f"c{i}", "calculator", {"expression": expression})


class ToolAdapter:
    """A scripted adapter for the tool conversation that also answers plain completions."""

    provider: ClassVar[str] = "fake"

    def __init__(self, *outcomes: ToolTurn | BaseException, plain: str = "plain") -> None:
        self.outcomes = list(outcomes)
        self.plain = plain
        self.tool_calls: list[tuple[tuple[Turn, ...], tuple[str, ...], str]] = []
        self.plain_calls = 0

    async def complete_with_tools_once(
        self, system: str, turns: Sequence[Turn], tools: Sequence[ToolSpec], params: ModelParams, timeout: float
    ) -> ToolTurn:
        self.tool_calls.append((tuple(turns), tuple(t.name for t in tools), params.model))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def complete_once(self, system: Any, messages: Any, params: Any, timeout: Any) -> Completion:
        self.plain_calls += 1
        return Completion(self.plain, usage(5))

    async def ping(self) -> bool:
        return True


async def no_sleep(_: float) -> None:
    return None


class Seen:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def tool_call(self, tool: str, outcome: str) -> None:
        self.calls.append((tool, outcome))


def registry(*enabled: str) -> ToolRegistry:
    return build_registry(ToolsConfig.model_validate({"enabled": list(enabled or ["calculator"])}), FakeClock())


def job(*capabilities: str, schema: dict | None = None) -> JobDispatchPayload:
    data: dict = {
        "request_id": "r1", "job_id": "a", "attempt": 1, "instruction": "add 2 and 3",
        "required_capabilities": list(capabilities or ["llm", "tool-calculator"]),
    }
    if schema is not None:
        data["output_schema"] = schema
    return JobDispatchPayload.model_validate(data)


def executor(
    adapter: ToolAdapter,
    tools: ToolRegistry | None = None,
    *,
    rounds: int = 4,
    timeout: float = 5,
    observer: Seen | None = None,
    models: dict[str, str] | None = None,
) -> JobExecutor:
    llm = LLMProvider(adapter, PARAMS, retry_count=0, timeout=2, sleep=no_sleep)  # type: ignore[arg-type]
    return JobExecutor(
        llm, "w1", job_timeout=timeout, capability_models=models,
        tools=tools if tools is not None else registry(), max_tool_rounds=rounds, tool_observer=observer,
    )


# --------------------------------------------------------------------------- which Jobs use tools (Req 5.5)


async def test_a_job_that_requires_a_tool_capability_runs_the_loop() -> None:
    adapter = ToolAdapter(turn("", calc()), turn("5", tokens=20))
    result = await executor(adapter).execute(job(), asyncio.Event())
    assert (result.status, result.content, result.error) == ("Succeeded", "5", None)
    assert adapter.plain_calls == 0 and [t[1] for t in adapter.tool_calls] == [("calculator",), ("calculator",)]
    results = adapter.tool_calls[1][0][-1]
    assert isinstance(results, ToolResults) and results.results[0].content == "5"


async def test_a_job_that_requires_no_tool_runs_exactly_as_in_phase_5() -> None:
    adapter = ToolAdapter()
    result = await executor(adapter).execute(job("llm"), asyncio.Event())
    assert (result.status, result.content) == ("Succeeded", "plain")
    assert adapter.tool_calls == [] and adapter.plain_calls == 1


async def test_the_job_is_offered_exactly_the_tools_it_requires() -> None:
    reg = registry("calculator", "current_time")
    adapter = ToolAdapter(turn("ok"))
    await executor(adapter, reg).execute(job("llm", "tool-current-time"), asyncio.Event())
    assert adapter.tool_calls[0][1] == ("current_time",)


async def test_a_worker_with_no_tools_ignores_the_tool_capabilities_of_a_job() -> None:
    adapter = ToolAdapter()
    llm = LLMProvider(adapter, PARAMS, retry_count=0, timeout=2, sleep=no_sleep)  # type: ignore[arg-type]
    result = await JobExecutor(llm, "w1").execute(job(), asyncio.Event())
    assert result.content == "plain" and adapter.tool_calls == []


# --------------------------------------------------------------------------- usage and the model (Req 5.6, 2.5)


async def test_the_tokens_of_every_round_are_in_the_job_result() -> None:
    adapter = ToolAdapter(turn("", calc(), tokens=10), turn("", calc(), tokens=20), turn("done", tokens=30))
    result = await executor(adapter).execute(job(), asyncio.Event())
    assert result.usage == TokenUsage(input_tokens=60, output_tokens=3, total_tokens=63)


async def test_the_capability_model_runs_the_whole_loop_and_is_remembered_for_the_headers() -> None:
    adapter = ToolAdapter(turn("", calc()), turn("5"))
    ex = executor(adapter, models={"llm": "small"})
    result = await ex.execute(job(), asyncio.Event(), routing_capability="llm")
    assert [t[2] for t in adapter.tool_calls] == ["small", "small"]
    assert ex.headers_for(result)["x-llm-model"] == "small"


# --------------------------------------------------------------------------- failures (Req 5.6)


async def test_a_model_that_never_stops_asking_fails_the_job_with_tool_rounds_exceeded_and_its_tokens() -> None:
    adapter = ToolAdapter(*[turn("", calc(), tokens=10) for _ in range(3)])
    result = await executor(adapter, rounds=2).execute(job(), asyncio.Event())
    assert (result.status, result.error) == ("Failed", "tool_rounds_exceeded")
    assert result.usage.total_tokens == 33


async def test_a_completion_that_fails_mid_loop_fails_the_job_keeping_the_tokens_spent() -> None:
    adapter = ToolAdapter(turn("", calc(), tokens=10), LLMCallFailed("fake", "big", "boom", 1))
    result = await executor(adapter).execute(job(), asyncio.Event())
    assert result.status == "Failed" and "LLM call failed" in (result.error or "") and "boom" in (result.error or "")
    assert result.usage.total_tokens == 11


async def test_a_first_completion_that_fails_is_an_ordinary_llm_failure() -> None:
    adapter = ToolAdapter(LLMCallFailed("fake", "big", "boom", 1))
    result = await executor(adapter).execute(job(), asyncio.Event())
    assert result.status == "Failed" and "boom" in (result.error or "") and result.usage == TokenUsage.zero()


async def test_a_tool_error_does_not_fail_the_job() -> None:
    adapter = ToolAdapter(turn("", calc("1/0")), turn("I cannot do that."))
    result = await executor(adapter).execute(job(), asyncio.Event())
    assert (result.status, result.content) == ("Succeeded", "I cannot do that.")


async def test_the_tool_observer_hears_every_call() -> None:
    seen = Seen()
    adapter = ToolAdapter(turn("", calc(), calc("1/0", 2)), turn("ok"))
    await executor(adapter, observer=seen).execute(job(), asyncio.Event())
    assert seen.calls == [("calculator", "ok"), ("calculator", "error")]


# --------------------------------------------------------------------------- the timeout and a cancel (Req 5.6)


class Hanging:
    """A registry whose only tool never returns; records whether it was cancelled."""

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled = False

    async def run(self, arguments: dict[str, Any]) -> str:
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        return "never"

    def registry(self) -> ToolRegistry:
        return ToolRegistry([Tool(ToolSpec("calculator", "x", {"type": "object"}), self.run)])


async def test_the_job_timeout_stops_the_loop_during_a_tool_call() -> None:
    hang = Hanging()
    adapter = ToolAdapter(turn("", calc()))
    result = await executor(adapter, hang.registry(), timeout=0.05).execute(job(), asyncio.Event())
    assert result.status == "TimedOut" and hang.cancelled


async def test_a_cancel_stops_the_loop_during_a_tool_call() -> None:
    hang = Hanging()
    adapter = ToolAdapter(turn("", calc()))
    cancel = asyncio.Event()
    task = asyncio.create_task(executor(adapter, hang.registry()).execute(job(), cancel))
    await hang.started.wait()
    cancel.set()
    result = await task
    assert result.status == "Cancelled" and hang.cancelled


async def test_a_job_already_cancelled_makes_no_call() -> None:
    adapter = ToolAdapter(turn("ok"))
    cancel = asyncio.Event()
    cancel.set()
    result = await executor(adapter).execute(job(), cancel)
    assert result.status == "Cancelled" and adapter.tool_calls == []


# --------------------------------------------------------------------------- a Structured_Job that also uses tools


async def test_a_structured_job_with_tools_returns_canonical_json() -> None:
    adapter = ToolAdapter(turn("", calc()), turn('{ "n": 5 }', tokens=20))
    result = await executor(adapter).execute(job(schema=SCHEMA), asyncio.Event())
    assert (result.status, result.content) == ("Succeeded", '{"n":5}')


async def test_a_structured_job_with_tools_gets_one_corrective_completion() -> None:
    adapter = ToolAdapter(turn("", calc(), tokens=10), turn("five", tokens=10), turn('{"n": 5}', tokens=10))
    result = await executor(adapter).execute(job(schema=SCHEMA), asyncio.Event())
    assert (result.status, result.content) == ("Succeeded", '{"n":5}')
    assert result.usage.total_tokens == 33 and len(adapter.tool_calls) == 3


async def test_a_structured_job_with_tools_that_stays_wrong_fails_with_the_tokens() -> None:
    adapter = ToolAdapter(turn("five", tokens=10), turn("still five", tokens=10))
    result = await executor(adapter).execute(job(schema=SCHEMA), asyncio.Event())
    assert result.status == "Failed" and (result.error or "").startswith("output_schema_violation")
    assert result.usage.total_tokens == 22


async def test_a_corrective_completion_with_valid_text_but_a_tool_call_is_still_a_violation() -> None:
    adapter = ToolAdapter(turn("five", tokens=10), turn('{"n": 5}', calc(), tokens=10))
    result = await executor(adapter).execute(job(schema=SCHEMA), asyncio.Event())
    assert result.status == "Failed" and (result.error or "").startswith("output_schema_violation")


async def test_a_corrective_completion_that_fails_keeps_the_tokens_already_spent() -> None:
    adapter = ToolAdapter(turn("", calc(), tokens=10), turn("five", tokens=10), LLMCallFailed("fake", "big", "boom", 1))
    result = await executor(adapter).execute(job(schema=SCHEMA), asyncio.Event())
    assert result.status == "Failed" and "boom" in (result.error or "") and result.usage.total_tokens == 22


async def test_the_model_that_answered_the_correction_is_remembered_for_the_headers() -> None:
    adapter = ToolAdapter(turn("five", model="small"), turn('{"n": 5}'))
    ex = executor(adapter, models={"llm": "small"})
    result = await ex.execute(job(schema=SCHEMA), asyncio.Event(), routing_capability="llm")
    assert result.status == "Succeeded" and ex.headers_for(result)["x-llm-model"] == "small"
    assert [t[2] for t in adapter.tool_calls] == ["small", "small"]


async def test_a_corrective_completion_that_asks_for_tools_again_is_a_violation_not_a_new_round() -> None:
    adapter = ToolAdapter(turn("five", tokens=10), turn("", calc(), tokens=10))
    result = await executor(adapter).execute(job(schema=SCHEMA), asyncio.Event())
    assert result.status == "Failed" and (result.error or "").startswith("output_schema_violation")
    assert len(adapter.tool_calls) == 2
