"""The ``run_python`` tool and the ``code-execution`` Capability (das-smarter-agents task 6.3; Phase 6 Req 6.1, 6.3,
6.9): the model returns code through one tool, a Job that requires ``code-execution`` is offered it, and what the
sandbox produced goes back as the tool's result text."""

from __future__ import annotations

from typing import Any

import pytest

from das.clock import FakeClock
from das.config.schema import ToolsConfig
from das.observability.metrics import CATALOG_BY_NAME, SANDBOX_OUTCOMES, MetricsRecorder, WorkerMetrics
from das.sandbox.runner import MAX_CODE_BYTES, RunResult
from das.sandbox.tool import CODE_EXECUTION, RUN_PYTHON
from das.tools.registry import ToolRegistry, build_registry
from das.tools.types import ToolCall, ToolError, ToolTimeout


class FakeRunner:
    def __init__(self, result: RunResult | Exception) -> None:
        self.result = result
        self.codes: list[Any] = []

    async def run(self, code: Any) -> RunResult:
        self.codes.append(code)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def result(outcome: str = "ok", code: int | None = 0, output: str = "", truncated: bool = False) -> RunResult:
    return RunResult(outcome, code, output, truncated, 0.1)


def registry(runner: FakeRunner, *enabled: str) -> ToolRegistry:
    config = ToolsConfig.model_validate({"enabled": list(enabled)})
    return build_registry(config, FakeClock(), sandbox=runner)  # type: ignore[arg-type]


def call(code: object = "print(1)") -> ToolCall:
    return ToolCall("c1", "run_python", {"code": code})


# ----------------------------------------------------------------------- the Capability and the offer (Req 6.1, 6.3)


def test_the_tool_is_named_run_python_and_takes_one_required_string() -> None:
    assert RUN_PYTHON.name == "run_python" and RUN_PYTHON.description
    assert RUN_PYTHON.parameters["required"] == ["code"] and RUN_PYTHON.parameters["additionalProperties"] is False
    assert RUN_PYTHON.parameters["properties"]["code"]["type"] == "string"


def test_a_registry_with_a_sandbox_advertises_code_execution_and_nothing_else() -> None:
    assert registry(FakeRunner(result())).capabilities == frozenset({CODE_EXECUTION}) == frozenset({"code-execution"})


def test_the_capability_adds_to_the_enabled_tools_capabilities() -> None:
    reg = registry(FakeRunner(result()), "calculator")
    assert reg.capabilities == frozenset({"tool-calculator", "code-execution"})


def test_a_job_requiring_code_execution_is_offered_run_python_only() -> None:
    reg = registry(FakeRunner(result()), "calculator")
    assert [s.name for s in reg.specs_for(["llm", "code-execution"])] == ["run_python"]
    assert [s.name for s in reg.specs_for(["code-execution", "tool-calculator"])] == ["calculator", "run_python"]


def test_the_tool_is_not_reachable_by_a_tool_style_capability_name() -> None:
    assert registry(FakeRunner(result())).specs_for(["tool-run-python"]) == ()


def test_without_a_sandbox_there_is_no_run_python_and_no_capability() -> None:
    reg = build_registry(ToolsConfig.model_validate({"enabled": ["calculator"]}), FakeClock())
    assert "run_python" not in reg.names and CODE_EXECUTION not in reg.capabilities
    assert reg.specs_for(["code-execution"]) == ()


def test_run_python_cannot_be_enabled_by_the_tools_section() -> None:
    with pytest.raises(ValueError):
        ToolsConfig.model_validate({"enabled": ["run_python"]})


# --------------------------------------------------------------------------- what the model reads (Req 6.3)


async def test_the_code_goes_to_the_sandbox_and_the_exit_code_and_output_come_back() -> None:
    runner = FakeRunner(result(output="hello\n"))
    assert await registry(runner).run(call("print('hello')")) == "exit code 0\nhello\n"
    assert runner.codes == ["print('hello')"]


async def test_a_program_that_prints_nothing_says_so() -> None:
    assert await registry(FakeRunner(result(output=""))).run(call()) == "exit code 0\n(no output)"


async def test_a_failing_program_is_a_result_the_model_can_read_not_a_tool_error() -> None:
    out = await registry(FakeRunner(result("error", 1, "Traceback...\nZeroDivisionError"))).run(call("1/0"))
    assert out == "exit code 1\nTraceback...\nZeroDivisionError"


async def test_a_killed_container_is_explained() -> None:
    out = await registry(FakeRunner(result("killed", 137, ""))).run(call())
    assert out.startswith("exit code 137") and "killed" in out and "memory" in out


async def test_output_that_was_cut_says_so() -> None:
    out = await registry(FakeRunner(result(output="x" * 10, truncated=True))).run(call())
    assert out.endswith("[output truncated]") and "x" * 10 in out


async def test_a_timeout_is_a_tool_timeout_that_keeps_the_output_so_far() -> None:
    with pytest.raises(ToolTimeout) as info:
        await registry(FakeRunner(result("timeout", None, "step 1\n"))).run(call("while True: pass"))
    assert "time" in str(info.value) and "step 1" in str(info.value)


async def test_a_timeout_with_no_output_has_no_dangling_output_section() -> None:
    with pytest.raises(ToolTimeout) as info:
        await registry(FakeRunner(result("timeout", None, ""))).run(call())
    assert "output so far" not in str(info.value)


async def test_a_container_that_could_not_start_is_a_tool_error_without_internals() -> None:
    with pytest.raises(ToolError) as info:
        await registry(FakeRunner(result("error", None, ""))).run(call())
    assert "could not" in str(info.value)


# --------------------------------------------------------------------------- bad arguments


@pytest.mark.parametrize("bad", [None, 5, ["print(1)"], {"a": 1}, b"print(1)"])
async def test_code_that_is_not_text_is_a_tool_error_and_nothing_runs(bad: object) -> None:
    runner = FakeRunner(result())
    with pytest.raises(ToolError, match="code"):
        await registry(runner).run(call(bad))
    assert runner.codes == []


async def test_a_missing_code_argument_is_a_tool_error() -> None:
    runner = FakeRunner(result())
    with pytest.raises(ToolError, match="code"):
        await registry(runner).run(ToolCall("c1", "run_python", {}))
    assert runner.codes == []


async def test_code_that_is_too_large_is_a_tool_error_the_model_can_act_on() -> None:
    runner = FakeRunner(ValueError(f"the code is too large (at most {MAX_CODE_BYTES} bytes)"))
    with pytest.raises(ToolError, match="too large"):
        await registry(runner).run(call("x"))


async def test_a_blank_program_is_refused_without_starting_a_container() -> None:
    runner = FakeRunner(result())
    with pytest.raises(ToolError, match="empty"):
        await registry(runner).run(call("   \n"))
    assert runner.codes == []


# --------------------------------------------------------------------------- the metric (Req 6.9)


def test_the_sandbox_counter_has_exactly_the_four_outcomes() -> None:
    assert SANDBOX_OUTCOMES == frozenset({"ok", "error", "timeout", "killed"})
    spec = CATALOG_BY_NAME["das_sandbox_runs_total"]
    assert spec.kind == "counter" and spec.labels == ("outcome",)


def test_each_run_is_counted_under_its_outcome() -> None:
    rec = MetricsRecorder(clock=FakeClock())
    metrics = WorkerMetrics(rec)
    for outcome in ("ok", "ok", "error", "timeout", "killed"):
        metrics.sandbox_run(outcome)
    get = rec.registry.get_sample_value
    assert get("das_sandbox_runs_total", {"outcome": "ok"}) == 2
    assert [get("das_sandbox_runs_total", {"outcome": o}) for o in ("error", "timeout", "killed")] == [1, 1, 1]


def test_an_outcome_outside_the_four_is_not_counted_and_does_not_raise() -> None:
    rec = MetricsRecorder(clock=FakeClock())
    WorkerMetrics(rec).sandbox_run("exploded")
    assert rec.registry.get_sample_value("das_sandbox_runs_total", {"outcome": "exploded"}) is None
