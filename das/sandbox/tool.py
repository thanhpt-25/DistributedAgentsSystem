"""``run_python``: the one tool through which a model's code reaches the sandbox (Phase 6 Req 6.3).

A Job that requires the ``code-execution`` Capability is offered this tool and runs through the ordinary tool loop
(Req 5). What the sandbox produced goes back as the tool's result: the exit code and the output (stdout and stderr). A
program that fails is a result the model can read and fix, not a tool error; a run out of time is a
:class:`ToolTimeout` that keeps the output so far.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Final, Protocol

from das.config.schema import CODE_EXECUTION
from das.sandbox.runner import RunResult
from das.tools.types import ToolError, ToolSpec, ToolTimeout

RUN_PYTHON: Final = ToolSpec(
    "run_python",
    "Run a Python 3 program in an isolated sandbox with no network and no files. Returns its exit code and what it "
    "printed (stdout and stderr). Print the values you need.",
    {
        "type": "object",
        "properties": {"code": {"type": "string", "description": "The complete program"}},
        "required": ["code"],
        "additionalProperties": False,
    },
)
_KILLED: Final = "[the container was killed, probably for using too much memory]"


class _Runner(Protocol):
    async def run(self, code: str) -> RunResult: ...


def _result_text(result: RunResult) -> str:
    text = f"exit code {result.exit_code}\n{result.output or '(no output)'}"
    if result.truncated:
        text += "\n[output truncated]"
    if result.outcome == "killed":
        text += f"\n{_KILLED}"
    return text


def run_python_tool(runner: _Runner) -> Callable[[dict[str, Any]], Awaitable[str]]:
    """The tool's function: validate the code, run it, and say what happened."""

    async def run(arguments: dict[str, Any]) -> str:
        if "code" not in arguments:
            raise ToolError("missing argument: code")
        code = arguments["code"]
        if not isinstance(code, str):
            raise ToolError("the code argument must be a string")
        if not code.strip():
            raise ToolError("the code is empty")
        try:
            result = await runner.run(code)
        except ValueError as exc:  # too large: the only thing the runner refuses
            raise ToolError(str(exc)) from None
        if result.outcome == "timeout":
            so_far = f"; output so far:\n{result.output}" if result.output else ""
            raise ToolTimeout(f"the code did not finish in time and was stopped{so_far}")
        if result.exit_code is None:
            raise ToolError("the code could not be run")
        return _result_text(result)

    return run


__all__ = ["CODE_EXECUTION", "RUN_PYTHON", "run_python_tool"]
