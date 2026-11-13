"""The tool loop: complete, run the requested tools, append the results, complete again (Phase 6 Req 5.5 to 5.7, 5.11).

A *round* is one completion that asks for tools, plus running them. A Job may use ``max_rounds`` rounds; the model is
then asked once more, and a model that still asks for tools fails the Job (:class:`ToolRoundsExceeded`), its last
requests unrun. A tool's refusal or failure is never raised: it goes back to the model as the result text (Req 5.7),
cut to ``max_result_bytes``. Tool names and result sizes are logged; arguments and results never are (Req 5.11).

The loop does not race the Job timeout or a cancel: the executor runs it as a task and cancels the task, which stops
the loop between rounds and a tool in the middle of its call.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Final, Protocol

from das.llm.errors import LLMCallFailed
from das.llm.retry import LLMProvider
from das.models.results import TokenUsage
from das.observability import get_logger
from das.tools.registry import ToolRegistry
from das.tools.types import (
    ToolCall, ToolError, ToolResult, ToolResults, ToolSpec, ToolTimeout, Turn,
)

log = get_logger("das.tools.loop")

#: The ``tool`` label of a call to a tool this Job was not offered, so the metric's label stays bounded (Req 5.12).
UNKNOWN_TOOL: Final = "unknown"
_TRUNCATION_NOTE: Final = "\n[result truncated]"
_CRASHED: Final = "the tool failed"


class ToolObserver(Protocol):
    """Told the outcome of each tool call: ``ok``, ``error`` or ``timeout`` (Req 5.12)."""

    def tool_call(self, tool: str, outcome: str) -> None: ...


@dataclass(frozen=True, slots=True)
class LoopOutcome:
    """The model's final text, the tokens of every round, the model that answered last (``None``: the provider's own),
    and the whole conversation, ending with the final answer."""

    text: str
    usage: TokenUsage
    model: str | None
    turns: tuple[Turn, ...]


class ToolRoundsExceeded(Exception):
    """The model still asked for tools after ``max_rounds`` rounds (Req 5.6). Carries what was spent."""

    def __init__(self, usage: TokenUsage, model: str | None) -> None:
        super().__init__("tool_rounds_exceeded")
        self.usage = usage
        self.model = model


class ToolLoopAborted(Exception):
    """A completion after the first failed for good; ``usage`` is what the earlier rounds spent (Req 5.6)."""

    def __init__(self, cause: LLMCallFailed, usage: TokenUsage, model: str | None) -> None:
        super().__init__(str(cause))
        self.cause = cause
        self.usage = usage
        self.model = model


def _cut(text: str, limit: int) -> str:
    """``text`` within ``limit`` UTF-8 bytes, ending in a note when it was cut, never inside a character."""
    raw = text.encode()
    if len(raw) <= limit:
        return text
    room = max(0, limit - len(_TRUNCATION_NOTE.encode()))
    return raw[:room].decode(errors="ignore") + _TRUNCATION_NOTE


class ToolLoop:
    """Runs a conversation with tools to its final text."""

    def __init__(
        self,
        provider: LLMProvider,
        registry: ToolRegistry,
        *,
        max_rounds: int,
        max_result_bytes: int,
        observer: ToolObserver | None = None,
    ) -> None:
        self._provider = provider
        self._registry = registry
        self._max_rounds = max_rounds
        self._max_result_bytes = max_result_bytes
        self._observer = observer

    async def run(
        self, system: str, turns: Sequence[Turn], specs: Sequence[ToolSpec], *, model: str | None = None
    ) -> LoopOutcome:
        """Complete until the model answers in text. ``specs`` are the tools offered, and the only ones run."""
        conversation: list[Turn] = list(turns)
        offered = {spec.name for spec in specs}
        usage = TokenUsage.zero()
        answered_by = model
        rounds = 0
        while True:
            try:
                reply = await self._provider.complete_with_tools(
                    system, conversation, specs, operation="job", model=answered_by
                )
            except LLMCallFailed as failure:
                if rounds == 0:
                    raise
                raise ToolLoopAborted(failure, usage, answered_by) from failure
            usage = usage + reply.usage
            answered_by = reply.model if reply.model is not None else answered_by
            conversation.append(reply.as_turn())
            if not reply.calls:
                return LoopOutcome(reply.text, usage, reply.model, tuple(conversation))
            if rounds >= self._max_rounds:
                raise ToolRoundsExceeded(usage, answered_by)
            rounds += 1
            results = [await self._run_call(call, offered) for call in reply.calls]
            conversation.append(ToolResults(tuple(results)))

    async def _run_call(self, call: ToolCall, offered: set[str]) -> ToolResult:
        tool = call.name if call.name in offered else UNKNOWN_TOOL
        if call.error is not None:
            return self._answer(call, tool, "error", call.error, is_error=True)
        if tool == UNKNOWN_TOOL:
            return self._answer(call, tool, "error", f"unknown tool: {call.name[:60]!r}", is_error=True)
        try:
            content = await self._registry.run(call)
        except ToolTimeout as exc:
            return self._answer(call, tool, "timeout", str(exc), is_error=True)
        except ToolError as exc:
            return self._answer(call, tool, "error", str(exc), is_error=True)
        except Exception:  # noqa: BLE001 - a crashing tool must not fail the Job, nor leak its message to the model
            return self._answer(call, tool, "error", _CRASHED, is_error=True)
        return self._answer(call, tool, "ok", content, is_error=False)

    def _answer(self, call: ToolCall, tool: str, outcome: str, content: str, *, is_error: bool) -> ToolResult:
        capped = _cut(content, self._max_result_bytes)
        log.debug("tool call finished", tool=tool, outcome=outcome, result_bytes=len(capped.encode()))
        if self._observer is not None:
            self._observer.tool_call(tool, outcome)
        return ToolResult(call.id, capped, is_error, call.name)


__all__ = [
    "UNKNOWN_TOOL", "LoopOutcome", "ToolLoop", "ToolLoopAborted", "ToolObserver", "ToolRoundsExceeded",
]
