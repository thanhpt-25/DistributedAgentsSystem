"""Tool types shared by the Provider_Adapters, the LLMProvider and the Worker (Phase 6 Req 5.1, 5.2). Pure data.

A conversation with tools is a list of *turns*: plain chat messages, what the assistant said and asked for
(:class:`AssistantTurn`), and what the tools answered (:class:`ToolResults`). Each adapter maps them to its own
provider's format. A model's answer is a :class:`ToolTurn`: text, tool calls, or both.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from das.llm.base import ChatMessage
from das.models.results import TokenUsage


class ToolError(Exception):
    """A tool could not do what it was asked. The text is what the model reads as the tool's answer (Req 5.7), so it
    says what was wrong in a few words and never repeats what the model sent."""


@dataclass(frozen=True, slots=True)
class ToolSpec:
    """A tool offered to the model: its name, what it does, and a JSON Schema object for its arguments."""

    name: str
    description: str
    parameters: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ToolCall:
    """The model asks for a tool. ``error`` is set when the arguments could not be read as a JSON object: the
    loop then answers the model with that error instead of running anything."""

    id: str
    name: str
    arguments: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


@dataclass(frozen=True, slots=True)
class ToolResult:
    """What a tool answered for one call; an error is text the model can read, never an exception (Req 5.7)."""

    call_id: str
    content: str
    is_error: bool = False
    name: str = ""  # some providers want the tool's name beside the call id


@dataclass(frozen=True, slots=True)
class AssistantTurn:
    """What the model said, with the tool calls it made, as it appears in the conversation."""

    text: str
    calls: tuple[ToolCall, ...]


@dataclass(frozen=True, slots=True)
class ToolResults:
    """The results of every call of the previous :class:`AssistantTurn`, in one turn."""

    results: tuple[ToolResult, ...]


Turn = ChatMessage | AssistantTurn | ToolResults


@dataclass(frozen=True, slots=True)
class ToolTurn:
    """One answer of the model in a conversation with tools.

    ``usage``, ``usage_reported``, ``usage_problems`` and ``model`` mean what they mean on
    :class:`das.llm.base.Completion`, so the LLMProvider treats both alike.
    """

    text: str
    calls: tuple[ToolCall, ...]
    usage: TokenUsage
    usage_reported: bool = True
    usage_problems: tuple[str, ...] = ()
    model: str | None = None

    def as_turn(self) -> AssistantTurn:
        return AssistantTurn(self.text, self.calls)


__all__ = [
    "AssistantTurn", "ToolCall", "ToolError", "ToolResult", "ToolResults", "ToolSpec", "ToolTurn", "Turn",
]
