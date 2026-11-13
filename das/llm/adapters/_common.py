"""Helpers shared by the Provider_Adapters."""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from typing import Any

from das.llm.base import ChatMessage, Completion
from das.llm.errors import AuthLLMError, LLMError, PermanentLLMError, RateLimitedLLMError, TransientLLMError
from das.models.results import TokenUsage
from das.ops.usage import normalize_usage
from das.tools.types import ToolCall, ToolTurn, Turn

#: HTTP statuses that indicate a retryable failure: request timeout, rate
#: limit, and every server-side error (including Anthropic's 529 "overloaded").
TRANSIENT_STATUSES: frozenset[int] = frozenset({408, 429})

#: HTTP status that means the provider is rate limiting the caller.
RATE_LIMIT_STATUS = 429

#: Unauthenticated or unauthorized: no other model will do better (Phase 6 Req 2.4).
AUTH_STATUSES: frozenset[int] = frozenset({401, 403})


def is_transient_status(status: int) -> bool:
    return status in TRANSIENT_STATUSES or status >= 500


def error_for_status(provider: str, status: int, detail: str) -> LLMError:
    """Classify an HTTP error status as transient or permanent."""
    reason = f"{provider} HTTP {status}"
    if detail:
        reason = f"{reason}: {detail}"
    if status in AUTH_STATUSES:
        return AuthLLMError(reason)
    if status == RATE_LIMIT_STATUS:
        return RateLimitedLLMError(reason)
    if is_transient_status(status):
        return TransientLLMError(reason)
    return PermanentLLMError(reason)


def split_system(system: str, messages: Sequence[ChatMessage]) -> tuple[str, list[ChatMessage]]:
    """Fold ``system``-role messages into the system prompt.

    Anthropic and Bedrock accept only user/assistant turns in the message list,
    so system-role messages are appended to the system prompt in order.
    """
    parts = [system] if system else []
    turns: list[ChatMessage] = []
    for m in messages:
        if m.role == "system":
            if m.content:
                parts.append(m.content)
        else:
            turns.append(m)
    return "\n\n".join(parts), turns


def usage(input_tokens: object, output_tokens: object, total: object = None) -> TokenUsage:
    """Build normalized TokenUsage, computing the total when the provider omits it."""
    return normalize_usage(input_tokens, output_tokens, total).usage


def make_completion(
    content: str, input_tokens: object, output_tokens: object, total: object = None
) -> Completion:
    """Build a Completion from raw provider token counts via ``normalize_usage``.

    Missing, negative or non-integer counts become 0 and are listed in
    ``usage_problems``; all three absent means ``usage_reported=False`` (Req 9.6, 9.7).
    """
    norm = normalize_usage(input_tokens, output_tokens, total)
    return Completion(
        content=content,
        usage=norm.usage,
        usage_reported=norm.reported,
        usage_problems=norm.problems if norm.reported else (),
    )


def make_tool_turn(
    text: str, calls: Iterable[ToolCall], input_tokens: object, output_tokens: object, total: object = None
) -> ToolTurn:
    """A ToolTurn with usage normalized exactly as :func:`make_completion` does (Req 9.6, 9.7)."""
    norm = normalize_usage(input_tokens, output_tokens, total)
    return ToolTurn(
        text=text,
        calls=tuple(calls),
        usage=norm.usage,
        usage_reported=norm.reported,
        usage_problems=norm.problems if norm.reported else (),
    )


def parse_arguments(raw: object) -> tuple[dict[str, Any], str | None]:
    """Tool-call arguments as a dict, or ``({}, why)`` when they are not a JSON object (Phase 6 Req 5.7).

    Providers send arguments as an object or as JSON text; no arguments at all (``None`` or ``""``) is ``{}``.
    """
    if isinstance(raw, dict):
        return raw, None
    if raw is None or raw == "":
        return {}, None
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
        except ValueError:
            return {}, "the arguments are not valid JSON"
        if isinstance(value, dict):
            return value, None
    return {}, "the arguments must be a JSON object"


def split_system_turns(system: str, turns: Sequence[Turn]) -> tuple[str, list[Turn]]:
    """Like :func:`split_system` for a conversation with tool turns: system-role messages join the system prompt."""
    parts = [system] if system else []
    rest: list[Turn] = []
    for turn in turns:
        if isinstance(turn, ChatMessage) and turn.role == "system":
            if turn.content:
                parts.append(turn.content)
        else:
            rest.append(turn)
    return "\n\n".join(parts), rest


def truncate(text: str, limit: int = 300) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= limit else text[: limit - 3] + "..."


__all__ = [
    "error_for_status",
    "is_transient_status",
    "make_completion",
    "make_tool_turn",
    "parse_arguments",
    "split_system",
    "split_system_turns",
    "truncate",
    "usage",
]
