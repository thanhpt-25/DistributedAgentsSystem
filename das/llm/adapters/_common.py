"""Helpers shared by the Provider_Adapters."""

from __future__ import annotations

from collections.abc import Sequence

from das.llm.base import ChatMessage, Completion
from das.llm.errors import LLMError, PermanentLLMError, RateLimitedLLMError, TransientLLMError
from das.models.results import TokenUsage
from das.ops.usage import normalize_usage

#: HTTP statuses that indicate a retryable failure: request timeout, rate
#: limit, and every server-side error (including Anthropic's 529 "overloaded").
TRANSIENT_STATUSES: frozenset[int] = frozenset({408, 429})

#: HTTP status that means the provider is rate limiting the caller.
RATE_LIMIT_STATUS = 429


def is_transient_status(status: int) -> bool:
    return status in TRANSIENT_STATUSES or status >= 500


def error_for_status(provider: str, status: int, detail: str) -> LLMError:
    """Classify an HTTP error status as transient or permanent."""
    reason = f"{provider} HTTP {status}"
    if detail:
        reason = f"{reason}: {detail}"
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


def truncate(text: str, limit: int = 300) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= limit else text[: limit - 3] + "..."


__all__ = [
    "error_for_status",
    "is_transient_status",
    "make_completion",
    "split_system",
    "truncate",
    "usage",
]
