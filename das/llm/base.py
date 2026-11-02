"""LLM_Provider data types and the Provider_Adapter protocol (Requirement 10.1).

Agent code never talks to a Provider_Adapter directly; it goes through
:class:`das.llm.retry.LLMProvider`, which adds validation, timeouts, and retries.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import ClassVar, Literal, Protocol, runtime_checkable

from das.models.results import TokenUsage

Role = Literal["system", "user", "assistant"]
ROLES: frozenset[str] = frozenset({"system", "user", "assistant"})


@dataclass(frozen=True, slots=True)
class ChatMessage:
    """One chat message with a role and text content."""

    role: Role
    content: str


@dataclass(frozen=True, slots=True)
class ModelParams:
    """Model selection and sampling parameters."""

    model: str
    temperature: float
    max_output_tokens: int


@dataclass(frozen=True, slots=True)
class Completion:
    """Generated text plus the token usage reported by the provider.

    ``usage`` is already normalized (Req 9.6, 9.7). ``usage_reported`` is False
    when the provider returned no usage at all. ``usage_problems`` names the
    usage fields that were missing or invalid and recorded as 0; the
    LLMProvider logs a warning when it is non-empty.
    """

    content: str
    usage: TokenUsage
    usage_reported: bool = True
    usage_problems: tuple[str, ...] = ()


@runtime_checkable
class ProviderAdapter(Protocol):
    """One vendor implementation of the completion operation.

    ``complete_once`` makes a single call with no retries. Implementations must
    translate SDK failures into :class:`das.llm.errors.TransientLLMError`
    (rate limit, connection failure/reset, server unavailability, timeout) or
    :class:`das.llm.errors.PermanentLLMError` (auth, authz, invalid request,
    unknown model, context exceeded).
    """

    provider: ClassVar[str]

    async def complete_once(
        self,
        system: str,
        messages: Sequence[ChatMessage],
        params: ModelParams,
        timeout: float,
    ) -> Completion: ...

    async def ping(self) -> bool: ...


__all__ = ["ROLES", "ChatMessage", "Completion", "ModelParams", "ProviderAdapter", "Role", "TokenUsage"]
