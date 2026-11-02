"""LLM error types (Requirements 10.4-10.6)."""
from __future__ import annotations


class LLMError(Exception):
    """Base class for LLM_Provider errors."""


class TransientLLMError(LLMError):
    """Retryable failure: rate limit, connection failure or reset, server unavailability, timeout."""


class RateLimitedLLMError(TransientLLMError):
    """Transient failure caused by HTTP 429 or provider throttling (Req 4.1 ``rate_limited``)."""


class PermanentLLMError(LLMError):
    """Non-retryable failure: auth, authz, invalid request, unknown model, context exceeded."""


class LLMInputError(LLMError, ValueError):
    """The completion call arguments are outside the allowed ranges (Req 10.1)."""


class LLMCallFailed(LLMError):
    """A completion call failed for good. Carries no partial content (Req 10.5, 10.6)."""

    def __init__(self, provider: str, model: str, reason: str, attempts: int) -> None:
        self.provider = provider
        self.model = model
        self.reason = reason
        self.attempts = attempts
        super().__init__(
            f"LLM call failed: provider={provider} model={model} "
            f"attempts={attempts} reason={reason}"
        )


__all__ = [
    "LLMCallFailed",
    "LLMError",
    "LLMInputError",
    "PermanentLLMError",
    "RateLimitedLLMError",
    "TransientLLMError",
]
