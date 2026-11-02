"""Summarizer: combine Succeeded Job_Results into one answer (Requirements 7.1, 7.6).

The Summarizer sees only the original prompt and the content of Succeeded
Job_Results; Failed, Skipped, and Cancelled Jobs contribute nothing to its input
(their reasons are reported separately by ``build_final_result``). Each call is
bounded by a 120 s timeout and the Summarizer makes up to 3 attempts. If every
attempt fails it raises :class:`SummarizationFailed`, whose ``error`` string is
what the Manager stores as the Final_Result error (7.6).

Usage (Phase 2 Req 9.3): every attempt whose LLM call returned a completion yields
one UsageRecord (operation ``summarize``). :meth:`Summarizer.usage_of` builds it for
the returned summary; :attr:`SummarizationFailed.usages` carries the records of the
attempts of a failed summarization that did return a completion (none today, since
an attempt either returns the summary or fails without a completion).
"""
from __future__ import annotations

import asyncio
from collections.abc import Iterable, Mapping

from das.llm.base import ChatMessage, Completion
from das.llm.errors import LLMCallFailed, LLMError, LLMInputError
from das.llm.retry import LLMProvider
from das.manager.attempts import AttemptHook, no_attempt_hook
from das.manager.llm_usage import usage_record
from das.models.usage import UsageRecord

SUMMARIZER_CALL_TIMEOUT = 120.0
SUMMARIZER_MAX_ATTEMPTS = 3

SUMMARIZER_SYSTEM_PROMPT = (
    "You combine the outputs of several sub-tasks into one coherent final answer "
    "to the user's original request. Use only the information in the provided "
    "job results. Answer the original request directly; do not describe the "
    "sub-task process."
)


class SummarizationFailed(LLMError):
    """All Summarizer attempts failed (Requirement 7.6)."""

    def __init__(
        self, reason: str, attempts: int, usages: Iterable[UsageRecord] = ()
    ) -> None:
        self.reason = reason
        self.attempts = attempts
        self.usages: tuple[UsageRecord, ...] = tuple(usages)
        super().__init__(self.error)

    @property
    def error(self) -> str:
        """Final_Result error reason: ``summarization failed: <last reason>``."""
        return f"summarization failed: {self.reason} (attempts={self.attempts})"


def build_summary_input(prompt: str, succeeded: Mapping[str, str]) -> str:
    """Render the Summarizer user message from the prompt and Succeeded contents.

    ``succeeded`` maps Job_ID to Job_Result content, in the caller's (plan) order.
    """
    parts = ["Original request:", prompt, "", "Job results:"]
    for job_id, content in succeeded.items():
        parts.append(f"--- job {job_id} ---")
        parts.append(content)
    parts.append("--- end of job results ---")
    return "\n".join(parts)


class Summarizer:
    """Produces the Final_Result summary through the agent's ``LLMProvider``."""

    def __init__(
        self,
        llm: LLMProvider,
        call_timeout: float = SUMMARIZER_CALL_TIMEOUT,
        max_attempts: int = SUMMARIZER_MAX_ATTEMPTS,
    ) -> None:
        if call_timeout <= 0:
            raise ValueError("call_timeout must be > 0")
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self._llm = llm
        self._call_timeout = float(call_timeout)
        self._max_attempts = max_attempts

    def usage_of(self, completion: Completion) -> UsageRecord:
        """The UsageRecord (operation ``summarize``) of a returned summary (Req 9.3)."""
        return usage_record(self._llm, "summarize", completion)

    async def summarize(
        self,
        prompt: str,
        succeeded: Mapping[str, str],
        *,
        on_attempt: AttemptHook = no_attempt_hook,
    ) -> Completion:
        """Return the summary Completion or raise :class:`SummarizationFailed`.

        ``succeeded`` must contain only Succeeded Job_Results (Job_ID -> content)
        and must be non-empty; the all-failed case skips the Summarizer (7.3).
        ``on_attempt`` wraps every attempt (the driver's ``manager.summarize`` Span,
        Phase 2 Req 6.3); a failed attempt is reported through ``observer.fail``.
        """
        if not succeeded:
            raise ValueError("Summarizer requires at least one Succeeded Job_Result")
        messages = (ChatMessage(role="user", content=build_summary_input(prompt, succeeded)),)
        reason = "no attempt made"
        for attempt in range(1, self._max_attempts + 1):
            with on_attempt(attempt) as observer:
                try:
                    async with asyncio.timeout(self._call_timeout):
                        return await self._llm.complete(
                            SUMMARIZER_SYSTEM_PROMPT, messages, operation="summarize"
                        )
                except TimeoutError as exc:
                    observer.fail(type(exc).__name__)
                    reason = f"timeout after {self._call_timeout:g}s"
                except LLMInputError as exc:
                    # Retrying identical invalid input cannot succeed.
                    raise SummarizationFailed(str(exc) or type(exc).__name__, attempt) from exc
                except LLMCallFailed as exc:
                    observer.fail(type(exc).__name__)
                    reason = exc.reason
        raise SummarizationFailed(reason, self._max_attempts)


__all__ = [
    "SUMMARIZER_CALL_TIMEOUT",
    "SUMMARIZER_MAX_ATTEMPTS",
    "SUMMARIZER_SYSTEM_PROMPT",
    "SummarizationFailed",
    "Summarizer",
    "build_summary_input",
]
