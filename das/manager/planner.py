"""Planner: LLM decomposition of a Request into a Job_Plan (Requirement 3).

``Planner.plan`` never touches the State_Store. It returns a :data:`PlanOutcome` and
the Manager_Agent persists the plan (3.9) or fails the Request (3.6, 3.10, 3.11).

Flow:

1. No advertised Capabilities -> :class:`NoCapabilities`, with no LLM call (3.10).
2. Send the planning prompt: the Request prompt, the advertised Capability list, the
   Job_Plan JSON schema, and ``max_jobs`` (3.1, 3.3).
3. Parse and validate the output (3.4). On failure, send a corrected-plan request that
   names every failed check, keeping the conversation so the LLM sees its previous
   output (3.5). At most ``planning_retries + 1`` attempts in total.
4. Return :class:`PlanAccepted` or :class:`PlanRejected` with the last attempt's
   failed checks (3.6). A provider failure (``LLMCallFailed``) is returned as
   :class:`PlanLLMFailed` (3.11).

Every outcome carries ``usages``: one UsageRecord (operation ``plan``) for every
attempt whose LLM call returned a completion, including attempts whose output was
rejected (Phase 2 Req 9.1). The Manager persists them with the plan or the failure.
"""
from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Final, Protocol, Union

from das.llm.base import ChatMessage, Completion
from das.llm.errors import LLMCallFailed
from das.manager.attempts import AttemptHook, no_attempt_hook
from das.manager.llm_usage import usage_record
from das.models.plan import JobPlan
from das.models.results import TokenUsage
from das.models.usage import LLMOperation, UsageRecord
from das.plan.codec import plan_json_schema_text
from das.plan.validate import (
    MAX_MAX_JOBS,
    MIN_MAX_JOBS,
    CheckFailure,
    CheckIssue,
    validate_plan_text,
)

MIN_PLANNING_RETRIES: Final = 0
MAX_PLANNING_RETRIES: Final = 5
NO_CAPABILITIES_MESSAGE: Final = "no Worker_Agent Capabilities are available"
#: ``error.type`` of a planning attempt (and Request) whose plan was rejected (Req 6.10).
PLAN_REJECTED_CODE: Final = "PlanRejected"


class CompletionClient(Protocol):
    """What the Planner needs from :class:`das.llm.retry.LLMProvider`."""

    async def complete(
        self, system: str, messages: Sequence[ChatMessage], *, operation: LLMOperation
    ) -> Completion: ...


@dataclass(frozen=True, slots=True)
class PlannerConfig:
    """Planning limits from the Agent_Config ``limits`` section (3.1, 3.5)."""

    max_jobs: int = 10
    planning_retries: int = 2

    def __post_init__(self) -> None:
        for name, value, lo, hi in (
            ("max_jobs", self.max_jobs, MIN_MAX_JOBS, MAX_MAX_JOBS),
            ("planning_retries", self.planning_retries, MIN_PLANNING_RETRIES, MAX_PLANNING_RETRIES),
        ):
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"{name} must be an int, got {type(value).__name__}")
            if not lo <= value <= hi:
                raise ValueError(f"{name} must be in {lo}..{hi}, got {value}")

    @property
    def max_attempts(self) -> int:
        return self.planning_retries + 1


# --------------------------------------------------------------------------- outcomes


@dataclass(frozen=True, slots=True)
class PlanAccepted:
    plan: JobPlan
    attempts: int
    usage: TokenUsage
    usages: tuple[UsageRecord, ...] = ()


@dataclass(frozen=True, slots=True)
class PlanRejected:
    """No valid plan after the final attempt; ``issues`` belong to the last attempt."""

    issues: tuple[CheckIssue, ...]
    attempts: int
    usage: TokenUsage
    usages: tuple[UsageRecord, ...] = ()

    @property
    def failed_checks(self) -> frozenset[CheckFailure]:
        return frozenset(issue.check for issue in self.issues)

    @property
    def error_message(self) -> str:
        """Request error text naming the checks the last attempt failed (3.6)."""
        checks = ", ".join(sorted(c.value for c in self.failed_checks))
        return (
            f"planning failed after {self.attempts} attempt(s); "
            f"last attempt failed checks: {checks}"
        )


@dataclass(frozen=True, slots=True)
class NoCapabilities:
    """The Agent_Registry advertised zero Capabilities; no LLM call was made (3.10)."""

    error_message: str = NO_CAPABILITIES_MESSAGE

    @property
    def usages(self) -> tuple[UsageRecord, ...]:
        return ()


def llm_failure_message(exc: LLMCallFailed) -> str:
    """Request error text with the provider, model, and reason (3.11)."""
    return f"planning LLM call failed: provider={exc.provider} model={exc.model} reason={exc.reason}"


@dataclass(frozen=True, slots=True)
class PlanLLMFailed:
    """The provider failed for good on some attempt (3.11).

    ``usages`` holds the UsageRecords of the earlier attempts that did return a
    completion (Phase 2 Req 9.1); the failed call itself has none.
    """

    error: LLMCallFailed
    usages: tuple[UsageRecord, ...] = field(default=())

    @property
    def error_message(self) -> str:
        return llm_failure_message(self.error)


PlanOutcome = Union[PlanAccepted, PlanRejected, NoCapabilities, PlanLLMFailed]


# --------------------------------------------------------------------------- prompts

SYSTEM_PROMPT: Final = (
    "You are the planner of a distributed agent system. You decompose a user's request "
    "into a small DAG of independent jobs that specialized workers execute in parallel. "
    "Reply with a single JSON object that conforms to the given JSON schema and nothing else."
)


def build_planning_prompt(prompt: str, capabilities: frozenset[str], max_jobs: int) -> str:
    """The first planning message: request, advertised Capabilities, schema, and max_jobs."""
    caps = "\n".join(f"- {c}" for c in sorted(capabilities))
    return (
        "Decompose the request below into a Job_Plan.\n\n"
        "Rules:\n"
        f"- The plan must contain between 1 and {max_jobs} jobs.\n"
        "- Each job_id must be unique within the plan.\n"
        "- Each instruction must be non-empty and self-contained.\n"
        "- Each job must require at least one capability, chosen only from the "
        "advertised capabilities listed below.\n"
        "- depends_on lists job_ids of jobs in this plan whose results the job needs; "
        "a job must not depend on itself and dependencies must not form a cycle.\n"
        "- A job may carry an optional output_schema (a JSON Schema object) when its result must be "
        "machine-readable JSON; leave it out otherwise.\n\n"
        f"Advertised capabilities:\n{caps}\n\n"
        f"Job_Plan JSON schema:\n{plan_json_schema_text()}\n\n"
        f"Request:\n{prompt}"
    )


def build_correction_prompt(
    issues: Sequence[CheckIssue], capabilities: frozenset[str], max_jobs: int
) -> str:
    """A corrected-plan request naming every failed check of the previous output (3.5)."""
    checks = sorted({i.check for i in issues}, key=lambda c: c.value)
    lines = "\n".join(f"- {c.value}" for c in checks)
    details = "\n".join(f"- {issue}" for issue in issues)
    caps = ", ".join(sorted(capabilities))
    return (
        "Your previous Job_Plan is invalid.\n\n"
        f"Failed checks:\n{lines}\n\n"
        f"Details:\n{details}\n\n"
        f"Return a corrected Job_Plan as a single JSON object conforming to the schema, "
        f"with between 1 and {max_jobs} jobs and capabilities only from: {caps}."
    )


# --------------------------------------------------------------------------- planner


def _add_usage(a: TokenUsage, b: TokenUsage) -> TokenUsage:
    return TokenUsage(
        input_tokens=a.input_tokens + b.input_tokens,
        output_tokens=a.output_tokens + b.output_tokens,
        total_tokens=a.total_tokens + b.total_tokens,
    )


class Planner:
    def __init__(self, llm: CompletionClient) -> None:
        self._llm = llm

    async def plan(
        self,
        prompt: str,
        capabilities: frozenset[str],
        cfg: PlannerConfig,
        *,
        on_attempt: AttemptHook = no_attempt_hook,
    ) -> PlanOutcome:
        """Plan ``prompt`` against the advertised ``capabilities`` (Req 3.1-3.6, 3.10, 3.11).

        A provider failure after its own retries is returned as :class:`PlanLLMFailed`.
        ``on_attempt`` wraps every planning attempt (the driver's ``manager.plan`` Span,
        Phase 2 Req 6.3); a rejected or failed attempt is reported through
        ``observer.fail`` (Req 6.10).
        """
        advertised = frozenset(capabilities)
        if not advertised:
            return NoCapabilities()

        messages: list[ChatMessage] = [
            ChatMessage("user", build_planning_prompt(prompt, advertised, cfg.max_jobs))
        ]
        usage = TokenUsage.zero()
        usages: list[UsageRecord] = []
        issues: tuple[CheckIssue, ...] = ()
        for attempt in range(1, cfg.max_attempts + 1):
            with on_attempt(attempt) as observer:
                try:
                    reply = await self._llm.complete(SYSTEM_PROMPT, messages, operation="plan")
                except LLMCallFailed as exc:
                    observer.fail(type(exc).__name__)
                    return PlanLLMFailed(exc, tuple(usages))
                usage = _add_usage(usage, reply.usage)
                usages.append(usage_record(self._llm, "plan", reply))
                result = validate_plan_text(reply.content, advertised, cfg.max_jobs)
                if result.plan is not None:
                    return PlanAccepted(result.plan, attempt, usage, tuple(usages))
                observer.fail(PLAN_REJECTED_CODE)
            issues = result.issues
            if attempt < cfg.max_attempts:
                messages.append(ChatMessage("assistant", reply.content))
                messages.append(
                    ChatMessage("user", build_correction_prompt(issues, advertised, cfg.max_jobs))
                )
        return PlanRejected(issues, cfg.max_attempts, usage, tuple(usages))


__all__ = [
    "NO_CAPABILITIES_MESSAGE",
    "PLAN_REJECTED_CODE",
    "SYSTEM_PROMPT",
    "CompletionClient",
    "NoCapabilities",
    "PlanAccepted",
    "PlanLLMFailed",
    "PlanOutcome",
    "PlanRejected",
    "Planner",
    "PlannerConfig",
    "build_correction_prompt",
    "build_planning_prompt",
    "llm_failure_message",
]
