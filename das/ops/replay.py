"""Replay and discard decisions for Dead_Letter_Records (Req 13).

Pure module (Req 19.5): no clock, no I/O. ``replay_dead_letter.lua`` and
``discard_dead_letter.lua`` implement the same check order; Property 20 compares them.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, Protocol

from das.models.dlq import DeadLetterReason, RecordStatus
from das.models.job import JobRecord, JobState
from das.models.plan import JobPlan
from das.models.request import RequestStatus

__all__ = [
    "REPLAY_CHECK_ORDER",
    "DeadLetterView",
    "DiscardDecision",
    "DiscardOutcome",
    "ReplayDecision",
    "ReplayOutcome",
    "RequestView",
    "decide_discard",
    "decide_replay",
    "skipped_to_reset",
]

ReplayOutcome = Literal[
    "accepted",
    "not_found",
    "record_not_pending",
    "not_replayable",
    "request_gone",
    "request_state",
    "budget_exceeded",
    "job_state",
    "stale_attempt",
    "lease_held",
]
DiscardOutcome = Literal["accepted", "already", "not_found", "record_not_pending"]

# Full Replay check order (Req 13.7): first violated condition wins.
REPLAY_CHECK_ORDER: tuple[ReplayOutcome, ...] = (
    "not_found",
    "record_not_pending",
    "not_replayable",
    "request_gone",
    "request_state",
    "budget_exceeded",
    "job_state",
    "stale_attempt",
    "lease_held",
)

#: Errors a Request stores when its budget stopped it (``das.manager.machine.BUDGET_EXCEEDED`` and
#: ``BUDGET_UNENFORCEABLE``; ops cannot import the Manager). Replaying such a Request would spend the
#: budget again, so it is refused (Phase 4 Req 5.10).
BUDGET_FAILURES: frozenset[str] = frozenset({"budget_exceeded", "budget_unenforceable"})

_REPLAYABLE_REQUEST: frozenset[RequestStatus] = frozenset(
    {RequestStatus.RUNNING, RequestStatus.FAILED}
)
_ROOT_CAUSE: frozenset[JobState] = frozenset({JobState.FAILED, JobState.CANCELLED})


class DeadLetterView(Protocol):
    """The Dead_Letter_Record fields a decision needs (satisfied by ``DeadLetterRecord``)."""

    @property
    def status(self) -> RecordStatus: ...
    @property
    def reason(self) -> DeadLetterReason: ...
    @property
    def attempt(self) -> int | None: ...


@dataclass(frozen=True, slots=True)
class RequestView:
    """Current state of the Request referenced by a Dead_Letter_Record.

    ``error`` is the stored error of a Failed Request (``None`` when it has none).
    """

    status: RequestStatus
    error: str | None = None


@dataclass(frozen=True, slots=True)
class ReplayDecision:
    """``outcome == "accepted"`` carries ``new_attempt`` and ``request_status_before``;
    otherwise ``current_state`` is the state of the violating object (or ``None``)."""

    outcome: ReplayOutcome
    current_state: str | None = None
    new_attempt: int | None = None
    request_status_before: RequestStatus | None = None

    @property
    def accepted(self) -> bool:
        return self.outcome == "accepted"


@dataclass(frozen=True, slots=True)
class DiscardDecision:
    """``accepted`` = Pending → Discarded; ``already`` = idempotent 200 (Req 13.12)."""

    outcome: DiscardOutcome
    current_state: RecordStatus | None = None


def decide_replay(
    rec: DeadLetterView | None,
    req: RequestView | None,
    job: JobRecord | None,
    lease_active: bool,
) -> ReplayDecision:
    """Decide a Replay following the check order of Req 13.6, 13.7 and 13.8.

    An attempt number greater than the Job's attempt count (not covered by the
    requirements) is treated as ``stale_attempt``. ``lease_held`` only applies to a
    Failed Request; a Running Request is replayed through its Lease owner (Req 13.3).
    """
    if rec is None:
        return ReplayDecision("not_found")
    if rec.status != "Pending":
        return ReplayDecision("record_not_pending", rec.status)
    if rec.reason == "malformed":
        return ReplayDecision("not_replayable", rec.reason)
    if req is None:
        return ReplayDecision("request_gone")
    if req.status not in _REPLAYABLE_REQUEST:
        return ReplayDecision("request_state", str(req.status))
    if req.status is RequestStatus.FAILED and req.error in BUDGET_FAILURES:
        return ReplayDecision("budget_exceeded")
    if job is None or job.state is not JobState.FAILED:
        return ReplayDecision("job_state", None if job is None else str(job.state))
    if rec.attempt is None or rec.attempt != job.attempt:
        return ReplayDecision("stale_attempt", str(job.attempt))
    if req.status is RequestStatus.FAILED and lease_active:
        return ReplayDecision("lease_held", str(req.status))
    return ReplayDecision(
        "accepted", new_attempt=job.attempt + 1, request_status_before=req.status
    )


def decide_discard(rec: DeadLetterView | None) -> DiscardDecision:
    """Decide a Discard (Req 13.11, 13.12, 13.13)."""
    if rec is None:
        return DiscardDecision("not_found")
    if rec.status == "Pending":
        return DiscardDecision("accepted", rec.status)
    if rec.status == "Discarded":
        return DiscardDecision("already", rec.status)
    return DiscardDecision("record_not_pending", rec.status)


def skipped_to_reset(
    plan: JobPlan, jobs: Mapping[str, JobRecord], replayed: str
) -> frozenset[str]:
    """Skipped Jobs whose root causes are exactly ``{replayed}`` (Req 13.4).

    A root cause of Job ``j`` is an ancestor (transitively, via ``depends_on``) in state
    Failed or Cancelled, reached only through Skipped intermediates. Ancestors in other
    states end the walk without contributing a cause. Jobs missing from ``jobs`` are
    ignored.
    """
    deps = {pj.job_id: pj.depends_on for pj in plan.jobs}
    memo: dict[str, frozenset[str]] = {}

    def causes(job_id: str) -> frozenset[str]:
        # Root causes of a Skipped job; the plan is a validated DAG so recursion ends.
        if job_id in memo:
            return memo[job_id]
        found: set[str] = set()
        for dep in deps.get(job_id, ()):
            rec = jobs.get(dep)
            if rec is None:
                continue
            if rec.state in _ROOT_CAUSE:
                found.add(dep)
            elif rec.state is JobState.SKIPPED:
                found |= causes(dep)
        memo[job_id] = frozenset(found)
        return memo[job_id]

    target = frozenset({replayed})
    return frozenset(
        jid
        for jid in deps
        if (rec := jobs.get(jid)) is not None
        and rec.state is JobState.SKIPPED
        and causes(jid) == target
    )
