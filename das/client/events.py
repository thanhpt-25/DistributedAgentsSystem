"""Event_Stream payloads (Phase 4 Req 6.1, 6.2). Pure.

Payloads never carry the prompt, Job instructions, Job_Result content or the
Final_Result (Req 6.2). The I/O shell passes them to the Lua scripts, which append
them in the same atomic write as the state change they describe.
"""
from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC
from typing import Any, Final, Literal

from das.client.budget import Budget, budget_status, budget_view
from das.models.job import TransitionEntry
from das.models.plan import JobPlan
from das.models.request import RequestStatus
from das.models.usage import UsageRecord
from das.ops.cost import PriceTable

EventType = Literal[
    "request.created",
    "request.planned",
    "job.transition",
    "request.reopened",
    "request.replanned",
    "request.completed",
    "request.failed",
    "request.cancelled",
]
EVENT_TYPES: Final[tuple[EventType, ...]] = (
    "request.created",
    "request.planned",
    "job.transition",
    "request.reopened",
    "request.replanned",  # Phase 6 Req 3.6
    "request.completed",
    "request.failed",
    "request.cancelled",
)
TERMINAL_EVENT_TYPES: Final[frozenset[EventType]] = frozenset(
    {"request.completed", "request.failed", "request.cancelled"}
)

_TERMINAL: Final[dict[RequestStatus, EventType]] = {
    RequestStatus.COMPLETED: "request.completed",
    RequestStatus.FAILED: "request.failed",
    RequestStatus.CANCELLED: "request.cancelled",
}


def terminal_event_type(status: RequestStatus) -> EventType:
    """Terminal_Event type for a terminal Request_Status."""
    try:
        return _TERMINAL[status]
    except KeyError:
        raise ValueError(f"not a terminal status: {status.value}") from None


def created_event_data() -> dict[str, Any]:
    return {"status": RequestStatus.PENDING.value}


def planned_event_data(plan: JobPlan) -> dict[str, Any]:
    """Job_IDs, Capabilities and dependencies; never instructions."""
    return {
        "jobs": [
            {"job_id": j.job_id, "capabilities": list(j.capabilities), "depends_on": list(j.depends_on)}
            for j in plan.jobs
        ]
    }


def replanned_event_data(replans: int, failed_jobs: Sequence[str], new_jobs: Sequence[str]) -> dict[str, Any]:
    """Phase 6 Req 3.6: how often the Request was replanned, the Jobs that were replaced and the Jobs that replace them.

    Job_IDs only, as for ``request.planned``: never instructions or results."""
    return {"replans": replans, "failed_jobs": list(failed_jobs), "new_jobs": list(new_jobs)}


def transition_event_data(entry: TransitionEntry) -> dict[str, Any]:
    """One ``job.transition`` Event per transition-log entry."""
    return {
        "job_id": entry.job_id,
        "from": entry.from_state.value if entry.from_state is not None else None,
        "to": entry.to_state.value,
        "attempt": entry.attempt,
        "worker_id": entry.worker_id,
        "at": entry.at.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    }


def reopened_event_data() -> dict[str, Any]:
    """``request.reopened`` data, written by the Replay that reopens a Failed Request
    (Failed -> Pending, Req 6.4); resuming it (``reopen_request``) writes no Event."""
    return {"status": RequestStatus.PENDING.value}


def _usage_total(usage: Sequence[UsageRecord]) -> dict[str, int]:
    return {
        "input_tokens": sum(r.usage.input_tokens for r in usage),
        "output_tokens": sum(r.usage.output_tokens for r in usage),
        "total_tokens": sum(r.usage.total_tokens for r in usage),
    }


def terminal_event_data(
    status: RequestStatus,
    error: str | None,
    usage: Sequence[UsageRecord],
    budget: Budget | None,
    prices: PriceTable | None,
) -> dict[str, Any]:
    """Terminal_Event data: status, stored error, token usage total, and Budget_Used when budgeted."""
    terminal_event_type(status)
    data: dict[str, Any] = {"status": status.value, "error": error, "usage": _usage_total(usage)}
    if budget is not None:
        data["budget"] = budget_view(budget, budget_status(usage, budget, prices))
    return data
