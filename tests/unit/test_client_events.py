"""Event_Stream payloads (Phase 4 Req 6.1, 6.2)."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from das.client.budget import Budget
from das.client.events import (
    EVENT_TYPES,
    TERMINAL_EVENT_TYPES,
    created_event_data,
    planned_event_data,
    reopened_event_data,
    terminal_event_data,
    terminal_event_type,
    transition_event_data,
)
from das.models.job import JobState, TransitionEntry
from das.models.plan import JobPlan, PlannedJob
from das.models.request import RequestStatus
from das.models.results import TokenUsage
from das.models.usage import UsageRecord

USAGE = [
    UsageRecord(
        operation="job",
        provider="openai",
        model="m",
        usage=TokenUsage(input_tokens=10, output_tokens=5, total_tokens=15),
        job_id="j1",
        attempt=1,
    )
]


def test_terminal_types_are_event_types() -> None:
    assert TERMINAL_EVENT_TYPES <= set(EVENT_TYPES)
    assert len(EVENT_TYPES) == 8  # + request.replanned (Phase 6 Req 3.6)


@pytest.mark.parametrize(
    ("status", "event_type"),
    [
        (RequestStatus.COMPLETED, "request.completed"),
        (RequestStatus.FAILED, "request.failed"),
        (RequestStatus.CANCELLED, "request.cancelled"),
    ],
)
def test_terminal_event_type(status: RequestStatus, event_type: str) -> None:
    assert terminal_event_type(status) == event_type


@pytest.mark.parametrize("status", [RequestStatus.PENDING, RequestStatus.RUNNING])
def test_terminal_event_type_rejects_non_terminal(status: RequestStatus) -> None:
    with pytest.raises(ValueError):
        terminal_event_type(status)


def test_created_and_reopened() -> None:
    assert created_event_data() == {"status": "Pending"}
    assert reopened_event_data() == {"status": "Pending"}


def test_planned_event_omits_instructions() -> None:
    plan = JobPlan(
        jobs=[
            PlannedJob(job_id="j1", instruction="secret instruction", capabilities=["research"]),
            PlannedJob(job_id="j2", instruction="other", capabilities=["summarize"], depends_on=["j1"]),
        ]
    )
    data = planned_event_data(plan)
    assert data == {
        "jobs": [
            {"job_id": "j1", "capabilities": ["research"], "depends_on": []},
            {"job_id": "j2", "capabilities": ["summarize"], "depends_on": ["j1"]},
        ]
    }
    assert "secret instruction" not in json.dumps(data)


def test_transition_event() -> None:
    entry = TransitionEntry(
        job_id="j1",
        from_state=JobState.READY,
        to_state=JobState.DISPATCHED,
        attempt=1,
        worker_id=None,
        at=datetime(2026, 10, 30, 1, 2, 3, 456000, tzinfo=UTC),
    )
    assert transition_event_data(entry) == {
        "job_id": "j1",
        "from": "Ready",
        "to": "Dispatched",
        "attempt": 1,
        "worker_id": None,
        "at": "2026-10-30T01:02:03.456Z",
    }


def test_transition_event_from_none() -> None:
    entry = TransitionEntry(
        job_id="j1",
        from_state=None,
        to_state=JobState.WAITING,
        attempt=0,
        worker_id="w1",
        at=datetime(2026, 10, 30, tzinfo=UTC),
    )
    data = transition_event_data(entry)
    assert data["from"] is None
    assert data["worker_id"] == "w1"
    assert data["at"] == "2026-10-30T00:00:00.000Z"


def test_terminal_event_without_budget() -> None:
    assert terminal_event_data(RequestStatus.COMPLETED, None, USAGE, None, None) == {
        "status": "Completed",
        "error": None,
        "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15},
    }


def test_terminal_event_with_budget() -> None:
    data = terminal_event_data(RequestStatus.FAILED, "budget_exceeded", USAGE, Budget(max_total_tokens=10), None)
    assert data["error"] == "budget_exceeded"
    assert data["budget"] == {
        "limits": {"max_total_tokens": 10},
        "used": {"total_tokens": 15, "cost": None},
        "exceeded": True,
    }


def test_terminal_event_with_no_usage() -> None:
    data = terminal_event_data(RequestStatus.CANCELLED, None, [], Budget(max_cost=Decimal("1")), None)
    assert data["usage"] == {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0}
    assert data["budget"]["exceeded"] is True  # max_cost without a Price_Table is unenforceable


def test_terminal_event_rejects_non_terminal_status() -> None:
    with pytest.raises(ValueError):
        terminal_event_data(RequestStatus.RUNNING, None, [], None, None)
