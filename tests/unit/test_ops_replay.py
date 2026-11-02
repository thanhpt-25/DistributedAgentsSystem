"""Unit tests for das.ops.replay and the Phase 2 JobRecord/TransitionEntry fields."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from das.models import JobPlan, JobRecord, JobState, PlannedJob, RequestStatus, TransitionEntry
from das.models.dlq import DeadLetterRecord
from das.ops.replay import RequestView, decide_discard, decide_replay, skipped_to_reset

AT = datetime(2025, 1, 1, tzinfo=UTC)
DLID = "0" * 32


def _job(jid: str, deps: list[str] | None = None) -> PlannedJob:
    return PlannedJob(job_id=jid, instruction="do", capabilities=["c"], depends_on=deps or [])


def _rec(status: str = "Pending", reason: str = "retries_exhausted", attempt: int | None = 2):
    return DeadLetterRecord(
        dead_letter_id=DLID,
        reason=reason,  # type: ignore[arg-type]
        status=status,  # type: ignore[arg-type]
        request_id="r1",
        job_id="a",
        attempt=attempt,
        dead_lettered_at=AT,
    )


def _jobrec(state: JobState = JobState.FAILED, attempt: int = 2) -> JobRecord:
    return JobRecord(job=_job("a"), state=state, attempt=attempt)


RUNNING = RequestView(RequestStatus.RUNNING)
FAILED = RequestView(RequestStatus.FAILED)


# --- model compatibility ------------------------------------------------------


def test_job_record_json_unchanged_without_attempt_limit() -> None:
    rec = JobRecord(job=_job("a"), state=JobState.WAITING)
    assert "attempt_limit" not in json.loads(rec.model_dump_json())
    assert "attempt_limit" not in rec.model_dump(mode="json")
    limited = rec.model_copy(update={"attempt_limit": 5})
    assert json.loads(limited.model_dump_json())["attempt_limit"] == 5
    assert JobRecord.model_validate_json(limited.model_dump_json()) == limited
    with pytest.raises(ValidationError):
        JobRecord(job=_job("a"), state=JobState.WAITING, attempt_limit=0)


def test_transition_entry_json_unchanged_without_dead_letter_id() -> None:
    entry = TransitionEntry(job_id="a", from_state=None, to_state=JobState.WAITING, attempt=0, at=AT)
    assert set(json.loads(entry.model_dump_json())) == {
        "job_id", "from_state", "to_state", "attempt", "worker_id", "at"
    }
    tagged = entry.model_copy(update={"dead_letter_id": DLID})
    assert json.loads(tagged.model_dump_json())["dead_letter_id"] == DLID
    assert TransitionEntry.model_validate_json(tagged.model_dump_json()) == tagged


# --- decide_replay ------------------------------------------------------------


def test_replay_accepted_running_and_failed() -> None:
    d = decide_replay(_rec(), RUNNING, _jobrec(), lease_active=True)
    assert d.accepted and d.new_attempt == 3 and d.request_status_before is RequestStatus.RUNNING
    d = decide_replay(_rec(), FAILED, _jobrec(), lease_active=False)
    assert d.accepted and d.request_status_before is RequestStatus.FAILED


@pytest.mark.parametrize(
    ("rec", "req", "job", "lease", "outcome", "state"),
    [
        (None, RUNNING, _jobrec(), False, "not_found", None),
        (_rec("Replayed"), RUNNING, _jobrec(), False, "record_not_pending", "Replayed"),
        (_rec("Discarded", "malformed"), None, None, True, "record_not_pending", "Discarded"),
        (_rec(reason="malformed"), None, None, True, "not_replayable", "malformed"),
        (_rec(), None, None, True, "request_gone", None),
        (_rec(), RequestView(RequestStatus.PENDING), None, False, "request_state", "Pending"),
        (_rec(), RequestView(RequestStatus.COMPLETED), None, False, "request_state", "Completed"),
        (_rec(), RequestView(RequestStatus.CANCELLED), None, False, "request_state", "Cancelled"),
        (_rec(), RUNNING, None, False, "job_state", None),
        (_rec(), RUNNING, _jobrec(JobState.SUCCEEDED), False, "job_state", "Succeeded"),
        (_rec(attempt=1), FAILED, _jobrec(), True, "stale_attempt", "2"),
        (_rec(attempt=3), FAILED, _jobrec(), True, "stale_attempt", "2"),
        (_rec(), FAILED, _jobrec(), True, "lease_held", "Failed"),
    ],
)
def test_replay_check_order(rec, req, job, lease, outcome, state) -> None:  # type: ignore[no-untyped-def]
    d = decide_replay(rec, req, job, lease)
    assert (d.outcome, d.current_state) == (outcome, state)
    assert d.new_attempt is None


# --- decide_discard -----------------------------------------------------------


def test_discard_decisions() -> None:
    assert decide_discard(None).outcome == "not_found"
    assert decide_discard(_rec("Pending")).outcome == "accepted"
    assert decide_discard(_rec("Discarded")).outcome == "already"
    d = decide_discard(_rec("Replayed"))
    assert (d.outcome, d.current_state) == ("record_not_pending", "Replayed")


# --- skipped_to_reset ---------------------------------------------------------


def _state_map(plan: JobPlan, states: dict[str, JobState]) -> dict[str, JobRecord]:
    return {pj.job_id: JobRecord(job=pj, state=states[pj.job_id]) for pj in plan.jobs}


def test_skipped_to_reset_transitive_and_multiple_causes() -> None:
    # a -> b -> d ; c -> d ; a -> e ; f (Succeeded) -> g ; a -> g
    plan = JobPlan(
        jobs=[
            _job("a"), _job("b", ["a"]), _job("c"), _job("d", ["b", "c"]),
            _job("e", ["a"]), _job("f"), _job("g", ["a", "f"]), _job("h", ["c"]),
        ]
    )
    S, F, OK, X = JobState.SKIPPED, JobState.FAILED, JobState.SUCCEEDED, JobState.CANCELLED
    states = {"a": F, "b": S, "c": X, "d": S, "e": S, "f": OK, "g": S, "h": S}
    # d has root causes {a, c}; h only {c}; b, e, g only {a}.
    assert skipped_to_reset(plan, _state_map(plan, states), "a") == {"b", "e", "g"}
    states["c"] = OK
    assert skipped_to_reset(plan, _state_map(plan, states), "a") == {"b", "d", "e", "g"}


def test_skipped_to_reset_diamond() -> None:
    plan = JobPlan(jobs=[_job("a"), _job("b", ["a"]), _job("c", ["a"]), _job("d", ["b", "c"])])
    states = {"a": JobState.FAILED, "b": JobState.SKIPPED, "c": JobState.SKIPPED, "d": JobState.SKIPPED}
    assert skipped_to_reset(plan, _state_map(plan, states), "a") == {"b", "c", "d"}
    assert skipped_to_reset(plan, _state_map(plan, states), "b") == frozenset()
