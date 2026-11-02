from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from das.models import (
    TERMINAL_JOB,
    TERMINAL_REQUEST,
    AgentRecord,
    FinalResult,
    JobDispatchPayload,
    JobPlan,
    JobRecord,
    JobReport,
    JobResultPayload,
    JobState,
    Lease,
    PlannedJob,
    RequestStatus,
    TokenUsage,
    TransitionEntry,
    plans_equivalent,
)


def _job(job_id: str = "a", caps=("research",), deps=(), instruction: str = "do it") -> PlannedJob:
    return PlannedJob(
        job_id=job_id, instruction=instruction, capabilities=list(caps), depends_on=list(deps)
    )


# --- enums and terminal sets -------------------------------------------------


def test_terminal_sets() -> None:
    assert TERMINAL_REQUEST == {
        RequestStatus.COMPLETED,
        RequestStatus.FAILED,
        RequestStatus.CANCELLED,
    }
    assert TERMINAL_JOB == {
        JobState.SUCCEEDED,
        JobState.FAILED,
        JobState.SKIPPED,
        JobState.CANCELLED,
    }
    assert JobState.TIMED_OUT not in TERMINAL_JOB
    assert JobState.TIMED_OUT.value == "TimedOut"


# --- frozen / extra=forbid ---------------------------------------------------


def test_models_are_frozen() -> None:
    job = _job()
    with pytest.raises(ValidationError):
        job.job_id = "b"  # type: ignore[misc]


def test_models_forbid_extra_fields() -> None:
    with pytest.raises(ValidationError):
        PlannedJob(job_id="a", instruction="x", capabilities=["c"], extra_field=1)  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        TokenUsage(input_tokens=1, output_tokens=1, total_tokens=2, cost=3)  # type: ignore[call-arg]


# --- PlannedJob constraints (Req 3.2) ----------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"job_id": ""},
        {"job_id": "has space"},
        {"job_id": "x" * 65},
        {"instruction": ""},
        {"instruction": "x" * 8001},
        {"capabilities": []},
    ],
)
def test_planned_job_rejects_invalid_fields(kwargs: dict) -> None:
    base = {"job_id": "a", "instruction": "do", "capabilities": ["research"]}
    with pytest.raises(ValidationError):
        PlannedJob(**(base | kwargs))


def test_planned_job_accepts_boundaries() -> None:
    job = PlannedJob(job_id="A_b-1" + "x" * 59, instruction="x" * 8000, capabilities=["c"])
    assert len(job.job_id) == 64
    assert job.depends_on == []


# --- JobPlan equivalence (Req 3.8) -------------------------------------------


def test_equivalence_ignores_job_and_list_order() -> None:
    a = JobPlan(jobs=[_job("a", caps=["x", "y"]), _job("b", deps=["a"])])
    b = JobPlan(jobs=[_job("b", deps=["a", "a"]), _job("a", caps=["y", "x", "x"])])
    assert plans_equivalent(a, b)


@pytest.mark.parametrize(
    "other",
    [
        JobPlan(jobs=[_job("a"), _job("c", deps=["a"])]),  # different id set
        JobPlan(jobs=[_job("a", instruction="other"), _job("b", deps=["a"])]),  # instruction
        JobPlan(jobs=[_job("a", caps=["code-review"]), _job("b", deps=["a"])]),  # capability set
        JobPlan(jobs=[_job("a"), _job("b")]),  # dependency set
        JobPlan(jobs=[_job("a")]),  # missing job
    ],
)
def test_equivalence_detects_differences(other: JobPlan) -> None:
    base = JobPlan(jobs=[_job("a"), _job("b", deps=["a"])])
    assert not plans_equivalent(base, other)
    assert not plans_equivalent(other, base)


def test_equivalence_distinguishes_duplicate_ids() -> None:
    single = JobPlan(jobs=[_job("a")])
    dup = JobPlan(jobs=[_job("a"), _job("a", instruction="second")])
    assert not plans_equivalent(single, dup)


def test_plan_json_round_trip_is_equivalent() -> None:
    plan = JobPlan(jobs=[_job("a", caps=["x", "y"]), _job("b", deps=["a"])])
    assert plans_equivalent(JobPlan.model_validate_json(plan.model_dump_json()), plan)


# --- JobRecord / TransitionEntry (Req 13.7) ----------------------------------


def test_job_record_defaults() -> None:
    rec = JobRecord(job=_job(), state=JobState.WAITING)
    assert rec.attempt == 0
    assert rec.publish_failures == 0
    assert rec.worker_id is None and rec.reason is None and rec.ready_since is None
    assert rec.model_copy(update={"state": JobState.READY}).state is JobState.READY
    assert rec.state is JobState.WAITING


def test_transition_entry_truncates_to_ms_utc() -> None:
    tz = timezone(timedelta(hours=7))
    at = datetime(2025, 1, 1, 7, 0, 0, 123456, tzinfo=tz)
    entry = TransitionEntry(
        job_id="a", from_state=None, to_state=JobState.WAITING, attempt=0, at=at
    )
    assert entry.at == datetime(2025, 1, 1, 0, 0, 0, 123000, tzinfo=UTC)
    assert entry.at.utcoffset() == timedelta(0)
    restored = TransitionEntry.model_validate_json(entry.model_dump_json())
    assert restored == entry


def test_transition_entry_rejects_naive_timestamp() -> None:
    with pytest.raises(ValidationError):
        TransitionEntry(
            job_id="a",
            from_state=JobState.WAITING,
            to_state=JobState.READY,
            attempt=0,
            at=datetime(2025, 1, 1),
        )


# --- Payloads (Req 5.5) ------------------------------------------------------


def _result(**kw) -> JobResultPayload:
    base = dict(
        request_id="r1",
        job_id="a",
        attempt=1,
        worker_id="w1",
        status="Succeeded",
        content="out",
        usage=TokenUsage(input_tokens=3, output_tokens=4, total_tokens=7),
        duration_ms=12,
    )
    return JobResultPayload(**(base | kw))


def test_job_result_payload_round_trip() -> None:
    r = _result(status="Failed", content="", error="openai/gpt: rate limited")
    assert JobResultPayload.model_validate_json(r.model_dump_json()) == r


@pytest.mark.parametrize(
    "kw", [{"attempt": 0}, {"duration_ms": -1}, {"status": "Waiting"}, {"worker_id": ""}]
)
def test_job_result_payload_rejects_invalid(kw: dict) -> None:
    with pytest.raises(ValidationError):
        _result(**kw)


def test_dispatch_payload_requires_instruction_and_capabilities() -> None:
    ok = JobDispatchPayload(
        request_id="r1",
        job_id="b",
        attempt=1,
        instruction="go",
        required_capabilities=["research"],
        dependency_results={"a": "out"},
    )
    assert ok.dependency_results == {"a": "out"}
    with pytest.raises(ValidationError):
        JobDispatchPayload(
            request_id="r1", job_id="b", attempt=1, instruction="", required_capabilities=["x"]
        )


# --- Lease / AgentRecord / FinalResult ---------------------------------------


def test_lease_requires_positive_epoch() -> None:
    assert Lease(request_id="r", owner="m1", epoch=1, expires_at=10.0).epoch == 1
    with pytest.raises(ValidationError):
        Lease(request_id="r", owner="m1", epoch=0, expires_at=10.0)


def test_agent_record_capabilities_round_trip() -> None:
    rec = AgentRecord(
        agent_id="w1",
        role="worker",
        host="10.0.0.2",
        capabilities=frozenset({"research", "summarize"}),
        concurrency_limit=4,
        status="Available",
        last_heartbeat=100.0,
    )
    assert AgentRecord.model_validate_json(rec.model_dump_json()) == rec
    with pytest.raises(ValidationError):
        rec.model_validate(rec.model_dump() | {"status": "Dead"})


def test_final_result_defaults() -> None:
    fr = FinalResult(jobs=[JobReport(job_id="a", state=JobState.SKIPPED)])
    assert fr.jobs[0].worker_id is None
    assert fr.jobs[0].input_tokens == 0 and fr.jobs[0].output_tokens == 0
    assert fr.failures == []
    assert fr.summarizer_usage == TokenUsage.zero()
    assert FinalResult.model_validate_json(fr.model_dump_json()) == fr
