"""When a Manager replans, and what it tells the Planner (das-smarter-agents task 3.4; Phase 6 Req 3.2, 3.3, 3.8).

Pure: the decisions are separate from the driver, which does the I/O."""

from __future__ import annotations

import pytest

from das.manager.replan import build_context, replan_wanted
from das.models import JobRecord, JobResultPayload, JobState, PlannedJob, TokenUsage

S = JobState


def planned(job_id: str) -> PlannedJob:
    return PlannedJob(job_id=job_id, instruction=f"do {job_id}", capabilities=["research"])


def record(job_id: str, state: JobState, attempt: int = 1) -> JobRecord:
    return JobRecord(job=planned(job_id), state=state, attempt=attempt)


def result(job_id: str, attempt: int, status: str, content: str = "", error: str | None = None) -> JobResultPayload:
    return JobResultPayload(
        request_id="r", job_id=job_id, attempt=attempt, worker_id="w", status=status,  # type: ignore[arg-type]
        content=content, error=error, usage=TokenUsage(), duration_ms=1,
    )


def jobs(**states: JobState) -> dict[str, JobRecord]:
    return {job_id: record(job_id, state) for job_id, state in states.items()}


# --------------------------------------------------------------------------- when to replan (Req 3.2)


@pytest.mark.parametrize("bad", [S.FAILED, S.SKIPPED])
def test_a_request_with_a_failed_or_skipped_job_is_replanned_when_allowed(bad: JobState) -> None:
    assert replan_wanted(None, jobs(a=S.SUCCEEDED, b=bad), replans=0, max_replans=1)


def test_a_request_where_nothing_succeeded_is_replanned_too() -> None:
    assert replan_wanted(None, jobs(a=S.FAILED, b=S.SKIPPED), replans=0, max_replans=2)


def test_a_request_where_every_job_succeeded_is_never_replanned() -> None:
    assert not replan_wanted(None, jobs(a=S.SUCCEEDED, b=S.SUCCEEDED), replans=0, max_replans=3)


def test_replanning_is_off_at_zero() -> None:
    assert not replan_wanted(None, jobs(a=S.FAILED), replans=0, max_replans=0)


@pytest.mark.parametrize(
    ("replans", "max_replans", "wanted"), [(0, 1, True), (1, 1, False), (1, 2, True), (3, 3, False)]
)
def test_the_limit_counts_the_replans_already_made(replans: int, max_replans: int, wanted: bool) -> None:
    assert replan_wanted(None, jobs(a=S.FAILED), replans, max_replans) is wanted


@pytest.mark.parametrize("kind", ["budget_exceeded", "budget_unenforceable"])
def test_a_budget_stop_is_never_replanned(kind: str) -> None:
    assert not replan_wanted(kind, jobs(a=S.FAILED), replans=0, max_replans=3)  # type: ignore[arg-type]


def test_a_request_with_a_job_still_open_is_not_ready_for_a_replan() -> None:
    assert not replan_wanted(None, jobs(a=S.FAILED, b=S.DISPATCHED), replans=0, max_replans=1)


# --------------------------------------------------------------------------- what the Planner is told (Req 3.3)


def test_the_context_pairs_each_succeeded_job_with_its_result_content() -> None:
    recs = {"a": record("a", S.SUCCEEDED, attempt=2), "b": record("b", S.FAILED, attempt=3)}
    results = {
        ("a", 1): result("a", 1, "Failed", error="first try"),
        ("a", 2): result("a", 2, "Succeeded", content="the facts"),
        ("b", 3): result("b", 3, "Failed", error="provider exploded"),
    }
    ctx = build_context(recs, results)
    assert [(s.job.job_id, s.result) for s in ctx.succeeded] == [("a", "the facts")]  # the attempt that succeeded
    assert [(f.job.job_id, f.error) for f in ctx.failed] == [("b", "provider exploded")]


def test_a_skipped_job_has_no_error_to_show() -> None:
    ctx = build_context({"a": record("a", S.SUCCEEDED), "b": record("b", S.SKIPPED, attempt=0)},
                        {("a", 1): result("a", 1, "Succeeded", content="ok")})
    assert [(f.job.job_id, f.error) for f in ctx.failed] == [("b", None)]


def test_a_failed_job_whose_result_is_missing_has_no_error_either() -> None:
    ctx = build_context({"b": record("b", S.FAILED, attempt=2)}, {})
    assert ctx.failed[0].error is None


def test_a_timed_out_last_attempt_is_reported_as_such() -> None:
    ctx = build_context({"b": record("b", S.FAILED, attempt=2)},
                        {("b", 2): result("b", 2, "TimedOut", error="job timed out after 300s")})
    assert ctx.failed[0].error == "job timed out after 300s"


def test_the_order_of_the_jobs_is_the_plans() -> None:
    recs = jobs(z=S.SUCCEEDED, a=S.SUCCEEDED, m=S.FAILED, b=S.FAILED)
    results = {(j, 1): result(j, 1, "Succeeded", content=j) for j in ("z", "a")}
    ctx = build_context(recs, results)
    assert [s.job.job_id for s in ctx.succeeded] == ["z", "a"] and [f.job.job_id for f in ctx.failed] == ["m", "b"]


def test_jobs_that_are_neither_succeeded_nor_failed_are_left_out() -> None:
    ctx = build_context(jobs(a=S.SUCCEEDED, b=S.CANCELLED, c=S.WAITING), {("a", 1): result("a", 1, "Succeeded")})
    assert [s.job.job_id for s in ctx.succeeded] == ["a"] and ctx.failed == ()
