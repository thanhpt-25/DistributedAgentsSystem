"""Final_Result assembly (Requirements 7.2, 7.3, 7.5, 7.6). Pure: no I/O."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Final

from das.llm.base import Completion
from das.models.job import JobRecord, JobState
from das.models.plan import JobPlan, PlannedJob
from das.models.results import (
    FailureEntry,
    FinalResult,
    JobReport,
    JobResultPayload,
    TokenUsage,
)

# Same text as das.state.store.CANCEL_REASON; duplicated so das.manager stays free of
# the Redis-backed store module.
CANCEL_REASON: Final = "cancelled: request cancelled"
FAILURE_STATES: Final = frozenset({JobState.FAILED, JobState.SKIPPED, JobState.CANCELLED})


def skip_reason(dependency_id: str) -> str:
    return f"skipped: dependency {dependency_id} failed"


def current_result(
    record: JobRecord, results: Mapping[tuple[str, int], JobResultPayload]
) -> JobResultPayload | None:
    """The Job_Result for the Job's current attempt, if one was recorded."""
    if record.attempt < 1:
        return None
    return results.get((record.job.job_id, record.attempt))


def _blocking_dependency(job: PlannedJob, jobs: Mapping[str, JobRecord]) -> str | None:
    """First dependency (in declared order) that did not succeed."""
    for dep in job.depends_on:
        rec = jobs.get(dep)
        if rec is None or rec.state is not JobState.SUCCEEDED:
            return dep
    return None


def _failure_reason(
    job: PlannedJob,
    record: JobRecord,
    result: JobResultPayload | None,
    jobs: Mapping[str, JobRecord],
) -> str:
    # Job_Result error first, then the reason recorded on the Job, then a derived one.
    if result is not None and result.status != "Succeeded" and result.error:
        return result.error
    if record.reason:
        return record.reason
    if record.state is JobState.SKIPPED:
        dep = _blocking_dependency(job, jobs)
        return skip_reason(dep) if dep is not None else "skipped"
    if record.state is JobState.CANCELLED:
        return CANCEL_REASON
    return "failed"


def build_final_result(
    plan: JobPlan,
    jobs: Mapping[str, JobRecord],
    results: Mapping[tuple[str, int], JobResultPayload],
    summary: Completion | None,
    error: str | None,
) -> FinalResult:
    """Assemble the Final_Result for a Request whose Jobs are all terminal.

    ``jobs`` maps Job_ID to its record and ``results`` maps ``(job_id, attempt)`` to
    the Job_Result (the shape of ``StoredRequest``); each Job is reported using the
    result of its current attempt. Reports follow plan order. ``summary`` is None
    when the Summarizer was skipped (7.3) or failed (7.6, with ``error`` set).
    """
    reports: list[JobReport] = []
    failures: list[FailureEntry] = []
    for job in plan.jobs:
        record = jobs.get(job.job_id)
        if record is None:
            raise ValueError(f"no job record for planned job {job.job_id!r}")
        result = current_result(record, results)
        reports.append(
            JobReport(
                job_id=job.job_id,
                state=record.state,
                worker_id=result.worker_id if result is not None else None,
                input_tokens=result.usage.input_tokens if result is not None else 0,
                output_tokens=result.usage.output_tokens if result is not None else 0,
            )
        )
        if record.state in FAILURE_STATES:
            failures.append(
                FailureEntry(
                    job_id=job.job_id,
                    state=record.state,
                    reason=_failure_reason(job, record, result, jobs),
                )
            )
    return FinalResult(
        summary=summary.content if summary is not None else None,
        jobs=reports,
        failures=failures,
        summarizer_usage=summary.usage if summary is not None else TokenUsage(),
        error=error,
    )
