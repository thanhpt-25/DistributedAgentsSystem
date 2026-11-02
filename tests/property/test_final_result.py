# Feature: distributed-agent-system, Property 24: Final_Result completeness
"""Property 24: Final_Result completeness.

For any plan, terminal Job records, Job_Results, and summarizer usage,
``build_final_result`` returns:

- One ``JobReport`` per Job (plan order) with the Job's state, the worker that
  produced the current attempt's Job_Result or ``None``, and that result's token
  counts or 0 (Req 7.5).
- One ``FailureEntry`` per Job in Failed, Skipped, or Cancelled state and no others,
  whose reason is the Job_Result error, the recorded reason, the failed dependency's
  Job_ID, or the cancellation (Req 7.2).
- The summarizer's token usage.

Stale results for earlier attempts are also generated, so the test checks that only
the current attempt's result is reported.

**Validates: Requirements 7.2, 7.5**
"""
from __future__ import annotations

from dataclasses import dataclass

from hypothesis import given
from hypothesis import strategies as st

from das.llm.base import Completion
from das.manager.final_result import CANCEL_REASON, build_final_result
from das.models.job import JobRecord, JobState
from das.models.plan import JobPlan
from das.models.results import JobResultPayload, TokenUsage
from tests.strategies import job_plans

REQUEST_ID = "req-1"
FAILURE_STATES = {JobState.FAILED, JobState.SKIPPED, JobState.CANCELLED}
TERMINAL = [JobState.SUCCEEDED, JobState.FAILED, JobState.SKIPPED, JobState.CANCELLED]

worker_ids = st.from_regex(r"\Aworker-[a-z0-9]{1,6}\Z")
token_counts = st.integers(min_value=0, max_value=100_000)
reasons = st.text(min_size=1, max_size=40)


def usages() -> st.SearchStrategy[TokenUsage]:
    return st.builds(
        lambda i, o: TokenUsage(input_tokens=i, output_tokens=o, total_tokens=i + o),
        token_counts,
        token_counts,
    )


@dataclass
class Scenario:
    plan: JobPlan
    jobs: dict[str, JobRecord]
    results: dict[tuple[str, int], JobResultPayload]
    summary: Completion | None
    error: str | None


def _result(draw: st.DrawFn, job_id: str, attempt: int, status: str) -> JobResultPayload:
    error = None if status == "Succeeded" else draw(st.none() | reasons)
    return JobResultPayload(
        request_id=REQUEST_ID,
        job_id=job_id,
        attempt=attempt,
        worker_id=draw(worker_ids),
        status=status,
        content=draw(st.text(max_size=20)),
        error=error,
        usage=draw(usages()),
        duration_ms=draw(st.integers(min_value=0, max_value=10_000)),
    )


@st.composite
def scenarios(draw: st.DrawFn) -> Scenario:
    plan = draw(job_plans(max_jobs=8))
    states = {j.job_id: draw(st.sampled_from(TERMINAL)) for j in plan.jobs}
    # Skipped only makes sense when some dependency did not succeed.
    for job in plan.jobs:
        if states[job.job_id] is JobState.SKIPPED and all(
            states[d] is JobState.SUCCEEDED for d in job.depends_on
        ):
            states[job.job_id] = JobState.FAILED

    jobs: dict[str, JobRecord] = {}
    results: dict[tuple[str, int], JobResultPayload] = {}
    for job in plan.jobs:
        state = states[job.job_id]
        min_attempt = 1 if state in (JobState.SUCCEEDED, JobState.FAILED) else 0
        attempt = draw(st.integers(min_value=min_attempt, max_value=3))
        current = None
        if attempt >= 1:
            if state is JobState.SUCCEEDED:
                current = _result(draw, job.job_id, attempt, "Succeeded")
            elif draw(st.booleans()):
                status = draw(st.sampled_from(["Failed", "TimedOut", "Cancelled"]))
                current = _result(draw, job.job_id, attempt, status)
        if current is not None:
            results[(job.job_id, attempt)] = current
        # Stale result from an earlier attempt that must be ignored.
        if attempt >= 2 and draw(st.booleans()):
            stale = draw(st.integers(min_value=1, max_value=attempt - 1))
            results[(job.job_id, stale)] = _result(draw, job.job_id, stale, "Failed")

        reason = None
        if state in FAILURE_STATES:
            reason = draw(st.none() | reasons)
            has_error = current is not None and current.error
            if state is JobState.FAILED and not has_error and reason is None:
                reason = draw(reasons)  # a Failed Job always has some recorded cause
        jobs[job.job_id] = JobRecord(
            job=job,
            state=state,
            attempt=attempt,
            worker_id=current.worker_id if current is not None else None,
            reason=reason,
        )

    summary = draw(
        st.none() | st.builds(Completion, content=st.text(max_size=40), usage=usages())
    )
    error = draw(st.none() | reasons) if summary is None else None
    return Scenario(plan, jobs, results, summary, error)


def expected_reason(scn: Scenario, job_id: str) -> str:
    rec = scn.jobs[job_id]
    result = scn.results.get((job_id, rec.attempt)) if rec.attempt >= 1 else None
    if result is not None and result.status != "Succeeded" and result.error:
        return result.error
    if rec.reason:
        return rec.reason
    if rec.state is JobState.SKIPPED:
        dep = next(d for d in rec.job.depends_on if scn.jobs[d].state is not JobState.SUCCEEDED)
        return f"skipped: dependency {dep} failed"
    assert rec.state is JobState.CANCELLED
    return CANCEL_REASON


@given(scenarios())
def test_final_result_completeness(scn: Scenario) -> None:
    final = build_final_result(scn.plan, scn.jobs, scn.results, scn.summary, scn.error)

    # One report per Job, in plan order.
    assert [r.job_id for r in final.jobs] == [j.job_id for j in scn.plan.jobs]
    for report in final.jobs:
        rec = scn.jobs[report.job_id]
        assert report.state is rec.state
        current = scn.results.get((report.job_id, rec.attempt)) if rec.attempt >= 1 else None
        if current is None:
            assert report.worker_id is None
            assert (report.input_tokens, report.output_tokens) == (0, 0)
        else:
            assert report.worker_id == current.worker_id
            assert report.input_tokens == current.usage.input_tokens
            assert report.output_tokens == current.usage.output_tokens

    # Failure entries: exactly the Failed/Skipped/Cancelled Jobs, with their reasons.
    expected_failures = [j.job_id for j in scn.plan.jobs if scn.jobs[j.job_id].state in FAILURE_STATES]
    assert [f.job_id for f in final.failures] == expected_failures
    for entry in final.failures:
        assert entry.state is scn.jobs[entry.job_id].state
        assert entry.reason == expected_reason(scn, entry.job_id)

    # Summarizer usage and summary.
    if scn.summary is None:
        assert final.summary is None
        assert final.summarizer_usage == TokenUsage()
    else:
        assert final.summary == scn.summary.content
        assert final.summarizer_usage == scn.summary.usage
    assert final.error == scn.error
