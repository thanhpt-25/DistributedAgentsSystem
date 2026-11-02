"""Unit tests for das.manager.backoff and das.manager.final_result (Req 6.2, 7.2, 7.3, 7.5, 7.6)."""
from __future__ import annotations

import pytest

from das.llm.base import Completion
from das.manager.backoff import backoff
from das.manager.final_result import CANCEL_REASON, build_final_result
from das.models.job import JobRecord, JobState
from das.models.plan import JobPlan, PlannedJob
from das.models.results import FailureEntry, JobReport, JobResultPayload, TokenUsage
from das.state import store

S = JobState


class TestBackoff:
    def test_doubles_from_base(self) -> None:
        assert [backoff(n, 2, 60) for n in (1, 2, 3, 4, 5)] == [2, 4, 8, 16, 32]

    def test_capped_at_max(self) -> None:
        assert backoff(6, 2, 60) == 60
        assert backoff(10_000, 0.1, 600) == 600

    def test_rejects_attempt_below_one(self) -> None:
        with pytest.raises(ValueError):
            backoff(0, 2, 60)


def _plan(**deps: list[str]) -> JobPlan:
    return JobPlan(
        jobs=[
            PlannedJob(job_id=j, instruction=f"do {j}", capabilities=["research"], depends_on=d)
            for j, d in deps.items()
        ]
    )


def _rec(plan: JobPlan, job_id: str, state: JobState, attempt: int, reason: str | None = None):
    return JobRecord(job=plan.by_id()[job_id], state=state, attempt=attempt, reason=reason)


def _res(job_id: str, attempt: int, status: str, error: str | None = None, tokens=(0, 0)):
    return JobResultPayload(
        request_id="r1",
        job_id=job_id,
        attempt=attempt,
        worker_id=f"w-{job_id}-{attempt}",
        status=status,  # type: ignore[arg-type]
        content=f"out {job_id}",
        error=error,
        usage=TokenUsage(input_tokens=tokens[0], output_tokens=tokens[1]),
        duration_ms=5,
    )


def test_cancel_reason_matches_store() -> None:
    assert CANCEL_REASON == store.CANCEL_REASON


def test_mixed_outcomes() -> None:
    # a ok; b fails twice (attempt 2 current); c skipped because of b; d cancelled.
    plan = _plan(a=[], b=["a"], c=["a", "b"], d=[])
    jobs = {
        "a": _rec(plan, "a", S.SUCCEEDED, 1),
        "b": _rec(plan, "b", S.FAILED, 2),
        "c": _rec(plan, "c", S.SKIPPED, 0),
        "d": _rec(plan, "d", S.CANCELLED, 1),
    }
    results = {
        ("a", 1): _res("a", 1, "Succeeded", tokens=(10, 20)),
        ("b", 1): _res("b", 1, "Failed", "first error", tokens=(1, 1)),
        ("b", 2): _res("b", 2, "Failed", "last error", tokens=(3, 4)),
    }
    summary = Completion(content="the answer", usage=TokenUsage(input_tokens=7, output_tokens=8))

    fr = build_final_result(plan, jobs, results, summary, None)

    assert fr.summary == "the answer"
    assert fr.summarizer_usage.input_tokens == 7 and fr.summarizer_usage.output_tokens == 8
    assert fr.error is None
    assert fr.jobs == [
        JobReport(job_id="a", state=S.SUCCEEDED, worker_id="w-a-1", input_tokens=10, output_tokens=20),
        JobReport(job_id="b", state=S.FAILED, worker_id="w-b-2", input_tokens=3, output_tokens=4),
        JobReport(job_id="c", state=S.SKIPPED, worker_id=None, input_tokens=0, output_tokens=0),
        JobReport(job_id="d", state=S.CANCELLED, worker_id=None, input_tokens=0, output_tokens=0),
    ]
    assert fr.failures == [
        FailureEntry(job_id="b", state=S.FAILED, reason="last error"),
        FailureEntry(job_id="c", state=S.SKIPPED, reason="skipped: dependency b failed"),
        FailureEntry(job_id="d", state=S.CANCELLED, reason=CANCEL_REASON),
    ]


def test_recorded_reason_used_when_no_result() -> None:
    plan = _plan(a=[], b=["a"])
    jobs = {
        "a": _rec(plan, "a", S.FAILED, 3, reason="repeated unacknowledged delivery"),
        "b": _rec(plan, "b", S.SKIPPED, 0, reason="skipped: dependency a failed"),
    }
    fr = build_final_result(plan, jobs, {}, None, None)
    assert [f.reason for f in fr.failures] == [
        "repeated unacknowledged delivery",
        "skipped: dependency a failed",
    ]


def test_all_failed_skips_summary() -> None:
    # Req 7.3: no Succeeded Job, no Summarizer output.
    plan = _plan(a=[])
    jobs = {"a": _rec(plan, "a", S.FAILED, 1)}
    results = {("a", 1): _res("a", 1, "Failed", "boom")}
    fr = build_final_result(plan, jobs, results, None, None)
    assert fr.summary is None
    assert fr.summarizer_usage == TokenUsage()
    assert fr.failures == [FailureEntry(job_id="a", state=S.FAILED, reason="boom")]


def test_summarization_failure_keeps_failure_list() -> None:
    # Req 7.6: summary None, error set, failure list still present.
    plan = _plan(a=[], b=[])
    jobs = {"a": _rec(plan, "a", S.SUCCEEDED, 1), "b": _rec(plan, "b", S.CANCELLED, 0)}
    results = {("a", 1): _res("a", 1, "Succeeded")}
    fr = build_final_result(plan, jobs, results, None, "summarization failed: timeout")
    assert fr.summary is None
    assert fr.error == "summarization failed: timeout"
    assert fr.failures == [FailureEntry(job_id="b", state=S.CANCELLED, reason=CANCEL_REASON)]


def test_missing_record_rejected() -> None:
    plan = _plan(a=[])
    with pytest.raises(ValueError, match="'a'"):
        build_final_result(plan, {}, {}, None, None)
