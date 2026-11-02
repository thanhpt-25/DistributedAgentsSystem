"""Unit tests for Summarizer retries (Requirement 7.6).

Each Summarizer attempt is one adapter call: the LLMProvider is built with
``retry_count=0`` and a no-op sleep. Timeouts use a tiny Summarizer
``call_timeout`` with the adapter blocking (``HANG``) until cancelled.
"""
from __future__ import annotations

import pytest

from das.llm.base import ModelParams
from das.llm.errors import PermanentLLMError, TransientLLMError
from das.llm.retry import LLMProvider
from das.manager.final_result import build_final_result
from das.manager.summarizer import SUMMARIZER_MAX_ATTEMPTS, SummarizationFailed, Summarizer
from das.models.job import JobRecord, JobState
from das.models.plan import JobPlan, PlannedJob
from das.models.results import JobResultPayload, TokenUsage
from tests.fakes.llm_adapter import HANG, FakeAdapter, completion

SHORT_TIMEOUT = 0.05
SUCCEEDED = {"j1": "alpha"}


async def _no_sleep(_: float) -> None:
    return None


def _summarizer(adapter: FakeAdapter, call_timeout: float = SHORT_TIMEOUT) -> Summarizer:
    llm = LLMProvider(
        adapter,
        ModelParams(model="m", temperature=0.0, max_output_tokens=256),
        retry_count=0,
        timeout=60,
        sleep=_no_sleep,
    )
    return Summarizer(llm, call_timeout=call_timeout)


def test_default_max_attempts_is_three() -> None:
    assert SUMMARIZER_MAX_ATTEMPTS == 3


@pytest.mark.parametrize(
    "first_two",
    [
        pytest.param([PermanentLLMError("e1"), PermanentLLMError("e2")], id="errors"),
        pytest.param([TransientLLMError("e1"), TransientLLMError("e2")], id="transient-errors"),
        pytest.param([HANG, HANG], id="timeouts"),
        pytest.param([HANG, PermanentLLMError("e2")], id="timeout-then-error"),
    ],
)
async def test_success_on_third_attempt(first_two: list) -> None:
    adapter = FakeAdapter([*first_two, completion("final", input_tokens=4, output_tokens=2)])

    result = await _summarizer(adapter).summarize("p", SUCCEEDED)

    assert result.content == "final"
    assert result.usage.input_tokens == 4 and result.usage.output_tokens == 2
    assert len(adapter.calls) == 3
    # Every attempt sends the same input.
    assert len({c.messages for c in adapter.calls}) == 1


async def test_success_on_first_attempt_makes_one_call() -> None:
    adapter = FakeAdapter([completion("final")])
    assert (await _summarizer(adapter).summarize("p", SUCCEEDED)).content == "final"
    assert len(adapter.calls) == 1


async def test_error_on_every_attempt_fails_after_three() -> None:
    adapter = FakeAdapter(
        [PermanentLLMError("e1"), PermanentLLMError("e2"), PermanentLLMError("last error")]
    )

    with pytest.raises(SummarizationFailed) as info:
        await _summarizer(adapter).summarize("p", SUCCEEDED)

    assert len(adapter.calls) == 3
    assert info.value.attempts == 3
    assert info.value.error.startswith("summarization failed:")
    # The reported reason is the last attempt's.
    assert "last error" in info.value.error
    assert "e1" not in info.value.error


async def test_timeout_on_every_attempt_fails_after_three() -> None:
    adapter = FakeAdapter(default=HANG)

    with pytest.raises(SummarizationFailed) as info:
        await _summarizer(adapter).summarize("p", SUCCEEDED)

    assert len(adapter.calls) == 3
    assert info.value.attempts == 3
    assert info.value.error.startswith("summarization failed:")
    assert "timeout" in info.value.error


async def test_mixed_failures_report_last_reason() -> None:
    adapter = FakeAdapter([PermanentLLMError("e1"), PermanentLLMError("e2"), HANG])

    with pytest.raises(SummarizationFailed) as info:
        await _summarizer(adapter).summarize("p", SUCCEEDED)

    assert len(adapter.calls) == 3
    assert "timeout" in info.value.error and "e2" not in info.value.error


async def test_no_fourth_attempt_after_failures() -> None:
    # A fourth scripted success must never be reached.
    adapter = FakeAdapter([HANG, PermanentLLMError("e"), HANG, completion("too late")])

    with pytest.raises(SummarizationFailed):
        await _summarizer(adapter).summarize("p", SUCCEEDED)

    assert len(adapter.calls) == 3


def _plan() -> JobPlan:
    return JobPlan(
        jobs=[
            PlannedJob(job_id="j1", instruction="do j1", capabilities=["research"]),
            PlannedJob(job_id="j2", instruction="do j2", capabilities=["research"]),
        ]
    )


async def test_failure_yields_final_result_with_error_and_no_summary() -> None:
    plan = _plan()
    by_id = plan.by_id()
    jobs = {
        "j1": JobRecord(job=by_id["j1"], state=JobState.SUCCEEDED, attempt=1),
        "j2": JobRecord(job=by_id["j2"], state=JobState.FAILED, attempt=1),
    }
    results = {
        ("j1", 1): JobResultPayload(
            request_id="r1", job_id="j1", attempt=1, worker_id="w1", status="Succeeded",
            content="alpha", error=None, usage=TokenUsage(input_tokens=3, output_tokens=5),
            duration_ms=1,
        ),
        ("j2", 1): JobResultPayload(
            request_id="r1", job_id="j2", attempt=1, worker_id="w2", status="Failed",
            content="", error="worker broke", usage=TokenUsage(), duration_ms=1,
        ),
    }
    adapter = FakeAdapter(default=PermanentLLMError("provider down"))

    with pytest.raises(SummarizationFailed) as info:
        await _summarizer(adapter).summarize("p", {"j1": "alpha"})

    fr = build_final_result(plan, jobs, results, summary=None, error=info.value.error)

    assert fr.summary is None
    assert fr.error == info.value.error
    assert fr.error is not None and fr.error.startswith("summarization failed:")
    assert "provider down" in fr.error
    assert fr.summarizer_usage == TokenUsage()
    # Job reports and failures are still present.
    assert [r.job_id for r in fr.jobs] == ["j1", "j2"]
    assert fr.jobs[0].input_tokens == 3 and fr.jobs[0].output_tokens == 5
    assert [(f.job_id, f.reason) for f in fr.failures] == [("j2", "worker broke")]
