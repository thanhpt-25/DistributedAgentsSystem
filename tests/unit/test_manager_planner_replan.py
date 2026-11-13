"""``Planner.replan``: ask for a plan that keeps the succeeded jobs and replaces the rest
(das-smarter-agents task 3.3; Phase 6 Req 3.3, 3.5, 3.8)."""

from __future__ import annotations

import json
from contextlib import contextmanager

import pytest

from das.llm.base import ModelParams
from das.llm.errors import PermanentLLMError
from das.llm.retry import LLMProvider
from das.manager.planner import (
    RESULT_EXCERPT_LIMIT,
    NoCapabilities,
    PlanAccepted,
    Planner,
    PlanLLMFailed,
    PlannerConfig,
    PlanRejected,
)
from das.models.plan import PlannedJob
from das.plan.replan import FailedJob, ReplanContext, SucceededJob
from das.plan.validate import CheckFailure
from tests.fakes.llm_adapter import FakeAdapter, completion

PARAMS = ModelParams(model="m-1", temperature=0.0, max_output_tokens=256)
CAPS = frozenset({"search", "write"})
A = PlannedJob(job_id="a", instruction="find the facts", capabilities=["search"])
B = PlannedJob(job_id="b", instruction="write it up", capabilities=["write"], depends_on=["a"])
CTX = ReplanContext(succeeded=(SucceededJob(A, "the facts: 42"),), failed=(FailedJob(B, "provider exploded"),))
JOB_A = {"job_id": "a", "instruction": "find the facts", "capabilities": ["search"], "depends_on": []}
JOB_C = {"job_id": "c", "instruction": "write it differently", "capabilities": ["write"], "depends_on": ["a"]}


def text(*jobs: dict) -> str:
    return json.dumps({"jobs": list(jobs)})


GOOD = text(JOB_A, JOB_C)


def make(outcomes: list) -> tuple[Planner, FakeAdapter]:
    adapter = FakeAdapter(outcomes)
    return Planner(LLMProvider(adapter, PARAMS, retry_count=0)), adapter


async def replan(planner: Planner, ctx: ReplanContext = CTX, caps: frozenset[str] = CAPS, **cfg: int):  # noqa: ANN201
    return await planner.replan("do the research", caps, ctx, PlannerConfig(**cfg))


# --------------------------------------------------------------------------- the first request


async def test_a_good_replacement_is_accepted_on_the_first_attempt() -> None:
    planner, adapter = make([completion(GOOD, 10, 5)])
    out = await replan(planner)
    assert isinstance(out, PlanAccepted) and out.attempts == 1
    assert [j.job_id for j in out.plan.jobs] == ["a", "c"]
    assert len(adapter.calls) == 1 and len(out.usages) == 1 and out.usages[0].operation == "plan"


async def test_the_prompt_tells_the_model_what_succeeded_what_failed_and_the_rules() -> None:
    planner, adapter = make([GOOD])
    await replan(planner)
    prompt = adapter.calls[0].messages[0].content
    assert "do the research" in prompt  # the original request
    assert "the facts: 42" in prompt  # what the succeeded job produced
    assert json.dumps(JOB_A, ensure_ascii=False) in prompt  # the succeeded job, to copy exactly
    assert "provider exploded" in prompt and '"b"' in prompt  # what failed, and why
    assert "search" in prompt and "write" in prompt  # the advertised Capabilities
    assert "exactly" in prompt and "not used before" in prompt
    assert "between 1 and 10 jobs" in prompt


async def test_a_long_result_is_cut_to_the_excerpt_limit() -> None:
    long = SucceededJob(A, "x" * (RESULT_EXCERPT_LIMIT * 5))
    planner, adapter = make([GOOD])
    await replan(planner, ReplanContext(succeeded=(long,), failed=CTX.failed))
    prompt = adapter.calls[0].messages[0].content
    assert "x" * RESULT_EXCERPT_LIMIT in prompt and "x" * (RESULT_EXCERPT_LIMIT + 1) not in prompt
    assert "truncated" in prompt


async def test_a_long_error_is_cut_too() -> None:
    noisy = FailedJob(B, "e" * (RESULT_EXCERPT_LIMIT * 5))
    planner, adapter = make([GOOD])
    await replan(planner, ReplanContext(succeeded=CTX.succeeded, failed=(noisy,)))
    assert "e" * (RESULT_EXCERPT_LIMIT + 1) not in adapter.calls[0].messages[0].content


# --------------------------------------------------------------------------- the correction loop (Req 3.3)


async def test_a_plan_that_changes_a_succeeded_job_is_corrected_with_the_original_shown() -> None:
    changed = dict(JOB_A, instruction="something else entirely")
    planner, adapter = make([text(changed, JOB_C), GOOD])
    out = await replan(planner)
    assert isinstance(out, PlanAccepted) and out.attempts == 2
    messages = adapter.calls[1].messages
    assert [m.role for m in messages] == ["user", "assistant", "user"]
    assert CheckFailure.KEEPS_SUCCEEDED.value in messages[2].content
    assert "find the facts" in messages[2].content  # the original, to copy back
    assert len(out.usages) == 2  # both planning attempts are recorded


async def test_a_plan_that_drops_a_succeeded_job_is_refused_and_corrected() -> None:
    planner, _ = make([text(dict(JOB_C, depends_on=[])), GOOD])
    out = await replan(planner)
    assert isinstance(out, PlanAccepted) and out.attempts == 2


async def test_reusing_a_failed_jobs_id_is_corrected() -> None:
    reused = dict(JOB_C, job_id="b")
    planner, adapter = make([text(JOB_A, reused), GOOD])
    out = await replan(planner)
    assert isinstance(out, PlanAccepted)
    assert CheckFailure.FRESH_JOB_IDS.value in adapter.calls[1].messages[-1].content


async def test_a_plan_with_nothing_new_is_corrected() -> None:
    planner, adapter = make([text(JOB_A), GOOD])
    out = await replan(planner)
    assert isinstance(out, PlanAccepted)
    assert CheckFailure.NEW_JOB_REQUIRED.value in adapter.calls[1].messages[-1].content


async def test_a_structurally_broken_plan_is_corrected_by_the_ordinary_checks_first() -> None:
    broken = text(JOB_A, dict(JOB_C, depends_on=["nobody"]))
    planner, adapter = make([broken, GOOD])
    out = await replan(planner)
    assert isinstance(out, PlanAccepted)
    assert CheckFailure.UNKNOWN_DEPENDENCY.value in adapter.calls[1].messages[-1].content


async def test_still_wrong_after_the_retries_the_replan_is_rejected_with_the_replan_check_named() -> None:
    changed = text(dict(JOB_A, instruction="nope"), JOB_C)
    planner, adapter = make([changed] * 5)
    out = await replan(planner, planning_retries=1)
    assert isinstance(out, PlanRejected) and out.attempts == 2 and len(adapter.calls) == 2
    assert CheckFailure.KEEPS_SUCCEEDED in out.failed_checks
    assert "keeps_succeeded" in out.error_message


# --------------------------------------------------------------------------- Capabilities and size


async def test_a_kept_job_may_use_a_capability_that_is_no_longer_advertised() -> None:
    planner, _ = make([GOOD])
    out = await replan(planner, caps=frozenset({"write"}))  # the search Worker is gone, but job a already succeeded
    assert isinstance(out, PlanAccepted)


async def test_a_new_job_needs_an_advertised_capability() -> None:
    needs_search = dict(JOB_C, capabilities=["search"])
    planner, adapter = make([text(JOB_A, needs_search), text(JOB_A, JOB_C)])
    out = await replan(planner, caps=frozenset({"write"}))
    assert isinstance(out, PlanAccepted) and out.attempts == 2
    assert CheckFailure.UNADVERTISED_CAPABILITY.value in adapter.calls[1].messages[-1].content


async def test_the_job_limit_covers_the_whole_plan_kept_jobs_included() -> None:
    planner, adapter = make([GOOD])
    out = await planner.replan("p", CAPS, CTX, PlannerConfig(max_jobs=1, planning_retries=0))
    assert isinstance(out, PlanRejected)
    assert CheckFailure.JOB_COUNT_OUT_OF_RANGE in out.failed_checks
    assert len(adapter.calls) == 1


# --------------------------------------------------------------------------- the other outcomes


async def test_no_advertised_capabilities_means_no_call() -> None:
    planner, adapter = make([])
    out = await replan(planner, caps=frozenset())
    assert isinstance(out, NoCapabilities) and adapter.calls == []


async def test_a_failing_provider_is_reported_not_raised() -> None:
    planner, _ = make([PermanentLLMError("down for good")])
    out = await replan(planner)
    assert isinstance(out, PlanLLMFailed) and "down for good" in out.error_message


async def test_every_attempt_runs_inside_the_attempt_hook() -> None:
    seen: list[tuple[int, str]] = []

    @contextmanager
    def hook(attempt: int):  # noqa: ANN202
        class Observer:
            def fail(self, code: str) -> None:
                seen.append((attempt, code))

        yield Observer()

    planner, _ = make([text(JOB_A), GOOD])
    out = await planner.replan("p", CAPS, CTX, PlannerConfig(), on_attempt=hook)
    assert isinstance(out, PlanAccepted)
    assert seen == [(1, "PlanRejected")]  # attempt 1 was rejected; attempt 2 passed


# --------------------------------------------------------------------------- the limit (Req 3.1)


def test_replanning_is_off_in_a_default_planner_config() -> None:
    assert PlannerConfig().max_replans == 0


@pytest.mark.parametrize("n", [0, 1, 2, 3])
def test_a_planner_config_takes_zero_to_three_replans(n: int) -> None:
    assert PlannerConfig(max_replans=n).max_replans == n


@pytest.mark.parametrize("n", [-1, 4, 100])
def test_a_planner_config_refuses_a_count_out_of_range(n: int) -> None:
    with pytest.raises(ValueError, match="max_replans"):
        PlannerConfig(max_replans=n)


@pytest.mark.parametrize("n", [True, 1.5, "2"])
def test_a_planner_config_refuses_a_count_that_is_not_an_int(n: object) -> None:
    with pytest.raises(TypeError, match="max_replans"):
        PlannerConfig(max_replans=n)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- plan() is untouched


@pytest.mark.parametrize("retries", [0, 2])
async def test_the_ordinary_planner_still_works_and_knows_nothing_of_replans(retries: int) -> None:
    planner, adapter = make([text(JOB_A, JOB_C)])
    out = await planner.plan("p", CAPS, PlannerConfig(planning_retries=retries))
    assert isinstance(out, PlanAccepted)
    assert "succeeded" not in adapter.calls[0].messages[0].content.lower()
