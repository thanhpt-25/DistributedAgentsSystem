"""Unit tests for the pure Job_Plan validator (Requirements 3.1, 3.2, 3.4)."""

from __future__ import annotations

import pytest

from das.models.plan import JobPlan, PlannedJob
from das.plan.codec import print_plan
from das.plan.validate import (
    CheckFailure as CF,
    validate_plan,
    validate_plan_detailed,
    validate_plan_text,
)

CAPS = frozenset({"research", "summarize"})


def job(jid: str, deps: list[str] | None = None, caps: list[str] | None = None,
        instruction: str = "do it") -> dict:
    return {
        "job_id": jid,
        "instruction": instruction,
        "capabilities": caps if caps is not None else ["research"],
        "depends_on": deps or [],
    }


def plan(*jobs: dict) -> dict:
    return {"jobs": list(jobs)}


def test_valid_plan_object_and_model() -> None:
    obj = plan(job("a"), job("b", ["a"], ["summarize"]), job("c", ["a", "b"]))
    assert validate_plan(obj, CAPS, 10) == set()
    model = JobPlan.model_validate(obj)
    result = validate_plan_detailed(model, CAPS, 10)
    assert result.ok and result.plan == model


@pytest.mark.parametrize(
    ("candidate", "expected"),
    [
        (plan(job("a"), job("a")), {CF.DUPLICATE_JOB_ID}),
        (plan(job("a", instruction="")), {CF.EMPTY_INSTRUCTION}),
        (plan(job("a", instruction=" \n\t ")), {CF.EMPTY_INSTRUCTION}),
        (plan(job("a", instruction="x" * 8001)), {CF.INSTRUCTION_TOO_LONG}),
        (plan(job("a", caps=[])), {CF.NO_CAPABILITIES}),
        (plan(job("a", ["a"])), {CF.SELF_DEPENDENCY}),
        (plan(job("a", ["zzz"])), {CF.UNKNOWN_DEPENDENCY}),
        (plan(job("a", ["c"]), job("b", ["a"]), job("c", ["b"])), {CF.CYCLE}),
        (plan(), {CF.JOB_COUNT_OUT_OF_RANGE}),
        (plan(job("a", caps=["code-review"])), {CF.UNADVERTISED_CAPABILITY}),
        ({"jobs": "nope"}, {CF.SCHEMA}),
        ([], {CF.SCHEMA}),
        (plan({**job("a"), "extra": 1}), {CF.SCHEMA}),
        (plan(job("bad id!")), {CF.SCHEMA}),
    ],
)
def test_single_failure(candidate: object, expected: set[CF]) -> None:
    assert validate_plan(candidate, CAPS, 10) == expected


def test_job_count_above_max() -> None:
    obj = plan(job("a"), job("b"), job("c"))
    assert validate_plan(obj, CAPS, 2) == {CF.JOB_COUNT_OUT_OF_RANGE}
    assert validate_plan(obj, CAPS, 3) == set()


def test_reports_every_failure_at_once() -> None:
    obj = plan(
        job("a", ["b"], instruction=""),
        job("b", ["a", "ghost"], caps=["code-review"]),
        job("b"),
        job("s", ["s"]),
    )
    assert validate_plan(obj, CAPS, 3) == {
        CF.EMPTY_INSTRUCTION,
        CF.CYCLE,
        CF.UNKNOWN_DEPENDENCY,
        CF.UNADVERTISED_CAPABILITY,
        CF.DUPLICATE_JOB_ID,
        CF.SELF_DEPENDENCY,
        CF.JOB_COUNT_OUT_OF_RANGE,
    }


def test_self_loop_is_not_also_a_cycle() -> None:
    assert CF.CYCLE not in validate_plan(plan(job("a", ["a"])), CAPS, 10)


def test_empty_advertised_set_fails_every_capability() -> None:
    assert validate_plan(plan(job("a")), frozenset(), 10) == {CF.UNADVERTISED_CAPABILITY}


def test_cycle_message_names_the_cycle() -> None:
    result = validate_plan_detailed(plan(job("a", ["b"]), job("b", ["a"])), CAPS, 10)
    assert result.failures == {CF.CYCLE}
    assert result.plan is None
    (issue,) = result.issues
    assert "a" in issue.message and "b" in issue.message and "->" in issue.message


def test_validate_text_with_fences_and_invalid_json() -> None:
    model = JobPlan(jobs=[PlannedJob(job_id="a", instruction="x", capabilities=["research"])])
    fenced = f"Here you go:\n```json\n{print_plan(model)}\n```\n"
    ok = validate_plan_text(fenced, CAPS, 10)
    assert ok.ok and ok.plan == model

    bad = validate_plan_text("{not json", CAPS, 10)
    assert bad.failures == {CF.SCHEMA} and bad.plan is None


@pytest.mark.parametrize("max_jobs", [0, 101, -1])
def test_max_jobs_out_of_range_raises(max_jobs: int) -> None:
    with pytest.raises(ValueError):
        validate_plan(plan(job("a")), CAPS, max_jobs)
