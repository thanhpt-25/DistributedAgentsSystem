"""Unit tests for das.plan.codec (Requirements 3.4, 3.7, 3.8)."""

from __future__ import annotations

import json

import pytest
from hypothesis import given

from das.models.plan import JobPlan, PlannedJob, plans_equivalent
from das.plan.codec import (
    PLAN_JSON_SCHEMA,
    IssueKind,
    PlanSchemaError,
    parse_plan,
    parse_plan_obj,
    plan_json_schema_text,
    print_plan,
    strip_code_fences,
)
from tests.strategies import job_plans


def _plan() -> JobPlan:
    return JobPlan(
        jobs=[
            PlannedJob(job_id="a", instruction="Research X", capabilities=["research"]),
            PlannedJob(
                job_id="b",
                instruction="Résumé of a ✓",
                capabilities=["summarize", "research"],
                depends_on=["a"],
            ),
        ]
    )


# ------------------------------------------------------------------ printer


def test_print_plan_is_deterministic_and_conforms_to_schema_shape() -> None:
    text = print_plan(_plan())
    assert text == print_plan(_plan())
    doc = json.loads(text)
    assert set(doc) == set(PLAN_JSON_SCHEMA["required"])
    job_schema = PLAN_JSON_SCHEMA["properties"]["jobs"]["items"]
    for job in doc["jobs"]:
        assert set(job) <= set(job_schema["properties"])
        assert set(job_schema["required"]) <= set(job)
    assert doc["jobs"][1]["depends_on"] == ["a"]
    assert "Résumé of a ✓" in text  # ensure_ascii=False


def test_schema_text_is_valid_json() -> None:
    assert json.loads(plan_json_schema_text()) == PLAN_JSON_SCHEMA


# ------------------------------------------------------------------ parser


def test_parse_round_trip() -> None:
    plan = _plan()
    assert plans_equivalent(parse_plan(print_plan(plan)), plan)


def test_parse_defaults_depends_on() -> None:
    plan = parse_plan('{"jobs":[{"job_id":"x","instruction":"do","capabilities":["c"]}]}')
    assert plan.jobs[0].depends_on == []


@pytest.mark.parametrize(
    "text",
    [
        '```json\n{"jobs": []}\n```',
        'Here is the plan:\n```\n{"jobs": []}\n```\nThanks',
        '~~~json\n{"jobs": []}\n~~~',
        '  {"jobs": []}  ',
    ],
)
def test_code_fences_are_stripped(text: str) -> None:
    assert json.loads(strip_code_fences(text)) == {"jobs": []}
    assert parse_plan(text) == JobPlan(jobs=[])


@pytest.mark.parametrize(
    ("obj", "kind", "path"),
    [
        ([], IssueKind.NOT_AN_OBJECT, ()),
        ({}, IssueKind.MISSING_FIELD, ("jobs",)),
        ({"jobs": [], "extra": 1}, IssueKind.UNKNOWN_FIELD, ("extra",)),
        ({"jobs": "nope"}, IssueKind.WRONG_TYPE, ("jobs",)),
        (
            {"jobs": [{"job_id": "a", "instruction": "", "capabilities": ["c"]}]},
            IssueKind.EMPTY_INSTRUCTION,
            ("jobs", 0, "instruction"),
        ),
        (
            {"jobs": [{"job_id": "a", "instruction": "x" * 8001, "capabilities": ["c"]}]},
            IssueKind.INSTRUCTION_TOO_LONG,
            ("jobs", 0, "instruction"),
        ),
        (
            {"jobs": [{"job_id": "a", "instruction": "x", "capabilities": []}]},
            IssueKind.NO_CAPABILITIES,
            ("jobs", 0, "capabilities"),
        ),
        (
            {"jobs": [{"job_id": "a b", "instruction": "x", "capabilities": ["c"]}]},
            IssueKind.INVALID_JOB_ID,
            ("jobs", 0, "job_id"),
        ),
        (
            {"jobs": [{"job_id": "a", "instruction": 5, "capabilities": ["c"]}]},
            IssueKind.WRONG_TYPE,
            ("jobs", 0, "instruction"),
        ),
    ],
)
def test_schema_failures_are_classified(obj: object, kind: IssueKind, path: tuple) -> None:
    with pytest.raises(PlanSchemaError) as info:
        parse_plan_obj(obj)
    assert any(i.kind == kind and i.path == path for i in info.value.issues)


def test_invalid_json_raises_plan_schema_error() -> None:
    with pytest.raises(PlanSchemaError) as info:
        parse_plan("not json {")
    assert info.value.kinds == {IssueKind.INVALID_JSON}


def test_all_field_issues_are_reported_with_job_index() -> None:
    obj = {
        "jobs": [
            {"job_id": "a", "instruction": "", "capabilities": ["c"]},
            {"job_id": "b", "instruction": "ok", "capabilities": []},
        ]
    }
    with pytest.raises(PlanSchemaError) as info:
        parse_plan_obj(obj)
    by_kind = {i.kind: i.job_index for i in info.value.issues}
    assert by_kind == {IssueKind.EMPTY_INSTRUCTION: 0, IssueKind.NO_CAPABILITIES: 1}


def test_error_message_does_not_echo_input_values() -> None:
    secret = "VERY-SECRET-INSTRUCTION-TEXT"
    with pytest.raises(PlanSchemaError) as info:
        parse_plan_obj({"jobs": [{"job_id": "a", "instruction": secret, "capabilities": 1}]})
    assert secret not in str(info.value)


# ------------------------------------------------------------------ strategy sanity


@given(job_plans(max_jobs=8))
def test_job_plans_strategy_produces_valid_dags(plan: JobPlan) -> None:
    ids = plan.job_ids()
    assert 1 <= len(ids) <= 8
    assert len(set(ids)) == len(ids)
    # Acyclic: repeatedly remove Jobs whose deps are all removed.
    remaining = {j.job_id: set(j.depends_on) for j in plan.jobs}
    for deps in remaining.values():
        assert deps <= set(ids)
    done: set[str] = set()
    while remaining:
        ready = [j for j, d in remaining.items() if d <= done]
        assert ready, "cycle in generated plan"
        for j in ready:
            done.add(j)
            del remaining[j]
    assert plans_equivalent(parse_plan(print_plan(plan)), plan)


@given(job_plans(max_jobs=3, capabilities=frozenset({"research", "code-review"})))
def test_job_plans_strategy_respects_capability_pool(plan: JobPlan) -> None:
    for job in plan.jobs:
        assert set(job.capabilities) <= {"research", "code-review"}
