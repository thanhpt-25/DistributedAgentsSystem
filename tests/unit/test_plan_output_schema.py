"""``output_schema`` on planned Jobs and dispatches (das-smarter-agents task 1.2; Req 1.1, 1.2, 1.3).

The field is optional and absent when unset, so every Phase 5 plan, stored record and wire message keeps its bytes."""

from __future__ import annotations

import json
from typing import Any

import pytest

from das.codec import MessageCodec, new_message
from das.manager.machine import PublishDispatch
from das.manager.planner import build_planning_prompt
from das.models.plan import JobPlan, PlannedJob
from das.models.results import JobDispatchPayload
from das.plan.codec import PLAN_JSON_SCHEMA, parse_plan, print_plan
from das.plan.validate import CheckFailure, validate_plan, validate_plan_detailed

SCHEMA = {"type": "object", "properties": {"n": {"type": "integer"}}, "required": ["n"]}
CAPS = ["llm"]


def job(**extra: Any) -> dict[str, Any]:
    return {"job_id": "a", "instruction": "count", "capabilities": ["llm"], "depends_on": [], **extra}


# --------------------------------------------------------------------------- the model


def test_a_job_without_a_schema_serializes_exactly_as_in_phase_5() -> None:
    planned = PlannedJob(job_id="a", instruction="x", capabilities=["llm"])
    assert planned.output_schema is None
    assert set(planned.model_dump()) == {"job_id", "instruction", "capabilities", "depends_on"}
    assert "output_schema" not in planned.model_dump_json()
    assert "output_schema" not in json.dumps(JobPlan(jobs=[planned]).model_dump(mode="json"))


def test_a_job_with_a_schema_serializes_it() -> None:
    planned = PlannedJob(job_id="a", instruction="x", capabilities=["llm"], output_schema=SCHEMA)
    assert planned.model_dump()["output_schema"] == SCHEMA
    assert json.loads(planned.model_dump_json())["output_schema"] == SCHEMA


def test_a_plan_with_a_schema_round_trips_through_its_stored_json() -> None:
    plan = JobPlan(jobs=[PlannedJob(job_id="a", instruction="x", capabilities=["llm"], output_schema=SCHEMA)])
    assert JobPlan.model_validate_json(plan.model_dump_json()) == plan


# --------------------------------------------------------------------------- the plan text (Req 1.2)


def test_the_plan_schema_offers_output_schema_as_an_optional_object() -> None:
    item = PLAN_JSON_SCHEMA["properties"]["jobs"]["items"]
    assert item["properties"]["output_schema"]["type"] == "object"
    assert "output_schema" not in item["required"]


def test_the_planning_prompt_tells_the_model_it_may_add_one() -> None:
    text = build_planning_prompt("do it", frozenset({"llm"}), 5)
    assert "output_schema" in text


def test_print_plan_includes_the_schema_only_when_set() -> None:
    bare = JobPlan(jobs=[PlannedJob(job_id="a", instruction="x", capabilities=["llm"])])
    with_schema = JobPlan(jobs=[PlannedJob(job_id="a", instruction="x", capabilities=["llm"], output_schema=SCHEMA)])
    assert "output_schema" not in print_plan(bare)
    assert json.loads(print_plan(with_schema))["jobs"][0]["output_schema"] == SCHEMA
    assert parse_plan(print_plan(with_schema)) == with_schema


# --------------------------------------------------------------------------- validation (Req 1.1)


def test_a_plan_with_a_valid_schema_passes() -> None:
    assert validate_plan({"jobs": [job(output_schema=SCHEMA)]}, CAPS, 5) == set()


def test_a_plan_without_a_schema_passes_as_before() -> None:
    assert validate_plan({"jobs": [job()]}, CAPS, 5) == set()


def test_an_explicit_null_schema_counts_as_none() -> None:
    assert validate_plan({"jobs": [job(output_schema=None)]}, CAPS, 5) == set()


def test_an_invalid_schema_fails_invalid_output_schema_and_names_the_job() -> None:
    result = validate_plan_detailed({"jobs": [job(output_schema={"type": "nonsense"})]}, CAPS, 5)
    assert CheckFailure.INVALID_OUTPUT_SCHEMA in result.failures
    issue = next(i for i in result.issues if i.check is CheckFailure.INVALID_OUTPUT_SCHEMA)
    assert issue.job_id == "a" and "'a'" in issue.message
    assert CheckFailure.INVALID_OUTPUT_SCHEMA.value == "invalid_output_schema"


def test_a_schema_that_is_not_an_object_fails_the_check() -> None:
    assert CheckFailure.INVALID_OUTPUT_SCHEMA in validate_plan({"jobs": [job(output_schema=["x"])]}, CAPS, 5)


def test_an_oversized_schema_fails_the_check() -> None:
    big = {"type": "object", "description": "x" * 20_000}
    assert CheckFailure.INVALID_OUTPUT_SCHEMA in validate_plan({"jobs": [job(output_schema=big)]}, CAPS, 5)


def test_a_remote_reference_fails_the_check() -> None:
    bad = {"$ref": "https://example.com/s.json"}
    assert CheckFailure.INVALID_OUTPUT_SCHEMA in validate_plan({"jobs": [job(output_schema=bad)]}, CAPS, 5)


def test_only_the_bad_job_is_named_when_a_plan_has_several() -> None:
    second = job(output_schema={"type": "nonsense"})
    second["job_id"] = "b"
    result = validate_plan_detailed({"jobs": [job(output_schema=SCHEMA), second]}, CAPS, 5)
    named = {i.job_id for i in result.issues if i.check is CheckFailure.INVALID_OUTPUT_SCHEMA}
    assert named == {"b"}


# --------------------------------------------------------------------------- the dispatch (Req 1.3)


def payload(**extra: Any) -> JobDispatchPayload:
    return JobDispatchPayload(
        request_id="r", job_id="a", attempt=1, instruction="x", required_capabilities=["llm"], **extra
    )


def test_a_dispatch_without_a_schema_has_no_such_key() -> None:
    assert "output_schema" not in payload().model_dump()
    assert "output_schema" not in payload().model_dump_json()


def test_a_dispatch_with_a_schema_carries_it() -> None:
    assert payload(output_schema=SCHEMA).model_dump()["output_schema"] == SCHEMA


def test_the_effect_copies_the_jobs_schema_into_the_payload() -> None:
    effect = PublishDispatch("a", 1, "llm", "x", ("llm",), {}, output_schema=SCHEMA)
    assert effect.to_payload("r").output_schema == SCHEMA
    assert PublishDispatch("a", 1, "llm", "x", ("llm",), {}).to_payload("r").output_schema is None


def test_the_wire_bytes_of_a_dispatch_without_a_schema_have_no_such_key() -> None:
    """The codec corpus pins whole messages; this states it for the one new field, on the real encoder."""
    wire = MessageCodec().encode(new_message(payload(), correlation_id="r"))
    assert b"output_schema" not in wire


def test_a_dispatch_with_a_schema_survives_encode_and_decode() -> None:
    codec = MessageCodec()
    decoded = codec.decode(codec.encode(new_message(payload(output_schema=SCHEMA), correlation_id="r")))
    assert decoded.payload.output_schema == SCHEMA  # type: ignore[union-attr]


@pytest.mark.parametrize("bad", ["text", 3, ["a"]])
def test_a_dispatch_rejects_a_schema_that_is_not_an_object(bad: object) -> None:
    with pytest.raises(ValueError):
        payload(output_schema=bad)
