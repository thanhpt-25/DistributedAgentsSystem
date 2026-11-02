# Feature: distributed-agent-system, Property 1: Job_Plan round trip
"""Property 1: Job_Plan round trip.

For any valid ``JobPlan``:

- ``print_plan(p)`` is JSON that validates against ``PLAN_JSON_SCHEMA`` (Req 3.7).
- ``print_plan(parse_plan(print_plan(p)))`` is equivalent to ``print_plan(p)``:
  same Job_ID set and, per Job_ID, the same instruction, Capability set, and
  dependency set (Req 3.8).

``jsonschema`` isn't a project dependency, so conformance is checked with a small
validator for exactly the draft 2020-12 keywords ``PLAN_JSON_SCHEMA`` uses. It
fails loudly on any keyword it doesn't understand, so a schema change can't make
the check silently vacuous.

**Validates: Requirements 3.7, 3.8**
"""

from __future__ import annotations

import json
import re
from typing import Any

from hypothesis import given

from das.models.plan import JobPlan, plans_equivalent
from das.plan.codec import PLAN_JSON_SCHEMA, parse_plan, print_plan
from tests.strategies import job_plans

# Keywords that are annotations only and impose no constraint.
_ANNOTATIONS = frozenset({"$schema", "$id", "title", "description", "default"})

_JSON_TYPES: dict[str, tuple[type, ...]] = {
    "object": (dict,),
    "array": (list,),
    "string": (str,),
}


def _schema_errors(value: Any, schema: dict[str, Any], path: str = "$") -> list[str]:
    """Return every violation of ``schema`` by ``value`` (empty list means valid)."""
    errors: list[str] = []
    for keyword, arg in schema.items():
        if keyword in _ANNOTATIONS:
            continue
        if keyword == "type":
            if not isinstance(value, _JSON_TYPES[arg]):
                errors.append(f"{path}: expected {arg}, got {type(value).__name__}")
                return errors  # other keywords assume the right type
        elif keyword == "required":
            errors += [f"{path}: missing {k!r}" for k in arg if k not in value]
        elif keyword == "properties":
            for key, sub in arg.items():
                if key in value:
                    errors += _schema_errors(value[key], sub, f"{path}.{key}")
        elif keyword == "additionalProperties":
            assert arg is False, "validator only supports additionalProperties: false"
            extra = set(value) - set(schema.get("properties", {}))
            errors += [f"{path}: unexpected property {k!r}" for k in sorted(extra)]
        elif keyword == "items":
            for i, item in enumerate(value):
                errors += _schema_errors(item, arg, f"{path}[{i}]")
        elif keyword == "minItems":
            if len(value) < arg:
                errors.append(f"{path}: fewer than {arg} items")
        elif keyword == "minLength":
            if len(value) < arg:
                errors.append(f"{path}: shorter than {arg}")
        elif keyword == "maxLength":
            if len(value) > arg:
                errors.append(f"{path}: longer than {arg}")
        elif keyword == "pattern":
            if re.search(arg, value) is None:
                errors.append(f"{path}: does not match {arg!r}")
        else:
            raise AssertionError(f"test validator does not support schema keyword {keyword!r}")
    return errors


@given(job_plans(max_jobs=10))
def test_job_plan_round_trip(plan: JobPlan) -> None:
    """**Validates: Requirements 3.7, 3.8**"""
    printed = print_plan(plan)

    # Req 3.7: printed plan is JSON conforming to the Job_Plan schema.
    document = json.loads(printed)
    assert _schema_errors(document, PLAN_JSON_SCHEMA) == []

    # Req 3.8: print -> parse -> print is equivalent to print.
    parsed = parse_plan(printed)
    reprinted = print_plan(parsed)
    assert plans_equivalent(parse_plan(reprinted), parse_plan(printed))
    assert plans_equivalent(parsed, plan)
    # The printer is deterministic, so the reprint is also textually identical.
    assert reprinted == printed


def test_schema_validator_rejects_violations() -> None:
    """Sanity check that the test validator isn't vacuous."""
    bad = {"jobs": [{"job_id": "bad id!", "instruction": "", "capabilities": [], "x": 1}]}
    errors = _schema_errors(bad, PLAN_JSON_SCHEMA)
    assert any("unexpected property 'x'" in e for e in errors)
    assert any("job_id" in e for e in errors)
    assert any("instruction" in e for e in errors)
    assert any("capabilities" in e for e in errors)
    assert _schema_errors({}, PLAN_JSON_SCHEMA) == ["$: missing 'jobs'"]
