"""Property 1: the Worker's validation accepts a value if and only if it matches the schema (das-smarter-agents
task 1.4; Req 8.1, 1.4, 1.7).

The oracle is ``jsonschema`` itself. What is under test is everything the wrapper adds around it: fence stripping, the
size cap, canonical output, and a reason that never repeats the rejected value."""

from __future__ import annotations

import json
from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st
from jsonschema import Draft202012Validator

from das.structured.schema import check_schema
from das.structured.validate import MAX_MESSAGE, Valid, Violation, parse_and_validate

KEYS = st.sampled_from(["a", "b", "c", "name", "age"])
SCALARS = st.one_of(
    st.none(), st.booleans(), st.integers(-1000, 1000), st.floats(-1e6, 1e6, allow_nan=False, allow_infinity=False),
    st.text(max_size=12),
)
VALUES = st.recursive(
    SCALARS,
    lambda inner: st.one_of(st.lists(inner, max_size=4), st.dictionaries(KEYS, inner, max_size=4)),
    max_leaves=12,
)
TYPES = st.sampled_from(["null", "boolean", "integer", "number", "string", "array", "object"])


def leaf_schemas() -> st.SearchStrategy[dict[str, Any]]:
    return st.one_of(
        st.just({}),
        TYPES.map(lambda t: {"type": t}),
        st.lists(TYPES, min_size=1, max_size=3, unique=True).map(lambda ts: {"type": ts}),
        st.lists(SCALARS, min_size=1, max_size=3).map(lambda vs: {"enum": vs}),
        SCALARS.map(lambda v: {"const": v}),
        st.integers(-50, 50).map(lambda n: {"type": "number", "minimum": n}),
        st.integers(-50, 50).map(lambda n: {"type": "number", "exclusiveMaximum": n}),
        st.integers(0, 6).map(lambda n: {"type": "string", "minLength": n}),
        st.integers(0, 6).map(lambda n: {"type": "string", "maxLength": n}),
        st.integers(0, 3).map(lambda n: {"type": "array", "minItems": n}),
        st.just({"type": "array", "uniqueItems": True}),
    )


def schemas() -> st.SearchStrategy[dict[str, Any]]:
    def extend(inner: st.SearchStrategy[dict[str, Any]]) -> st.SearchStrategy[dict[str, Any]]:
        return st.one_of(
            st.builds(lambda s: {"type": "array", "items": s}, inner),
            st.builds(
                lambda props, req, extra: {
                    "type": "object", "properties": props,
                    "required": sorted({k for k in req if k in props}), "additionalProperties": extra,
                },
                st.dictionaries(KEYS, inner, max_size=3), st.lists(KEYS, max_size=3), st.booleans(),
            ),
            st.builds(lambda a, b: {"anyOf": [a, b]}, inner, inner),
            st.builds(lambda a, b: {"allOf": [a, b]}, inner, inner),
            st.builds(lambda s: {"not": s}, inner),
        )

    return st.recursive(leaf_schemas(), extend, max_leaves=6)


@given(schema=schemas(), value=VALUES)
@settings(deadline=None)
def test_the_wrapper_accepts_exactly_what_jsonschema_accepts(schema: dict[str, Any], value: Any) -> None:
    assert check_schema(schema) is None  # the generator only builds schemas the platform would accept
    verdict = parse_and_validate(json.dumps(value), schema)
    assert isinstance(verdict, Valid) == Draft202012Validator(schema).is_valid(value)


@given(schema=schemas(), value=VALUES)
@settings(deadline=None)
def test_a_valid_result_is_the_parsed_value_in_canonical_text(schema: dict[str, Any], value: Any) -> None:
    verdict = parse_and_validate(json.dumps(value), schema)
    if isinstance(verdict, Valid):
        assert verdict.value == value
        assert json.loads(verdict.text) == value
        again = json.dumps(json.loads(verdict.text), sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        assert again == verdict.text


@given(schema=schemas(), value=VALUES)
@settings(deadline=None)
def test_a_code_fence_does_not_change_the_verdict(schema: dict[str, Any], value: Any) -> None:
    plain = parse_and_validate(json.dumps(value), schema)
    fenced = parse_and_validate("```json\n" + json.dumps(value) + "\n```", schema)
    assert plain == fenced


@given(schema=schemas(), value=VALUES)
@settings(deadline=None)
def test_a_violation_is_a_short_json_path_and_reason(schema: dict[str, Any], value: Any) -> None:
    verdict = parse_and_validate(json.dumps(value), schema)
    if isinstance(verdict, Violation):
        assert verdict.path.startswith("$")
        assert 0 < len(verdict.message) <= MAX_MESSAGE


MARKER = "zq9-unlikely-marker-zq9"


@given(schema=schemas(), value=st.dictionaries(KEYS, VALUES, max_size=3))
@settings(deadline=None)
def test_a_value_the_schema_rejects_is_never_repeated_in_the_reason(
    schema: dict[str, Any], value: dict[str, Any]
) -> None:
    """Values only: a *key* may appear in a path when the schema says ``additionalProperties: {...}``."""
    tainted = {**value, "a": [MARKER, {"b": MARKER}]}
    verdict = parse_and_validate(json.dumps(tainted), schema)
    if isinstance(verdict, Violation):
        assert MARKER not in verdict.path + verdict.message
