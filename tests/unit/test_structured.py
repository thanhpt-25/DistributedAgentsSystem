"""Structured output: schema checks and completion validation (das-smarter-agents task 1.1; Req 1.1, 1.4, 1.7, 1.8)."""

from __future__ import annotations

import json

import pytest

from das.structured.schema import MAX_SCHEMA_BYTES, MAX_SCHEMA_DEPTH, check_schema
from das.structured.validate import MAX_COMPLETION_BYTES, Valid, Violation, parse_and_validate

PERSON = {
    "type": "object",
    "properties": {"name": {"type": "string"}, "age": {"type": "integer", "minimum": 0}},
    "required": ["name", "age"],
    "additionalProperties": False,
}


def nested(depth: int) -> dict:
    schema: dict = {"type": "string"}
    for _ in range(depth):
        schema = {"type": "object", "properties": {"x": schema}}
    return schema


# --------------------------------------------------------------------------- check_schema (Req 1.1)


def test_a_valid_schema_has_no_error() -> None:
    assert check_schema(PERSON) is None


@pytest.mark.parametrize("bad", [{"type": "nonsense"}, {"minimum": "x"}, {"properties": 3}])
def test_a_schema_that_is_not_valid_json_schema_is_refused(bad: dict) -> None:
    assert check_schema(bad)


@pytest.mark.parametrize("not_a_dict", [[], "{}", 3, None])
def test_a_schema_that_is_not_an_object_is_refused(not_a_dict: object) -> None:
    assert check_schema(not_a_dict)  # type: ignore[arg-type]


def test_a_schema_over_the_size_limit_is_refused() -> None:
    big = {"type": "object", "description": "x" * (MAX_SCHEMA_BYTES + 1)}
    assert "16 KiB" in (check_schema(big) or "")


def test_a_schema_nested_deeper_than_the_limit_is_refused() -> None:
    # Each step of ``nested`` adds two containers (the schema and its "properties"), so 7 steps is 15 levels.
    assert check_schema(nested(7)) is None
    assert "deep" in (check_schema(nested(8)) or "")  # 17 containers: the first one over the limit
    assert "deep" in (check_schema(nested(MAX_SCHEMA_DEPTH)) or "")


def test_a_remote_reference_is_refused_so_validation_never_fetches() -> None:
    assert check_schema({"$ref": "http://example.com/schema.json"})


# --------------------------------------------------------------------------- parse_and_validate (Req 1.4, 1.7)


def test_valid_json_is_accepted_and_returned_canonically() -> None:
    result = parse_and_validate('{"name": "Ada", "age": 36}', PERSON)
    assert isinstance(result, Valid)
    assert result.value == {"name": "Ada", "age": 36}
    assert result.text == '{"age":36,"name":"Ada"}'  # sorted keys, no extra whitespace


def test_one_markdown_code_fence_around_the_json_is_accepted() -> None:
    fenced = '```json\n{"name": "Ada", "age": 36}\n```'
    assert isinstance(parse_and_validate(fenced, PERSON), Valid)


def test_text_that_is_not_json_is_a_violation_at_the_root() -> None:
    result = parse_and_validate("Sure! Here is the person.", PERSON)
    assert isinstance(result, Violation)
    assert result.path == "$"
    assert "JSON" in result.message


def test_a_value_that_does_not_match_names_the_path_and_the_message() -> None:
    result = parse_and_validate('{"name": "Ada", "age": -1}', PERSON)
    assert isinstance(result, Violation)
    assert result.path == "$.age"
    assert "minimum" in result.message or "-1" in result.message


def test_a_missing_required_field_is_reported_at_its_parent() -> None:
    result = parse_and_validate('{"name": "Ada"}', PERSON)
    assert isinstance(result, Violation)
    assert result.path == "$"
    assert "age" in result.message


def test_the_first_violation_by_path_is_the_one_reported() -> None:
    schema = {"type": "object", "properties": {"a": {"type": "string"}, "b": {"type": "string"}}}
    result = parse_and_validate('{"b": 1, "a": 2}', schema)
    assert isinstance(result, Violation)
    assert result.path == "$.a"  # "a" sorts before "b", whatever the document order


def test_array_indexes_appear_in_the_path() -> None:
    schema = {"type": "array", "items": {"type": "integer"}}
    result = parse_and_validate('[1, 2, "x"]', schema)
    assert isinstance(result, Violation)
    assert result.path == "$[2]"


def test_a_violation_never_contains_the_rejected_content() -> None:
    secret = "hunter2-very-secret"
    result = parse_and_validate(json.dumps({"name": 12, "age": 1, "note": secret}), PERSON)
    assert isinstance(result, Violation)
    assert secret not in result.path + result.message


def test_a_message_that_would_quote_a_long_value_is_cut_short() -> None:
    result = parse_and_validate(json.dumps({"name": "x" * 5000, "age": "old"}), PERSON)
    assert isinstance(result, Violation)
    assert len(result.message) <= 300


# --------------------------------------------------------------------------- the size cap (Req 1.8)


def test_a_completion_over_the_limit_is_a_violation_without_being_parsed() -> None:
    huge = '{"name": "' + "a" * MAX_COMPLETION_BYTES + '", "age": 1}'
    result = parse_and_validate(huge, PERSON)
    assert isinstance(result, Violation)
    assert "256 KiB" in result.message


def test_a_completion_just_under_the_limit_is_still_parsed() -> None:
    text = json.dumps({"name": "a" * (MAX_COMPLETION_BYTES - 100), "age": 1})
    assert len(text) < MAX_COMPLETION_BYTES
    assert isinstance(parse_and_validate(text, PERSON), Valid)


def test_the_limit_counts_bytes_not_characters() -> None:
    text = json.dumps({"name": "é" * (MAX_COMPLETION_BYTES // 2 + 10), "age": 1}, ensure_ascii=False)
    assert len(text) < MAX_COMPLETION_BYTES < len(text.encode())
    assert isinstance(parse_and_validate(text, PERSON), Violation)


def test_deeply_nested_json_is_a_violation_not_a_crash() -> None:
    text = "[" * 50_000 + "]" * 50_000
    assert isinstance(parse_and_validate(text, {"type": "array"}), Violation)
