"""Check an Output_Schema before it is accepted into a plan (Phase 6 Req 1.1).

Pure: no I/O. A schema is accepted only when it is a JSON object, fits in 16 KiB, nests at most 16 containers
deep, refers to nothing outside itself (a remote ``$ref`` would make validation fetch a URL), and is itself valid
draft 2020-12.
"""

from __future__ import annotations

import json
from typing import Any, Final

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError

MAX_SCHEMA_BYTES: Final = 16 * 1024
MAX_SCHEMA_DEPTH: Final = 16
_MAX_REASON: Final = 200


def _depth(value: Any) -> int:
    """Container nesting depth, without recursion (a hostile schema must not overflow the stack)."""
    deepest = 0
    stack: list[tuple[Any, int]] = [(value, 1)]
    while stack:
        node, level = stack.pop()
        if isinstance(node, dict):
            children = list(node.values())
        elif isinstance(node, list):
            children = node
        else:
            continue
        deepest = max(deepest, level)
        stack.extend((child, level + 1) for child in children)
    return deepest


def _has_remote_ref(value: Any) -> bool:
    stack = [value]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            for key, child in node.items():
                if key in ("$ref", "$dynamicRef") and not (isinstance(child, str) and child.startswith("#")):
                    return True
                stack.append(child)
        elif isinstance(node, list):
            stack.extend(node)
    return False


def check_schema(schema: Any) -> str | None:
    """Why ``schema`` cannot be an Output_Schema, or ``None`` when it can."""
    if not isinstance(schema, dict):
        return "output_schema must be a JSON object"
    try:
        size = len(json.dumps(schema, separators=(",", ":")).encode())
    except (TypeError, ValueError):
        return "output_schema must be JSON"
    if size > MAX_SCHEMA_BYTES:
        return "output_schema is larger than 16 KiB"
    if _depth(schema) > MAX_SCHEMA_DEPTH:
        return f"output_schema is nested too deep (more than {MAX_SCHEMA_DEPTH} levels)"
    if _has_remote_ref(schema):
        return "output_schema may only refer to itself ($ref must start with '#')"
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        return f"output_schema is not valid JSON Schema: {exc.message[:_MAX_REASON]}"
    return None


__all__ = ["MAX_SCHEMA_BYTES", "MAX_SCHEMA_DEPTH", "check_schema"]
