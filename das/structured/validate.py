"""Validate a completion against an Output_Schema (Phase 6 Req 1.4, 1.7, 1.8).

Pure: no I/O. ``parse_and_validate`` returns either :class:`Valid` (the parsed value and its canonical text) or
:class:`Violation` (a JSON path and a short reason). The reason is built from the schema keyword that failed, never
from the rejected content: jsonschema's own messages quote the instance, and a Violation travels back to the model
and into a Job_Result error, where user data must not end up.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Final

from jsonschema import Draft202012Validator
from jsonschema.exceptions import ValidationError

from das.plan.codec import strip_code_fences

MAX_COMPLETION_BYTES: Final = 256 * 1024
MAX_MESSAGE: Final = 300
_ROOT: Final = "$"


@dataclass(frozen=True, slots=True)
class Valid:
    """The completion matches. ``text`` is canonical JSON: sorted keys, no extra whitespace (Req 1.7)."""

    value: Any
    text: str


@dataclass(frozen=True, slots=True)
class Violation:
    """The completion does not match. ``path`` is a JSON path such as ``$.items[2].name``."""

    path: str
    message: str


def _path(parts: Any) -> str:
    return _ROOT + "".join(f"[{p}]" if isinstance(p, int) else f".{p}" for p in parts)


def _order(error: ValidationError) -> tuple[Any, ...]:
    # Ints before strings at the same depth, so $[2] sorts before $[10]; the keyword breaks ties.
    return (tuple((0, p) if isinstance(p, int) else (1, p) for p in error.absolute_path), error.validator or "")


def _describe(error: ValidationError) -> str:
    keyword = str(error.validator)
    expected = error.validator_value
    if keyword == "required" and isinstance(expected, list) and isinstance(error.instance, dict):
        missing = [name for name in expected if name not in error.instance]
        return f"missing required property {', '.join(repr(name) for name in missing)}"
    if keyword == "additionalProperties":
        return "has a property the schema does not allow"
    try:
        wanted = json.dumps(expected, separators=(",", ":"), sort_keys=True)
    except (TypeError, ValueError):
        wanted = "?"
    return f"does not satisfy {keyword}: {wanted}"[:MAX_MESSAGE]


def parse_and_validate(text: str, schema: dict[str, Any]) -> Valid | Violation:
    """Parse ``text`` (one Markdown code fence is accepted) and check it against ``schema``."""
    if len(text.encode()) > MAX_COMPLETION_BYTES:
        return Violation(_ROOT, "the completion is larger than 256 KiB")
    try:
        value = json.loads(strip_code_fences(text))
    except json.JSONDecodeError as exc:
        return Violation(_ROOT, f"the completion is not valid JSON: {exc.msg}")
    except RecursionError:
        return Violation(_ROOT, "the completion is not valid JSON: nested too deeply")
    errors = sorted(Draft202012Validator(schema).iter_errors(value), key=_order)
    if errors:
        first = errors[0]
        return Violation(_path(first.absolute_path), _describe(first))
    return Valid(value, json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False))


__all__ = ["MAX_COMPLETION_BYTES", "Valid", "Violation", "parse_and_validate"]
