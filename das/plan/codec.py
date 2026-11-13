"""Job_Plan printer, parser, and JSON schema (Requirements 3.4, 3.7, 3.8).

``print_plan`` renders a :class:`~das.models.plan.JobPlan` as deterministic JSON
that conforms to :data:`PLAN_JSON_SCHEMA`. ``parse_plan`` turns LLM output (optionally
wrapped in Markdown code fences) back into a ``JobPlan`` and raises
:class:`PlanSchemaError` when the text doesn't conform to the schema.

``PlanSchemaError`` carries structured :class:`SchemaIssue` entries so the plan
validator (``das.plan.validate``) can tell field-level failures apart, for example
an empty instruction versus an over-long one versus an empty Capability list, even
though ``PlannedJob`` itself rejects those values at construction time.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final

from pydantic import ValidationError

from das.models.plan import JOB_ID_PATTERN, MAX_INSTRUCTION_LENGTH, JobPlan

# --------------------------------------------------------------------------- schema

PLAN_JSON_SCHEMA: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "https://das.local/schemas/job_plan.json",
    "title": "JobPlan",
    "type": "object",
    "additionalProperties": False,
    "required": ["jobs"],
    "properties": {
        "jobs": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["job_id", "instruction", "capabilities"],
                "properties": {
                    "job_id": {"type": "string", "pattern": JOB_ID_PATTERN},
                    "instruction": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": MAX_INSTRUCTION_LENGTH,
                    },
                    "capabilities": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                    },
                    "depends_on": {
                        "type": "array",
                        "items": {"type": "string"},
                        "default": [],
                    },
                    "output_schema": {
                        "type": "object",
                        "description": (
                            "Optional JSON Schema (draft 2020-12, at most 16 KiB, no remote $ref). When present, "
                            "the job's result must be JSON that matches it."
                        ),
                    },
                },
            },
        }
    },
}
"""Job_Plan JSON schema (draft 2020-12). Mirrors the constraints of ``PlannedJob``.

Structural rules that JSON Schema can't express (unique Job_IDs, known dependencies,
no self-dependency, acyclicity, Job count bounds, advertised Capabilities) are checked
by ``das.plan.validate``.
"""


def plan_json_schema_text() -> str:
    """The schema as pretty JSON, for embedding in the planning prompt."""
    return json.dumps(PLAN_JSON_SCHEMA, indent=2, sort_keys=True, ensure_ascii=False)


# --------------------------------------------------------------------------- errors


class IssueKind(StrEnum):
    """Classification of a single schema failure."""

    INVALID_JSON = "invalid_json"
    NOT_AN_OBJECT = "not_an_object"
    MISSING_FIELD = "missing_field"
    UNKNOWN_FIELD = "unknown_field"
    WRONG_TYPE = "wrong_type"
    INVALID_JOB_ID = "invalid_job_id"
    EMPTY_INSTRUCTION = "empty_instruction"
    INSTRUCTION_TOO_LONG = "instruction_too_long"
    NO_CAPABILITIES = "no_capabilities"


@dataclass(frozen=True, slots=True)
class SchemaIssue:
    """One schema failure. ``path`` is the JSON location, e.g. ``("jobs", 2, "instruction")``."""

    kind: IssueKind
    path: tuple[str | int, ...]
    message: str

    @property
    def job_index(self) -> int | None:
        """Index into ``jobs`` when the issue concerns a single Job, else ``None``."""
        if len(self.path) >= 2 and self.path[0] == "jobs" and isinstance(self.path[1], int):
            return self.path[1]
        return None

    def __str__(self) -> str:
        where = ".".join(str(p) for p in self.path) or "<root>"
        return f"{where}: {self.message}"


class PlanSchemaError(ValueError):
    """The text is not a Job_Plan that conforms to :data:`PLAN_JSON_SCHEMA`."""

    def __init__(self, issues: list[SchemaIssue] | tuple[SchemaIssue, ...]) -> None:
        if not issues:
            raise ValueError("PlanSchemaError requires at least one issue")
        self.issues: tuple[SchemaIssue, ...] = tuple(issues)
        super().__init__(
            "Job_Plan does not conform to the schema:\n"
            + "\n".join(f"  - {issue}" for issue in self.issues)
        )

    @property
    def kinds(self) -> frozenset[IssueKind]:
        return frozenset(issue.kind for issue in self.issues)


# --------------------------------------------------------------------------- printer


def print_plan(plan: JobPlan) -> str:
    """Render ``plan`` as deterministic, schema-conforming, pretty-printed JSON (Req 3.7).

    Keys are sorted and Job, Capability, and dependency order is preserved, so the same
    plan always prints to the same text.
    """
    document = {
        "jobs": [
            {
                "job_id": job.job_id,
                "instruction": job.instruction,
                "capabilities": list(job.capabilities),
                "depends_on": list(job.depends_on),
                **({"output_schema": job.output_schema} if job.output_schema is not None else {}),
            }
            for job in plan.jobs
        ]
    }
    return json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False)


# --------------------------------------------------------------------------- parser

# A fenced block: ``` or ~~~, optional info string (e.g. "json"), body, closing fence.
_FENCE_RE: Final = re.compile(
    r"(?P<fence>`{3,}|~{3,})[ \t]*[\w+-]*[ \t]*\r?\n(?P<body>.*?)\r?\n[ \t]*(?P=fence)",
    re.DOTALL,
)


def strip_code_fences(text: str) -> str:
    """Return the body of the first Markdown code fence in ``text``, stripped.

    LLMs often wrap JSON in ```` ```json ... ``` ````. Text without a fence is returned
    stripped of surrounding whitespace.
    """
    match = _FENCE_RE.search(text)
    if match is not None:
        return match.group("body").strip()
    return text.strip()


def load_plan_json(text: str) -> Any:
    """Strip code fences and decode JSON, raising ``PlanSchemaError`` on bad JSON."""
    body = strip_code_fences(text)
    try:
        return json.loads(body)
    except (json.JSONDecodeError, RecursionError) as exc:
        reason = exc.msg if isinstance(exc, json.JSONDecodeError) else "nesting too deep"
        raise PlanSchemaError(
            [SchemaIssue(IssueKind.INVALID_JSON, (), f"invalid JSON: {reason}")]
        ) from None


def parse_plan_obj(obj: Any) -> JobPlan:
    """Validate an already-decoded JSON value against the schema and build a ``JobPlan``.

    Validation is strict: values must already have the JSON types the schema requires
    (no string/number coercion).
    """
    if not isinstance(obj, dict):
        raise PlanSchemaError(
            [
                SchemaIssue(
                    IssueKind.NOT_AN_OBJECT,
                    (),
                    f"expected a JSON object, got {_json_type(obj)}",
                )
            ]
        )
    try:
        return JobPlan.model_validate(obj, strict=True)
    except ValidationError as exc:
        raise PlanSchemaError(_issues_from_validation(exc)) from None


def parse_plan(text: str) -> JobPlan:
    """Parse LLM output or ``print_plan`` output into a ``JobPlan`` (Req 3.4).

    Raises :class:`PlanSchemaError` listing every schema failure found.
    """
    return parse_plan_obj(load_plan_json(text))


def _json_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _classify(loc: tuple[str | int, ...], err_type: str) -> IssueKind:
    field = loc[-1] if loc else None
    if err_type == "missing":
        return IssueKind.MISSING_FIELD
    if err_type == "extra_forbidden":
        return IssueKind.UNKNOWN_FIELD
    # Only classify Job-level field constraints when the location is jobs[i].<field>.
    in_job = len(loc) == 3 and loc[0] == "jobs" and isinstance(loc[1], int)
    if in_job and field == "instruction":
        if err_type == "string_too_short":
            return IssueKind.EMPTY_INSTRUCTION
        if err_type == "string_too_long":
            return IssueKind.INSTRUCTION_TOO_LONG
    if in_job and field == "capabilities" and err_type == "too_short":
        return IssueKind.NO_CAPABILITIES
    if in_job and field == "job_id" and err_type == "string_pattern_mismatch":
        return IssueKind.INVALID_JOB_ID
    return IssueKind.WRONG_TYPE


def _issues_from_validation(exc: ValidationError) -> list[SchemaIssue]:
    issues: list[SchemaIssue] = []
    seen: set[tuple[IssueKind, tuple[str | int, ...]]] = set()
    for err in exc.errors(include_url=False, include_input=False):
        loc = tuple(err["loc"])
        kind = _classify(loc, err["type"])
        key = (kind, loc)
        if key in seen:
            continue
        seen.add(key)
        issues.append(SchemaIssue(kind, loc, err["msg"]))
    return issues


__all__ = [
    "PLAN_JSON_SCHEMA",
    "IssueKind",
    "PlanSchemaError",
    "SchemaIssue",
    "load_plan_json",
    "parse_plan",
    "parse_plan_obj",
    "plan_json_schema_text",
    "print_plan",
    "strip_code_fences",
]
