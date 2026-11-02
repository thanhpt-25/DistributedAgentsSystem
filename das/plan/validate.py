"""Pure Job_Plan validator (Requirements 3.1, 3.2, 3.4).

``validate_plan`` returns the *complete* set of failed checks for a candidate plan, so
the Planner can name every problem in a single corrected-plan request (Req 3.5).

A candidate may be a :class:`~das.models.plan.JobPlan`, an already-decoded JSON value
(for example a ``dict`` from the LLM), or raw LLM text (see :func:`validate_plan_text`).
Schema failures don't stop structural checking: whatever parts of the candidate are
well-formed enough (``jobs`` is a list of objects, ``job_id`` is a string, ...) are still
checked for duplicates, unknown dependencies, cycles, and so on.

Decisions documented here:

* A whitespace-only instruction counts as ``EMPTY_INSTRUCTION`` even though it passes
  the JSON schema's ``minLength: 1``. Such an instruction carries no work for a Worker,
  so Req 3.2's "non-empty instruction" is read as "non-blank".
* A Job that lists its own Job_ID in ``depends_on`` fails ``SELF_DEPENDENCY`` only. Self
  loops are not also reported as ``CYCLE``; ``CYCLE`` means a cycle through two or more
  distinct Job_IDs.
* Dependencies on unknown Job_IDs are reported as ``UNKNOWN_DEPENDENCY`` and ignored for
  cycle detection. Jobs sharing a duplicated Job_ID are merged into one graph node.
* A plan with zero Jobs fails ``JOB_COUNT_OUT_OF_RANGE``. A missing or non-list ``jobs``
  field fails ``SCHEMA`` only, since there is no Job count to check.
* Schema issues map to checks as: ``EMPTY_INSTRUCTION``, ``INSTRUCTION_TOO_LONG`` and
  ``NO_CAPABILITIES`` one-to-one; every other schema issue maps to ``SCHEMA``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final

from das.models.plan import MAX_INSTRUCTION_LENGTH, JobPlan
from das.plan.codec import (
    IssueKind,
    PlanSchemaError,
    load_plan_json,
    parse_plan_obj,
)

MIN_MAX_JOBS: Final = 1
MAX_MAX_JOBS: Final = 100


class CheckFailure(StrEnum):
    """One validity check of Req 3.2 / 3.4 that a candidate plan failed."""

    SCHEMA = "schema"
    DUPLICATE_JOB_ID = "duplicate_job_id"
    EMPTY_INSTRUCTION = "empty_instruction"
    INSTRUCTION_TOO_LONG = "instruction_too_long"
    NO_CAPABILITIES = "no_capabilities"
    SELF_DEPENDENCY = "self_dependency"
    UNKNOWN_DEPENDENCY = "unknown_dependency"
    CYCLE = "cycle"
    JOB_COUNT_OUT_OF_RANGE = "job_count_out_of_range"
    UNADVERTISED_CAPABILITY = "unadvertised_capability"


@dataclass(frozen=True, slots=True)
class CheckIssue:
    """One concrete failure, with a human-readable message for corrected-plan prompts."""

    check: CheckFailure
    message: str
    job_id: str | None = None

    def __str__(self) -> str:
        return f"[{self.check.value}] {self.message}"


@dataclass(frozen=True, slots=True)
class PlanValidation:
    """Outcome of validating a candidate. ``plan`` is set only when the candidate is valid."""

    issues: tuple[CheckIssue, ...]
    plan: JobPlan | None

    @property
    def failures(self) -> frozenset[CheckFailure]:
        return frozenset(issue.check for issue in self.issues)

    @property
    def ok(self) -> bool:
        return not self.issues


_ISSUE_TO_CHECK: Final[Mapping[IssueKind, CheckFailure]] = {
    IssueKind.EMPTY_INSTRUCTION: CheckFailure.EMPTY_INSTRUCTION,
    IssueKind.INSTRUCTION_TOO_LONG: CheckFailure.INSTRUCTION_TOO_LONG,
    IssueKind.NO_CAPABILITIES: CheckFailure.NO_CAPABILITIES,
}


def check_for_issue(kind: IssueKind) -> CheckFailure:
    """Map a codec schema issue kind to the validator check it fails."""
    return _ISSUE_TO_CHECK.get(kind, CheckFailure.SCHEMA)


# --------------------------------------------------------------------------- public API


def validate_plan(
    plan: JobPlan | Any, capabilities: Iterable[str], max_jobs: int
) -> set[CheckFailure]:
    """Return every check the candidate fails; an empty set means the plan is valid.

    ``plan`` is a ``JobPlan`` or a decoded JSON value. ``capabilities`` is the advertised
    Capability set (Req 3.3). ``max_jobs`` must be in 1..100 (Req 3.1).
    """
    return set(validate_plan_detailed(plan, capabilities, max_jobs).failures)


def validate_plan_detailed(
    plan: JobPlan | Any, capabilities: Iterable[str], max_jobs: int
) -> PlanValidation:
    """Like :func:`validate_plan` but returns per-failure messages and the parsed plan."""
    _check_max_jobs(max_jobs)
    advertised = frozenset(capabilities)
    obj = plan.model_dump(mode="json") if isinstance(plan, JobPlan) else plan

    issues: list[CheckIssue] = []
    parsed: JobPlan | None = None
    try:
        parsed = parse_plan_obj(obj)
    except PlanSchemaError as exc:
        for si in exc.issues:
            issues.append(CheckIssue(check_for_issue(si.kind), str(si), _job_id_at(obj, si.job_index)))

    issues.extend(_structural_issues(obj, advertised, max_jobs))
    issues = _dedupe(issues)
    return PlanValidation(tuple(issues), parsed if not issues else None)


def validate_plan_text(text: str, capabilities: Iterable[str], max_jobs: int) -> PlanValidation:
    """Parse raw LLM output (code fences allowed) and validate it (Req 3.4).

    Invalid JSON yields a single ``SCHEMA`` issue. Used by the Planner (task 13.1).
    """
    _check_max_jobs(max_jobs)
    try:
        obj = load_plan_json(text)
    except PlanSchemaError as exc:
        return PlanValidation(
            tuple(CheckIssue(CheckFailure.SCHEMA, str(si)) for si in exc.issues), None
        )
    return validate_plan_detailed(obj, capabilities, max_jobs)


# --------------------------------------------------------------------------- internals


def _check_max_jobs(max_jobs: int) -> None:
    if isinstance(max_jobs, bool) or not isinstance(max_jobs, int):
        raise TypeError(f"max_jobs must be an int, got {type(max_jobs).__name__}")
    if not MIN_MAX_JOBS <= max_jobs <= MAX_MAX_JOBS:
        raise ValueError(f"max_jobs must be in {MIN_MAX_JOBS}..{MAX_MAX_JOBS}, got {max_jobs}")


def _job_id_at(obj: Any, index: int | None) -> str | None:
    if index is None or not isinstance(obj, dict):
        return None
    jobs = obj.get("jobs")
    if isinstance(jobs, list) and 0 <= index < len(jobs) and isinstance(jobs[index], dict):
        jid = jobs[index].get("job_id")
        return jid if isinstance(jid, str) else None
    return None


def _str_list(value: Any) -> list[str]:
    """The string members of a list value; anything else yields nothing (schema reports it)."""
    if not isinstance(value, list):
        return []
    return [v for v in value if isinstance(v, str)]


def _structural_issues(obj: Any, advertised: frozenset[str], max_jobs: int) -> list[CheckIssue]:
    """Checks beyond the JSON schema, run on whatever parts of ``obj`` are well-formed."""
    if not isinstance(obj, dict) or not isinstance(obj.get("jobs"), list):
        return []  # schema already reports this; no Job count or graph to inspect
    raw_jobs: list[Any] = obj["jobs"]
    issues: list[CheckIssue] = []

    count = len(raw_jobs)
    if not 1 <= count <= max_jobs:
        issues.append(
            CheckIssue(
                CheckFailure.JOB_COUNT_OUT_OF_RANGE,
                f"plan has {count} jobs; must have between 1 and {max_jobs}",
            )
        )

    jobs = [j for j in raw_jobs if isinstance(j, dict)]
    ids = [j["job_id"] for j in jobs if isinstance(j.get("job_id"), str)]
    known = set(ids)

    seen: set[str] = set()
    for jid in ids:
        if jid in seen:
            issues.append(
                CheckIssue(CheckFailure.DUPLICATE_JOB_ID, f"job_id {jid!r} is used more than once", jid)
            )
        seen.add(jid)

    graph: dict[str, set[str]] = {jid: set() for jid in known}
    for job in jobs:
        jid = job.get("job_id") if isinstance(job.get("job_id"), str) else None
        label = repr(jid) if jid is not None else "<unnamed>"

        instruction = job.get("instruction")
        # Whitespace-only passes the schema (minLength 1) but is still blank; empty and
        # over-long strings are already reported by the schema mapping.
        if isinstance(instruction, str) and instruction and not instruction.strip():
            if len(instruction) <= MAX_INSTRUCTION_LENGTH:
                issues.append(
                    CheckIssue(CheckFailure.EMPTY_INSTRUCTION, f"job {label} has a blank instruction", jid)
                )

        for cap in _str_list(job.get("capabilities")):
            if cap not in advertised:
                issues.append(
                    CheckIssue(
                        CheckFailure.UNADVERTISED_CAPABILITY,
                        f"job {label} requires capability {cap!r}, which is not advertised "
                        f"(advertised: {sorted(advertised)})",
                        jid,
                    )
                )

        for dep in _str_list(job.get("depends_on")):
            if jid is not None and dep == jid:
                issues.append(
                    CheckIssue(CheckFailure.SELF_DEPENDENCY, f"job {label} depends on itself", jid)
                )
            elif dep not in known:
                issues.append(
                    CheckIssue(
                        CheckFailure.UNKNOWN_DEPENDENCY,
                        f"job {label} depends on unknown job_id {dep!r}",
                        jid,
                    )
                )
            elif jid is not None:
                graph[jid].add(dep)

    cycle = _find_cycle(graph)
    if cycle is not None:
        issues.append(
            CheckIssue(
                CheckFailure.CYCLE,
                "dependency cycle: " + " -> ".join(cycle + [cycle[0]]),
                cycle[0],
            )
        )
    return issues


def _find_cycle(graph: Mapping[str, set[str]]) -> list[str] | None:
    """Return one cycle (as a node list) in ``graph`` (edges job -> dependency), or ``None``.

    Iterative three-colour DFS; self loops must already be excluded from ``graph``.
    """
    white, grey, black = 0, 1, 2
    colour = dict.fromkeys(graph, white)
    for root in sorted(graph):
        if colour[root] != white:
            continue
        stack: list[tuple[str, list[str]]] = [(root, sorted(graph[root]))]
        path = [root]
        colour[root] = grey
        while stack:
            node, pending = stack[-1]
            if not pending:
                stack.pop()
                path.pop()
                colour[node] = black
                continue
            nxt = pending.pop()
            if colour[nxt] == grey:
                return path[path.index(nxt):]
            if colour[nxt] == white:
                colour[nxt] = grey
                path.append(nxt)
                stack.append((nxt, sorted(graph[nxt])))
    return None


def _dedupe(issues: list[CheckIssue]) -> list[CheckIssue]:
    seen: set[CheckIssue] = set()
    out: list[CheckIssue] = []
    for issue in issues:
        if issue not in seen:
            seen.add(issue)
            out.append(issue)
    return out


__all__ = [
    "MAX_MAX_JOBS",
    "MIN_MAX_JOBS",
    "CheckFailure",
    "CheckIssue",
    "PlanValidation",
    "check_for_issue",
    "validate_plan",
    "validate_plan_detailed",
    "validate_plan_text",
]
