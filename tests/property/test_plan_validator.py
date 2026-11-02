# Feature: distributed-agent-system, Property 2: Plan validator matches reference oracle
"""Property 2: Plan validator matches reference oracle.

For any candidate plan, advertised Capability set, and ``max_jobs`` in 1..100,
``validate_plan`` returns exactly the set of failed checks computed by a simple
reference oracle, and the plan is valid iff that set is empty. Candidates are valid
``job_plans`` optionally mutated by ``invalid_plan_mutations``.

The oracle follows the semantics documented in ``das.plan.validate``: blank
instructions are ``EMPTY_INSTRUCTION``; a self-loop is ``SELF_DEPENDENCY`` only;
unknown dependencies are excluded from cycle detection; zero Jobs is
``JOB_COUNT_OUT_OF_RANGE``.

**Validates: Requirements 3.1, 3.2, 3.4**
"""

from __future__ import annotations

import re
from typing import Any

from hypothesis import given, note

from das.models.plan import MAX_INSTRUCTION_LENGTH
from das.plan.validate import CheckFailure as F
from das.plan.validate import validate_plan, validate_plan_detailed
from tests.strategies import invalid_plan_mutations

_JOB_ID_RE = re.compile(r"[A-Za-z0-9_-]{1,64}")


def _has_cycle(ids: set[str], edges: set[tuple[str, str]]) -> bool:
    """Peel off nodes with no remaining dependencies; leftovers lie on (or behind) a cycle."""
    remaining = set(ids)
    while True:
        free = {n for n in remaining if not any(a == n and b in remaining for a, b in edges)}
        if not free:
            return bool(remaining)
        remaining -= free


def oracle(plan: dict[str, Any], advertised: frozenset[str], max_jobs: int) -> set[F]:
    """Reference set of failed checks for a plan whose fields all have the right JSON types."""
    jobs = plan["jobs"]
    ids = [j["job_id"] for j in jobs]
    known = set(ids)
    failed: set[F] = set()

    if not 1 <= len(jobs) <= max_jobs:
        failed.add(F.JOB_COUNT_OUT_OF_RANGE)
    if len(ids) != len(known):
        failed.add(F.DUPLICATE_JOB_ID)

    edges: set[tuple[str, str]] = set()
    for job in jobs:
        jid = job["job_id"]
        if not _JOB_ID_RE.fullmatch(jid):
            failed.add(F.SCHEMA)
        instruction = job["instruction"]
        if len(instruction) > MAX_INSTRUCTION_LENGTH:
            failed.add(F.INSTRUCTION_TOO_LONG)
        elif instruction.strip() == "":
            failed.add(F.EMPTY_INSTRUCTION)
        if not job["capabilities"]:
            failed.add(F.NO_CAPABILITIES)
        if any(c not in advertised for c in job["capabilities"]):
            failed.add(F.UNADVERTISED_CAPABILITY)
        for dep in job["depends_on"]:
            if dep == jid:
                failed.add(F.SELF_DEPENDENCY)
            elif dep not in known:
                failed.add(F.UNKNOWN_DEPENDENCY)
            else:
                edges.add((jid, dep))

    if _has_cycle(known, edges):
        failed.add(F.CYCLE)
    return failed


@given(invalid_plan_mutations())
def test_validator_matches_oracle(case: tuple[Any, ...]) -> None:
    """**Validates: Requirements 3.1, 3.2, 3.4**"""
    plan, advertised, max_jobs, mutations = case
    note(f"mutations={mutations}")
    expected = oracle(plan, advertised, max_jobs)

    assert validate_plan(plan, advertised, max_jobs) == expected

    detailed = validate_plan_detailed(plan, advertised, max_jobs)
    assert detailed.failures == expected
    assert detailed.ok == (not expected)
    assert (detailed.plan is not None) == (not expected)
    if detailed.plan is not None:
        assert detailed.plan.model_dump(mode="json") == plan
