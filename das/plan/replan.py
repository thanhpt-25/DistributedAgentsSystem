"""Checks of a replacement plan (Phase 6 Req 3.3). Pure: no I/O.

A replan asks the Planner for a plan that keeps every Succeeded Job exactly as it was and replaces everything else.
``validate_plan_text`` already checks the plan as a plan (structure, dependencies, cycles, Job count). What a
replacement plan must satisfy on top of that is checked here, against the :class:`ReplanContext` of the Request:

* ``keeps_succeeded``: each Succeeded Job is in the plan once, with the same instruction, Capabilities, dependencies
  and Output_Schema (compared as the plan equivalence of Req 3.8 does: Capabilities and dependencies as sets).
* ``fresh_job_ids``: a new Job does not reuse the id of a Job the plan had. Results are keyed by ``(job_id, attempt)``,
  so a new ``b`` would meet the old ``b``'s results and its attempt 1 would be taken for a duplicate.
* ``new_job_required``: at least one Job is new; a plan that changes nothing would only loop.
* ``unadvertised_capability``: the new Jobs need Capabilities that are advertised now. A kept Job does not: it has
  already succeeded, and its Worker may be gone.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from das.models.plan import JobPlan, PlannedJob
from das.plan.validate import CheckFailure, CheckIssue


@dataclass(frozen=True, slots=True)
class SucceededJob:
    """A Job that succeeded, with its result (shown to the Planner, never stored again)."""

    job: PlannedJob
    result: str


@dataclass(frozen=True, slots=True)
class FailedJob:
    """A Job that failed for good, or was skipped because a Job it depends on failed."""

    job: PlannedJob
    error: str | None


@dataclass(frozen=True, slots=True)
class ReplanContext:
    """What a replan knows about the plan it replaces."""

    succeeded: tuple[SucceededJob, ...]
    failed: tuple[FailedJob, ...]

    @property
    def kept_ids(self) -> frozenset[str]:
        return frozenset(s.job.job_id for s in self.succeeded)

    @property
    def failed_ids(self) -> frozenset[str]:
        return frozenset(f.job.job_id for f in self.failed)

    @property
    def kept_capabilities(self) -> frozenset[str]:
        return frozenset(cap for s in self.succeeded for cap in s.job.capabilities)


def _shape(job: PlannedJob) -> tuple[str, frozenset[str], frozenset[str], str | None]:
    schema = json.dumps(job.output_schema, sort_keys=True) if job.output_schema is not None else None
    return (job.instruction, frozenset(job.capabilities), frozenset(job.depends_on), schema)


def job_definition(job: PlannedJob) -> dict[str, Any]:
    """A Job as the plan JSON writes it, for showing the Planner what to copy."""
    definition: dict[str, Any] = {
        "job_id": job.job_id,
        "instruction": job.instruction,
        "capabilities": list(job.capabilities),
        "depends_on": list(job.depends_on),
    }
    if job.output_schema is not None:
        definition["output_schema"] = job.output_schema
    return definition


def replan_issues(plan: JobPlan, context: ReplanContext, advertised: Iterable[str]) -> list[CheckIssue]:
    """Every way ``plan`` fails as a replacement of the plan ``context`` describes; empty means it is one."""
    live = frozenset(advertised)
    issues: list[CheckIssue] = []

    for kept in context.succeeded:
        original = kept.job
        mine = [job for job in plan.jobs if job.job_id == original.job_id]
        copy = json.dumps(job_definition(original), ensure_ascii=False)
        if not mine:
            message = f"succeeded job {original.job_id!r} is missing; keep it exactly as {copy}"
        elif len(mine) > 1:
            message = f"succeeded job {original.job_id!r} is listed {len(mine)} times; keep it once, exactly as {copy}"
        elif _shape(mine[0]) != _shape(original):
            message = f"succeeded job {original.job_id!r} was changed; it must stay exactly as {copy}"
        else:
            continue
        issues.append(CheckIssue(CheckFailure.KEEPS_SUCCEEDED, message, original.job_id))

    new_jobs = [job for job in plan.jobs if job.job_id not in context.kept_ids]
    for job in new_jobs:
        if job.job_id in context.failed_ids:
            issues.append(
                CheckIssue(
                    CheckFailure.FRESH_JOB_IDS,
                    f"job_id {job.job_id!r} belongs to a job that failed; give each new job an id not used before",
                    job.job_id,
                )
            )
        for capability in job.capabilities:
            if capability not in live:
                issues.append(
                    CheckIssue(
                        CheckFailure.UNADVERTISED_CAPABILITY,
                        f"new job {job.job_id!r} requires capability {capability!r}, which is not advertised "
                        f"(advertised: {sorted(live)})",
                        job.job_id,
                    )
                )
    if not new_jobs:
        issues.append(
            CheckIssue(CheckFailure.NEW_JOB_REQUIRED, "the plan has no new job; replace the failed jobs with new ones")
        )
    return issues


__all__ = ["FailedJob", "ReplanContext", "SucceededJob", "job_definition", "replan_issues"]
