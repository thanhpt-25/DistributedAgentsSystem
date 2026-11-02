"""Job_Plan models and the plan equivalence helper (Req 3.2, 3.8)."""

from __future__ import annotations

from pydantic import Field

from das.models.base import DasModel

JOB_ID_PATTERN = r"^[A-Za-z0-9_-]{1,64}$"
MAX_INSTRUCTION_LENGTH = 8000


class PlannedJob(DasModel):
    """One Job of a Job_Plan. ``capabilities`` and ``depends_on`` are compared as sets."""

    job_id: str = Field(pattern=JOB_ID_PATTERN)
    instruction: str = Field(min_length=1, max_length=MAX_INSTRUCTION_LENGTH)
    capabilities: list[str] = Field(min_length=1)
    depends_on: list[str] = Field(default_factory=list)


class JobPlan(DasModel):
    """A DAG of Jobs. Structural checks (unique ids, acyclicity, …) live in ``das.plan.validate``."""

    jobs: list[PlannedJob]

    def job_ids(self) -> list[str]:
        return [job.job_id for job in self.jobs]

    def by_id(self) -> dict[str, PlannedJob]:
        """Map Job_ID to Job. Assumes unique ids (a validated plan); later duplicates win."""
        return {job.job_id: job for job in self.jobs}


NormalizedJob = tuple[str, str, frozenset[str], frozenset[str]]


def normalize_plan(plan: JobPlan) -> list[NormalizedJob]:
    """Order- and duplicate-insensitive canonical form used for equivalence.

    Each Job becomes ``(job_id, instruction, capability set, dependency set)``; the list
    is sorted so Job order in the plan doesn't matter. Duplicate Job_IDs are preserved
    as separate entries so that two plans with different duplicates are not equivalent.
    """
    return sorted(
        (
            (job.job_id, job.instruction, frozenset(job.capabilities), frozenset(job.depends_on))
            for job in plan.jobs
        ),
        key=lambda t: (t[0], t[1], sorted(t[2]), sorted(t[3])),
    )


def plans_equivalent(a: JobPlan, b: JobPlan) -> bool:
    """True when both plans have the same Job_ID set and, per Job_ID, the same
    instruction, the same Capability set, and the same dependency set (Req 3.8)."""
    return normalize_plan(a) == normalize_plan(b)
