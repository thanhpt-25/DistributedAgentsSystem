"""Manager-side decisions of a replan (Phase 6 Req 3.2, 3.3). Pure: the driver does the I/O.

``replan_wanted`` says whether to replan instead of finishing the Request; ``build_context`` turns a snapshot's Jobs and
results into what the Planner is told. Whether a replacement plan is acceptable is ``das.plan.replan``'s concern.
"""

from __future__ import annotations

from collections.abc import Mapping

from das.models.job import JobRecord, JobState, is_terminal_job
from das.models.results import JobResultPayload
from das.plan.replan import FailedJob, ReplanContext, SucceededJob

#: Jobs that did not do their work: they are replaced by a replan.
_UNFINISHED_WORK = frozenset({JobState.FAILED, JobState.SKIPPED})


def replan_wanted(
    budget_kind: str | None,
    jobs: Mapping[str, JobRecord],
    replans: int,
    max_replans: int,
) -> bool:
    """Replan instead of finishing when every Job is terminal and some Failed or were Skipped (Req 3.2).

    Never at ``max_replans`` 0, never once ``replans`` reached ``max_replans``, and never for a budget stop
    (``budget_kind`` set): a budget bounds the whole Request, replans included (Req 3.8).
    """
    if max_replans <= 0 or replans >= max_replans or budget_kind is not None:
        return False
    if not all(is_terminal_job(record.state) for record in jobs.values()):
        return False
    return any(record.state in _UNFINISHED_WORK for record in jobs.values())


def build_context(
    jobs: Mapping[str, JobRecord], results: Mapping[tuple[str, int], JobResultPayload]
) -> ReplanContext:
    """What the Planner is told: each Succeeded Job with the result of the attempt that succeeded, and each Failed or
    Skipped Job with the error of its last attempt (``None`` for a Job that never ran). Plan order."""
    succeeded: list[SucceededJob] = []
    failed: list[FailedJob] = []
    for job_id, record in jobs.items():
        last = results.get((job_id, record.attempt))
        if record.state is JobState.SUCCEEDED:
            succeeded.append(SucceededJob(record.job, last.content if last is not None else ""))
        elif record.state in _UNFINISHED_WORK:
            failed.append(FailedJob(record.job, last.error if last is not None else None))
    return ReplanContext(succeeded=tuple(succeeded), failed=tuple(failed))


__all__ = ["build_context", "replan_wanted"]
