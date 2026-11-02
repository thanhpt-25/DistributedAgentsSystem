"""Pure DAG scheduling helpers over a Job_Plan (Req 4.3, 4.4, 7.1).

All functions are pure and tolerate unvalidated plans where practical:

- Dependencies on Job_IDs not in the plan are ignored by ``topo_order`` and
  ``transitive_dependents`` (``validate_plan`` reports them separately), but they block
  readiness in ``ready_jobs`` because such a dependency can never be Succeeded.
- Duplicate Job_IDs are collapsed to the first occurrence's position; edges from every
  occurrence are kept.
"""

from __future__ import annotations

import heapq
from collections.abc import Mapping

from das.models.job import JobState, is_terminal_job
from das.models.plan import JobPlan

__all__ = [
    "CycleError",
    "topo_order",
    "ready_jobs",
    "transitive_dependents",
    "all_terminal",
]


class CycleError(ValueError):
    """The plan's dependency graph contains a cycle.

    ``cycle`` lists the Job_IDs of one cycle in dependency order, closed by repeating
    the first id (e.g. ``["a", "b", "a"]`` means a depends on b and b depends on a).
    A self-dependency yields ``["a", "a"]``.
    """

    def __init__(self, cycle: list[str]) -> None:
        self.cycle = list(cycle)
        super().__init__("dependency cycle: " + " -> ".join(self.cycle))


def _graph(plan: JobPlan) -> tuple[list[str], dict[str, set[str]]]:
    """Return unique Job_IDs in first-appearance order and id -> known dependency ids."""
    order: list[str] = []
    deps: dict[str, set[str]] = {}
    for job in plan.jobs:
        if job.job_id not in deps:
            order.append(job.job_id)
            deps[job.job_id] = set()
        deps[job.job_id].update(job.depends_on)
    for job_id, ds in deps.items():
        deps[job_id] = {d for d in ds if d in deps}
    return order, deps


def _find_cycle(remaining: set[str], deps: Mapping[str, set[str]], order: list[str]) -> list[str]:
    """Find one cycle among ``remaining`` nodes (each of which has an unresolved dependency)."""
    # Every remaining node has at least one dependency inside ``remaining``; walking those
    # edges must eventually revisit a node.
    start = next(j for j in order if j in remaining)
    path: list[str] = []
    seen: dict[str, int] = {}
    node = start
    while node not in seen:
        seen[node] = len(path)
        path.append(node)
        node = min(d for d in deps[node] if d in remaining)
    return path[seen[node]:] + [node]


def topo_order(plan: JobPlan) -> list[str]:
    """Job_IDs ordered so every Job comes after all of its dependencies.

    Deterministic: among Jobs whose dependencies are all placed, the one appearing
    earliest in ``plan.jobs`` goes first. Raises ``CycleError`` on a cycle.
    """
    order, deps = _graph(plan)
    index = {job_id: i for i, job_id in enumerate(order)}
    dependents: dict[str, list[str]] = {j: [] for j in order}
    pending = {j: len(ds) for j, ds in deps.items()}
    for job_id, ds in deps.items():
        for d in ds:
            dependents[d].append(job_id)

    heap = [index[j] for j in order if pending[j] == 0]
    heapq.heapify(heap)
    result: list[str] = []
    while heap:
        job_id = order[heapq.heappop(heap)]
        result.append(job_id)
        for child in dependents[job_id]:
            pending[child] -= 1
            if pending[child] == 0:
                heapq.heappush(heap, index[child])

    if len(result) != len(order):
        remaining = set(order) - set(result)
        raise CycleError(_find_cycle(remaining, deps, order))
    return result


def ready_jobs(plan: JobPlan, states: Mapping[str, JobState]) -> set[str]:
    """Waiting Jobs whose dependencies are all Succeeded (Req 4.3).

    A Job absent from ``states`` is not Waiting, so it is never returned. A dependency
    absent from ``states`` (or unknown to the plan) counts as not Succeeded.
    """
    return {
        job.job_id
        for job in plan.jobs
        if states.get(job.job_id) == JobState.WAITING
        and all(states.get(d) == JobState.SUCCEEDED for d in job.depends_on)
    }


def transitive_dependents(plan: JobPlan, job_id: str) -> set[str]:
    """Every Job that directly or transitively depends on ``job_id``, excluding ``job_id``
    itself even when it lies on a cycle (Req 4.4)."""
    _, deps = _graph(plan)
    dependents: dict[str, set[str]] = {j: set() for j in deps}
    for child, ds in deps.items():
        for d in ds:
            dependents[d].add(child)

    found: set[str] = set()
    stack = list(dependents.get(job_id, ()))
    while stack:
        node = stack.pop()
        if node in found or node == job_id:
            continue
        found.add(node)
        stack.extend(dependents[node])
    return found


def all_terminal(states: Mapping[str, JobState]) -> bool:
    """True when every Job is Succeeded, Failed, Skipped, or Cancelled (Req 7.1).

    TimedOut is not terminal. An empty mapping is vacuously terminal; callers decide
    whether an empty plan is meaningful (a valid plan always has ≥ 1 Job).
    """
    return all(is_terminal_job(state) for state in states.values())
