"""Property 2: for all plans and replacement plans, a replan keeps every Succeeded Job unchanged and the resulting plan
is a valid Job_Plan (das-smarter-agents task 3.3; Phase 6 Req 8.2, 3.3).

The oracle below restates the rules independently of ``das.plan.replan``. Two properties follow: the checks agree with
the oracle on arbitrary (often broken) candidates, and a replacement built the right way always passes the replan
checks *and* the ordinary plan validator."""

from __future__ import annotations

from typing import Any

from hypothesis import given, settings
from hypothesis import strategies as st

from das.models.plan import JobPlan, PlannedJob
from das.plan.replan import FailedJob, ReplanContext, SucceededJob, replan_issues
from das.plan.validate import validate_plan

CAPS = ["research", "summarize", "write"]
ADVERTISED = frozenset(CAPS)
MAX_JOBS = 100
SCHEMAS = [None, None, {"type": "object"}]


@st.composite
def histories(draw: st.DrawFn) -> tuple[list[PlannedJob], ReplanContext]:
    """A plan with some jobs Succeeded (never one whose dependency did not) and the rest failed."""
    size = draw(st.integers(1, 6))
    jobs: list[PlannedJob] = []
    for i in range(size):
        earlier = [j.job_id for j in jobs]
        deps = draw(st.lists(st.sampled_from(earlier), unique=True, max_size=3)) if earlier else []
        jobs.append(
            PlannedJob(
                job_id=f"j{i}",
                instruction=draw(st.from_regex(r"[a-z0-9]{1,8}", fullmatch=True)),  # never blank: a valid instruction
                capabilities=draw(st.lists(st.sampled_from(CAPS), min_size=1, max_size=2, unique=True)),
                depends_on=deps,
                output_schema=draw(st.sampled_from(SCHEMAS)),
            )
        )
    done: set[str] = set()
    for job in jobs:
        if set(job.depends_on) <= done and draw(st.booleans()):
            done.add(job.job_id)
    succeeded = tuple(SucceededJob(j, "result") for j in jobs if j.job_id in done)
    failed = tuple(FailedJob(j, "boom") for j in jobs if j.job_id not in done)
    return jobs, ReplanContext(succeeded, failed)


def shape(job: PlannedJob) -> tuple[Any, ...]:
    return (job.instruction, frozenset(job.capabilities), frozenset(job.depends_on), repr(job.output_schema))


def oracle(plan: JobPlan, ctx: ReplanContext, advertised: frozenset[str]) -> bool:
    """The rules of Req 3.3, restated."""
    by_id: dict[str, list[PlannedJob]] = {}
    for job in plan.jobs:
        by_id.setdefault(job.job_id, []).append(job)
    for kept in ctx.succeeded:
        listed = by_id.get(kept.job.job_id, [])
        if len(listed) != 1 or shape(listed[0]) != shape(kept.job):
            return False
    new = [j for j in plan.jobs if j.job_id not in ctx.kept_ids]
    return (
        len(new) >= 1
        and all(j.job_id not in ctx.failed_ids for j in new)
        and all(set(j.capabilities) <= advertised for j in new)
    )


@st.composite
def candidates(draw: st.DrawFn) -> tuple[JobPlan, ReplanContext]:
    """A replacement that is right, or broken in one of the ways a model breaks it."""
    _, ctx = draw(histories())
    out: list[PlannedJob] = []
    for kept in ctx.succeeded:
        job = kept.job
        move = draw(st.sampled_from(["keep", "keep", "keep", "drop", "edit", "twice"]))
        if move == "drop":
            continue
        if move == "edit":
            job = job.model_copy(update={"instruction": job.instruction + "!"})
        out.append(job)
        if move == "twice":
            out.append(job)
    ids = [j.job_id for j in out]
    failed_ids = [f.job.job_id for f in ctx.failed]
    for i in range(draw(st.integers(0, 3))):
        name = draw(st.sampled_from([f"n{i}", f"n{i}", f"n{i}", *failed_ids]))  # sometimes reuses a failed id
        if name in ids:
            continue
        caps = draw(st.lists(st.sampled_from([*CAPS, "translate"]), min_size=1, max_size=2, unique=True))
        deps = draw(st.lists(st.sampled_from(ids), unique=True, max_size=2)) if ids else []
        out.append(PlannedJob(job_id=name, instruction="new work", capabilities=caps, depends_on=deps))
        ids.append(name)
    return JobPlan(jobs=list(draw(st.permutations(out)))), ctx


@given(candidate=candidates())
@settings(deadline=None)
def test_the_checks_agree_with_the_rules_on_any_candidate(candidate: tuple[JobPlan, ReplanContext]) -> None:
    plan, ctx = candidate
    assert (replan_issues(plan, ctx, ADVERTISED) == []) == oracle(plan, ctx, ADVERTISED)


@st.composite
def good_replacements(draw: st.DrawFn) -> tuple[JobPlan, ReplanContext]:
    """Kept jobs untouched, plus one to three new jobs with fresh ids, advertised Capabilities and dependencies only
    on kept jobs or on earlier new jobs."""
    _, ctx = draw(histories())
    jobs = [s.job for s in ctx.succeeded]
    known = [j.job_id for j in jobs]
    for i in range(draw(st.integers(1, 3))):
        deps = draw(st.lists(st.sampled_from(known), unique=True, max_size=2)) if known else []
        caps = draw(st.lists(st.sampled_from(CAPS), min_size=1, max_size=2, unique=True))
        jobs.append(PlannedJob(job_id=f"n{i}", instruction="new work", capabilities=caps, depends_on=deps))
        known.append(f"n{i}")
    return JobPlan(jobs=list(draw(st.permutations(jobs)))), ctx


@given(replacement=good_replacements())
@settings(deadline=None)
def test_a_replacement_built_the_right_way_is_accepted_and_is_a_valid_plan(
    replacement: tuple[JobPlan, ReplanContext],
) -> None:
    plan, ctx = replacement
    assert replan_issues(plan, ctx, ADVERTISED) == []
    # Valid as a plan: structure, dependencies, no cycle. The kept jobs' Capabilities need not be advertised.
    assert validate_plan(plan, ADVERTISED | ctx.kept_capabilities, MAX_JOBS) == set()


@given(replacement=good_replacements())
@settings(deadline=None)
def test_every_succeeded_job_is_in_an_accepted_replacement_unchanged(
    replacement: tuple[JobPlan, ReplanContext],
) -> None:
    plan, ctx = replacement
    assert replan_issues(plan, ctx, ADVERTISED) == []
    by_id = {j.job_id: j for j in plan.jobs}
    for kept in ctx.succeeded:
        assert by_id[kept.job.job_id] == kept.job
    assert not set(by_id) & ctx.failed_ids  # and nothing of the failed part survives
