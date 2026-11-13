"""The pure checks of a replacement plan (das-smarter-agents task 3.3; Phase 6 Req 3.3): every Succeeded Job stays
exactly as it was, the new Jobs have fresh ids and advertised Capabilities, and at least one Job is new."""

from __future__ import annotations

from das.models.plan import JobPlan, PlannedJob
from das.plan.replan import FailedJob, ReplanContext, SucceededJob, replan_issues
from das.plan.validate import CheckFailure

SCHEMA = {"type": "object", "properties": {"n": {"type": "integer"}}}
A = PlannedJob(job_id="a", instruction="research it", capabilities=["research"])
B = PlannedJob(job_id="b", instruction="summarize it", capabilities=["summarize"], depends_on=["a"])
CTX = ReplanContext(succeeded=(SucceededJob(A, "facts"),), failed=(FailedJob(B, "boom"),))
ADVERTISED = {"research", "summarize"}


def job(job_id: str, caps: tuple[str, ...] = ("research",), deps: tuple[str, ...] = ("a",)) -> PlannedJob:
    return PlannedJob(job_id=job_id, instruction=f"do {job_id}", capabilities=list(caps), depends_on=list(deps))


def checks(plan: JobPlan, ctx: ReplanContext = CTX, advertised=ADVERTISED) -> set[CheckFailure]:  # noqa: ANN001
    return {issue.check for issue in replan_issues(plan, ctx, advertised)}


# --------------------------------------------------------------------------- a good replacement


def test_a_plan_that_keeps_the_succeeded_job_and_adds_a_fresh_one_is_fine() -> None:
    assert replan_issues(JobPlan(jobs=[A, job("c")]), CTX, ADVERTISED) == []


def test_job_order_does_not_matter() -> None:
    assert replan_issues(JobPlan(jobs=[job("c"), A]), CTX, ADVERTISED) == []


def test_the_order_of_capabilities_and_dependencies_does_not_matter() -> None:
    two = PlannedJob(job_id="a", instruction="research it", capabilities=["x", "research"], depends_on=["q", "r"])
    ctx = ReplanContext(succeeded=(SucceededJob(two, "r"),), failed=())
    shuffled = PlannedJob(job_id="a", instruction="research it", capabilities=["research", "x"], depends_on=["r", "q"])
    assert replan_issues(JobPlan(jobs=[shuffled, job("c")]), ctx, {"research"}) == []


def test_a_new_job_may_depend_on_a_kept_job_and_on_another_new_one() -> None:
    plan = JobPlan(jobs=[A, job("c"), job("d", deps=("a", "c"))])
    assert replan_issues(plan, CTX, ADVERTISED) == []


# --------------------------------------------------------------------------- keeps_succeeded (Req 3.3)


def test_dropping_a_succeeded_job_is_refused_and_named() -> None:
    issues = replan_issues(JobPlan(jobs=[job("c", deps=())]), CTX, ADVERTISED)
    assert {i.check for i in issues} == {CheckFailure.KEEPS_SUCCEEDED}
    assert issues[0].job_id == "a" and "'a'" in issues[0].message


def test_changing_the_instruction_of_a_succeeded_job_is_refused() -> None:
    changed = PlannedJob(job_id="a", instruction="research something else", capabilities=["research"])
    assert checks(JobPlan(jobs=[changed, job("c")])) == {CheckFailure.KEEPS_SUCCEEDED}


def test_changing_the_capabilities_of_a_succeeded_job_is_refused() -> None:
    changed = PlannedJob(job_id="a", instruction="research it", capabilities=["summarize"])
    assert checks(JobPlan(jobs=[changed, job("c")])) == {CheckFailure.KEEPS_SUCCEEDED}


def test_changing_the_dependencies_of_a_succeeded_job_is_refused() -> None:
    changed = PlannedJob(job_id="a", instruction="research it", capabilities=["research"], depends_on=["z"])
    assert checks(JobPlan(jobs=[changed, job("c")])) == {CheckFailure.KEEPS_SUCCEEDED}


def test_changing_the_output_schema_of_a_succeeded_job_is_refused() -> None:
    with_schema = PlannedJob(job_id="a", instruction="research it", capabilities=["research"], output_schema=SCHEMA)
    assert checks(JobPlan(jobs=[with_schema, job("c")])) == {CheckFailure.KEEPS_SUCCEEDED}


def test_the_refusal_shows_the_stored_job_so_the_model_can_copy_it() -> None:
    changed = PlannedJob(job_id="a", instruction="research something else", capabilities=["research"])
    [issue] = replan_issues(JobPlan(jobs=[changed, job("c")]), CTX, ADVERTISED)
    assert "research it" in issue.message  # the original instruction, verbatim


def test_a_succeeded_job_listed_twice_is_refused() -> None:
    assert CheckFailure.KEEPS_SUCCEEDED in checks(JobPlan(jobs=[A, A, job("c")]))


# --------------------------------------------------------------------------- fresh ids (the result-key hazard)


def test_a_new_job_under_the_id_of_a_failed_job_is_refused() -> None:
    reused = PlannedJob(job_id="b", instruction="try again", capabilities=["summarize"], depends_on=["a"])
    assert checks(JobPlan(jobs=[A, reused])) == {CheckFailure.FRESH_JOB_IDS}


def test_the_failed_jobs_id_is_named() -> None:
    reused = PlannedJob(job_id="b", instruction="try again", capabilities=["summarize"], depends_on=["a"])
    [issue] = replan_issues(JobPlan(jobs=[A, reused]), CTX, ADVERTISED)
    assert issue.job_id == "b"


# --------------------------------------------------------------------------- something new, and runnable


def test_a_plan_with_no_new_job_is_refused() -> None:
    assert checks(JobPlan(jobs=[A])) == {CheckFailure.NEW_JOB_REQUIRED}


def test_a_new_job_needs_an_advertised_capability() -> None:
    assert checks(JobPlan(jobs=[A, job("c", caps=("translate",))])) == {CheckFailure.UNADVERTISED_CAPABILITY}


def test_a_kept_job_does_not_need_its_capability_to_be_advertised_any_more() -> None:
    """Its Worker may be gone: the Job already succeeded, so only the new Jobs must be runnable now."""
    assert replan_issues(JobPlan(jobs=[A, job("c", caps=("summarize",))]), CTX, {"summarize"}) == []


def test_every_problem_is_reported_at_once() -> None:
    plan = JobPlan(jobs=[PlannedJob(job_id="b", instruction="again", capabilities=["translate"])])
    assert checks(plan) == {
        CheckFailure.KEEPS_SUCCEEDED, CheckFailure.FRESH_JOB_IDS, CheckFailure.UNADVERTISED_CAPABILITY,
    }


# --------------------------------------------------------------------------- the context


def test_the_context_knows_its_ids_and_capabilities() -> None:
    assert CTX.kept_ids == frozenset({"a"}) and CTX.failed_ids == frozenset({"b"})
    assert CTX.kept_capabilities == frozenset({"research"})
    assert ReplanContext(succeeded=(), failed=()).kept_capabilities == frozenset()
