"""Scoring and the report, for all inputs (das-smarter-agents task 4.1; Phase 6 Req 4.5, 4.3, 4.4).

An oracle restates the expectations independently of ``das.eval.plans``; the report's invariants hold whatever the
scores."""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from das.eval.plans import Case, CaseScore, Expect, score, summarize
from das.manager.planner import PlanAccepted
from das.models.plan import JobPlan, PlannedJob
from das.models.results import TokenUsage

CAPS = ["search", "write", "paint", "review"]


@st.composite
def plans(draw: st.DrawFn) -> JobPlan:
    jobs = []
    for i in range(draw(st.integers(1, 6))):
        jobs.append(
            PlannedJob(
                job_id=f"j{i}", instruction="x",
                capabilities=draw(st.lists(st.sampled_from(CAPS), min_size=1, max_size=3, unique=True)),
                output_schema=draw(st.sampled_from([None, None, {"type": "object"}])),
            )
        )
    return JobPlan(jobs=jobs)


@st.composite
def expectations(draw: st.DrawFn) -> Expect:
    low = draw(st.one_of(st.none(), st.integers(1, 6)))
    high = draw(st.one_of(st.none(), st.integers(low or 1, 8)))
    required = draw(st.sets(st.sampled_from(CAPS), max_size=2))
    forbidden = draw(st.sets(st.sampled_from([c for c in CAPS if c not in required]), max_size=2))
    return Expect(low, high, frozenset(required), frozenset(forbidden), draw(st.booleans()))


def oracle(plan: JobPlan, expect: Expect) -> bool:
    used = {cap for job in plan.jobs for cap in job.capabilities}
    n = len(plan.jobs)
    return (
        (expect.min_jobs is None or n >= expect.min_jobs)
        and (expect.max_jobs is None or n <= expect.max_jobs)
        and expect.require_capabilities <= used
        and not (expect.forbid_capabilities & used)
        and (not expect.require_output_schema or any(j.output_schema is not None for j in plan.jobs))
    )


def accepted(plan: JobPlan) -> PlanAccepted:
    return PlanAccepted(plan, 1, TokenUsage(), ())


@given(plan=plans(), expect=expectations())
@settings(deadline=None)
def test_a_valid_plan_meets_its_expectations_exactly_when_the_oracle_says_so(plan: JobPlan, expect: Expect) -> None:
    result = score(Case(1, "p", frozenset(CAPS), expect), accepted(plan))
    assert result.valid
    assert result.expectations_met == oracle(plan, expect)
    # A failure is listed for every unmet expectation, and only then.
    assert result.expectations_met == (not result.failures)


@given(plan=plans(), expect=expectations(), extra=expectations())
@settings(deadline=None)
def test_a_stricter_expectation_never_makes_an_unmet_case_met(
    plan: JobPlan, expect: Expect, extra: Expect
) -> None:
    both = Expect(
        max(filter(None, (expect.min_jobs, extra.min_jobs)), default=None),
        min(filter(None, (expect.max_jobs, extra.max_jobs)), default=None),
        expect.require_capabilities | extra.require_capabilities,
        expect.forbid_capabilities | extra.forbid_capabilities,
        expect.require_output_schema or extra.require_output_schema,
    )
    if oracle(plan, both):
        assert oracle(plan, expect) and oracle(plan, extra)


scores = st.builds(
    CaseScore,
    line=st.integers(1, 1000),
    valid=st.booleans(),
    attempts=st.integers(0, 6),
    tokens=st.integers(0, 10_000),
    expectations_met=st.booleans(),
    failures=st.just(()),
).filter(lambda s: s.valid or not s.expectations_met)  # an invalid plan can never meet its expectations


@given(items=st.lists(scores, min_size=1, max_size=30))
@settings(deadline=None)
def test_the_report_totals_are_consistent(items: list[CaseScore]) -> None:
    totals = summarize(items).totals
    assert totals["cases"] == len(items)
    assert 0.0 <= totals["expectation_ratio"] <= totals["valid_ratio"] <= 1.0
    assert totals["total_tokens"] == sum(s.tokens for s in items)
    assert min(s.attempts for s in items) <= totals["mean_attempts"] <= max(s.attempts for s in items)


@given(items=st.lists(scores, min_size=1, max_size=20), low=st.floats(0, 1), high=st.floats(0, 1))
@settings(deadline=None)
def test_passing_is_monotone_in_the_threshold(items: list[CaseScore], low: float, high: float) -> None:
    report = summarize(items)
    low, high = min(low, high), max(low, high)
    assert report.passes(0.0)  # no threshold never fails
    if report.passes(high):
        assert report.passes(low)
