"""Plan evaluation: the dataset, the scoring and the report (das-smarter-agents task 4.1; Phase 6 Req 4.2 to 4.5).

Pure: scoring is a function of the Planner's outcome and the expectations, so none of it needs an LLM."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from das.eval.plans import Case, DatasetError, Expect, load_dataset, parse_case, score, summarize
from das.llm.errors import LLMCallFailed
from das.manager.planner import PlanAccepted, PlanLLMFailed, PlanRejected
from das.models.plan import JobPlan, PlannedJob
from das.models.results import TokenUsage
from das.models.usage import UsageRecord
from das.plan.validate import CheckFailure, CheckIssue


def line(**fields: object) -> str:
    return json.dumps({"prompt": "research it", "capabilities": ["search", "write"], **fields})


def usage(total: int) -> UsageRecord:
    return UsageRecord(
        operation="plan", provider="p", model="m",
        usage=TokenUsage(input_tokens=total - 1, output_tokens=1, total_tokens=total),
    )


def plan(*jobs: PlannedJob) -> JobPlan:
    return JobPlan(jobs=list(jobs))


A = PlannedJob(job_id="a", instruction="find", capabilities=["search"])
B = PlannedJob(job_id="b", instruction="write", capabilities=["write"], depends_on=["a"])


def accepted(p: JobPlan, attempts: int = 1, *tokens: int) -> PlanAccepted:
    records = tuple(usage(t) for t in (tokens or (10,)))
    return PlanAccepted(p, attempts, TokenUsage(), records)


def rejected(attempts: int = 3, *tokens: int) -> PlanRejected:
    issue = CheckIssue(CheckFailure.CYCLE, "cycle")
    return PlanRejected((issue,), attempts, TokenUsage(), tuple(usage(t) for t in (tokens or (5, 5, 5))))


def case(**expect: object) -> Case:
    return parse_case(line(expect=expect) if expect else line(), 1)


# --------------------------------------------------------------------------- reading a line (Req 4.2)


def test_a_line_with_only_a_prompt_and_capabilities_is_a_case_without_expectations() -> None:
    parsed = parse_case(line(), 7)
    assert (parsed.line, parsed.prompt, parsed.capabilities) == (7, "research it", frozenset({"search", "write"}))
    assert parsed.expect == Expect()


def test_every_expectation_is_read() -> None:
    parsed = case(min_jobs=2, max_jobs=4, require_capabilities=["write"], forbid_capabilities=["paint"],
                  require_output_schema=True)
    assert parsed.expect == Expect(2, 4, frozenset({"write"}), frozenset({"paint"}), True)


def bad(**fields: object) -> str:
    return json.dumps({"prompt": "p", "capabilities": ["a"], **fields})


@pytest.mark.parametrize(
    ("text", "fragment"),
    [
        ("not json", "JSON"),
        ("[1]", "object"),
        (json.dumps({"capabilities": ["a"]}), "prompt"),
        (json.dumps({"prompt": "", "capabilities": ["a"]}), "prompt"),
        (json.dumps({"prompt": 3, "capabilities": ["a"]}), "prompt"),
        (json.dumps({"prompt": "p"}), "capabilities"),
        (bad(capabilities=[]), "capabilities"),
        (bad(capabilities="search"), "capabilities"),
        (bad(capabilities=["not valid!"]), "capabilities"),
        (bad(capabilities=["a", "a"]), "capabilities"),
        (bad(extra=1), "extra"),
        (bad(expect=[]), "expect"),
        (bad(expect={"min_job": 1}), "min_job"),
        (bad(expect={"min_jobs": 0}), "min_jobs"),
        (bad(expect={"min_jobs": "2"}), "min_jobs"),
        (bad(expect={"min_jobs": True}), "min_jobs"),
        (bad(expect={"min_jobs": 3, "max_jobs": 2}), "max_jobs"),
        (bad(expect={"require_capabilities": "a"}), "require_capabilities"),
        (bad(expect={"require_output_schema": "yes"}), "require_output_schema"),
        (bad(expect={"require_capabilities": ["a"], "forbid_capabilities": ["a"]}), "both"),
    ],
)
def test_a_bad_line_is_refused_naming_its_line_number_and_the_problem(text: str, fragment: str) -> None:
    with pytest.raises(DatasetError) as info:
        parse_case(text, 12)
    assert info.value.line == 12 and str(info.value).startswith("line 12: ")
    assert fragment in str(info.value)


def test_a_prompt_over_the_planning_limit_is_refused() -> None:
    with pytest.raises(DatasetError, match="prompt"):
        parse_case(json.dumps({"prompt": "x" * 32_001, "capabilities": ["a"]}), 1)


# --------------------------------------------------------------------------- the file


def write(tmp_path: Path, *lines: str) -> Path:
    path = tmp_path / "cases.jsonl"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def test_a_dataset_is_read_in_order_with_blank_lines_skipped_and_numbered_correctly(tmp_path: Path) -> None:
    cases = load_dataset(write(tmp_path, line(prompt="one"), "", "   ", line(prompt="two")))
    assert [(c.line, c.prompt) for c in cases] == [(1, "one"), (4, "two")]  # a case keeps its real line number


def test_the_first_bad_line_is_reported_with_its_number(tmp_path: Path) -> None:
    with pytest.raises(DatasetError) as info:
        load_dataset(write(tmp_path, line(), "", "{broken", line()))
    assert info.value.line == 3


def test_an_empty_dataset_is_refused(tmp_path: Path) -> None:
    with pytest.raises(DatasetError, match="no cases"):
        load_dataset(write(tmp_path, "", "  "))


def test_a_missing_file_is_a_dataset_error_naming_the_path(tmp_path: Path) -> None:
    with pytest.raises(DatasetError, match="cases.jsonl"):
        load_dataset(tmp_path / "cases.jsonl")


def test_a_file_that_is_not_text_is_a_dataset_error(tmp_path: Path) -> None:
    path = tmp_path / "cases.jsonl"
    path.write_bytes(b"\xff\xfe\x00not utf8")
    with pytest.raises(DatasetError):
        load_dataset(path)


# --------------------------------------------------------------------------- scoring (Req 4.3, 4.5)


def test_a_valid_plan_without_expectations_scores_as_met() -> None:
    result = score(case(), accepted(plan(A, B), 2, 10, 20))
    assert (result.valid, result.attempts, result.tokens, result.expectations_met, result.failures) == (
        True, 2, 30, True, (),
    )


def test_a_rejected_plan_is_invalid_and_its_expectations_cannot_be_met() -> None:
    result = score(case(), rejected(3, 5, 6, 7))
    assert (result.valid, result.attempts, result.tokens, result.expectations_met) == (False, 3, 18, False)
    assert result.failures and "cycle" in result.failures[0]


def test_a_failing_provider_is_invalid_and_counts_the_attempts_made() -> None:
    error = LLMCallFailed("p", "m", "down", 1)
    result = score(case(), PlanLLMFailed(error, (usage(4),)))
    assert (result.valid, result.attempts, result.tokens, result.expectations_met) == (False, 2, 4, False)
    assert "down" in result.failures[0]


WITH_SCHEMA = PlannedJob(job_id="s", instruction="x", capabilities=["search"], output_schema={"type": "object"})


@pytest.mark.parametrize(
    ("expect", "jobs", "met"),
    [
        ({"min_jobs": 2}, (A, B), True),
        ({"min_jobs": 2}, (A,), False),
        ({"max_jobs": 1}, (A,), True),
        ({"max_jobs": 1}, (A, B), False),
        ({"min_jobs": 2, "max_jobs": 2}, (A, B), True),
        ({"require_capabilities": ["write"]}, (A, B), True),
        ({"require_capabilities": ["write"]}, (A,), False),
        ({"require_capabilities": ["search", "write"]}, (A, B), True),
        ({"forbid_capabilities": ["write"]}, (A,), True),
        ({"forbid_capabilities": ["write"]}, (A, B), False),
        ({"require_output_schema": True}, (A, B), False),
        ({"require_output_schema": True}, (A, WITH_SCHEMA), True),
        ({"require_output_schema": False}, (A,), True),
    ],
)
def test_each_expectation_is_checked_against_the_plan(expect: dict, jobs: tuple, met: bool) -> None:
    assert score(case(**expect), accepted(plan(*jobs))).expectations_met is met


def test_every_unmet_expectation_is_listed_by_name() -> None:
    result = score(case(min_jobs=3, forbid_capabilities=["search"], require_capabilities=["paint"]),
                   accepted(plan(A, B)))
    assert not result.expectations_met and len(result.failures) == 3
    names = " ".join(result.failures)
    assert "min_jobs" in names and "forbid_capabilities" in names and "require_capabilities" in names


def test_a_case_that_meets_all_but_one_expectation_is_not_met() -> None:
    assert not score(case(min_jobs=1, require_capabilities=["paint"]), accepted(plan(A))).expectations_met


def test_scoring_is_deterministic() -> None:
    outcome = accepted(plan(A, B), 1, 7)
    assert score(case(min_jobs=1), outcome) == score(case(min_jobs=1), outcome)


# --------------------------------------------------------------------------- the report (Req 4.4)


def scores(*outcomes):  # noqa: ANN201, ANN002
    return [score(case(), o) for o in outcomes]


def test_the_totals_are_ratios_means_and_a_sum() -> None:
    report = summarize(scores(accepted(plan(A), 1, 10), accepted(plan(A), 3, 20), rejected(3, 5), rejected(3, 5)))
    assert report.totals["cases"] == 4
    assert report.totals["valid_ratio"] == 0.5 and report.totals["expectation_ratio"] == 0.5
    assert report.totals["mean_attempts"] == (1 + 3 + 3 + 3) / 4
    assert report.totals["total_tokens"] == 10 + 20 + 5 + 5


def test_expectations_met_can_be_fewer_than_valid_plans() -> None:
    cases = [score(case(min_jobs=2), accepted(plan(A))), score(case(), accepted(plan(A)))]
    report = summarize(cases)
    assert report.totals["valid_ratio"] == 1.0 and report.totals["expectation_ratio"] == 0.5


def test_the_report_lists_every_case_with_its_line_and_serializes_to_json() -> None:
    first = score(parse_case(line(), 3), accepted(plan(A), 1, 9))
    report = summarize([first, score(parse_case(line(), 9), rejected())])
    document = json.loads(json.dumps(report.to_dict()))
    assert [c["line"] for c in document["cases"]] == [3, 9]
    assert document["cases"][0] == {
        "line": 3, "valid": True, "attempts": 1, "tokens": 9, "expectations_met": True, "failures": [],
    }
    assert set(document["totals"]) == {"cases", "valid_ratio", "expectation_ratio", "mean_attempts", "total_tokens"}


def test_an_empty_report_is_refused_rather_than_dividing_by_zero() -> None:
    with pytest.raises(ValueError):
        summarize([])


@pytest.mark.parametrize(("min_pass", "passes"), [(0.0, True), (0.5, True), (0.51, False), (1.0, False)])
def test_both_ratios_must_reach_the_threshold(min_pass: float, passes: bool) -> None:
    report = summarize(scores(accepted(plan(A)), rejected()))  # both ratios are 0.5
    assert report.passes(min_pass) is passes


def test_a_report_whose_expectation_ratio_is_below_the_threshold_fails_even_if_all_plans_are_valid() -> None:
    cases = [score(case(min_jobs=2), accepted(plan(A))), score(case(), accepted(plan(A)))]
    report = summarize(cases)
    assert report.totals["valid_ratio"] == 1.0 and not report.passes(0.75)
