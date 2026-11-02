# Feature: das-observability-operations, Property 12: Bất biến tổng hợp Token_Usage_Summary
"""Property 12: Token_Usage_Summary aggregation invariants.

For any set of Usage_Records (every operation, provider/model, Job_ID and attempt)
and any Job_Plan Job_ID list, ``summarize_usage`` gives:

- ``total`` = sum of ``by_operation`` over ``plan``, ``job``, ``summarize`` = sum of
  ``by_model``.
- ``by_operation["job"]`` = sum of ``by_job``.
- ``by_operation`` always has all three keys.
- ``by_job`` has exactly the Job_IDs of the Job_Plan, 0 for Jobs with no record.
- ``by_model`` has exactly the provider/model pairs seen in at least one record.

**Validates: Requirements 9.9, 9.10**
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from das.models.plan import JobPlan
from das.models.results import TokenUsage
from das.models.usage import UsageRecord
from das.ops.usage import summarize_usage
from tests.strategies import job_plans, usage_records

_ZERO = (0, 0, 0)


def _triple(u: TokenUsage) -> tuple[int, int, int]:
    return (u.input_tokens, u.output_tokens, u.total_tokens)


def _sum(usages: list[TokenUsage]) -> tuple[int, int, int]:
    return (
        sum(u.input_tokens for u in usages),
        sum(u.output_tokens for u in usages),
        sum(u.total_tokens for u in usages),
    )


@st.composite
def _plan_and_records(draw: st.DrawFn) -> tuple[list[str], list[UsageRecord]]:
    plan: JobPlan = draw(job_plans(max_jobs=8))
    ids = [job.job_id for job in plan.jobs]
    records = draw(st.lists(usage_records(job_id_pool=ids, as_model=True), max_size=30))
    return ids, records


@given(_plan_and_records())
def test_token_usage_summary_invariants(
    case: tuple[list[str], list[UsageRecord]],
) -> None:
    """**Validates: Requirements 9.9, 9.10**"""
    job_ids, records = case
    summary = summarize_usage(records, job_ids)

    # by_operation always has exactly the three LLM_Operations (Req 9.9).
    assert set(summary.by_operation) == {"plan", "job", "summarize"}

    # total = sum(by_operation) = sum(by_model) = sum of all records (Req 9.10).
    total = _triple(summary.total)
    assert total == _sum([r.usage for r in records])
    assert total == _sum(list(summary.by_operation.values()))
    assert total == _sum([m.usage for m in summary.by_model])

    # Each operation equals the sum of its records.
    for op, usage in summary.by_operation.items():
        assert _triple(usage) == _sum([r.usage for r in records if r.operation == op])

    # by_job has exactly the plan's Job_IDs, summed over all attempts, 0 when none.
    assert set(summary.by_job) == set(job_ids)
    for jid, usage in summary.by_job.items():
        expected = _sum([r.usage for r in records if r.operation == "job" and r.job_id == jid])
        assert _triple(usage) == expected
        if not any(r.operation == "job" and r.job_id == jid for r in records):
            assert _triple(usage) == _ZERO

    # by_operation["job"] = sum(by_job).
    assert _triple(summary.by_operation["job"]) == _sum(list(summary.by_job.values()))

    # by_model has exactly the pairs seen, once each, sorted by (provider, model).
    pairs = [(m.provider, m.model) for m in summary.by_model]
    assert pairs == sorted({(r.provider, r.model) for r in records})
    for m in summary.by_model:
        expected = _sum(
            [r.usage for r in records if (r.provider, r.model) == (m.provider, m.model)]
        )
        assert _triple(m.usage) == expected


def test_empty_records_give_all_zero_summary() -> None:
    summary = summarize_usage([], ["a", "b"])
    assert _triple(summary.total) == _ZERO
    assert {op: _triple(u) for op, u in summary.by_operation.items()} == {
        "plan": _ZERO,
        "job": _ZERO,
        "summarize": _ZERO,
    }
    assert {j: _triple(u) for j, u in summary.by_job.items()} == {"a": _ZERO, "b": _ZERO}
    assert summary.by_model == []
