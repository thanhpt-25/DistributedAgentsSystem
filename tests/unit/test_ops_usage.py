"""Unit tests for das.models.usage and das.ops.usage (Req 9.6-9.10)."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic import ValidationError

from das.models.results import TokenUsage
from das.models.usage import ModelUsage, TokenUsageSummary, UsageRecord
from das.ops.usage import normalize_usage, summarize_usage
from tests.strategies import usage_records


def _u(i: int, o: int, t: int) -> TokenUsage:
    return TokenUsage(input_tokens=i, output_tokens=o, total_tokens=t)


def _rec(op: str, provider: str, model: str, usage: TokenUsage, **kw: object) -> UsageRecord:
    return UsageRecord(operation=op, provider=provider, model=model, usage=usage, **kw)  # type: ignore[arg-type]


# ----------------------------------------------------------------- models
def test_usage_record_is_frozen_and_forbids_extra() -> None:
    r = _rec("plan", "openai", "gpt", _u(1, 2, 3))
    with pytest.raises(ValidationError):
        r.provider = "x"  # type: ignore[misc]
    with pytest.raises(ValidationError):
        UsageRecord.model_validate(
            {"operation": "plan", "provider": "p", "model": "m", "usage": {}, "extra": 1}
        )


@pytest.mark.parametrize(
    "bad",
    [
        {"operation": "other"},
        {"provider": ""},
        {"model": ""},
        {"attempt": 0},
        {"usage": {"input_tokens": -1}},
    ],
)
def test_usage_record_rejects_invalid(bad: dict[str, object]) -> None:
    base: dict[str, object] = {"operation": "job", "provider": "p", "model": "m", "usage": {}}
    with pytest.raises(ValidationError):
        UsageRecord.model_validate({**base, **bad})


def test_usage_record_json_roundtrip() -> None:
    r = _rec("job", "p", "m", _u(1, 2, 3), job_id="j1", attempt=2)
    assert UsageRecord.model_validate_json(r.model_dump_json()) == r


@settings(max_examples=50)
@given(usage_records(as_model=True))
def test_shared_strategy_builds_model(record: object) -> None:
    assert isinstance(record, UsageRecord)


# ----------------------------------------------------------------- normalize_usage
def test_normalize_valid_values_kept() -> None:
    n = normalize_usage(10, 5, 20)
    assert n.usage == _u(10, 5, 20)
    assert n.reported is True
    assert n.problems == ()


def test_normalize_missing_total_is_sum() -> None:
    n = normalize_usage(10, 5, None)
    assert n.usage == _u(10, 5, 15)
    assert n.problems == ()


@pytest.mark.parametrize("bad", [-1, 1.0, 2.5, True, False, "3", None, [1]])
def test_normalize_invalid_values_become_zero(bad: object) -> None:
    n = normalize_usage(bad, 7, 9)
    assert n.usage == _u(0, 7, 9)
    assert n.problems == ("input_tokens",)
    n = normalize_usage(4, bad, 9)
    assert n.usage == _u(4, 0, 9)
    assert n.problems == ("output_tokens",)


@pytest.mark.parametrize("bad", [-1, 1.0, True, "3"])
def test_normalize_invalid_total_becomes_zero(bad: object) -> None:
    n = normalize_usage(4, 5, bad)
    assert n.usage == _u(4, 5, 0)
    assert n.problems == ("total_tokens",)


def test_normalize_absent_usage_not_reported() -> None:
    n = normalize_usage(None, None, None)
    assert n.usage == _u(0, 0, 0)
    assert n.reported is False
    assert n.problems == ("input_tokens", "output_tokens")


# ----------------------------------------------------------------- summarize_usage
def test_summarize_empty_has_all_keys_and_zeros() -> None:
    s = summarize_usage([], ["a", "b"])
    zero = _u(0, 0, 0)
    assert s.total == zero
    assert s.by_operation == {"plan": zero, "job": zero, "summarize": zero}
    assert s.by_job == {"a": zero, "b": zero}
    assert s.by_model == []


def test_summarize_aggregates() -> None:
    records = [
        _rec("plan", "openai", "gpt", _u(1, 2, 3)),
        _rec("plan", "openai", "gpt", _u(1, 1, 2)),
        _rec("job", "anthropic", "claude", _u(10, 20, 30), job_id="a", attempt=1),
        _rec("job", "anthropic", "claude", _u(5, 5, 10), job_id="a", attempt=2),
        _rec("summarize", "anthropic", "a-model", _u(100, 0, 100)),
    ]
    s = summarize_usage(records, ["a", "b"])
    assert s.total == _u(117, 28, 145)
    assert s.by_operation["plan"] == _u(2, 3, 5)
    assert s.by_operation["job"] == _u(15, 25, 40)
    assert s.by_operation["summarize"] == _u(100, 0, 100)
    assert s.by_job == {"a": _u(15, 25, 40), "b": _u(0, 0, 0)}
    assert s.by_model == [
        ModelUsage(provider="anthropic", model="a-model", usage=_u(100, 0, 100)),
        ModelUsage(provider="anthropic", model="claude", usage=_u(15, 25, 40)),
        ModelUsage(provider="openai", model="gpt", usage=_u(2, 3, 5)),
    ]
    assert TokenUsageSummary.model_validate_json(s.model_dump_json()) == s


def test_summarize_provider_model_case_sensitive() -> None:
    records = [_rec("plan", "P", "m", _u(1, 1, 2)), _rec("plan", "p", "m", _u(1, 1, 2))]
    s = summarize_usage(records, [])
    assert [(m.provider, m.model) for m in s.by_model] == [("P", "m"), ("p", "m")]


def _sum(usages: list[TokenUsage]) -> tuple[int, int, int]:
    return (
        sum(u.input_tokens for u in usages),
        sum(u.output_tokens for u in usages),
        sum(u.total_tokens for u in usages),
    )


@settings(max_examples=100)
@given(
    st.lists(st.sampled_from(["j1", "j2", "j3", "j4"]), min_size=1, max_size=4, unique=True).flatmap(
        lambda ids: st.tuples(
            st.just(ids), st.lists(usage_records(job_id_pool=ids, as_model=True), max_size=20)
        )
    )
)
def test_summarize_totals_consistent(data: tuple[list[str], list[UsageRecord]]) -> None:
    ids, records = data
    s = summarize_usage(records, ids)
    t = (s.total.input_tokens, s.total.output_tokens, s.total.total_tokens)
    assert set(s.by_operation) == {"plan", "job", "summarize"}
    assert t == _sum(list(s.by_operation.values()))
    assert t == _sum([m.usage for m in s.by_model])
    assert _sum([s.by_operation["job"]]) == _sum(list(s.by_job.values()))
    assert list(s.by_job) == ids
    keys = [(m.provider, m.model) for m in s.by_model]
    assert keys == sorted({(r.provider, r.model) for r in records})
