"""Token usage normalization and Token_Usage_Summary (Req 9).

Pure module: no I/O, no clock, no randomness (Req 19.5).
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import NamedTuple

from das.models.results import TokenUsage
from das.models.usage import (
    LLM_OPERATIONS,
    LLMOperation,
    ModelUsage,
    TokenUsageSummary,
    UsageRecord,
)

__all__ = ["NormalizedUsage", "normalize_usage", "summarize_usage"]


class NormalizedUsage(NamedTuple):
    """Result of :func:`normalize_usage`.

    ``reported`` is False when the provider returned no usage at all (all three
    values absent). ``problems`` names the fields that were missing or invalid and
    were recorded as 0; a non-empty tuple means the caller logs a warning (Req 9.6).
    A missing total alone is not a problem: it is derived as input + output (Req 9.7).
    """

    usage: TokenUsage
    reported: bool
    problems: tuple[str, ...]


def _valid_count(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def normalize_usage(i: object, o: object, t: object) -> NormalizedUsage:
    """Normalize raw provider token counts into a :class:`TokenUsage`.

    Missing (``None``), negative, non-integer or bool values become 0. When the total
    is missing it equals the normalized input + output.
    """
    problems: list[str] = []
    counts: dict[str, int] = {}
    for name, raw in (("input_tokens", i), ("output_tokens", o)):
        if _valid_count(raw):
            counts[name] = raw  # type: ignore[assignment]
        else:
            counts[name] = 0
            problems.append(name)
    if t is None:
        total = counts["input_tokens"] + counts["output_tokens"]
    elif _valid_count(t):
        total = t  # type: ignore[assignment]
    else:
        total = 0
        problems.append("total_tokens")
    usage = TokenUsage(
        input_tokens=counts["input_tokens"],
        output_tokens=counts["output_tokens"],
        total_tokens=total,
    )
    reported = not (i is None and o is None and t is None)
    return NormalizedUsage(usage=usage, reported=reported, problems=tuple(problems))


class _Acc:
    __slots__ = ("i", "o", "t")

    def __init__(self) -> None:
        self.i = 0
        self.o = 0
        self.t = 0

    def add(self, u: TokenUsage) -> None:
        self.i += u.input_tokens
        self.o += u.output_tokens
        self.t += u.total_tokens

    def freeze(self) -> TokenUsage:
        return TokenUsage(input_tokens=self.i, output_tokens=self.o, total_tokens=self.t)


def summarize_usage(
    records: Iterable[UsageRecord], job_ids: Sequence[str]
) -> TokenUsageSummary:
    """Aggregate Usage_Records of one Request (Req 9.8-9.10).

    ``by_operation`` always has ``plan``, ``job`` and ``summarize``; ``by_job`` has
    exactly the Job_IDs of the Job_Plan (0 when no record); ``by_model`` lists every
    provider/model pair seen, sorted by (provider, model). Job records whose Job_ID
    is not in ``job_ids`` still count toward ``job`` / ``total`` / ``by_model``.
    """
    total = _Acc()
    by_op: dict[LLMOperation, _Acc] = {op: _Acc() for op in LLM_OPERATIONS}
    by_job: dict[str, _Acc] = {jid: _Acc() for jid in job_ids}
    by_model: dict[tuple[str, str], _Acc] = {}
    for record in records:
        u = record.usage
        total.add(u)
        by_op[record.operation].add(u)
        key = (record.provider, record.model)
        acc = by_model.get(key)
        if acc is None:
            acc = by_model[key] = _Acc()
        acc.add(u)
        if record.operation == "job" and record.job_id in by_job:
            by_job[record.job_id].add(u)
    return TokenUsageSummary(
        total=total.freeze(),
        by_operation={op: acc.freeze() for op, acc in by_op.items()},
        by_model=[
            ModelUsage(provider=p, model=m, usage=by_model[(p, m)].freeze())
            for p, m in sorted(by_model)
        ],
        by_job={jid: acc.freeze() for jid, acc in by_job.items()},
    )
