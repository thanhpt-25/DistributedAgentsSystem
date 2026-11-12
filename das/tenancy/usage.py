"""Usage report assembly (Phase 5 Req 6.3, 6.5). Pure: no I/O, no clock (``today`` is passed in).

A Usage_Rollup is a Redis hash per tenant and UTC day (``das:tenant:{t}:usage:{yyyymmdd}``) written by the State_Store
scripts: counters (``submitted``, ``finished/{status}``) and token totals per ``{provider}/{model}``
(``tokens/{in,out,total}/{provider}/{model}``). This module parses those hashes and builds the report. Cost is computed
here, when the report is read, from the Gateway's Price_Table (v2 Req 10), so a price change re-prices history and no
price is ever needed inside a Lua script.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from das.ops.cost import PriceTable, estimate_cost, format_amount, record_cost

__all__ = [
    "DEFAULT_RANGE_DAYS",
    "FINISHED_STATUSES",
    "MAX_RANGE_DAYS",
    "DayRange",
    "DayUsage",
    "ModelTokens",
    "UsageRangeError",
    "build_report",
    "day_key",
    "parse_range",
    "parse_rollup",
]

DEFAULT_RANGE_DAYS = 7
MAX_RANGE_DAYS = 92
FINISHED_STATUSES = ("Completed", "Failed", "Cancelled")
_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")  # ASCII digits only: ``\d`` would accept other scripts


class UsageRangeError(ValueError):
    """The requested date range is malformed, inverted or too long. ``parameter`` names the query parameter."""

    def __init__(self, parameter: str, reason: str) -> None:
        super().__init__(f"{parameter}: {reason}")
        self.parameter = parameter


@dataclass(frozen=True, slots=True)
class DayRange:
    """An inclusive range of UTC days."""

    start: date
    end: date

    def days(self) -> list[date]:
        return [self.start + timedelta(days=i) for i in range((self.end - self.start).days + 1)]


def _parse_date(value: str, parameter: str) -> date:
    if not _DATE.fullmatch(value):
        raise UsageRangeError(parameter, "expected a date as YYYY-MM-DD")
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise UsageRangeError(parameter, "not a calendar date") from None


def parse_range(from_: str | None, to: str | None, today: date) -> DayRange:
    """The range of ``from`` and ``to`` (inclusive), defaulting to the last seven days ending ``today``.

    A missing ``to`` is ``today``; a missing ``from`` is six days before ``to``. An inverted range names ``from``; a
    range of more than 92 days names ``to``.
    """
    given_start = _parse_date(from_, "from") if from_ is not None else None
    end = _parse_date(to, "to") if to is not None else today
    start = given_start if given_start is not None else end - timedelta(days=DEFAULT_RANGE_DAYS - 1)
    if start > end:
        raise UsageRangeError("from", "must not be after to")
    if (end - start).days + 1 > MAX_RANGE_DAYS:
        raise UsageRangeError("to", f"the range may span at most {MAX_RANGE_DAYS} days")
    return DayRange(start, end)


def day_key(day: date) -> str:
    """The ``yyyymmdd`` suffix of a Usage_Rollup key."""
    return f"{day.year:04d}{day.month:02d}{day.day:02d}"


@dataclass(frozen=True, slots=True)
class ModelTokens:
    provider: str
    model: str
    input_tokens: int
    output_tokens: int
    total_tokens: int


@dataclass(frozen=True, slots=True)
class DayUsage:
    submitted: int
    finished: Mapping[str, int]
    models: tuple[ModelTokens, ...]


def _int(raw: object) -> int | None:
    text = raw.decode("utf-8", "replace") if isinstance(raw, bytes) else raw
    return int(text) if isinstance(text, str) and text.isascii() and text.isdigit() else None


def parse_rollup(fields: Mapping[str, Any]) -> DayUsage:
    """One day's counters and per-model tokens. Unknown fields and non-integer values are ignored."""
    submitted = 0
    finished = dict.fromkeys(FINISHED_STATUSES, 0)
    tokens: dict[tuple[str, str], dict[str, int]] = {}
    for name, raw in fields.items():
        value = _int(raw)
        if value is None:
            continue
        if name == "submitted":
            submitted = value
        elif name.startswith("finished/"):
            status = name[len("finished/") :]
            if status in finished:
                finished[status] = value
        elif name.startswith("tokens/"):
            parts = name.split("/", 3)  # tokens, direction, provider, model (a model name may contain "/")
            if len(parts) == 4 and parts[1] in ("in", "out", "total") and parts[2] and parts[3]:
                tokens.setdefault((parts[2], parts[3]), {})[parts[1]] = value
    models = tuple(
        ModelTokens(p, m, t.get("in", 0), t.get("out", 0), t.get("total", t.get("in", 0) + t.get("out", 0)))
        for (p, m), t in sorted(tokens.items())
    )
    return DayUsage(submitted, finished, models)


@dataclass(frozen=True, slots=True)
class _Tokens:
    input_tokens: int
    output_tokens: int


@dataclass(frozen=True, slots=True)
class _Record:
    """Structural stand-in for a UsageRecord, as ``das.ops.cost`` reads it. Cost is linear in tokens, so an
    aggregate priced once equals the sum of the records it stands for."""

    provider: str
    model: str
    usage: _Tokens


def _model_cost(m: ModelTokens, table: PriceTable | None) -> str | None:
    if table is None:
        return None
    entry = table.lookup(m.provider, m.model)
    if entry is None:
        return None
    return format_amount(record_cost(_Record(m.provider, m.model, _Tokens(m.input_tokens, m.output_tokens)), entry))


def build_report(
    tenant: str, day_range: DayRange, rollups: Mapping[date, Mapping[str, Any]], price_table: PriceTable | None
) -> dict[str, Any]:
    """The Usage_Report of ``tenant``: one entry per day of the range, then totals (design §4 shape)."""
    days: list[dict[str, Any]] = []
    submitted = 0
    finished = dict.fromkeys(FINISHED_STATUSES, 0)
    by_pair: dict[tuple[str, str], list[int]] = {}
    for day in day_range.days():
        usage = parse_rollup(rollups.get(day, {}))
        submitted += usage.submitted
        for status in FINISHED_STATUSES:
            finished[status] += usage.finished[status]
        models = []
        for m in usage.models:
            models.append(
                {
                    "provider": m.provider,
                    "model": m.model,
                    "input_tokens": m.input_tokens,
                    "output_tokens": m.output_tokens,
                    "total_tokens": m.total_tokens,
                    "estimated_cost": _model_cost(m, price_table),
                }
            )
            acc = by_pair.setdefault((m.provider, m.model), [0, 0, 0])
            acc[0] += m.input_tokens
            acc[1] += m.output_tokens
            acc[2] += m.total_tokens
        days.append(
            {
                "date": day.isoformat(),
                "requests": {"submitted": usage.submitted, "finished": dict(usage.finished)},
                "models": models,
            }
        )
    cost: dict[str, Any] | None = None
    if price_table is not None:
        records = [_Record(p, m, _Tokens(a[0], a[1])) for (p, m), a in sorted(by_pair.items())]
        priced = estimate_cost(records, price_table)
        cost = {"currency": priced.currency, "amount": priced.amount, "complete": priced.complete}
    return {
        "tenant": tenant,
        "from": day_range.start.isoformat(),
        "to": day_range.end.isoformat(),
        "days": days,
        "totals": {
            "requests": {"submitted": submitted, "finished": finished},
            "input_tokens": sum(a[0] for a in by_pair.values()),
            "output_tokens": sum(a[1] for a in by_pair.values()),
            "total_tokens": sum(a[2] for a in by_pair.values()),
            "estimated_cost": cost,
        },
    }
