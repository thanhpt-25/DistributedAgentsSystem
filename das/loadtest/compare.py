"""``das loadtest-compare``: did a change make the stack slower? (das-scale-resilience task 12.1; Phase 7 Req 4.7).

Two Capacity_Reports of the same profile: the new run regressed when its throughput fell, or its p99 rose, by more than
``max_regression`` (a ratio: 0.1 is ten percent) against the base run. Reports of different profiles measure different
things and are refused; a different transport, machine or setting is reported as a note beside the verdict, because the
comparison is then between more than the change under test."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

MAX_REGRESSION_LIMIT: Final = 10.0


class CompareError(ValueError):
    """Two reports that cannot be compared; the message says why."""


@dataclass(frozen=True, slots=True)
class Comparison:
    throughput_base: float
    throughput_new: float
    p99_base_ms: float | None
    p99_new_ms: float | None
    regressions: tuple[str, ...]
    notes: tuple[str, ...]

    @property
    def regressed(self) -> bool:
        return bool(self.regressions)

    def to_dict(self) -> dict[str, Any]:
        return {
            "regressed": self.regressed,
            "throughput_rps": {"base": self.throughput_base, "new": self.throughput_new},
            "p99_ms": {"base": self.p99_base_ms, "new": self.p99_new_ms},
            "regressions": list(self.regressions),
            "notes": list(self.notes),
        }


def _results(report: Any, which: str) -> Mapping[str, Any]:
    if not isinstance(report, Mapping) or report.get("schema") != 1 or not isinstance(report.get("results"), Mapping):
        raise CompareError(f"the {which} report is not a Capacity_Report (schema 1)")
    results = report["results"]
    if not isinstance(results.get("throughput_rps"), int | float) or not isinstance(results.get("total_ms"), Mapping):
        raise CompareError(f"the {which} report has no throughput or total time")
    return results


def _name(report: Mapping[str, Any]) -> Any:
    profile = report.get("profile")
    return profile.get("name") if isinstance(profile, Mapping) else None


def compare(base: Mapping[str, Any], new: Mapping[str, Any], max_regression: float) -> Comparison:
    if not 0 <= max_regression <= MAX_REGRESSION_LIMIT:
        raise CompareError(f"max_regression must be from 0 to {MAX_REGRESSION_LIMIT:g}")
    base_results, new_results = _results(base, "base"), _results(new, "new")
    if _name(base) != _name(new):
        raise CompareError(f"not comparable: the base ran profile {_name(base)!r} and the new run {_name(new)!r}")

    notes: list[str] = []
    for field in ("transport", "settings", "machine", "das_version"):
        if base.get(field) != new.get(field):
            notes.append(f"{field} differs: {base.get(field)!r} against {new.get(field)!r}")

    regressions: list[str] = []
    t_base, t_new = float(base_results["throughput_rps"]), float(new_results["throughput_rps"])
    if t_new < t_base * (1 - max_regression):
        regressions.append(f"throughput fell from {t_base:.2f} to {t_new:.2f} requests per second")
    p_base, p_new = base_results["total_ms"].get("p99"), new_results["total_ms"].get("p99")
    if p_new is None and p_base is not None:
        regressions.append("no Request completed in the new run")
    elif p_new is not None and p_base is not None and p_new > p_base * (1 + max_regression):
        regressions.append(f"p99 rose from {p_base:.0f} ms to {p_new:.0f} ms")
    return Comparison(t_base, t_new, p_base, p_new, tuple(regressions), tuple(notes))


__all__ = ["MAX_REGRESSION_LIMIT", "CompareError", "Comparison", "compare"]
