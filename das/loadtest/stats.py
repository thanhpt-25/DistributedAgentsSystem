"""Percentiles and the saturation point from raw samples (das-scale-resilience task 12.1; Phase 7 Req 4.3, 4.6, 6.4).

Pure: no I/O, no clock. Percentiles are by the nearest-rank method, so every one is a value that was measured (a load
test does not publish a latency that no request had), they never decrease as the percentile grows, and they stay between
the smallest and the largest sample."""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from fractions import Fraction


def percentile(samples: Sequence[float], p: float) -> float:
    """The smallest sample that at least ``p`` percent of the samples are at or below (nearest rank)."""
    if not samples:
        raise ValueError("percentile of no samples")
    if not 0 <= p <= 100:
        raise ValueError("p must be between 0 and 100")
    ordered = sorted(samples)
    # Exact arithmetic on the decimal the caller wrote (64.4 is 64.4, not the nearest binary float), so 64.4 % of 250
    # is rank 161 as it reads and never 162 by a rounding error in the last bit.
    rank = max(1, math.ceil(Fraction(repr(p)) * len(ordered) / 100))
    return ordered[rank - 1]


@dataclass(frozen=True, slots=True)
class Summary:
    """What a run reports about one measurement (a latency in milliseconds): ``None`` throughout without samples."""

    count: int
    mean: float | None
    min: float | None
    max: float | None
    p50: float | None
    p90: float | None
    p99: float | None

    def to_dict(self) -> dict[str, float | int | None]:
        return {
            "count": self.count, "mean": self.mean, "min": self.min, "max": self.max,
            "p50": self.p50, "p90": self.p90, "p99": self.p99,
        }


def summarize(samples: Sequence[float]) -> Summary:
    if not samples:
        return Summary(0, None, None, None, None, None, None)
    return Summary(
        count=len(samples),
        mean=math.fsum(samples) / len(samples),
        min=min(samples),
        max=max(samples),
        p50=percentile(samples, 50),
        p90=percentile(samples, 90),
        p99=percentile(samples, 99),
    )


@dataclass(frozen=True, slots=True)
class StepStats:
    """One step of a profile: the rate it offered, what happened to the Requests, and the p99 of those that finished."""

    rate: float
    offered: int
    completed: int
    errors: int
    p99_ms: float | None

    @property
    def error_rate(self) -> float:
        return self.errors / self.offered if self.offered else 0.0


def within_limits(step: StepStats, max_error_rate: float, max_p99_ms: float) -> bool:
    """A step with no finished Request has no p99 to be inside a limit, so it is outside."""
    return step.p99_ms is not None and step.error_rate <= max_error_rate and step.p99_ms <= max_p99_ms


def saturation(steps: Sequence[StepStats], *, max_error_rate: float, max_p99_ms: float) -> float | None:
    """The highest rate the stack took with the error rate and p99 inside their limits: the rate of the last step before
    the first one outside them, walking from the lowest rate up. A later step that happens to be inside again does not
    count (a stack that failed at 20 rps has not shown it takes 40). ``None`` when even the lowest rate was outside."""
    best: float | None = None
    for step in sorted(steps, key=lambda s: s.rate):
        if not within_limits(step, max_error_rate, max_p99_ms):
            break
        best = step.rate
    return best


__all__ = ["StepStats", "Summary", "percentile", "saturation", "summarize", "within_limits"]
