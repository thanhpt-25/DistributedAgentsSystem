"""Metric label cardinality bounding (Requirement 5.2, 5.3, 5.7).

Pure core: no I/O, no clock, no ``prometheus_client``. The shell
(``MetricsRecorder``) calls :meth:`LabelBounder.bound` before every
observation and uses :attr:`BoundResult.overflowed` to emit the one-time
overflow warning (Requirement 5.4).

Rules:

* Enumerated labels (criterion 2): a value inside the allowed set is kept,
  anything else becomes :data:`OVERFLOW_VALUE`.
* Dynamic labels (criterion 3: ``capability``, ``queue``, ``provider``,
  ``model``): a value already recorded for ``(metric, label)`` is kept; a new
  value is recorded and kept while fewer than ``limit`` distinct values have
  been recorded; otherwise it becomes :data:`OVERFLOW_VALUE` and is *not*
  recorded. ``seen`` only grows and is never reset.
* Labels in neither group are passed through unchanged.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

OVERFLOW_VALUE = "_other"


@dataclass(frozen=True)
class LabelPolicy:
    """Allowed values for enumerated labels and limits for dynamic labels."""

    enumerated: Mapping[str, frozenset[str]] = field(default_factory=dict)
    dynamic_limits: Mapping[str, int] = field(default_factory=dict)


@dataclass(frozen=True)
class BoundResult:
    """Bounded labels plus the label names that overflowed on this call."""

    labels: dict[str, str]
    overflowed: tuple[str, ...]


@dataclass
class LabelBounder:
    policy: LabelPolicy
    seen: dict[tuple[str, str], set[str]] = field(default_factory=dict)

    def bound(self, metric: str, labels: Mapping[str, str]) -> BoundResult:
        out: dict[str, str] = {}
        overflowed: list[str] = []
        enumerated = self.policy.enumerated
        limits = self.policy.dynamic_limits
        for name, value in labels.items():
            if name in enumerated:
                if value in enumerated[name]:
                    out[name] = value
                else:
                    out[name] = OVERFLOW_VALUE
                    overflowed.append(name)
            elif name in limits:
                recorded = self.seen.setdefault((metric, name), set())
                if value in recorded:
                    out[name] = value
                elif len(recorded) < limits[name]:
                    recorded.add(value)
                    out[name] = value
                else:
                    out[name] = OVERFLOW_VALUE
                    overflowed.append(name)
            else:
                out[name] = value
        return BoundResult(labels=out, overflowed=tuple(overflowed))
