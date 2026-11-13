"""The resources a sandboxed run may use, each clamped between a floor and a ceiling (Phase 6 Req 6.4).

:class:`Limits` has a field for exactly the five values the requirement lets a configuration choose. Whatever is
asked for is clamped into its range in the constructor, so no configuration, however written, can exceed a ceiling.
There is deliberately no field for privileged mode, a network, a mount, a device, the user, a capability or the
container runtime's socket: a value that does not exist cannot be set, and ``Limits(privileged=True)`` is a
``TypeError``.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Final

#: What a configuration may not exceed (Req 6.4).
CEILINGS: Final[dict[str, float]] = {
    "timeout": 60.0, "memory_mb": 1024, "cpus": 2.0, "pids": 256, "output_bytes": 65_536,
}
#: The smallest value that still runs a Python interpreter.
FLOORS: Final[dict[str, float]] = {
    "timeout": 1.0, "memory_mb": 64, "cpus": 0.1, "pids": 16, "output_bytes": 256,
}
#: The writable scratch directory's size is fixed, not configurable (Req 6.4: "of bounded size only").
SCRATCH_MEGABYTES: Final = 16

_INTEGER_FIELDS: Final = frozenset({"memory_mb", "pids", "output_bytes"})


def _clamped(name: str, value: Any) -> float | int:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise TypeError(f"{name} must be a number")
    if not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number")
    number = min(max(value, FLOORS[name]), CEILINGS[name])
    return int(number) if name in _INTEGER_FIELDS else float(number)


@dataclass(frozen=True, slots=True)
class Limits:
    """Wall-clock ``timeout`` in seconds, ``memory_mb``, ``cpus``, ``pids`` and ``output_bytes`` kept."""

    timeout: float = 10.0
    memory_mb: int = 256
    cpus: float = 1.0
    pids: int = 64
    output_bytes: int = 16_384

    def __post_init__(self) -> None:
        for name in CEILINGS:
            object.__setattr__(self, name, _clamped(name, getattr(self, name)))


__all__ = ["CEILINGS", "FLOORS", "SCRATCH_MEGABYTES", "Limits"]
