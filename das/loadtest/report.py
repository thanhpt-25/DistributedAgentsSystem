"""The Capacity_Report (das-scale-resilience task 12.1; Phase 7 Req 4.3, 4.4).

What a run writes: the results (throughput, error rate, latency percentiles of the Requests that completed, the highest
rate that stayed inside the limits) together with the context that makes the numbers mean something: the machine, the
DAS version, the transport and the settings the stack was given. A number is never reported without them, so the
transport is required and the settings are written as given. The API key is never part of a report."""

from __future__ import annotations

import os
import platform
import re
import subprocess
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import metadata
from typing import Any, Final

from das.loadtest.runner import COMPLETED, RunResult
from das.loadtest.stats import StepStats, percentile, saturation, summarize

SCHEMA: Final = 1
DEFAULT_MAX_ERROR_RATE: Final = 0.01
DEFAULT_MAX_P99_MS: Final = 10_000.0
_SETTING_KEY: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
_TRANSPORT: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.+-]{0,63}")
MAX_SETTING_VALUE: Final = 200


class ReportError(ValueError):
    """Context that cannot go into a report; the message names it."""


@dataclass(frozen=True, slots=True)
class Limits:
    """What counts as the stack still coping: the error rate and the p99 of the total time, per step."""

    max_error_rate: float = DEFAULT_MAX_ERROR_RATE
    max_p99_ms: float = DEFAULT_MAX_P99_MS

    def __post_init__(self) -> None:
        if not 0 <= self.max_error_rate <= 1 or self.max_p99_ms <= 0:
            raise ReportError("max_error_rate must be from 0 to 1 and max_p99_ms above 0")


def parse_settings(pairs: list[str]) -> dict[str, str]:
    """``KEY=VALUE`` strings (``--setting``) into the settings of a report."""
    settings: dict[str, str] = {}
    for pair in pairs:
        key, sep, value = pair.partition("=")
        if not sep or _SETTING_KEY.fullmatch(key) is None:
            raise ReportError(f"setting {pair!r} must be KEY=VALUE, the key made of letters, digits, . - and _")
        if len(value) > MAX_SETTING_VALUE:
            raise ReportError(f"the value of setting {key} is longer than {MAX_SETTING_VALUE} characters")
        if key in settings:
            raise ReportError(f"setting {key} is given twice")
        settings[key] = value
    return settings


def check_transport(name: str) -> str:
    if _TRANSPORT.fullmatch(name) is None:
        raise ReportError("transport must be a name such as rabbitmq, http or kafka")
    return name


def memory_bytes() -> int | None:
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, OSError, AttributeError):
        pass
    if platform.system() == "Darwin":
        try:
            out = subprocess.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True, timeout=5)
            return int(out.stdout.strip())
        except (OSError, ValueError, subprocess.SubprocessError):
            return None
    return None


def machine_description() -> dict[str, Any]:
    return {
        "cpu_count": os.cpu_count(),
        "memory_bytes": memory_bytes(),
        "os": platform.platform(),
        "arch": platform.machine(),
        "python": platform.python_version(),
    }


def das_version() -> str:
    try:
        return metadata.version("das")
    except metadata.PackageNotFoundError:
        return "unknown"


def step_stats(result: RunResult) -> list[StepStats]:
    stats: list[StepStats] = []
    for number, step in enumerate(result.profile.steps):
        own = [s for s in result.samples if s.step == number]
        done = [s.total_ms for s in own if s.outcome == COMPLETED and s.total_ms is not None]
        p99 = percentile(done, 99) if done else None
        stats.append(StepStats(step.rate, len(own), len(done), len(own) - len(done), p99))
    return stats


def build_report(
    result: RunResult,
    *,
    transport: str,
    settings: Mapping[str, str],
    limits: Limits | None = None,
    machine: Mapping[str, Any] | None = None,
    version: str | None = None,
) -> dict[str, Any]:
    """The Capacity_Report of a run. Latencies are milliseconds and, for the total time, those of the Requests that
    completed (a Request that failed or timed out has no time to a result to put in a percentile)."""
    limits = limits or Limits()
    samples = result.samples
    completed = [s for s in samples if s.outcome == COMPLETED]
    errors = Counter(s.outcome for s in samples if s.outcome != COMPLETED)
    steps = step_stats(result)
    offered_seconds = result.profile.total_seconds
    return {
        "schema": SCHEMA,
        "started_at": result.started_at,
        "das_version": version or das_version(),
        "transport": check_transport(transport),
        "settings": dict(settings),
        "machine": dict(machine) if machine is not None else machine_description(),
        "profile": result.profile.to_dict(),
        "limits": {"max_error_rate": limits.max_error_rate, "max_p99_ms": limits.max_p99_ms},
        "results": {
            "requests": len(samples),
            "completed": len(completed),
            "errors": len(samples) - len(completed),
            "error_rate": (len(samples) - len(completed)) / len(samples) if samples else 0.0,
            "errors_by_kind": dict(sorted(errors.items())),
            "elapsed_s": round(result.elapsed_s, 3),
            "offered_rps": len(samples) / offered_seconds if offered_seconds else 0.0,
            "throughput_rps": len(completed) / result.elapsed_s if result.elapsed_s else 0.0,
            "submit_ms": summarize([s.submit_ms for s in samples if s.submit_ms is not None]).to_dict(),
            "total_ms": summarize([s.total_ms for s in completed if s.total_ms is not None]).to_dict(),
            "max_late_ms": max((s.late_ms for s in samples), default=0.0),
        },
        "steps": [
            {
                "step": number, "rate": step.rate, "seconds": step.seconds,
                "offered": stat.offered, "completed": stat.completed, "errors": stat.errors,
                "error_rate": stat.error_rate, "p99_ms": stat.p99_ms,
            }
            for number, (step, stat) in enumerate(zip(result.profile.steps, steps, strict=True))
        ],
        "saturation_rps": saturation(steps, max_error_rate=limits.max_error_rate, max_p99_ms=limits.max_p99_ms),
    }


__all__ = [
    "DEFAULT_MAX_ERROR_RATE", "DEFAULT_MAX_P99_MS", "SCHEMA", "Limits", "ReportError", "build_report",
    "check_transport", "das_version", "machine_description", "memory_bytes", "parse_settings", "step_stats",
]
