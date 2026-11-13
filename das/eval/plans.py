"""Plan evaluation (Phase 6 Req 4): run the real Planner over a dataset and score its plans.

A dataset is JSON Lines. Each line is an object::

    {"prompt": "...", "capabilities": ["search", "write"],
     "expect": {"min_jobs": 2, "max_jobs": 5, "require_capabilities": ["write"],
                "forbid_capabilities": ["paint"], "require_output_schema": false}}

``prompt`` and ``capabilities`` (the Capabilities the Planner is offered) are required; ``expect`` and each of its keys
are optional. Reading a line, scoring an outcome and summarizing scores are pure; only :func:`run` calls the Planner.

A case is *valid* when the Planner produced a valid plan, and its expectations are *met* when it is valid and every
listed expectation holds. A case without expectations is therefore met exactly when it is valid, and the expectation
ratio never exceeds the valid ratio.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from das.config.schema import CAPABILITY_PATTERN
from das.manager.planner import (
    NoCapabilities,
    PlanAccepted,
    Planner,
    PlannerConfig,
    PlanLLMFailed,
    PlanOutcome,
    PlanRejected,
    llm_failure_message,
)

#: The default of ``limits.max_prompt_length``: a dataset prompt is held to what a Request may carry.
MAX_PROMPT_LENGTH: Final = 32_000
_CAPABILITY: Final = re.compile(CAPABILITY_PATTERN)
_CASE_FIELDS: Final = frozenset({"prompt", "capabilities", "expect"})
_EXPECT_FIELDS: Final = frozenset(
    {"min_jobs", "max_jobs", "require_capabilities", "forbid_capabilities", "require_output_schema"}
)


class DatasetError(ValueError):
    """A dataset that cannot be used. ``line`` is the 1-based line at fault, 0 when it is the file as a whole."""

    def __init__(self, line: int, reason: str) -> None:
        self.line = line
        self.reason = reason
        super().__init__(f"line {line}: {reason}" if line else reason)


@dataclass(frozen=True, slots=True)
class Expect:
    """What a good plan must look like for one prompt (Req 4.2)."""

    min_jobs: int | None = None
    max_jobs: int | None = None
    require_capabilities: frozenset[str] = frozenset()
    forbid_capabilities: frozenset[str] = frozenset()
    require_output_schema: bool = False


@dataclass(frozen=True, slots=True)
class Case:
    line: int
    prompt: str
    capabilities: frozenset[str]
    expect: Expect


# --------------------------------------------------------------------------- reading


def _capabilities(value: Any, name: str, line: int, *, allow_empty: bool) -> frozenset[str]:
    if (
        not isinstance(value, list)
        or (not allow_empty and not value)
        or not all(isinstance(item, str) and _CAPABILITY.fullmatch(item) for item in value)
    ):
        raise DatasetError(line, f"{name} must be a{'' if allow_empty else ' non-empty'} list of capability names")
    if len(set(value)) != len(value):
        raise DatasetError(line, f"{name} must not repeat a capability")
    return frozenset(value)


def _job_count(raw: dict[str, Any], name: str, line: int) -> int | None:
    if name not in raw:
        return None
    value = raw[name]
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise DatasetError(line, f"{name} must be an integer of at least 1")
    return value


def _expect(raw: Any, line: int) -> Expect:
    if not isinstance(raw, dict):
        raise DatasetError(line, "expect must be an object")
    unknown = sorted(set(raw) - _EXPECT_FIELDS)
    if unknown:
        raise DatasetError(line, f"unknown expectation {unknown[0]!r}")
    low, high = _job_count(raw, "min_jobs", line), _job_count(raw, "max_jobs", line)
    if low is not None and high is not None and high < low:
        raise DatasetError(line, "max_jobs must not be below min_jobs")
    required = _capabilities(raw.get("require_capabilities", []), "require_capabilities", line, allow_empty=True)
    forbidden = _capabilities(raw.get("forbid_capabilities", []), "forbid_capabilities", line, allow_empty=True)
    both = sorted(required & forbidden)
    if both:
        raise DatasetError(line, f"require_capabilities and forbid_capabilities both name {both[0]!r}")
    schema = raw.get("require_output_schema", False)
    if not isinstance(schema, bool):
        raise DatasetError(line, "require_output_schema must be true or false")
    return Expect(low, high, required, forbidden, schema)


def parse_case(text: str, line: int) -> Case:
    """One dataset line as a :class:`Case`; :class:`DatasetError` names the line and the problem."""
    try:
        raw = json.loads(text)
    except (json.JSONDecodeError, RecursionError) as exc:
        reason = exc.msg if isinstance(exc, json.JSONDecodeError) else "nested too deeply"
        raise DatasetError(line, f"not valid JSON: {reason}") from None
    if not isinstance(raw, dict):
        raise DatasetError(line, "a case must be a JSON object")
    unknown = sorted(set(raw) - _CASE_FIELDS)
    if unknown:
        raise DatasetError(line, f"unknown field {unknown[0]!r}")
    prompt = raw.get("prompt")
    if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > MAX_PROMPT_LENGTH:
        raise DatasetError(line, f"prompt must be a non-empty string of at most {MAX_PROMPT_LENGTH} characters")
    capabilities = _capabilities(raw.get("capabilities"), "capabilities", line, allow_empty=False)
    return Case(line, prompt, capabilities, _expect(raw["expect"], line) if "expect" in raw else Expect())


def load_dataset(path: Path | str) -> list[Case]:
    """Every case of a JSON Lines file, in order. Blank lines are skipped and do not disturb the numbering."""
    try:
        text = Path(path).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        detail = exc.strerror if isinstance(exc, OSError) and exc.strerror else "not valid UTF-8 text"
        raise DatasetError(0, f"cannot read the dataset {path}: {detail}") from None
    cases = [parse_case(raw, number) for number, raw in enumerate(text.splitlines(), start=1) if raw.strip()]
    if not cases:
        raise DatasetError(0, f"the dataset {path} has no cases")
    return cases


# --------------------------------------------------------------------------- scoring


@dataclass(frozen=True, slots=True)
class CaseScore:
    """How the Planner did on one case (Req 4.3)."""

    line: int
    valid: bool
    attempts: int
    tokens: int
    expectations_met: bool
    failures: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "line": self.line, "valid": self.valid, "attempts": self.attempts, "tokens": self.tokens,
            "expectations_met": self.expectations_met, "failures": list(self.failures),
        }


def _tokens(outcome: PlanOutcome) -> int:
    return sum(record.usage.total_tokens for record in outcome.usages)


def _unmet(expect: Expect, outcome: PlanAccepted) -> tuple[str, ...]:
    jobs = outcome.plan.jobs
    used = {cap for job in jobs for cap in job.capabilities}
    found: list[str] = []
    if expect.min_jobs is not None and len(jobs) < expect.min_jobs:
        found.append(f"min_jobs: the plan has {len(jobs)} job(s), expected at least {expect.min_jobs}")
    if expect.max_jobs is not None and len(jobs) > expect.max_jobs:
        found.append(f"max_jobs: the plan has {len(jobs)} job(s), expected at most {expect.max_jobs}")
    missing = expect.require_capabilities - used
    if missing:
        found.append(f"require_capabilities: no job uses {sorted(missing)}")
    forbidden = expect.forbid_capabilities & used
    if forbidden:
        found.append(f"forbid_capabilities: the plan uses {sorted(forbidden)}")
    if expect.require_output_schema and not any(job.output_schema is not None for job in jobs):
        found.append("require_output_schema: no job has an output_schema")
    return tuple(found)


def score(case: Case, outcome: PlanOutcome) -> CaseScore:
    """Score one outcome against its case. Pure."""
    tokens = _tokens(outcome)
    if isinstance(outcome, PlanAccepted):
        unmet = _unmet(case.expect, outcome)
        return CaseScore(case.line, True, outcome.attempts, tokens, not unmet, unmet)
    if isinstance(outcome, PlanRejected):
        checks = ", ".join(sorted(c.value for c in outcome.failed_checks))
        reason = f"no valid plan after {outcome.attempts} attempt(s); failed checks: {checks}"
        return CaseScore(case.line, False, outcome.attempts, tokens, False, (reason,))
    if isinstance(outcome, PlanLLMFailed):
        reason = llm_failure_message(outcome.error)
        return CaseScore(case.line, False, len(outcome.usages) + 1, tokens, False, (reason,))
    assert isinstance(outcome, NoCapabilities)
    return CaseScore(case.line, False, 0, 0, False, (outcome.error_message,))


# --------------------------------------------------------------------------- the report (Req 4.4)


@dataclass(frozen=True, slots=True)
class Report:
    cases: tuple[CaseScore, ...]
    totals: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {"cases": [c.to_dict() for c in self.cases], "totals": dict(self.totals)}

    def passes(self, min_pass: float) -> bool:
        """Both ratios reach ``min_pass`` (Req 4.4)."""
        return self.totals["valid_ratio"] >= min_pass and self.totals["expectation_ratio"] >= min_pass


def summarize(scores: Iterable[CaseScore]) -> Report:
    """The ratios, the mean number of attempts and the tokens of a run. Refuses an empty run."""
    cases = tuple(scores)
    if not cases:
        raise ValueError("cannot summarize an empty evaluation")
    count = len(cases)
    totals = {
        "cases": count,
        "valid_ratio": sum(c.valid for c in cases) / count,
        "expectation_ratio": sum(c.expectations_met for c in cases) / count,
        "mean_attempts": sum(c.attempts for c in cases) / count,
        "total_tokens": sum(c.tokens for c in cases),
    }
    return Report(cases, totals)


# --------------------------------------------------------------------------- running


async def run(planner: Planner, cfg: PlannerConfig, cases: Sequence[Case]) -> list[CaseScore]:
    """Ask the real Planner for a plan for every case, one after the other, and score each outcome."""
    return [score(case, await planner.plan(case.prompt, case.capabilities, cfg)) for case in cases]


__all__ = [
    "MAX_PROMPT_LENGTH",
    "Case",
    "CaseScore",
    "DatasetError",
    "Expect",
    "Report",
    "load_dataset",
    "parse_case",
    "run",
    "score",
    "summarize",
]
