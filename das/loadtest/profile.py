"""Load_Profiles and the arrival schedule (das-scale-resilience task 12.1; Phase 7 Req 4.2).

A profile says how fast Requests arrive, for how long, and what they ask: a list of steps (rate in Requests per
second, duration in seconds) and a prompt mix. ``smoke``, ``steady`` and ``ramp`` are built in; a YAML file defines
others, either stepped (``steps``) or constant (``rate`` and ``duration``). :func:`arrivals` turns a profile into the
exact times at which Requests are to be sent, fixed in advance: an open-loop schedule, so a slow server cannot slow
the offered load."""

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

import yaml

MAX_RATE: Final = 1000.0
MAX_STEP_SECONDS: Final = 3600.0
MAX_STEPS: Final = 50
MAX_PROMPTS: Final = 100
MAX_PROMPT_LENGTH: Final = 4000
_NAME: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,63}")
_KEYS: Final = {"name", "steps", "rate", "duration", "prompts"}

#: What the built-in profiles ask. The stub LLM answers any prompt, so the mix only varies the work a real one would do.
DEFAULT_PROMPTS: Final = (
    "Explain the CAP theorem in two paragraphs.",
    "Review this snippet and list its problems: def add(a, b): return a - b",
    "Summarize the difference between at-least-once and exactly-once delivery.",
)


class ProfileError(ValueError):
    """A profile that cannot be used; the message names the field."""


@dataclass(frozen=True, slots=True)
class Step:
    rate: float  # Requests per second
    seconds: float


@dataclass(frozen=True, slots=True)
class Profile:
    name: str
    steps: tuple[Step, ...]
    prompts: tuple[str, ...] = DEFAULT_PROMPTS

    @property
    def total_seconds(self) -> float:
        return sum(s.seconds for s in self.steps)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "steps": [{"rate": s.rate, "seconds": s.seconds} for s in self.steps],
            "prompts": len(self.prompts),
        }


@dataclass(frozen=True, slots=True)
class Arrival:
    """One Request to send: which step it belongs to, when (seconds after the run starts) and what to ask."""

    index: int
    step: int
    at: float
    prompt: str


def requests_in(step: Step) -> int:
    """How many Requests a step offers: its rate times its duration, and at least one."""
    return max(1, round(step.rate * step.seconds))


def arrivals(profile: Profile) -> Iterator[Arrival]:
    """The schedule: each step's Requests evenly spaced from the step's start, the prompts taken in turn."""
    start, index = 0.0, 0
    for number, step in enumerate(profile.steps):
        for k in range(requests_in(step)):
            yield Arrival(index, number, start + k / step.rate, profile.prompts[index % len(profile.prompts)])
            index += 1
        start += step.seconds


BUILTIN: Final[dict[str, Profile]] = {
    "smoke": Profile("smoke", (Step(2, 10),)),
    "steady": Profile("steady", (Step(10, 60),)),
    "ramp": Profile("ramp", tuple(Step(rate, 30) for rate in (2, 5, 10, 20, 40))),
}


# --------------------------------------------------------------------------- reading a file


def _number(value: Any, field: str, low: float, high: float) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ProfileError(f"{field} must be a number")
    if not low < value <= high:
        raise ProfileError(f"{field} must be above {low:g} and at most {high:g}")
    return float(value)


def _step(raw: Any, where: str) -> Step:
    if not isinstance(raw, dict) or set(raw) != {"rate", "seconds"}:
        raise ProfileError(f"{where} must have exactly rate and seconds")
    rate = _number(raw["rate"], f"{where}.rate", 0, MAX_RATE)
    return Step(rate, _number(raw["seconds"], f"{where}.seconds", 0, MAX_STEP_SECONDS))


def _prompts(raw: Any) -> tuple[str, ...]:
    if raw is None:
        return DEFAULT_PROMPTS
    if not isinstance(raw, list) or not 1 <= len(raw) <= MAX_PROMPTS:
        raise ProfileError(f"prompts must be a list of 1 to {MAX_PROMPTS} texts")
    for number, prompt in enumerate(raw):
        if not isinstance(prompt, str) or not prompt.strip() or len(prompt) > MAX_PROMPT_LENGTH:
            raise ProfileError(f"prompts[{number}] must be text of 1 to {MAX_PROMPT_LENGTH} characters")
    return tuple(raw)


def parse_profile(data: Any, default_name: str) -> Profile:
    if not isinstance(data, dict):
        raise ProfileError("a profile file must hold a mapping")
    unknown = sorted(set(data) - _KEYS)
    if unknown:
        raise ProfileError(f"unknown field {unknown[0]}")
    name = data.get("name", default_name)
    if not isinstance(name, str) or _NAME.fullmatch(name) is None:
        raise ProfileError("name must be 1 to 64 letters, digits, dots, hyphens or underscores")
    if "steps" in data:
        if "rate" in data or "duration" in data:
            raise ProfileError("give either steps or rate and duration, not both")
        raw = data["steps"]
        if not isinstance(raw, list) or not 1 <= len(raw) <= MAX_STEPS:
            raise ProfileError(f"steps must be a list of 1 to {MAX_STEPS} entries")
        steps = tuple(_step(entry, f"steps[{n}]") for n, entry in enumerate(raw))
    elif "rate" in data and "duration" in data:
        rate = _number(data["rate"], "rate", 0, MAX_RATE)
        steps = (Step(rate, _number(data["duration"], "duration", 0, MAX_STEP_SECONDS)),)
    else:
        raise ProfileError("give steps, or both rate and duration")
    return Profile(name, steps, _prompts(data.get("prompts")))


def load_profile(name_or_path: str) -> Profile:
    """A built-in profile by name, or the profile in a YAML file."""
    if name_or_path in BUILTIN:
        return BUILTIN[name_or_path]
    path = Path(name_or_path)
    if not path.is_file():
        raise ProfileError(
            f"unknown profile {name_or_path!r}: use one of {', '.join(sorted(BUILTIN))} or the path of a YAML file"
        )
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ProfileError(f"{path} cannot be read as YAML: {type(exc).__name__}") from exc
    return parse_profile(data, path.stem)


__all__ = [
    "BUILTIN", "DEFAULT_PROMPTS", "Arrival", "Profile", "ProfileError", "Step", "arrivals", "load_profile",
    "parse_profile", "requests_in",
]
