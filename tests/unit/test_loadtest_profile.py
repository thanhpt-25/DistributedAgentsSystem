"""Load_Profiles and the arrival schedule (das-scale-resilience task 12.1; Phase 7 Req 4.2)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from das.loadtest.profile import (
    BUILTIN, DEFAULT_PROMPTS, MAX_PROMPTS, MAX_RATE, MAX_STEP_SECONDS, MAX_STEPS, Profile, ProfileError, Step, arrivals,
    load_profile, parse_profile, requests_in,
)

# --------------------------------------------------------------------------- the built-in profiles


def test_the_three_built_in_profiles_are_the_specified_ones() -> None:
    assert set(BUILTIN) == {"smoke", "steady", "ramp"}
    assert BUILTIN["smoke"].steps == (Step(2, 10),)
    assert BUILTIN["steady"].steps == (Step(10, 60),)
    assert BUILTIN["ramp"].steps == tuple(Step(rate, 30) for rate in (2, 5, 10, 20, 40))
    assert all(p.prompts == DEFAULT_PROMPTS and p.name == name for name, p in BUILTIN.items())


def test_a_built_in_profile_is_loaded_by_name() -> None:
    assert load_profile("smoke") is BUILTIN["smoke"]


def test_the_ramp_runs_150_seconds_and_offers_2310_requests() -> None:
    ramp = BUILTIN["ramp"]
    assert ramp.total_seconds == 150 and sum(requests_in(s) for s in ramp.steps) == 60 + 150 + 300 + 600 + 1200


# --------------------------------------------------------------------------- the schedule


def test_requests_are_evenly_spaced_within_each_step() -> None:
    schedule = list(arrivals(Profile("p", (Step(4, 2),))))
    assert [a.at for a in schedule] == [0.0, 0.25, 0.5, 0.75, 1.0, 1.25, 1.5, 1.75]


def test_the_next_step_starts_where_the_last_one_ended() -> None:
    schedule = list(arrivals(Profile("p", (Step(1, 2), Step(2, 1)))))
    assert [(a.step, a.at) for a in schedule] == [(0, 0.0), (0, 1.0), (1, 2.0), (1, 2.5)]


def test_the_prompts_are_taken_in_turn_across_steps() -> None:
    schedule = list(arrivals(Profile("p", (Step(1, 2), Step(1, 1)), ("a", "b"))))
    assert [a.prompt for a in schedule] == ["a", "b", "a"] and [a.index for a in schedule] == [0, 1, 2]


def test_a_step_always_offers_at_least_one_request() -> None:
    assert requests_in(Step(0.1, 1)) == 1 and len(list(arrivals(Profile("p", (Step(0.1, 1),))))) == 1


def test_the_schedule_is_fixed_before_anything_is_sent() -> None:
    profile = BUILTIN["smoke"]
    assert [a.at for a in arrivals(profile)] == [a.at for a in arrivals(profile)]  # nothing depends on a server


# --------------------------------------------------------------------------- a file


def write(tmp_path: Path, data: Any, name: str = "mine.yaml") -> str:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return str(path)


def test_a_constant_profile_is_one_step(tmp_path: Path) -> None:
    profile = load_profile(write(tmp_path, {"rate": 5, "duration": 30}))
    assert profile == Profile("mine", (Step(5, 30),), DEFAULT_PROMPTS)


def test_a_stepped_profile_with_its_own_name_and_prompts(tmp_path: Path) -> None:
    steps = [{"rate": 1, "seconds": 5}, {"rate": 3.5, "seconds": 10}]
    data = {"name": "my-ramp", "steps": steps, "prompts": ["a", "b"]}
    assert load_profile(write(tmp_path, data)) == Profile("my-ramp", (Step(1, 5), Step(3.5, 10)), ("a", "b"))


def test_the_name_defaults_to_the_file_name(tmp_path: Path) -> None:
    assert load_profile(write(tmp_path, {"rate": 1, "duration": 1}, "night-run.yaml")).name == "night-run"


@pytest.mark.parametrize(("data", "message"), [
    ([], "must hold a mapping"),
    ({"rate": 1}, "give steps, or both rate and duration"),
    ({"duration": 1}, "give steps, or both rate and duration"),
    ({}, "give steps, or both rate and duration"),
    ({"steps": [{"rate": 1, "seconds": 1}], "rate": 1}, "either steps or rate and duration"),
    ({"rate": 1, "duration": 1, "bogus": 1}, "unknown field bogus"),
    ({"rate": 0, "duration": 1}, "rate must be above 0"),
    ({"rate": -1, "duration": 1}, "rate must be above 0"),
    ({"rate": MAX_RATE + 1, "duration": 1}, "rate must be above 0 and at most"),
    ({"rate": 1, "duration": 0}, "duration must be above 0"),
    ({"rate": 1, "duration": MAX_STEP_SECONDS + 1}, "duration must be above 0 and at most"),
    ({"rate": "fast", "duration": 1}, "rate must be a number"),
    ({"rate": True, "duration": 1}, "rate must be a number"),
    ({"steps": []}, "steps must be a list of 1 to"),
    ({"steps": [{"rate": 1, "seconds": 1}] * (MAX_STEPS + 1)}, "steps must be a list of 1 to"),
    ({"steps": [{"rate": 1}]}, r"steps\[0\] must have exactly rate and seconds"),
    ({"steps": [{"rate": 1, "seconds": 1, "x": 2}]}, "exactly rate and seconds"),
    ({"steps": [{"rate": 1, "seconds": 1}, {"rate": 0, "seconds": 1}]}, r"steps\[1\].rate must be above 0"),
    ({"rate": 1, "duration": 1, "name": ""}, "name must be"),
    ({"rate": 1, "duration": 1, "name": "has space"}, "name must be"),
    ({"rate": 1, "duration": 1, "name": "x" * 65}, "name must be"),
    ({"rate": 1, "duration": 1, "prompts": []}, "prompts must be a list of 1 to"),
    ({"rate": 1, "duration": 1, "prompts": ["ok"] * (MAX_PROMPTS + 1)}, "prompts must be a list of 1 to"),
    ({"rate": 1, "duration": 1, "prompts": ["ok", "  "]}, r"prompts\[1\] must be text"),
    ({"rate": 1, "duration": 1, "prompts": [5]}, r"prompts\[0\] must be text"),
    ({"rate": 1, "duration": 1, "prompts": "one"}, "prompts must be a list"),
])
def test_a_profile_that_cannot_be_used_is_refused_naming_the_field(tmp_path: Path, data: Any, message: str) -> None:
    with pytest.raises(ProfileError, match=message):
        load_profile(write(tmp_path, data))


@pytest.mark.parametrize(
    "edge", [{"rate": MAX_RATE, "duration": MAX_STEP_SECONDS}, {"rate": 0.001, "duration": 0.001}]
)
def test_values_on_the_edge_of_their_range_are_kept(edge: dict[str, float]) -> None:
    assert parse_profile(edge, "edge").steps == (Step(edge["rate"], edge["duration"]),)


def test_the_longest_allowed_prompt_is_kept_and_one_more_is_refused() -> None:
    assert parse_profile({"rate": 1, "duration": 1, "prompts": ["x" * 4000]}, "p").prompts == ("x" * 4000,)
    with pytest.raises(ProfileError):
        parse_profile({"rate": 1, "duration": 1, "prompts": ["x" * 4001]}, "p")


def test_an_unknown_name_that_is_not_a_file_says_what_is_available() -> None:
    with pytest.raises(ProfileError, match=r"unknown profile 'nope'.*ramp, smoke, steady.*YAML"):
        load_profile("nope")


def test_a_file_that_is_not_yaml_is_refused_without_a_traceback(tmp_path: Path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text("a: [unclosed", encoding="utf-8")
    with pytest.raises(ProfileError, match="cannot be read as YAML"):
        load_profile(str(path))


def test_the_profile_dict_for_a_report_has_the_steps_and_the_size_of_the_prompt_mix() -> None:
    assert BUILTIN["smoke"].to_dict() == {"name": "smoke", "steps": [{"rate": 2, "seconds": 10}], "prompts": 3}
