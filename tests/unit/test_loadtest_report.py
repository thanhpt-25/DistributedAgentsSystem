"""The Capacity_Report (das-scale-resilience task 12.1; Phase 7 Req 4.3, 4.4): the numbers, the context that goes with
them, and that nothing secret is in it."""

from __future__ import annotations

import json
from importlib import metadata
from typing import Any

import pytest

from das.loadtest.profile import Profile, Step
from das.loadtest.report import (
    DEFAULT_MAX_ERROR_RATE, DEFAULT_MAX_P99_MS, MAX_SETTING_VALUE, SCHEMA, Limits, ReportError, build_report,
    check_transport, das_version, machine_description, parse_settings, step_stats,
)
from das.loadtest.runner import RunResult, Sample

MACHINE = {"cpu_count": 8, "memory_bytes": 16 * 2**30, "os": "TestOS-1", "arch": "arm64", "python": "3.12.0"}


def sample(index: int, step: int, outcome: str, submit: float | None, total: float | None, late: float = 0.0) -> Sample:
    return Sample(index, step, outcome, submit, total, late)


def two_steps() -> RunResult:
    """Step 0 (2 rps for 2 s): 4 offered, 4 completed. Step 1 (5 rps for 1 s): 5 offered, 2 done, 1 timeout, 2 429s."""
    samples = [sample(i, 0, "completed", 5.0, 100.0 + 10 * i) for i in range(4)]
    samples += [
        sample(4, 1, "completed", 6.0, 900.0), sample(5, 1, "completed", 7.0, 1200.0),
        sample(6, 1, "timeout", 8.0, None, 30.0),
        sample(7, 1, "submit_429", 2.0, None), sample(8, 1, "submit_429", 3.0, None),
    ]
    profile = Profile("two", (Step(2, 2), Step(5, 1)), ("x",))
    return RunResult(profile, tuple(samples), "2026-11-14T12:00:00+00:00", 5.0)


def report(**kwargs: Any) -> dict[str, Any]:
    return build_report(
        two_steps(), transport="rabbitmq", settings={"managers": "2"}, machine=MACHINE, version="9.9.9", **kwargs
    )


# --------------------------------------------------------------------------- the numbers


def test_the_totals_are_counted_from_the_samples() -> None:
    results = report()["results"]
    assert (results["requests"], results["completed"], results["errors"]) == (9, 6, 3)
    assert results["error_rate"] == pytest.approx(3 / 9)
    assert results["errors_by_kind"] == {"submit_429": 2, "timeout": 1}


def test_throughput_is_completions_per_second_of_the_run_and_the_offered_rate_is_from_the_profile() -> None:
    results = report()["results"]
    assert results["throughput_rps"] == pytest.approx(6 / 5.0) and results["elapsed_s"] == 5.0
    assert results["offered_rps"] == pytest.approx(9 / 3.0)  # 9 Requests over the 3 s the profile offers


def test_the_total_time_percentiles_are_those_of_the_completed_requests_only() -> None:
    total = report()["results"]["total_ms"]
    assert total["count"] == 6 and total["min"] == 100.0 and total["max"] == 1200.0
    assert total["p50"] == 120.0 and total["p99"] == 1200.0


def test_a_request_that_failed_has_no_place_in_the_completed_latencies_even_with_a_time() -> None:
    result = two_steps()
    failed = sample(9, 1, "failed", 4.0, 50_000.0)  # it reached a terminal state, but not a good one
    built = build_report(
        RunResult(result.profile, (*result.samples, failed), result.started_at, result.elapsed_s),
        transport="rabbitmq", settings={}, machine=MACHINE, version="1",
    )
    assert built["results"]["total_ms"]["count"] == 6 and built["results"]["total_ms"]["max"] == 1200.0
    assert built["results"]["errors_by_kind"]["failed"] == 1 and built["results"]["submit_ms"]["count"] == 10


def test_the_submit_time_percentiles_include_every_request_that_got_an_answer() -> None:
    submit = report()["results"]["submit_ms"]
    assert submit["count"] == 9 and submit["min"] == 2.0 and submit["max"] == 8.0


def test_the_lateness_of_the_worst_send_is_reported_so_a_slow_generator_shows() -> None:
    assert report()["results"]["max_late_ms"] == 30.0


def test_each_step_has_its_own_counts_and_p99() -> None:
    first, second = report()["steps"]
    assert (first["rate"], first["seconds"], first["offered"], first["completed"], first["errors"]) == (2, 2, 4, 4, 0)
    assert first["error_rate"] == 0.0 and first["p99_ms"] == 130.0
    assert (second["offered"], second["completed"], second["errors"]) == (5, 2, 3)
    assert second["error_rate"] == 0.6 and second["p99_ms"] == 1200.0


def test_step_stats_are_what_the_saturation_point_is_computed_from() -> None:
    stats = step_stats(two_steps())
    assert [(s.rate, s.offered, s.completed, s.errors) for s in stats] == [(2, 4, 4, 0), (5, 5, 2, 3)]


def test_the_saturation_point_is_the_highest_step_inside_the_limits() -> None:
    assert report(limits=Limits(max_error_rate=0.7, max_p99_ms=5000))["saturation_rps"] == 5
    assert report(limits=Limits(max_error_rate=0.1, max_p99_ms=5000))["saturation_rps"] == 2, "step 2 had 60 % errors"
    assert report(limits=Limits(max_error_rate=0.7, max_p99_ms=1000))["saturation_rps"] == 2, "step 2 p99 was 1200 ms"
    assert report(limits=Limits(max_error_rate=0.7, max_p99_ms=100))["saturation_rps"] is None


def test_the_default_limits_are_written_into_the_report() -> None:
    assert report()["limits"] == {"max_error_rate": DEFAULT_MAX_ERROR_RATE, "max_p99_ms": DEFAULT_MAX_P99_MS}


def test_a_step_where_nothing_completed_has_no_p99_and_the_report_still_builds() -> None:
    dead = RunResult(Profile("p", (Step(2, 1),), ("x",)), (sample(0, 0, "submit_failed", 1.0, None),) * 2, "t", 1.0)
    built = build_report(dead, transport="http", settings={}, machine=MACHINE, version="1")
    assert built["results"]["total_ms"]["count"] == 0 and built["results"]["total_ms"]["p99"] is None
    assert built["steps"][0]["p99_ms"] is None and built["saturation_rps"] is None
    assert built["results"]["throughput_rps"] == 0.0 and built["results"]["error_rate"] == 1.0


# --------------------------------------------------------------------------- the context (Req 4.4)


def test_a_report_says_what_machine_which_version_which_transport_and_which_settings() -> None:
    built = report()
    assert built["schema"] == SCHEMA == 1
    assert built["machine"] == MACHINE and built["das_version"] == "9.9.9"
    assert built["transport"] == "rabbitmq" and built["settings"] == {"managers": "2"}
    steps = [{"rate": 2, "seconds": 2}, {"rate": 5, "seconds": 1}]
    assert built["profile"] == {"name": "two", "steps": steps, "prompts": 1}
    assert built["started_at"] == "2026-11-14T12:00:00+00:00"


def test_without_a_machine_or_version_they_are_measured_here() -> None:
    built = build_report(two_steps(), transport="kafka", settings={})
    assert built["machine"]["cpu_count"] == machine_description()["cpu_count"] and built["das_version"] == das_version()


def test_the_machine_description_has_cpus_memory_and_os() -> None:
    machine = machine_description()
    assert machine["cpu_count"] >= 1 and machine["os"] and machine["arch"] and machine["python"].count(".") == 2
    assert machine["memory_bytes"] is None or machine["memory_bytes"] > 2**20


def test_the_version_is_the_installed_packages() -> None:
    assert das_version() == metadata.version("das")


@pytest.mark.parametrize("name", ["", "has space", "x" * 65, "-lead", "a/b"])
def test_a_transport_that_is_not_a_name_is_refused(name: str) -> None:
    with pytest.raises(ReportError, match="transport"):
        build_report(two_steps(), transport=name, settings={}, machine=MACHINE)


@pytest.mark.parametrize("name", ["rabbitmq", "http", "kafka", "rabbitmq+tls", "grpc.v2"])
def test_a_transport_name_is_kept(name: str) -> None:
    assert check_transport(name) == name


def test_a_report_is_json_and_holds_no_key() -> None:
    text = json.dumps(report())
    assert json.loads(text)["schema"] == 1 and "api_key" not in text.lower() and "x-api-key" not in text.lower()


# --------------------------------------------------------------------------- settings and limits


def test_settings_are_key_value_pairs_and_a_value_may_hold_equals_signs() -> None:
    assert parse_settings(["managers=2", "stub_latency_ms=50", "note=a=b"]) == {
        "managers": "2", "stub_latency_ms": "50", "note": "a=b",
    }


@pytest.mark.parametrize("bad", ["managers", "=2", "bad key=1", "-x=1", "a/b=1"])
def test_a_setting_that_is_not_key_equals_value_is_refused(bad: str) -> None:
    with pytest.raises(ReportError, match="KEY=VALUE"):
        parse_settings([bad])


def test_a_setting_given_twice_or_too_long_is_refused() -> None:
    with pytest.raises(ReportError, match="given twice"):
        parse_settings(["a=1", "a=2"])
    with pytest.raises(ReportError, match="longer than"):
        parse_settings(["a=" + "x" * (MAX_SETTING_VALUE + 1)])
    assert parse_settings(["a=" + "x" * MAX_SETTING_VALUE])


@pytest.mark.parametrize("bad", [(-0.1, 10.0), (1.1, 10.0), (0.1, 0.0), (0.1, -5.0)])
def test_limits_outside_their_range_are_refused(bad: tuple[float, float]) -> None:
    with pytest.raises(ReportError):
        Limits(*bad)


def test_limits_on_the_edge_are_kept() -> None:
    assert Limits(0.0, 0.001).max_error_rate == 0.0 and Limits(1.0, 1e9).max_error_rate == 1.0
