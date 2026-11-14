"""``das loadtest`` against the real stack (das-scale-resilience task 12.4; Phase 7 Req 4.1, 4.3, 4.5, 4.7).

Brings up the Compose stack with the stub LLM and the load test Gateway config under the project ``das-e2e-load`` and
runs the built-in ``smoke`` profile through the real CLI: every Request must be accepted and complete, the report must
carry the numbers and the context of the run, a run compared with itself must not regress and a halved throughput must,
and the stub's failure rate must reach the platform (every Request fails when every LLM call does). The stack is always
removed with ``docker compose down -v``; the module is skipped without Docker."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml

from das.cli import main
from tests.integration.e2e.stackkit import API_KEY, ComposeStack, docker_available, running_stack

pytestmark = pytest.mark.integration

OVERRIDE = Path(__file__).with_name("docker-compose.loadtest.yml")
LATENCY_MS = "20"


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[ComposeStack]:
    if not docker_available():
        pytest.skip("Docker / docker compose not available; the load test end-to-end test needs the Compose stack")
    with running_stack(
        "das-e2e-load", [OVERRIDE], (), tmp_path_factory.mktemp("load"), extra_env={"STUB_LATENCY_MS": LATENCY_MS}
    ) as running:
        yield running


def run(stack: ComposeStack, out: Path, profile: str, *extra: str) -> int:
    return main(
        ["loadtest", "--gateway", stack.gateway_url, "--key", API_KEY, "--profile", profile, "--out", str(out),
         "--transport", "rabbitmq", "--setting", "managers=2", "--setting", "workers=2",
         "--setting", f"stub_latency_ms={LATENCY_MS}", "--request-timeout", "90", *extra],
        {},
    )


@pytest.fixture(scope="module")
def smoke(stack: ComposeStack, tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    out = tmp_path_factory.mktemp("report") / "smoke.json"
    assert run(stack, out, "smoke") == 0
    return json.loads(out.read_text())


def test_every_request_of_the_smoke_profile_completes(smoke: dict[str, Any]) -> None:
    results = smoke["results"]
    assert results["requests"] == 20 and results["completed"] == 20 and results["errors"] == 0, results
    assert results["errors_by_kind"] == {} and results["throughput_rps"] > 0.5
    assert smoke["saturation_rps"] == 2


def test_the_latencies_are_plausible_for_a_planned_and_executed_request(smoke: dict[str, Any]) -> None:
    results = smoke["results"]
    assert results["submit_ms"]["p99"] < 500, "the Gateway accepts a Request within 500 ms (Phase 1 Req 1.1)"
    assert 20 < results["total_ms"]["p50"] < 30_000 and results["total_ms"]["p99"] >= results["total_ms"]["p50"]
    assert results["max_late_ms"] < 500, "the load generator kept to its own schedule"


def test_the_report_carries_the_context_of_the_run(smoke: dict[str, Any]) -> None:
    assert smoke["transport"] == "rabbitmq"
    assert smoke["settings"] == {"managers": "2", "workers": "2", "stub_latency_ms": LATENCY_MS}
    assert smoke["machine"]["cpu_count"] >= 1 and smoke["machine"]["os"] and smoke["das_version"]
    assert API_KEY not in json.dumps(smoke)


def test_a_run_compared_with_itself_has_not_regressed_and_half_the_throughput_has(
    smoke: dict[str, Any], tmp_path: Path
) -> None:
    base = tmp_path / "base.json"
    base.write_text(json.dumps(smoke))
    assert main(["loadtest-compare", str(base), str(base), "--max-regression", "0.1"], {}) == 0
    worse = json.loads(json.dumps(smoke))
    worse["results"]["throughput_rps"] /= 2
    new = tmp_path / "new.json"
    new.write_text(json.dumps(worse))
    assert main(["loadtest-compare", str(base), str(new), "--max-regression", "0.1"], {}) == 1


def test_the_stubs_failure_rate_reaches_the_platform(stack: ComposeStack, tmp_path: Path) -> None:
    """Req 4.5: with every LLM call failing, no Request can complete, and the run says how they ended."""
    stack.env["STUB_FAILURE_RATE"] = "1.0"
    try:
        stack.compose("up", "-d", "--no-deps", "--force-recreate", "llm-stub", timeout=120)
        profile = tmp_path / "short.yaml"
        profile.write_text(yaml.safe_dump({"name": "short", "rate": 1, "duration": 4}))
        out = tmp_path / "failing.json"
        assert run(stack, out, str(profile)) == 0
        results = json.loads(out.read_text())["results"]
        assert results["completed"] == 0 and results["errors"] == results["requests"] == 4, results
        assert set(results["errors_by_kind"]) <= {"failed", "timeout"}, results["errors_by_kind"]
    finally:
        stack.env["STUB_FAILURE_RATE"] = "0"
        stack.compose("up", "-d", "--no-deps", "--force-recreate", "llm-stub", timeout=120)
