"""The load test stack's files, read as text and YAML (das-scale-resilience task 12.4; Phase 7 Req 4.1, 4.5).

Its Gateway config is the production sample without the limits that would throttle a load generator, its Compose
override replaces the config mount, and the stub's settings, CI cleanup and CLAUDE.md name the new project."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import yaml

from das.config import load_config

ROOT = Path(__file__).resolve().parents[2]
E2E = ROOT / "tests" / "integration" / "e2e"
PRODUCTION = yaml.safe_load((ROOT / "deploy" / "config" / "gateway.yaml").read_text())
LOADTEST_FILE = E2E / "gateway.loadtest.yaml"
LOADTEST = yaml.safe_load(LOADTEST_FILE.read_text())
OVERRIDE = E2E / "docker-compose.loadtest.yml"
PROJECT = "das-e2e-load"


def test_the_config_is_the_production_sample_except_for_the_limits_it_drops() -> None:
    for section in LOADTEST:
        if section != "client_limits":
            assert LOADTEST[section] == PRODUCTION[section], f"{section} differs from deploy/config/gateway.yaml"
    assert {"role", "http_port", "health_port", "transports", "grpc", "limits"} <= set(LOADTEST)


def test_the_config_turns_off_the_per_key_rate_limit_and_the_quotas() -> None:
    default = LOADTEST["client_limits"]["default"]
    assert default == {"rate": 0.0, "max_active_requests": 0, "max_streams": 0}
    assert PRODUCTION["client_limits"]["default"]["rate"] == 60.0, "the production sample really does throttle"


def test_the_config_is_a_valid_gateway_agent_config() -> None:
    env = {  # what Compose supplies to the Gateway container
        "DAS__API_KEYS": "load-key-0123456789", "DAS__REDIS__URL": "redis://r:6379/0",
        "DAS__GRPC__ADVERTISE_HOST": "gateway",
    }
    config = load_config(LOADTEST_FILE, env)
    default = config.client_limits.default
    assert config.role == "gateway" and default.rate == 0 and default.max_active_requests == 0


def test_the_override_replaces_the_config_mount_instead_of_adding_to_it() -> None:
    text = OVERRIDE.read_text()
    assert "volumes: !override" in text, "appended, the production config would stay mounted"
    mounts = re.findall(r"^\s+- (\S+:/etc/das/config\.yaml:ro)$", text, re.MULTILINE)
    assert mounts == ["../tests/integration/e2e/gateway.loadtest.yaml:/etc/das/config.yaml:ro"]
    assert (ROOT / "deploy").joinpath("../tests/integration/e2e/gateway.loadtest.yaml").resolve() == LOADTEST_FILE


def test_the_stub_takes_its_latency_and_failure_rate_from_the_environment_defaulting_to_none() -> None:
    e2e = yaml.safe_load((E2E / "docker-compose.e2e.yml").read_text())
    env: dict[str, Any] = e2e["services"]["llm-stub"]["environment"]
    assert env == {"STUB_LATENCY_MS": "${STUB_LATENCY_MS:-0}", "STUB_FAILURE_RATE": "${STUB_FAILURE_RATE:-0}"}


def test_ci_cleans_up_the_load_project_and_claude_md_names_it() -> None:
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    assert ci.count(PROJECT) == 2, "the log collection and the teardown both name it"
    claude = (ROOT / "CLAUDE.md").read_text()
    for token in (PROJECT, "docker-compose.loadtest.yml", "gateway.loadtest.yaml", "STUB_LATENCY_MS", "reference_run"):
        assert token in claude, f"CLAUDE.md never mentions {token}"


def test_the_reference_run_script_exists_and_defaults_to_the_ramp() -> None:
    script = (E2E / "reference_run.py").read_text()
    assert '"--profile", default="ramp"' in script and "docs/capacity/" in script
