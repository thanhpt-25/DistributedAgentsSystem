"""The client API e2e stack's files agree with each other and with the product (das-client-api task 11.1).

The e2e tests bring up a Compose project from three files (``deploy/docker-compose.yml``, the Phase 3 e2e override,
and ``docker-compose.client-api.e2e.yml``) with a Gateway config of its own and a webhook sink. A mistake in any
of them would only show as a failed Docker run, minutes later. These checks catch it at once, without starting
anything: the config loads through the real loader, its fingerprint is the real one, the override wires the sink
and the secret consistently, ``ci.yml`` tears the project down, and (where Compose is installed) the three files
really merge into the stack the tests expect."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from das.config import load_config
from das.ops.keys import key_fingerprint
from tests.integration.e2e.client_api_constants import (
    GATEWAY_B_PORT_VAR,
    LIMITED_FINGERPRINT,
    LIMITED_KEY,
    MAIN_KEY,
    PROJECT,
    SINK_HOST_PORT_VAR,
    SINK_PORT,
    SINK_SERVICE,
    SINK_URL,
    WEBHOOK_SECRET,
    WEBHOOK_SECRET_NAME,
)

ROOT = Path(__file__).resolve().parents[2]
E2E = ROOT / "tests" / "integration" / "e2e"
DEPLOY = ROOT / "deploy"
GATEWAY_CONFIG = E2E / "gateway.client-api.yaml"
OVERRIDE = E2E / "docker-compose.client-api.e2e.yml"
PHASE3_OVERRIDE = E2E / "docker-compose.e2e.yml"
CI = ROOT / ".github" / "workflows" / "ci.yml"
ENV = {
    "DAS__REDIS__URL": "redis://redis:6379/0",
    "DAS__GRPC__ADVERTISE_HOST": "gateway",
    "DAS__API_KEYS": f"{MAIN_KEY},{LIMITED_KEY}",
    f"DAS__WEBHOOK_SECRETS__{WEBHOOK_SECRET_NAME.upper()}": WEBHOOK_SECRET,
}
PHASE_4_SECTIONS = {"client_limits", "events", "webhook"}
GATEWAYS = ("gateway", "gateway-b")


class ComposeLoader(yaml.SafeLoader):
    """A ``SafeLoader`` that accepts Compose's ``!override`` and ``!reset`` tags (the tag is not part of the data)."""


def _untag(loader: yaml.SafeLoader, tag_suffix: str, node: yaml.Node) -> Any:
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node, deep=True)
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node, deep=True)
    return loader.construct_scalar(node)


ComposeLoader.add_multi_constructor("!", _untag)


@pytest.fixture(scope="module")
def override() -> dict[str, Any]:
    return yaml.load(OVERRIDE.read_text(encoding="utf-8"), Loader=ComposeLoader)  # noqa: S506  (a SafeLoader subclass)


@pytest.fixture(scope="module")
def override_text() -> str:
    return OVERRIDE.read_text(encoding="utf-8")


# --------------------------------------------------------------------------- the e2e Gateway config


def test_the_gateway_config_loads_with_the_stacks_environment() -> None:
    assert load_config(GATEWAY_CONFIG, ENV).role == "gateway"


def test_the_gateway_config_differs_from_the_production_sample_only_in_the_client_api_sections() -> None:
    mine = yaml.safe_load(GATEWAY_CONFIG.read_text(encoding="utf-8"))
    sample = yaml.safe_load((DEPLOY / "config" / "gateway.yaml").read_text(encoding="utf-8"))

    def rest(config: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in config.items() if k not in PHASE_4_SECTIONS}

    assert rest(mine) == rest(sample)


def test_the_limited_key_fingerprint_is_the_real_one() -> None:
    assert key_fingerprint(LIMITED_KEY.encode()) == LIMITED_FINGERPRINT
    assert key_fingerprint(MAIN_KEY.encode()) != LIMITED_FINGERPRINT


def test_the_webhook_settings_point_at_the_sink_and_retry_quickly() -> None:
    config = load_config(GATEWAY_CONFIG, ENV)
    default = config.client_limits.default
    assert default.webhook_url == SINK_URL and default.webhook_secret_name == WEBHOOK_SECRET_NAME
    assert config.client_limits.allow_insecure_webhooks is True  # the sink is plain http
    assert config.webhook.base_backoff == 1.0  # a retry in about a second, so the test is not slow
    assert config.webhook.base_backoff <= config.webhook.max_backoff <= 10
    assert config.webhook.max_attempts >= 3  # the sink fails the first two attempts


def test_the_main_key_is_not_throttled_and_the_limited_key_is() -> None:
    limits = load_config(GATEWAY_CONFIG, ENV).client_limits
    main = limits.settings_for(key_fingerprint(MAIN_KEY.encode()))
    assert main.rate >= 100 and main.burst >= 1000 and main.max_active_requests >= 500 and main.max_streams >= 50
    limited = limits.settings_for(LIMITED_FINGERPRINT)
    assert (limited.rate, limited.burst) == (1.0, 5)  # exhausted by the shared-bucket test within a second or two


# --------------------------------------------------------------------------- the Compose override


def test_the_override_defines_two_gateways_and_a_sink(override: dict[str, Any]) -> None:
    assert {*GATEWAYS, SINK_SERVICE} <= set(override["services"])


def test_the_second_gateway_extends_the_first_with_its_own_port_and_no_second_build(
    override: dict[str, Any], override_text: str
) -> None:
    second = override["services"]["gateway-b"]
    assert second["extends"]["service"] == "gateway"
    # `extends.file` resolves against the project directory (deploy/), like every other relative path in the override.
    assert (DEPLOY / second["extends"]["file"]).resolve() == (DEPLOY / "docker-compose.yml").resolve()
    assert second["ports"] == [f"${{{GATEWAY_B_PORT_VAR}}}:8080"]
    # Compose appends list entries unless told otherwise: without !override the base port would be published twice.
    assert re.search(r"^\s+ports: !override\b", override_text, re.MULTILINE)
    assert re.search(r"^\s+build: !reset\b", override_text, re.MULTILINE)  # reuses the image the first one builds


def test_both_gateways_mount_the_e2e_config_and_get_the_secret_and_wait_for_the_sink(
    override: dict[str, Any], override_text: str
) -> None:
    for name in GATEWAYS:
        service = override["services"][name]
        assert service["volumes"] == [f"../tests/integration/e2e/{GATEWAY_CONFIG.name}:/etc/das/config.yaml:ro"]
        assert service["environment"][f"DAS__WEBHOOK_SECRETS__{WEBHOOK_SECRET_NAME.upper()}"] == WEBHOOK_SECRET
        assert service["depends_on"][SINK_SERVICE]["condition"] == "service_healthy"
    assert len(re.findall(r"^\s+volumes: !override\b", override_text, re.MULTILINE)) >= 2  # replaced, not appended
    assert GATEWAY_CONFIG.is_file()


def test_the_sink_service(override: dict[str, Any]) -> None:
    sink = override["services"][SINK_SERVICE]
    assert sink["image"].startswith("python:")  # like llm-stub: no image to build
    assert sink["environment"]["SINK_SECRET"] == WEBHOOK_SECRET
    assert str(sink["environment"]["SINK_PORT"]) == str(SINK_PORT)
    assert sink["ports"] == [f"127.0.0.1:${{{SINK_HOST_PORT_VAR}}}:{SINK_PORT}"]
    assert (E2E / "webhook_sink.py").is_file()
    mounts = [v for v in sink["volumes"] if v.startswith("../tests/integration/e2e/webhook_sink.py:")]
    assert len(mounts) == 1 and mounts[0].endswith(":ro")
    assert sink["healthcheck"]["test"] and sink["networks"] == ["das"]


def test_the_override_does_not_repeat_what_the_phase_3_override_sets() -> None:
    """The stub LLM and the 10 s lease come from the layer below; setting them again could only drift."""
    own = OVERRIDE.read_text(encoding="utf-8")
    for setting in ("llm-stub", "DAS__LLM__", "LEASE_DURATION"):
        assert setting not in own, f"the override repeats {setting}, which docker-compose.e2e.yml already sets"
    assert "llm-stub" in PHASE3_OVERRIDE.read_text(encoding="utf-8")


# --------------------------------------------------------------------------- CI


def test_ci_collects_logs_from_and_tears_down_the_client_api_project() -> None:
    lines = CI.read_text(encoding="utf-8").splitlines()
    loops = [line for line in lines if line.strip().startswith("for project in")]
    assert len(loops) == 2, "expected the log-collection loop and the teardown loop"
    for line in loops:
        named = re.search(rf"(?<![\w-]){re.escape(PROJECT)}(?![\w-])", line)
        assert named, f"{PROJECT} is missing from: {line.strip()}"


# --------------------------------------------------------------------------- the merged stack (needs the compose CLI)


def compose_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        return subprocess.run(["docker", "compose", "version"], capture_output=True, timeout=30).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


@pytest.mark.skipif(not compose_available(), reason="the docker compose CLI is not installed")
def test_the_three_compose_files_merge_into_the_stack_the_tests_expect(tmp_path: Path) -> None:
    """``docker compose config`` only reads files: no daemon, no pulls, no builds."""
    env_file = tmp_path / "e2e.env"
    env_file.write_text(
        "\n".join(
            [
                "RABBITMQ_USER=das",
                "RABBITMQ_PASS=e2e-rabbit-password",
                f"DAS_GATEWAY_API_KEYS={MAIN_KEY},{LIMITED_KEY}",
                "DAS_GATEWAY_PORT=18101",
                f"{GATEWAY_B_PORT_VAR}=18102",
                f"{SINK_HOST_PORT_VAR}=18103",
                "DAS_RABBITMQ_MGMT_PORT=18104",
            ]
        )
    )
    command = [
        "docker", "compose", "--project-directory", str(DEPLOY), "-p", PROJECT,
        "-f", str(DEPLOY / "docker-compose.yml"), "-f", str(PHASE3_OVERRIDE), "-f", str(OVERRIDE),
        "--env-file", str(env_file), "config", "--format", "json",
    ]  # fmt: skip
    done = subprocess.run(command, capture_output=True, text=True, timeout=120, stdin=subprocess.DEVNULL)
    assert done.returncode == 0, done.stderr[-3000:]
    services = json.loads(done.stdout)["services"]

    assert {"gateway", "gateway-b", SINK_SERVICE, "llm-stub", "manager"} <= set(services)
    for name, host_port in (("gateway", "18101"), ("gateway-b", "18102")):
        service = services[name]
        ports = [(p["published"], p["target"]) for p in service["ports"]]
        assert ports == [(host_port, 8080)], f"{name} publishes {ports}"  # one port: the base one was replaced
        mounts = [v for v in service["volumes"] if v["target"] == "/etc/das/config.yaml"]
        assert len(mounts) == 1 and mounts[0]["source"].endswith(GATEWAY_CONFIG.name) and mounts[0]["read_only"]
        env = service["environment"]
        assert env["DAS__WEBHOOK_SECRETS__E2E"] == WEBHOOK_SECRET
        assert env["DAS__API_KEYS"] == f"{MAIN_KEY},{LIMITED_KEY}"
        assert "DAS__REDIS__URL" in env, f"{name} did not inherit the base gateway's environment"
    assert "build" not in services["gateway-b"], "the second Gateway would build the same image again"
    assert services["gateway-b"]["image"] == services["gateway"]["image"]
    sink = services[SINK_SERVICE]
    assert [(p["published"], p["target"]) for p in sink["ports"]] == [("18103", SINK_PORT)]
    assert services["manager"]["environment"]["DAS__LIMITS__LEASE_DURATION"] == "10"  # the Phase 3 override applies
