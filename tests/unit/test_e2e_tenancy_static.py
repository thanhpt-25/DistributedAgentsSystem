"""The tenancy e2e stack's files agree with each other and with the product (das-multi-tenant task 12.1).

The e2e tests bring up a Compose project from three files (``deploy/docker-compose.yml``, the Phase 3 e2e override and
``docker-compose.tenancy.e2e.yml``) with a Gateway config of its own that names two tenants. A mistake in any of them
would only show as a failed Docker run, minutes later. These checks catch it at once, without starting anything: the
config loads through the real loader, every fingerprint is the real one, the override wires the keys and the config
consistently, ``ci.yml`` tears the project down, and (where Compose is installed) the three files really merge into the
stack the tests expect."""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from das.config import load_config
from das.ops.keys import key_fingerprint
from tests.integration.e2e.tenancy_constants import (
    API_KEYS,
    FINGERPRINTS,
    GATEWAY_B_PORT_VAR,
    PLAIN_KEY,
    PROJECT,
    ROOT_KEY,
    TEAM_A_ADMIN,
    TEAM_A_QUOTA,
    TEAM_A_READER,
    TEAM_A_WRITER,
    TEAM_B_ADMIN,
)
from tests.integration.e2e.tenancy_variants import VARIANTS, build, edited_config, patched_source
from tests.unit.test_e2e_client_api_static import ComposeLoader, compose_available

ROOT = Path(__file__).resolve().parents[2]
E2E = ROOT / "tests" / "integration" / "e2e"
DEPLOY = ROOT / "deploy"
GATEWAY_CONFIG = E2E / "gateway.tenancy.yaml"
OVERRIDE = E2E / "docker-compose.tenancy.e2e.yml"
PHASE3_OVERRIDE = E2E / "docker-compose.e2e.yml"
CI = ROOT / ".github" / "workflows" / "ci.yml"
ENV = {
    "DAS__REDIS__URL": "redis://redis:6379/0",
    "DAS__GRPC__ADVERTISE_HOST": "gateway",
    "DAS__API_KEYS": ",".join(API_KEYS),
    "DAS__ADMIN_API_KEYS": ROOT_KEY,
}
SECTIONS_OF_THIS_STACK = {"client_limits", "tenants"}
GATEWAYS = ("gateway", "gateway-b")


@pytest.fixture(scope="module")
def override() -> dict[str, Any]:
    return yaml.load(OVERRIDE.read_text(encoding="utf-8"), Loader=ComposeLoader)  # noqa: S506  (a SafeLoader subclass)


@pytest.fixture(scope="module")
def override_text() -> str:
    return OVERRIDE.read_text(encoding="utf-8")


# --------------------------------------------------------------------------- the constants


def test_every_fingerprint_is_the_real_one() -> None:
    for key, fingerprint in FINGERPRINTS.items():
        assert key_fingerprint(key.encode()) == fingerprint, key
    assert len(set(FINGERPRINTS.values())) == len(FINGERPRINTS)


def test_the_keys_are_valid_distinct_and_free_of_words_the_log_redactor_would_eat() -> None:
    every = (*API_KEYS, ROOT_KEY)
    assert len(set(every)) == len(every)
    for key in every:
        assert 16 <= len(key) <= 256 and key.isprintable()
        assert "tenant" not in key and "operator" not in key  # field names of the Audit_Entry
    assert ROOT_KEY not in API_KEYS  # the Admin_API_Key is never also an API key
    assert PLAIN_KEY not in FINGERPRINTS  # no tenant names it: the Default_Tenant


def test_the_project_is_its_own() -> None:
    assert PROJECT == "das-e2e-tenant"
    assert PROJECT not in {"das-e2e", "das-e2e-obs", "das-e2e-client"}


# --------------------------------------------------------------------------- the e2e Gateway config


def test_the_gateway_config_loads_with_the_stacks_environment() -> None:
    assert load_config(GATEWAY_CONFIG, ENV).role == "gateway"


def test_the_gateway_config_differs_from_the_production_sample_only_in_its_own_sections() -> None:
    mine = yaml.safe_load(GATEWAY_CONFIG.read_text(encoding="utf-8"))
    sample = yaml.safe_load((DEPLOY / "config" / "gateway.yaml").read_text(encoding="utf-8"))

    def rest(config: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in config.items() if k not in SECTIONS_OF_THIS_STACK}

    assert rest(mine) == rest(sample)


def test_the_tenants_are_the_ones_the_tests_assume() -> None:
    tenants = {t.id: t for t in load_config(GATEWAY_CONFIG, ENV).tenants}
    assert set(tenants) == {"team-a", "team-b"}
    a, b = tenants["team-a"], tenants["team-b"]
    scopes = {k.fingerprint: set(k.scopes) for k in a.keys}
    assert scopes == {
        FINGERPRINTS[TEAM_A_ADMIN]: {"submit", "read", "admin"},
        FINGERPRINTS[TEAM_A_READER]: {"read"},
        FINGERPRINTS[TEAM_A_WRITER]: {"submit"},
    }
    assert (a.max_active_requests, a.default_priority, a.max_priority) == (TEAM_A_QUOTA, "normal", "high")
    assert [(k.fingerprint, set(k.scopes)) for k in b.keys] == [
        (FINGERPRINTS[TEAM_B_ADMIN], {"submit", "read", "admin"})
    ]
    assert (b.max_active_requests, b.default_priority, b.max_priority) == (0, "normal", "normal")


def test_no_limit_other_than_the_tenants_own_can_throttle_a_test() -> None:
    default = load_config(GATEWAY_CONFIG, ENV).client_limits.default
    assert default.rate >= 100 and default.burst >= 1000 and default.max_active_requests >= 500


# --------------------------------------------------------------------------- the Compose override


def test_the_override_defines_two_gateways(override: dict[str, Any]) -> None:
    assert set(GATEWAYS) <= set(override["services"])


def test_the_second_gateway_extends_the_first_with_its_own_port_and_no_second_build(
    override: dict[str, Any], override_text: str
) -> None:
    second = override["services"]["gateway-b"]
    assert second["extends"]["service"] == "gateway"
    assert (DEPLOY / second["extends"]["file"]).resolve() == (DEPLOY / "docker-compose.yml").resolve()
    assert second["ports"] == [f"${{{GATEWAY_B_PORT_VAR}}}:8080"]
    assert re.search(r"^\s+ports: !override\b", override_text, re.MULTILINE)
    assert re.search(r"^\s+build: !reset\b", override_text, re.MULTILINE)


def test_both_gateways_mount_the_e2e_config_and_get_the_operator_key(
    override: dict[str, Any], override_text: str
) -> None:
    for name in GATEWAYS:
        service = override["services"][name]
        assert service["volumes"] == [f"../tests/integration/e2e/{GATEWAY_CONFIG.name}:/etc/das/config.yaml:ro"]
        assert service["environment"]["DAS__ADMIN_API_KEYS"] == ROOT_KEY
    assert len(re.findall(r"^\s+volumes: !override\b", override_text, re.MULTILINE)) >= 2  # replaced, not appended
    assert GATEWAY_CONFIG.is_file()


def test_the_override_does_not_repeat_what_the_phase_3_override_sets() -> None:
    own = OVERRIDE.read_text(encoding="utf-8")
    for setting in ("llm-stub", "DAS__LLM__", "LEASE_DURATION"):
        assert setting not in own, f"the override repeats {setting}, which docker-compose.e2e.yml already sets"


# --------------------------------------------------------------------------- the broken variants


def test_every_variant_patch_applies_exactly_once_to_the_current_source() -> None:
    """A refactor that moves a line must not leave a variant that breaks nothing (and so proves nothing)."""
    for variant in VARIANTS.values():
        for patch in variant.patches:
            text = (ROOT / patch.path).read_text(encoding="utf-8")
            patched = patched_source(patch, text)  # raises unless the old text occurs exactly once
            assert patched != text, variant.name
            if patch.path.endswith(".py"):
                compile(patched, patch.path, "exec")  # a variant that cannot even import breaks everything alike


def test_every_variant_config_edit_matches_the_e2e_config_and_still_loads() -> None:
    for variant in (v for v in VARIANTS.values() if v.config):
        text = edited_config(variant, GATEWAY_CONFIG.read_text(encoding="utf-8"))
        assert text != GATEWAY_CONFIG.read_text(encoding="utf-8"), variant.name
        edited = yaml.safe_load(text)
        assert edited["role"] == "gateway"  # still a document, with its other sections


def test_every_variant_names_tests_that_exist_and_a_service_that_exists(tmp_path: Path) -> None:
    source = (E2E / "test_tenancy_e2e.py").read_text(encoding="utf-8")
    for variant in VARIANTS.values():
        assert variant.fails, variant.name
        for name in variant.fails:
            assert re.search(rf"^def {re.escape(name)}\(", source, re.MULTILINE), f"{variant.name}: no test {name}"
        override = yaml.safe_load(build(variant, tmp_path / variant.name).read_text(encoding="utf-8"))
        assert set(override["services"]) <= {"gateway", "gateway-b", "manager"}, variant.name
        for service in override["services"].values():
            assert service["volumes"] and all(v.endswith(":ro") for v in service["volumes"]), variant.name
    assert len(VARIANTS) >= 10


def test_the_variants_cover_every_kind_of_test() -> None:
    broken = {name for variant in VARIANTS.values() for name in variant.fails}
    source = (E2E / "test_tenancy_e2e.py").read_text(encoding="utf-8")
    tests = set(re.findall(r"^def (test_\w+)\(", source, re.MULTILINE))
    uncovered = tests - broken - {  # the readiness test and the default-tenant one have nothing to break by config
        "test_the_stack_has_two_gateways_two_managers_two_workers_and_the_tenant_index",
        "test_a_key_without_a_tenant_is_the_default_tenant",
    }
    assert uncovered == set(), f"no variant makes these fail: {sorted(uncovered)}"


# --------------------------------------------------------------------------- CI


def test_ci_collects_logs_from_and_tears_down_the_tenancy_project() -> None:
    lines = CI.read_text(encoding="utf-8").splitlines()
    loops = [line for line in lines if line.strip().startswith("for project in")]
    assert len(loops) == 2, "expected the log-collection loop and the teardown loop"
    for line in loops:
        named = re.search(rf"(?<![\w-]){re.escape(PROJECT)}(?![\w-])", line)
        assert named, f"{PROJECT} is missing from: {line.strip()}"


# --------------------------------------------------------------------------- the merged stack (needs the compose CLI)


@pytest.mark.skipif(not compose_available(), reason="the docker compose CLI is not installed")
def test_the_three_compose_files_merge_into_the_stack_the_tests_expect(tmp_path: Path) -> None:
    """``docker compose config`` only reads files: no daemon, no pulls, no builds."""
    env_file = tmp_path / "e2e.env"
    env_file.write_text(
        "\n".join(
            [
                "RABBITMQ_USER=das",
                "RABBITMQ_PASS=e2e-rabbit-password",
                f"DAS_GATEWAY_API_KEYS={','.join(API_KEYS)}",
                "DAS_GATEWAY_PORT=18201",
                f"{GATEWAY_B_PORT_VAR}=18202",
                "DAS_RABBITMQ_MGMT_PORT=18203",
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

    assert {"gateway", "gateway-b", "llm-stub", "manager", "worker-research", "worker-review"} <= set(services)
    for name, host_port in (("gateway", "18201"), ("gateway-b", "18202")):
        service = services[name]
        ports = [(p["published"], p["target"]) for p in service["ports"]]
        assert ports == [(host_port, 8080)], f"{name} publishes {ports}"  # one port: the base one was replaced
        mounts = [v for v in service["volumes"] if v["target"] == "/etc/das/config.yaml"]
        assert len(mounts) == 1 and mounts[0]["source"].endswith(GATEWAY_CONFIG.name) and mounts[0]["read_only"]
        env = service["environment"]
        assert env["DAS__ADMIN_API_KEYS"] == ROOT_KEY
        assert env["DAS__API_KEYS"] == ",".join(API_KEYS)
        assert "DAS__REDIS__URL" in env, f"{name} did not inherit the base gateway's environment"
    assert "build" not in services["gateway-b"], "the second Gateway would build the same image again"
    assert services["gateway-b"]["image"] == services["gateway"]["image"]
    assert services["manager"]["environment"]["DAS__LIMITS__LEASE_DURATION"] == "10"  # the Phase 3 override applies
