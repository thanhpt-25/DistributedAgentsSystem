"""Smoke and static checks for the deployment artifacts and module boundaries.

Fast, Docker-free checks: parse deploy/ YAML/JSON and pyproject.toml, scan das/
source, and run import-linter.

Validates: Requirements 14.1, 14.3, 14.8, 15.3, 15.4
Validates (das-hardening-ci): Requirements 4.1, 4.2, 4.5, 4.6
"""

from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "deploy"
DAS = ROOT / "das"

FORBIDDEN_CLIENT_LIBS = {"aio_pika", "grpc", "redis"}


@pytest.fixture(scope="module")
def compose() -> dict:
    return yaml.safe_load((DEPLOY / "docker-compose.yml").read_text())


@pytest.fixture(scope="module")
def pyproject() -> dict:
    return tomllib.loads((ROOT / "pyproject.toml").read_text())


def _default_services(compose: dict) -> dict[str, dict]:
    """Services started by a plain `docker compose up` (no profile enabled)."""
    return {name: svc for name, svc in compose["services"].items() if not svc.get("profiles")}


def _dockerfile(svc: dict) -> str | None:
    build = svc.get("build")
    return build.get("dockerfile") if isinstance(build, dict) else None


def _services_of_role(compose: dict, role: str) -> dict[str, dict]:
    return {
        name: svc
        for name, svc in _default_services(compose).items()
        if _dockerfile(svc) == f"deploy/docker/{role}.Dockerfile"
    }


def _replicas(svc: dict) -> int:
    return int(svc.get("deploy", {}).get("replicas", 1))


def _capabilities(svc: dict) -> frozenset[str]:
    raw = svc.get("environment", {}).get("DAS__WORKER__CAPABILITIES") or ""
    return frozenset(c.strip() for c in str(raw).split(",") if c.strip())


# ---------------------------------------------------------------------------
# Requirement 14.1: Compose service counts and distinct Capability sets
# ---------------------------------------------------------------------------


def test_compose_has_one_rabbitmq_redis_and_gateway(compose: dict) -> None:
    services = _default_services(compose)
    rabbit = [s for s in services.values() if str(s.get("image", "")).startswith("rabbitmq:")]
    redis = [s for s in services.values() if str(s.get("image", "")).startswith("redis:")]
    assert len(rabbit) == 1 and _replicas(rabbit[0]) == 1
    assert len(redis) == 1 and _replicas(redis[0]) == 1
    gateways = _services_of_role(compose, "gateway")
    assert sum(_replicas(s) for s in gateways.values()) == 1


def test_compose_has_at_least_two_managers(compose: dict) -> None:
    managers = _services_of_role(compose, "manager")
    assert sum(_replicas(s) for s in managers.values()) >= 2


def test_compose_workers_declare_distinct_capability_sets(compose: dict) -> None:
    workers = _services_of_role(compose, "worker")
    assert sum(_replicas(s) for s in workers.values()) >= 2
    cap_sets = [_capabilities(s) for s in workers.values()]
    # A replicated worker service would give two containers the same set.
    assert all(_replicas(s) == 1 for s in workers.values())
    assert all(cap_sets), "every worker must declare at least one Capability"
    assert len(set(cap_sets)) == len(cap_sets), f"duplicate Capability sets: {cap_sets}"
    assert len(frozenset().union(*cap_sets)) >= 2


def test_ollama_is_optional_profile(compose: dict) -> None:
    assert compose["services"]["ollama"].get("profiles") == ["ollama"]


def test_worker_capabilities_have_job_queues(compose: dict) -> None:
    """Every declared Capability has a quorum job queue bound in the definitions."""
    defs = json.loads((DEPLOY / "rabbitmq" / "definitions.json").read_text())
    queues = {q["name"]: q for q in defs["queues"]}
    bindings = {(b["source"], b["routing_key"], b["destination"]) for b in defs["bindings"]}
    caps = frozenset().union(*(_capabilities(s) for s in _services_of_role(compose, "worker").values()))
    for cap in caps:
        queue = f"das.jobs.{cap}"
        assert queue in queues
        assert queues[queue]["arguments"]["x-queue-type"] == "quorum"
        assert ("das.jobs", cap, queue) in bindings


def test_delivery_limit_matches_manager_max_attempts() -> None:
    defs = json.loads((DEPLOY / "rabbitmq" / "definitions.json").read_text())
    policy = next(p for p in defs["policies"] if p["name"] == "das-jobs")
    manager_cfg = yaml.safe_load((DEPLOY / "config" / "manager.yaml").read_text())
    assert policy["definition"]["delivery-limit"] == manager_cfg["limits"]["max_attempts"] - 1


# ---------------------------------------------------------------------------
# Hardening Requirement 4: Admin_API keys are passed through by name only
# ---------------------------------------------------------------------------

ADMIN_KEYS_VAR = "DAS__ADMIN_API_KEYS"


def _passed_by_name(environment: dict | list | None, var: str) -> bool:
    """True when ``var`` is declared without a value (Compose passes it from the host).

    Mapping form: ``VAR:`` (null value). List form: a bare ``- VAR`` entry.
    Literals (``VAR: x``, ``- VAR=x``) and interpolations with defaults are rejected.
    """
    if isinstance(environment, dict):
        return var in environment and environment[var] is None
    if isinstance(environment, list):
        return var in environment and not any(str(e).startswith(f"{var}=") for e in environment)
    return False


def _env_example() -> dict[str, str]:
    values: dict[str, str] = {}
    for line in (DEPLOY / ".env.example").read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def test_gateway_passes_admin_api_keys_by_name(compose: dict) -> None:
    gateways = _services_of_role(compose, "gateway")
    assert gateways
    for name, svc in gateways.items():
        env = svc.get("environment")
        assert _passed_by_name(env, ADMIN_KEYS_VAR), (
            f"{name}: {ADMIN_KEYS_VAR} must be declared by name only, got {env!r}"
        )


def test_passed_by_name_rejects_literals_and_defaults() -> None:
    assert _passed_by_name({ADMIN_KEYS_VAR: None}, ADMIN_KEYS_VAR)
    assert _passed_by_name([ADMIN_KEYS_VAR], ADMIN_KEYS_VAR)
    assert not _passed_by_name({}, ADMIN_KEYS_VAR)
    assert not _passed_by_name({ADMIN_KEYS_VAR: "k" * 16}, ADMIN_KEYS_VAR)
    assert not _passed_by_name({ADMIN_KEYS_VAR: ""}, ADMIN_KEYS_VAR)
    assert not _passed_by_name({ADMIN_KEYS_VAR: "${X:-secret}"}, ADMIN_KEYS_VAR)
    assert not _passed_by_name([f"{ADMIN_KEYS_VAR}=secret"], ADMIN_KEYS_VAR)
    assert not _passed_by_name([f"{ADMIN_KEYS_VAR}=${{X:-secret}}"], ADMIN_KEYS_VAR)


def test_env_example_declares_admin_api_keys_empty() -> None:
    values = _env_example()
    assert ADMIN_KEYS_VAR in values
    assert values[ADMIN_KEYS_VAR] == ""


# ---------------------------------------------------------------------------
# Requirement 14.3: one image per role; role and Capabilities only from Agent_Config
# ---------------------------------------------------------------------------


def test_one_image_per_role(compose: dict) -> None:
    images: dict[str, set[str]] = {}
    for role in ("gateway", "manager", "worker"):
        services = _services_of_role(compose, role)
        assert services, f"no {role} service"
        images[role] = {s["image"] for s in services.values()}
        assert len(images[role]) == 1, f"{role} services use several images: {images[role]}"
    assert len(set().union(*images.values())) == 3


@pytest.mark.parametrize("role", ["gateway", "manager", "worker"])
def test_role_dockerfile_does_not_bake_role_or_capabilities(role: str) -> None:
    text = (DEPLOY / "docker" / f"{role}.Dockerfile").read_text()
    assert "DAS__ROLE" not in text
    assert "DAS__WORKER__CAPABILITIES" not in text


@pytest.mark.parametrize("role", ["gateway", "manager", "worker"])
def test_sample_config_declares_its_role(role: str) -> None:
    cfg = yaml.safe_load((DEPLOY / "config" / f"{role}.yaml").read_text())
    assert cfg["role"] == role


def test_worker_config_leaves_capabilities_to_environment() -> None:
    cfg = yaml.safe_load((DEPLOY / "config" / "worker.yaml").read_text())
    assert "capabilities" not in (cfg.get("worker") or {})


# ---------------------------------------------------------------------------
# Requirement 14.8: no address hard-coded to localhost or a Compose service name
# ---------------------------------------------------------------------------


def _string_constants(path: Path) -> list[tuple[int, str]]:
    tree = ast.parse(path.read_text(), filename=str(path))
    out: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            out.append((node.lineno, node.value))
    return out


def _host_pattern(names: set[str]) -> re.Pattern[str]:
    alt = "|".join(sorted(map(re.escape, names), key=len, reverse=True))
    # A service name used as a host: URL authority, user@host, or host:port.
    return re.compile(rf"(?:://|@)(?:{alt})\b|^(?:{alt}):\d+|\b(?:{alt}):\d{{2,5}}\b")


def test_das_source_has_no_hardcoded_hosts(compose: dict) -> None:
    service_hosts = set(compose["services"]) | {
        h for s in compose["services"].values() if (h := s.get("hostname"))
    }
    service_re = _host_pattern(service_hosts)
    loopback_re = re.compile(r"localhost|127\.0\.0\.1|\[::1\]")
    violations = []
    files = sorted(DAS.rglob("*.py"))
    assert files
    for path in files:
        if "proto" in path.relative_to(DAS).parts:
            continue  # generated gRPC stubs
        for lineno, value in _string_constants(path):
            if loopback_re.search(value) or service_re.search(value):
                violations.append(f"{path.relative_to(ROOT)}:{lineno}: {value!r}")
    assert not violations, "hard-coded addresses:\n" + "\n".join(violations)


def test_host_pattern_detects_service_addresses() -> None:
    pattern = _host_pattern({"rabbitmq", "redis"})
    assert pattern.search("amqp://guest:guest@rabbitmq:5672/")
    assert pattern.search("redis://redis:6379/0")
    assert pattern.search("redis:6379")
    assert not pattern.search("das.transport.rabbitmq")
    assert not pattern.search("redis")


# ---------------------------------------------------------------------------
# Requirement 15.3: v1 shipped no http or kafka adapter; Phase 7 adds both (waves 9 and 10)
# ---------------------------------------------------------------------------


def test_pyproject_transport_entry_points(pyproject: dict) -> None:
    eps = pyproject["project"]["entry-points"]["das.transports"]
    assert set(eps) == {"rabbitmq", "grpc", "http", "kafka"}


def test_installed_transport_entry_points_include_the_four_transports() -> None:
    from importlib.metadata import entry_points

    names = {ep.name for ep in entry_points(group="das.transports")}
    assert {"rabbitmq", "grpc", "http", "kafka"} <= names


# ---------------------------------------------------------------------------
# Requirement 15.4: Manager/Worker logic never imports protocol client libraries
# ---------------------------------------------------------------------------


def test_import_linter_contract_configured(pyproject: dict) -> None:
    contracts = pyproject["tool"]["importlinter"]["contracts"]
    matching = [
        c
        for c in contracts
        if c["type"] == "forbidden"
        and {"das.manager", "das.worker"} <= set(c["source_modules"])
        and FORBIDDEN_CLIENT_LIBS <= set(c["forbidden_modules"])
    ]
    assert matching
    assert pyproject["tool"]["importlinter"].get("include_external_packages") is True


def test_import_linter_contracts_kept() -> None:
    lint_imports = Path(sys.executable).parent / "lint-imports"
    if not lint_imports.exists():
        pytest.skip("import-linter is not installed")
    result = subprocess.run(
        [str(lint_imports), "--no-cache"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "0 broken" in result.stdout


@pytest.mark.parametrize("package", ["manager", "worker"])
def test_logic_modules_have_no_direct_client_imports(package: str) -> None:
    """AST backstop for the import-linter contract (direct imports only)."""
    violations = []
    for path in sorted((DAS / package).rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Import):
                mods = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                mods = [node.module]
            else:
                continue
            for mod in mods:
                if mod.split(".")[0] in FORBIDDEN_CLIENT_LIBS:
                    violations.append(f"{path.relative_to(ROOT)}:{node.lineno}: {mod}")
    assert not violations, "\n".join(violations)
