"""The ``sandbox`` configuration section (das-smarter-agents task 6.3; Phase 6 Req 6.1, 6.4, 6.5): off by default, the
limits bounded by their ceilings, nothing that would weaken the sandbox settable, and ``code-execution`` refused unless
the sandbox is enabled."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from das.config import load_config
from das.errors import ConfigError

ENV = {"DAS__LLM__API_KEY": "sk-test-cafebabe0123456789"}


def worker_cfg(capabilities: list[str] | None = None, **sandbox: object) -> dict:
    cfg: dict = {
        "role": "worker",
        "host": "10.0.0.7",
        "transports": {"jobs": "rabbitmq", "control": "grpc"},
        "rabbitmq": {"url": "amqp://rabbit.internal:5672/"},
        "redis": {"url": "redis://redis.internal:6379/0"},
        "grpc": {"bind": "0.0.0.0:50051", "advertise_host": "10.0.0.7", "advertise_port": 50051},
        "llm": {"provider": "anthropic", "model": "big-model"},
        "worker": {"capabilities": capabilities or ["research"]},
    }
    if sandbox:
        cfg["sandbox"] = sandbox
    return cfg


def load(tmp_path: Path, cfg: dict, env: dict[str, str] | None = None):  # noqa: ANN201
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return load_config(path, ENV if env is None else {**ENV, **env})


def refused(tmp_path: Path, cfg: dict) -> tuple[str, ...]:
    with pytest.raises(ConfigError) as info:
        load(tmp_path, cfg)
    return tuple(info.value.fields)


# --------------------------------------------------------------------------- defaults (Req 6.1)


def test_the_sandbox_is_off_by_default_with_ordinary_defaults(tmp_path: Path) -> None:
    sandbox = load(tmp_path, worker_cfg()).sandbox
    assert sandbox.enabled is False and sandbox.container_cli == "docker" and sandbox.oci_runtime is None
    assert sandbox.image == "python:3.12-slim"
    assert (sandbox.timeout, sandbox.memory_mb, sandbox.cpus, sandbox.pids, sandbox.output_bytes) == (
        10, 256, 1, 64, 16_384,
    )


def test_an_enabled_sandbox_loads_on_a_worker(tmp_path: Path) -> None:
    cfg = worker_cfg(["research", "code-execution"], enabled=True, container_cli="podman", oci_runtime="runsc")
    sandbox = load(tmp_path, cfg).sandbox
    assert sandbox.enabled and sandbox.container_cli == "podman" and sandbox.oci_runtime == "runsc"


def test_the_sandbox_can_be_enabled_from_the_environment(tmp_path: Path) -> None:
    cfg = load(tmp_path, worker_cfg(["code-execution"]), {"DAS__SANDBOX__ENABLED": "true"})
    assert cfg.sandbox.enabled is True


# --------------------------------------------------------------------------- code-execution needs the sandbox (Req 6.1)


def test_code_execution_with_the_sandbox_off_stops_the_worker_from_starting(tmp_path: Path) -> None:
    assert "sandbox.enabled" in refused(tmp_path, worker_cfg(["research", "code-execution"]))


def test_code_execution_with_the_sandbox_explicitly_disabled_is_refused_too(tmp_path: Path) -> None:
    assert "sandbox.enabled" in refused(tmp_path, worker_cfg(["code-execution"], enabled=False))


def test_an_enabled_sandbox_does_not_need_the_capability_to_be_listed(tmp_path: Path) -> None:
    assert load(tmp_path, worker_cfg(["research"], enabled=True)).sandbox.enabled


@pytest.mark.parametrize("role", ["manager", "gateway"])
def test_only_a_worker_can_enable_the_sandbox(tmp_path: Path, role: str) -> None:
    cfg = worker_cfg(enabled=True)
    cfg["role"] = role
    cfg["rabbitmq"]["management_url"] = "http://rabbit.internal:15672"
    cfg["api_keys"] = ["gateway-key-0000000001"]
    assert "sandbox.enabled" in refused(tmp_path, cfg)


# --------------------------------------------------------------------------- ceilings (Req 6.4)


@pytest.mark.parametrize(
    ("name", "good"),
    [("timeout", 60), ("memory_mb", 1024), ("cpus", 2), ("pids", 256), ("output_bytes", 65_536),
     ("timeout", 1), ("memory_mb", 64), ("cpus", 0.1), ("pids", 16), ("output_bytes", 256)],
)
def test_a_limit_at_its_ceiling_or_floor_loads(tmp_path: Path, name: str, good: float) -> None:
    assert getattr(load(tmp_path, worker_cfg(**{name: good})).sandbox, name) == good


@pytest.mark.parametrize(
    ("name", "bad"),
    [("timeout", 61), ("memory_mb", 1025), ("cpus", 2.5), ("pids", 257), ("output_bytes", 65_537),
     ("timeout", 0.5), ("memory_mb", 63), ("cpus", 0.05), ("pids", 15), ("output_bytes", 255)],
)
def test_a_limit_outside_its_range_is_a_configuration_error_naming_the_field(
    tmp_path: Path, name: str, bad: float
) -> None:
    assert f"sandbox.{name}" in refused(tmp_path, worker_cfg(**{name: bad}))


# --------------------------------------------------------------------------- nothing weaker is settable (Req 6.4, 6.5)


@pytest.mark.parametrize(
    "setting",
    ["privileged", "network", "host_network", "volumes", "mounts", "devices", "docker_socket", "user", "cap_add",
     "security_opt", "read_only", "env", "command"],
)
def test_a_setting_that_would_weaken_the_sandbox_does_not_exist(tmp_path: Path, setting: str) -> None:
    assert any(f.startswith("sandbox") for f in refused(tmp_path, worker_cfg(**{setting: True})))


@pytest.mark.parametrize("cli", ["sh", "kubectl", "/usr/bin/docker", "docker run", ""])
def test_only_docker_and_podman_can_be_the_container_cli(tmp_path: Path, cli: str) -> None:
    assert "sandbox.container_cli" in refused(tmp_path, worker_cfg(container_cli=cli))


@pytest.mark.parametrize("image", ["", "-v", "--privileged", "img --privileged", "UPPER", "a b", "x" * 300])
def test_the_image_must_be_a_plain_reference(tmp_path: Path, image: str) -> None:
    assert "sandbox.image" in refused(tmp_path, worker_cfg(image=image))


@pytest.mark.parametrize("runtime", ["--privileged", "runsc --privileged", "a b", "../x", "", "x" * 100])
def test_the_oci_runtime_must_be_a_plain_name(tmp_path: Path, runtime: str) -> None:
    assert "sandbox.oci_runtime" in refused(tmp_path, worker_cfg(oci_runtime=runtime))
