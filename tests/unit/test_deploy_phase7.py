"""The Phase 7 Compose files (das-scale-resilience task 11.1; Phase 7 Req 1.9, 2.11).

``docker-compose.sentinel.yml``: one master (the base ``redis`` service), one replica and three Sentinels under
``--profile sentinel``, with every agent switched to Sentinel mode. ``docker-compose.http-transport.yml``: the Gateway
serves the broker, the agents use it for Jobs, RabbitMQ is not started. Read as text and YAML (``!reset`` is read as a
marker), and, when ``docker compose`` is installed, merged with the base file the way an operator would."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

DEPLOY = Path(__file__).resolve().parents[2] / "deploy"
SENTINEL = DEPLOY / "docker-compose.sentinel.yml"
HTTP = DEPLOY / "docker-compose.http-transport.yml"
AGENTS = ("gateway", "manager", "worker-research", "worker-review")
RESET = "!reset"
TOKEN = "0123456789abcdef0123"


class _Loader(yaml.SafeLoader):
    """SafeLoader that reads Compose's ``!reset`` tag as the string ``"!reset"``."""


_Loader.add_constructor("!reset", lambda loader, node: RESET)


def load(path: Path) -> dict[str, Any]:
    return yaml.load(path.read_text(), Loader=_Loader)  # noqa: S506 - the loader above is a SafeLoader


def environment(service: dict[str, Any]) -> dict[str, Any]:
    return service.get("environment", {})


needs_compose = pytest.mark.skipif(shutil.which("docker") is None, reason="docker compose is not installed")


def compose_config(
    *files: Path, profiles: tuple[str, ...] = (), token: bool = True
) -> subprocess.CompletedProcess[str]:
    cmd = ["docker", "compose", "-f", str(DEPLOY / "docker-compose.yml"), *(a for f in files for a in ("-f", str(f)))]
    for profile in profiles:
        cmd += ["--profile", profile]
    cmd += ["config", "--format", "json"]
    env = {**os.environ, "RABBITMQ_USER": "das", "RABBITMQ_PASS": "pw", "DAS_GATEWAY_API_KEYS": "key"}
    env.pop("DAS_HTTP_BROKER_TOKEN", None)
    if token:
        env["DAS_HTTP_BROKER_TOKEN"] = TOKEN
    return subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=60, stdin=subprocess.DEVNULL)


def merged(*files: Path, profiles: tuple[str, ...] = ()) -> dict[str, Any]:
    out = compose_config(*files, profiles=profiles)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


# --------------------------------------------------------------------------- Sentinel (Req 1.9)


def test_the_sentinel_file_adds_one_replica_and_three_sentinels_under_the_sentinel_profile() -> None:
    services = load(SENTINEL)["services"]
    extra = {name for name in services if name not in AGENTS}
    assert extra == {"redis-replica", "sentinel-1", "sentinel-2", "sentinel-3"}
    assert all(services[name]["profiles"] == ["sentinel"] for name in extra)
    assert "redis" not in services, "the master is the base file's redis service"


def test_the_replica_follows_the_master_and_the_sentinels_watch_it_with_a_quorum_of_two() -> None:
    services = load(SENTINEL)["services"]
    command = services["redis-replica"]["command"]
    assert command[command.index("--replicaof") + 1 : command.index("--replicaof") + 3] == ["redis", "6379"]
    script = services["sentinel-1"]["command"][-1]
    assert "sentinel monitor das redis 6379 2" in script, "the master's name, address and a quorum of two of three"
    assert "sentinel down-after-milliseconds das 3000" in script and "resolve-hostnames yes" in script
    assert services["sentinel-1"]["command"] == services["sentinel-2"]["command"] == services["sentinel-3"]["command"]


def test_every_agent_is_switched_to_sentinel_mode_without_a_redis_url() -> None:
    services = load(SENTINEL)["services"]
    for name in AGENTS:
        env = environment(services[name])
        assert env["DAS__REDIS__URL"] == RESET, f"{name} keeps a standalone Redis URL"
        assert env["DAS__REDIS__MODE"] == "sentinel" and env["DAS__REDIS__MASTER_NAME"] == "das"
        assert env["DAS__REDIS__SENTINELS"] == "sentinel-1:26379,sentinel-2:26379,sentinel-3:26379"
        assert set(services[name]["depends_on"]) == {"sentinel-1", "sentinel-2", "sentinel-3"}


def test_the_master_name_the_agents_use_is_the_one_the_sentinels_monitor() -> None:
    services = load(SENTINEL)["services"]
    master = environment(services["gateway"])["DAS__REDIS__MASTER_NAME"]
    assert f"sentinel monitor {master} redis 6379" in services["sentinel-1"]["command"][-1]


def test_the_sentinel_file_holds_no_password() -> None:
    text = SENTINEL.read_text().replace("DAS__REDIS__PASSWORD", "").replace("DAS__REDIS__SENTINEL_PASSWORD", "")
    assert "PASSWORD" not in text and "requirepass" not in text


@needs_compose
def test_merged_with_the_base_file_the_agents_have_sentinel_settings_and_no_url() -> None:
    config = merged(SENTINEL, profiles=("sentinel",))
    for name in AGENTS:
        env = config["services"][name]["environment"]
        assert "DAS__REDIS__URL" not in env and env["DAS__REDIS__MODE"] == "sentinel", name
    assert {"redis", "redis-replica", "sentinel-1", "sentinel-2", "sentinel-3"} <= set(config["services"])


@needs_compose
def test_the_base_stack_is_unchanged_without_the_sentinel_file() -> None:
    config = merged()
    assert not {"redis-replica", "sentinel-1"} & set(config["services"])
    assert config["services"]["manager"]["environment"]["DAS__REDIS__URL"] == "redis://redis:6379/0"


# --------------------------------------------------------------------------- HTTP transport (Req 2.11)


def test_the_gateway_serves_the_broker_and_the_agents_use_it_for_jobs() -> None:
    services = load(HTTP)["services"]
    assert environment(services["gateway"])["DAS__HTTP_BROKER__ENABLED"] == "true"
    for name in ("manager", "worker-research", "worker-review"):
        env = environment(services[name])
        assert env["DAS__TRANSPORTS__JOBS"] == "http" and env["DAS__HTTP__URL"] == "http://gateway:8081", name
        assert env["DAS__RABBITMQ__URL"] == RESET, f"{name} is still given a RabbitMQ URL"


def test_the_token_comes_from_one_required_variable_and_is_never_written_in_the_file() -> None:
    services = load(HTTP)["services"]
    want = "${DAS_HTTP_BROKER_TOKEN:?set DAS_HTTP_BROKER_TOKEN in deploy/.env}"
    assert environment(services["gateway"])["DAS__HTTP_BROKER__TOKEN"] == want
    for name in ("manager", "worker-research", "worker-review"):
        assert environment(services[name])["DAS__HTTP__TOKEN"] == want
    text = HTTP.read_text()
    assert TOKEN not in text and "change-me" not in text


def test_rabbitmq_moves_to_its_own_profile_and_the_agents_no_longer_wait_for_it() -> None:
    services = load(HTTP)["services"]
    assert services["rabbitmq"]["profiles"] == ["amqp"]
    for name in ("manager", "worker-research", "worker-review"):
        deps = services[name]["depends_on"]
        assert deps["rabbitmq"]["required"] is False and deps["gateway"]["condition"] == "service_healthy"


@needs_compose
def test_merged_with_the_base_file_the_stack_runs_without_rabbitmq() -> None:
    config = merged(HTTP)
    assert "rabbitmq" not in config["services"]
    for name in ("manager", "worker-research", "worker-review"):
        env = config["services"][name]["environment"]
        assert env["DAS__TRANSPORTS__JOBS"] == "http" and env["DAS__HTTP__TOKEN"] == TOKEN
        assert "DAS__RABBITMQ__URL" not in env and "DAS__RABBITMQ__MANAGEMENT_URL" not in env
    assert config["services"]["gateway"]["environment"]["DAS__HTTP_BROKER__TOKEN"] == TOKEN


@needs_compose
def test_the_rabbitmq_profile_brings_it_back() -> None:
    assert "rabbitmq" in merged(HTTP, profiles=("amqp",))["services"]


@needs_compose
def test_the_http_stack_refuses_to_start_without_a_token() -> None:
    out = compose_config(HTTP, token=False)
    assert out.returncode != 0 and "DAS_HTTP_BROKER_TOKEN" in out.stderr


# --------------------------------------------------------------------------- the documentation


def test_env_example_documents_both_files_and_the_one_variable() -> None:
    text = (DEPLOY / ".env.example").read_text()
    assert "docker-compose.sentinel.yml" in text and "--profile sentinel" in text
    assert "docker-compose.http-transport.yml" in text and "DAS_HTTP_BROKER_TOKEN" in text
    live = [line for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#")]
    assert not [line for line in live if line.startswith("DAS_HTTP_BROKER_TOKEN")], "the example sets no token"
