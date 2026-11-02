"""Unit tests for the ``das`` CLI (Requirements 8.5, 14.4, 14.7, 15.5)."""

from __future__ import annotations

import asyncio
import io
import json
import socket
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest
import yaml

import das.cli as cli
from das.clock import FakeClock
from das.config import AgentConfig, load_config
from das.errors import (
    EXIT_CONFIG_ERROR,
    EXIT_DEPENDENCY_UNREACHABLE,
    EXIT_TRANSPORT_ERROR,
    DependencyUnreachable,
    TransportIncomplete,
    TransportNotInstalled,
)
from das.observability import configure_logging
from das.transport.loader import ENTRY_POINT_GROUP

_MODULE = __name__
ENV = {"DAS__LLM__API_KEY": "sk-test-0123456789abcdef"}


def worker_cfg(**overrides: object) -> dict:
    cfg: dict = {
        "role": "worker",
        "agent_id": "worker-test",
        "host": "127.0.0.1",
        "transports": {"jobs": "rabbitmq", "control": "grpc"},
        "rabbitmq": {"url": "amqp://127.0.0.1:1/"},
        "redis": {"url": "redis://127.0.0.1:1/0"},
        "grpc": {"bind": "127.0.0.1:50051", "advertise_host": "127.0.0.1", "advertise_port": 50051},
        "llm": {"provider": "anthropic", "model": "claude-sonnet-4"},
        "worker": {"capabilities": ["research"]},
        "limits": {"startup_timeout": 1, "startup_retry_interval": 0.1},
    }
    cfg.update(overrides)
    return cfg


def write(tmp_path: Path, data: object) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(yaml.safe_dump(data), encoding="utf-8")
    return p


@pytest.fixture(autouse=True)
def _restore_logging():
    # main() binds logging to the (captured) stdout of the test that called it;
    # rebind afterwards so later tests never write to a closed stream.
    yield
    configure_logging("test", stream=io.StringIO())


def run_main(path: Path, env: dict[str, str] | None = None) -> int:
    return cli.main(["run", "--config", str(path)], env=ENV if env is None else env)


# --------------------------------------------------------------------------- adapters


class CompleteAdapter:
    async def publish(self, destination, message, headers=None): ...
    async def subscribe(self, destination, handler, options=None): ...
    async def acknowledge(self, delivery, outcome=None): ...
    async def request_reply(self, destination, message, timeout=None): ...


class PartialAdapter:
    async def publish(self, destination, message, headers=None): ...


def _source(**targets: str):
    eps = [EntryPoint(name=n, value=v, group=ENTRY_POINT_GROUP) for n, v in targets.items()]
    return lambda: list(eps)


# --------------------------------------------------------------------------- exit 2: config


def test_missing_config_file_exits_2(tmp_path: Path) -> None:
    assert run_main(tmp_path / "absent.yaml") == EXIT_CONFIG_ERROR


def test_invalid_config_exits_2(tmp_path: Path) -> None:
    cfg = worker_cfg(limits={"max_attempts": 0})
    assert run_main(write(tmp_path, cfg)) == EXIT_CONFIG_ERROR


def test_missing_credential_exits_2(tmp_path: Path) -> None:
    assert run_main(write(tmp_path, worker_cfg()), env={}) == EXIT_CONFIG_ERROR


def test_usage_error_is_nonzero() -> None:
    assert cli.main(["run"], env={}) != 0


# --------------------------------------------------------------------------- exit 3: transport


def test_missing_transport_exits_3_before_any_connection(tmp_path: Path, monkeypatch) -> None:
    called = False

    async def fail_run_agent(*_a, **_k):
        nonlocal called
        called = True
        return 0

    monkeypatch.setattr(cli, "run_agent", fail_run_agent)
    cfg = worker_cfg(transports={"jobs": "kafka", "control": "grpc"})
    assert run_main(write(tmp_path, cfg)) == EXIT_TRANSPORT_ERROR
    assert not called  # nothing constructed or connected (Req 14.4)


def test_incomplete_transport_exits_3(tmp_path: Path, monkeypatch) -> None:
    source = _source(partial=f"{_MODULE}:PartialAdapter", grpc=f"{_MODULE}:CompleteAdapter")
    real = cli.load_transports
    monkeypatch.setattr(cli, "load_transports", lambda config: real(config, source))
    cfg = worker_cfg(transports={"jobs": "partial", "control": "grpc"})
    assert run_main(write(tmp_path, cfg)) == EXIT_TRANSPORT_ERROR


@pytest.fixture
def make_config(tmp_path: Path):
    def build(**overrides: object) -> AgentConfig:
        return load_config(write(tmp_path, worker_cfg(**overrides)), ENV)

    return build


def test_load_transports_by_name_and_errors(make_config) -> None:
    _config = make_config
    source = _source(
        rabbitmq=f"{_MODULE}:CompleteAdapter",
        grpc=f"{_MODULE}:CompleteAdapter",
        partial=f"{_MODULE}:PartialAdapter",
    )
    loaded = cli.load_transports(_config(), source)
    assert loaded == {"rabbitmq": CompleteAdapter, "grpc": CompleteAdapter}

    with pytest.raises(TransportNotInstalled) as info:
        cli.load_transports(_config(transports={"jobs": "http", "control": "grpc"}), source)
    assert info.value.name == "http"

    with pytest.raises(TransportIncomplete) as inc:
        cli.load_transports(_config(transports={"jobs": "partial", "control": "grpc"}), source)
    assert inc.value.missing == ("subscribe", "acknowledge", "request_reply")


# --------------------------------------------------------------------------- exit 4: dependency


async def test_connect_with_retry_succeeds_after_failures() -> None:
    clock = FakeClock()
    attempts = 0
    resets = 0

    async def connect() -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionError("refused")

    async def reset() -> None:
        nonlocal resets
        resets += 1

    await cli.connect_with_retry(
        "redis", connect, interval=2.0, timeout=60.0, reset=reset, clock=clock
    )
    assert attempts == 3
    assert resets == 2
    assert clock.sleeps == [2.0, 2.0]


async def test_connect_with_retry_gives_up_naming_dependency() -> None:
    clock = FakeClock()
    attempts = 0

    async def connect() -> None:
        nonlocal attempts
        attempts += 1
        raise ConnectionError("refused")

    with pytest.raises(DependencyUnreachable) as info:
        await cli.connect_with_retry("rabbitmq", connect, interval=2.0, timeout=5.0, clock=clock)
    assert info.value.dependency == "rabbitmq"
    assert info.value.exit_code == EXIT_DEPENDENCY_UNREACHABLE
    # Sleeps never overshoot the deadline: 2 + 2 + 1.
    assert clock.sleeps == [2.0, 2.0, 1.0]
    assert attempts == 4
    assert clock.monotonic() == pytest.approx(5.0)


async def test_connect_with_retry_bounds_hanging_attempt() -> None:
    async def hang() -> None:
        await asyncio.sleep(60)

    with pytest.raises(DependencyUnreachable, match="timed out"):
        await cli.connect_with_retry("redis", hang, interval=0.1, timeout=0.2)


def test_unreachable_redis_exits_4(tmp_path: Path, capsys) -> None:
    assert run_main(write(tmp_path, worker_cfg())) == EXIT_DEPENDENCY_UNREACHABLE
    out = capsys.readouterr()
    records = [json.loads(line) for line in (out.out + out.err).splitlines() if line.strip()]
    failure = next(r for r in records if r.get("message") == "startup failed")
    assert failure["dependency"] == "redis"
    assert failure["exit_code"] == EXIT_DEPENDENCY_UNREACHABLE


class UnreachableTransport(CompleteAdapter):
    events: list[str] = []

    async def start(self) -> None:
        self.events.append("start")
        raise ConnectionError("refused")

    async def close(self) -> None:
        self.events.append("close")


async def test_unreachable_transport_exits_4_naming_it(
    fake_redis, monkeypatch, make_config
) -> None:
    UnreachableTransport.events = []
    monkeypatch.setattr(cli, "_redis_client", lambda _config: fake_redis)
    # The Ops server binds health_port first (Req 16.11); use a free one.
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    config = make_config(transports={"jobs": "flaky", "control": "flaky"}, health_port=port)
    with pytest.raises(DependencyUnreachable) as info:
        await cli.run_agent(config, {"flaky": UnreachableTransport}, clock=FakeClock())
    assert info.value.dependency == "flaky"
    assert cli.exit_code_for(info.value) == EXIT_DEPENDENCY_UNREACHABLE
    starts = UnreachableTransport.events.count("start")
    assert starts >= 2  # retried every startup_retry_interval until the timeout
    assert UnreachableTransport.events[-1] == "close"
