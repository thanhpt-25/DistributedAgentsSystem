"""Ops server, Tracer and metrics wiring in the CLI and agents (task 15.2).

Covers Req 1.2 (ops paths only on the Ops_Port), 8.2/8.8 (Tracer built per role,
flushed at shutdown without changing the exit code), 16.2/16.9 (every
Critical_Loop registered with the LoopSupervisor; shutdown marked), 16.8 (v1
``/healthz`` stays on the Gateway's ``http_port``) and 16.11 (Ops_Port bind
failure is exit 2 naming the port).
"""

from __future__ import annotations

import asyncio
import io
import json
import socket
from pathlib import Path

import httpx
import pytest
import yaml

import das.cli as cli
import das.observability.tracing as tracing
from das.errors import EXIT_CONFIG_ERROR, EXIT_OK, OpsPortUnavailable, exit_code_for
from das.manager.agent import CRITICAL_LOOPS as MANAGER_LOOPS
from das.observability import configure_logging
from das.observability.tracing import NoopTracer
from das.worker.worker import CRITICAL_LOOPS as WORKER_LOOPS
from tests.fakes.llm_adapter import FakeAdapter
from tests.unit.test_manager_agent import make_env
from tests.unit.test_worker import Harness, _until

ENV = {"DAS__API_KEYS": "gw-key-0123456789"}


@pytest.fixture(autouse=True)
def _restore_logging():
    yield
    configure_logging("test", stream=io.StringIO())


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def gateway_config(tmp_path: Path, **overrides: object):
    data: dict = {
        "role": "gateway",
        "agent_id": "gw-test",
        "redis": {"url": "redis://127.0.0.1:1/0"},
        "http_port": free_port(),
        "health_port": free_port(),
        "limits": {"startup_timeout": 1, "startup_retry_interval": 0.1, "shutdown_timeout": 3},
    }
    data.update(overrides)
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


class RecordingTracer(NoopTracer):
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.shutdowns: list[float] = []

    async def shutdown(self, timeout: float) -> int:
        self.shutdowns.append(timeout)
        if self.fail:
            raise RuntimeError("exporter exploded")
        return 3


async def _wait_ready(client: httpx.AsyncClient, url: str) -> None:
    async with asyncio.timeout(5):
        while True:
            try:
                if (await client.get(url)).status_code == 200:
                    return
            except httpx.TransportError:
                pass
            await asyncio.sleep(0.02)


# --------------------------------------------------------------------------- CLI


@pytest.mark.parametrize("fail_flush", [False, True])
async def test_gateway_serves_ops_only_on_health_port_and_flushes_spans(
    tmp_path, fake_redis, monkeypatch, fail_flush
) -> None:
    config = cli.load_config(gateway_config(tmp_path), ENV)
    tracer = RecordingTracer(fail=fail_flush)
    built: list[tuple[str, str]] = []

    def fake_build_tracer(cfg, role, agent_id, clock=None, **_):
        built.append((role, agent_id))
        return tracer

    monkeypatch.setattr(tracing, "build_tracer", fake_build_tracer)
    monkeypatch.setattr(cli, "_redis_client", lambda _config: fake_redis)
    stop = asyncio.Event()
    runner = asyncio.create_task(cli.run_agent(config, {}, stop=stop))
    ops = f"http://127.0.0.1:{config.health_port}"
    http = f"http://127.0.0.1:{config.http_port}"
    try:
        async with httpx.AsyncClient(timeout=3) as client:
            await _wait_ready(client, f"{ops}/readyz")
            ready = (await client.get(f"{ops}/readyz")).json()
            assert ready["checks"] == {"startup": "pass", "state_store": "pass"}
            assert (await client.get(f"{ops}/livez")).status_code == 200
            metrics = await client.get(f"{ops}/metrics")
            assert metrics.status_code == 200 and "das_" in metrics.text
            # /healthz stays on http_port (Req 16.8); ops paths are 404 there (Req 1.2).
            assert (await client.get(f"{ops}/healthz")).status_code == 404
            assert (await client.get(f"{http}/healthz")).status_code == 200
            for path in ("/metrics", "/livez", "/readyz"):
                assert (await client.get(f"{http}{path}")).status_code == 404
    finally:
        stop.set()
        code = await asyncio.wait_for(runner, 10)
    assert built == [("gateway", "gw-test")]
    assert code == EXIT_OK  # a failing flush does not change the exit code (Req 8.8)
    assert len(tracer.shutdowns) == 1
    assert 0 <= tracer.shutdowns[0] <= config.limits.shutdown_timeout


async def test_gateway_metrics_disabled_returns_404_on_ops_port(
    tmp_path, fake_redis, monkeypatch
) -> None:
    config = cli.load_config(gateway_config(tmp_path, metrics={"enabled": False}), ENV)
    monkeypatch.setattr(cli, "_redis_client", lambda _config: fake_redis)
    stop = asyncio.Event()
    runner = asyncio.create_task(cli.run_agent(config, {}, stop=stop))
    ops = f"http://127.0.0.1:{config.health_port}"
    try:
        async with httpx.AsyncClient(timeout=3) as client:
            await _wait_ready(client, f"{ops}/livez")
            assert (await client.get(f"{ops}/metrics")).status_code == 404
    finally:
        stop.set()
        assert await asyncio.wait_for(runner, 10) == EXIT_OK


async def test_ops_port_bind_failure_names_port_and_exits_2(
    tmp_path, fake_redis, monkeypatch
) -> None:
    config = cli.load_config(gateway_config(tmp_path), ENV)
    pings: list[None] = []
    real_ping = fake_redis.ping

    async def ping(*a, **k):
        pings.append(None)
        return await real_ping(*a, **k)

    monkeypatch.setattr(fake_redis, "ping", ping)
    monkeypatch.setattr(cli, "_redis_client", lambda _config: fake_redis)
    with socket.socket() as busy:
        busy.bind(("0.0.0.0", config.health_port))
        busy.listen()
        with pytest.raises(OpsPortUnavailable) as info:
            await cli.run_agent(config, {})
    assert info.value.port == config.health_port
    assert str(config.health_port) in str(info.value)
    assert exit_code_for(info.value) == EXIT_CONFIG_ERROR
    assert pings == []  # fails before any dependency is contacted


def test_main_exits_2_when_ops_port_taken(tmp_path, capsys) -> None:
    path = gateway_config(tmp_path)
    port = yaml.safe_load(path.read_text())["health_port"]
    with socket.socket() as busy:
        busy.bind(("0.0.0.0", port))
        busy.listen()
        code = cli.main(["run", "--config", str(path)], env=ENV)
    assert code == EXIT_CONFIG_ERROR
    out = capsys.readouterr()
    records = [json.loads(line) for line in (out.out + out.err).splitlines() if line.strip()]
    failure = next(r for r in records if r.get("message") == "startup failed")
    assert str(port) in failure["error"]


# --------------------------------------------------------------------------- agents


async def test_manager_registers_every_critical_loop_and_marks_shutdown(
    state_store, fake_redis
) -> None:
    env = await make_env(state_store, fake_redis)
    supervisor = env.agent.supervisor
    assert not env.agent.started
    await env.agent.start()
    try:
        assert env.agent.started
        assert supervisor.names == list(MANAGER_LOOPS)
        assert set(MANAGER_LOOPS) == {
            "claim_loop", "result_ingest", "dead_letter_ingest",
            "dead_letter_recorder", "registry_sweeper", "heartbeat",
        }
        assert supervisor.ended() == []
        # A subscription that stops outside shutdown ends its Critical_Loop (Req 16.2).
        await env.agent.result_ingest.stop()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert supervisor.ended() == ["result_ingest"]
        assert not supervisor.shutting_down
    finally:
        await env.agent.stop()
    assert supervisor.shutting_down  # Req 16.9
    assert set(supervisor.ended()) == set(MANAGER_LOOPS)


async def test_worker_registers_consume_and_heartbeat_and_marks_shutdown(fake_redis) -> None:
    h = Harness(fake_redis, FakeAdapter(), guard_pause=0.1)
    await h.connect()
    supervisor = h.worker.supervisor
    await h.worker.start()
    try:
        assert h.worker.started
        assert supervisor.names == list(WORKER_LOOPS) == ["consume", "heartbeat"]
        # A capability-guard pause is not an ended consume loop.
        await h.worker._pause_capability("research")
        assert "research" not in h.worker.subscriptions
        await asyncio.sleep(0.01)
        assert supervisor.ended() == []
        await _until(lambda: "research" in h.worker.subscriptions)
        await asyncio.sleep(0.01)
        assert supervisor.ended() == []
        # Losing a Capability subscription outside shutdown ends the consume loop.
        await h.worker.subscriptions["research"].cancel()
        await _until(lambda: supervisor.ended() == ["consume"])
        assert not supervisor.shutting_down
    finally:
        await h.worker.stop()
    assert supervisor.shutting_down
    assert set(supervisor.ended()) == set(WORKER_LOOPS)
