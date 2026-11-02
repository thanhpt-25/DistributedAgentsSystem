"""Drain end to end: Admin_API -> real request_reply -> agent -> ``/v1/agents`` (task 16.5).

The Gateway app built by ``create_app`` sends ``agent_control`` over an
in-memory control transport to a real Worker or Manager agent; the operator then
sees ``drained`` in ``GET /v1/agents``. Also checks that ``das run`` wires the
Gateway's own agent ID so draining it is a 409 (Req 17.11).

Covers Req 17.1, 17.2, 17.3, 17.5, 17.7, 17.8, 17.9, 17.11.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

import das.cli as cli
from das.errors import EXIT_OK
from das.gateway.app import create_app
from das.registry import AgentRegistry
from das.state import StateStore
from tests.fakes.llm_adapter import FakeAdapter, completion
from tests.unit.test_manager_agent import make_env, wait_until
from tests.unit.test_ops_wiring import ENV, _wait_ready, gateway_config
from tests.unit.test_worker import Harness, ManualClock, _until

API_KEY = "client-api-key-0001"
ADMIN_KEY = "operator-admin-key-0001"
ADMIN = {"X-API-Key": ADMIN_KEY}
CLIENT = {"X-API-Key": API_KEY}


def _gateway(store: StateStore, control: Any) -> httpx.AsyncClient:
    app = create_app(
        store, AgentRegistry(store.client), control, [API_KEY], 100,
        admin_api_keys=[ADMIN_KEY], agent_id="gw-1",
    )
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw")


async def _listing(client: httpx.AsyncClient, aid: str) -> tuple[str, int, bool]:
    r = await client.get("/v1/agents", headers=CLIENT)
    assert r.status_code == 200
    a = next(a for a in r.json() if a["agent_id"] == aid)
    return a["status"], a["in_flight"], a["drained"]


async def test_worker_drain_via_admin_api_reaches_drained(fake_redis, state_store) -> None:
    release = asyncio.Event()

    async def slow(system, messages, params):
        await release.wait()
        return completion("done")

    clock = ManualClock()  # drives the Worker heartbeat deterministically
    h = Harness(fake_redis, FakeAdapter(default=slow), clock=clock, heartbeat_interval=3)
    await h.connect()
    await h.worker.start()
    try:
        async with _gateway(state_store, h.manager_control) as gw:
            await h.dispatch(job_id="running")
            await _until(lambda: h.worker.in_flight == 1)

            r = await gw.post("/v1/admin/agents/worker-1/drain", headers=ADMIN,
                              json={"release_leases": True})  # ignored by Workers
            assert r.status_code == 202
            assert r.json() == {"agent_id": "worker-1", "status": "Draining"}
            assert h.worker.draining
            # Running Job keeps the Worker from being drained (Req 17.7).
            assert await _listing(gw, "worker-1") == ("Draining", 1, False)

            # Idempotent through the API too (Req 17.9).
            again = await gw.post("/v1/admin/agents/worker-1/drain", headers=ADMIN)
            assert again.status_code == 202 and again.json()["status"] == "Draining"

            # The running Job finishes and publishes its result (Req 17.3).
            release.set()
            await _until(lambda: h.results())
            assert h.results()[0].payload.status == "Succeeded"
            await _until(lambda: h.worker.in_flight == 0)
            # The next heartbeat reports in_flight=0 -> drained.
            await _until(lambda: clock.sleeps == [3])
            clock.tick()
            await _until(lambda: len(clock.sleeps) == 2)
            assert await _listing(gw, "worker-1") == ("Draining", 0, True)

            r = await gw.post("/v1/admin/agents/worker-1/undrain", headers=ADMIN)
            assert r.status_code == 202
            assert r.json() == {"agent_id": "worker-1", "status": "Available"}
            assert await _listing(gw, "worker-1") == ("Available", 0, False)
            assert not h.worker.draining
    finally:
        await h.worker.stop()


async def test_manager_drain_release_leases_via_admin_api(fake_redis, state_store) -> None:
    env = await make_env(state_store, fake_redis, hold=True, heartbeat_interval=1.0)
    await env.agent.start()
    gw_control = type(env.control)(env.control.broker, agent_id="gw-1")
    await gw_control.start()
    try:
        async with _gateway(state_store, gw_control) as gw:
            rid = await state_store.create_request("hello")
            await wait_until(lambda: _true(len(env.worker.dispatches) > 0))
            assert rid in env.agent.drivers.owned

            r = await gw.post("/v1/admin/agents/m1/drain", headers=ADMIN,
                              json={"release_leases": True})
            assert r.status_code == 202
            assert r.json() == {"agent_id": "m1", "status": "Draining"}
            assert env.agent.claim_loop.paused

            # Leases released within 5 s (Req 17.5); heartbeat then reports 0 -> drained.
            async def drained() -> bool:
                return await _listing(gw, "m1") == ("Draining", 0, True)

            await wait_until(drained, timeout=7.0)
            assert await state_store.get_lease(rid) is None
            # Another Manager can take the Request over.
            assert (await state_store.acquire_lease(rid, "m2", 30)).ok
    finally:
        await gw_control.close()
        await env.agent.stop()


async def _true(value: bool) -> bool:
    return value


async def test_run_agent_wires_gateway_agent_id_so_self_drain_is_409(
    tmp_path, fake_redis, monkeypatch
) -> None:
    env = {**ENV, "DAS__ADMIN_API_KEYS": ADMIN_KEY}
    config = cli.load_config(gateway_config(tmp_path), env)
    monkeypatch.setattr(cli, "_redis_client", lambda _config: fake_redis)
    stop = asyncio.Event()
    runner = asyncio.create_task(cli.run_agent(config, {}, stop=stop))
    http = f"http://127.0.0.1:{config.http_port}"
    try:
        async with httpx.AsyncClient(timeout=3) as client:
            await _wait_ready(client, f"{http}/healthz")
            for command in ("drain", "undrain"):
                r = await client.post(f"{http}/v1/admin/agents/gw-test/{command}", headers=ADMIN)
                assert r.status_code == 409
                assert r.json() == {"error": "agent_state", "status": "gateway"}
    finally:
        stop.set()
        assert await asyncio.wait_for(runner, 10) == EXIT_OK
