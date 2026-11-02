"""Worker_Agent drain / undrain over ``agent_control`` request-reply (task 16.2).

Covers Req 17.2, 17.3, 17.6, 17.8, 17.9, 17.13, 17.15, 17.16.
"""

from __future__ import annotations

import asyncio

import pytest

from das.codec import AgentControlPayload, AgentControlReplyMessage, new_message
from das.transport.base import RequestTimeout
from das.worker.worker import CONSUME_LOOP
from tests.fakes.llm_adapter import FakeAdapter, completion
from tests.unit.test_worker import Harness, ManualClock, _until


async def _command(h: Harness, command: str, *, agent_id: str = "worker-1",
                   release_leases: bool = False, timeout: float = 2.0):
    payload = AgentControlPayload(command=command, agent_id=agent_id,
                                  release_leases=release_leases)
    reply = await h.manager_control.request_reply(
        "worker-1", new_message(payload, correlation_id=agent_id), timeout=timeout
    )
    assert isinstance(reply, AgentControlReplyMessage)
    return reply.payload


async def _record(h: Harness):
    return (await h.registry.snapshot()).get("worker-1")


async def test_drain_stops_pulling_sets_draining_and_keeps_consume_loop_alive(fake_redis):
    h = Harness(fake_redis, FakeAdapter(default="ok"), caps=("research", "summarize"))
    await h.connect()
    await h.worker.start()
    try:
        loop = asyncio.get_running_loop()
        started = loop.time()
        reply = await _command(h, "drain", release_leases=True)  # ignored (Req 17.15)
        assert loop.time() - started < 1.0  # Req 17.2
        assert (reply.agent_id, reply.status, reply.in_flight) == ("worker-1", "Draining", 0)
        assert h.worker.draining
        assert h.worker.subscriptions == {} and h.queue.subscriptions == []
        assert (await _record(h)).status == "Draining"
        # New dispatches stay queued; nothing executes.
        await h.dispatch(job_id="later")
        await asyncio.sleep(0.05)
        await h.broker.settle()
        assert len(h.broker.pending("jobs/research")) == 1
        assert h.adapter.calls == [] and h.results() == []
        # Draining isn't an ended Critical_Loop (Liveness_Probe).
        assert CONSUME_LOOP not in h.worker.supervisor.ended()
        # Idempotent (Req 17.9).
        again = await _command(h, "drain")
        assert again.status == "Draining" and h.worker.draining
    finally:
        await h.worker.stop()


async def test_received_but_not_started_dispatch_is_requeued_without_result(fake_redis):
    h = Harness(fake_redis, FakeAdapter(default="ok"))
    await h.connect()
    await h.worker.start()
    try:
        await _command(h, "drain")
        # A dispatch that reaches the handler after drain (e.g. fetched just before).
        seen: list[int] = []

        async def handler(delivery):
            seen.append(1)
            await h.worker._on_dispatch(delivery)

        sub = await h.queue.subscribe("jobs/research", handler)
        await h.dispatch(job_id="raced")
        await _until(lambda: seen)
        await sub.cancel()
        await h.broker.settle()
        assert [m.payload.job_id for m in h.broker.pending_messages("jobs/research")] == ["raced"]
        assert h.adapter.calls == [] and h.results() == []
    finally:
        await h.worker.stop()


async def test_running_job_finishes_and_publishes_while_draining(fake_redis):
    release = asyncio.Event()

    async def slow(system, messages, params):
        await release.wait()
        return completion("late")

    h = Harness(fake_redis, FakeAdapter(default=slow))
    await h.connect()
    await h.worker.start()
    try:
        await h.dispatch(job_id="running")
        await _until(lambda: h.worker.in_flight == 1)
        reply = await _command(h, "drain")
        assert (reply.status, reply.in_flight) == ("Draining", 1)
        rec = await _record(h)
        assert (rec.status, rec.in_flight) == ("Draining", 1)
        release.set()
        await _until(lambda: h.results())
        await h.broker.settle()
        [result] = h.results()
        assert (result.payload.job_id, result.payload.status) == ("running", "Succeeded")
        assert h.broker.unacked_count("jobs/research") == 0
        assert h.worker.in_flight == 0 and h.worker.draining
    finally:
        await h.worker.stop()


async def test_heartbeat_reports_draining_and_in_flight(fake_redis):
    clock = ManualClock()
    h = Harness(fake_redis, FakeAdapter(), clock=clock, heartbeat_interval=3)
    await h.connect()
    await h.worker.start()
    try:
        await _until(lambda: clock.sleeps == [3])
        await _command(h, "drain")
        # Something else flipped the record back; the heartbeat repairs it (Req 17.3).
        await h.registry.set_status("worker-1", "Available", 5)
        clock.tick()
        await _until(lambda: len(clock.sleeps) == 2)
        rec = await _record(h)
        assert (rec.status, rec.in_flight) == ("Draining", 0)
        # Re-registered after being forgotten: still Draining.
        await h.registry.deregister("worker-1")
        clock.tick()
        await _until(lambda: len(clock.sleeps) == 3)
        assert (await _record(h)).status == "Draining"
    finally:
        await h.worker.stop()


async def test_undrain_resumes_pulling_and_sets_available(fake_redis):
    h = Harness(fake_redis, FakeAdapter(default="ok"))
    await h.connect()
    await h.worker.start()
    try:
        # Undrain while Available is a no-op confirmation (Req 17.9).
        reply = await _command(h, "undrain")
        assert reply.status == "Available" and not h.worker.draining
        assert list(h.worker.subscriptions) == ["research"]

        await _command(h, "drain")
        await h.dispatch(job_id="queued")
        await asyncio.sleep(0.05)
        assert h.results() == []
        loop = asyncio.get_running_loop()
        started = loop.time()
        reply = await _command(h, "undrain")
        assert reply.status == "Available" and not h.worker.draining
        assert (await _record(h)).status == "Available"
        await _until(lambda: h.results(), timeout=1.0)  # Req 17.8: within 1 s
        assert loop.time() - started < 1.0
        assert [m.payload.job_id for m in h.results()] == ["queued"]
        assert CONSUME_LOOP not in h.worker.supervisor.ended()
        # The consume loop still watches the new subscription after undrain.
        await h.worker.subscriptions["research"].cancel()
        await _until(lambda: CONSUME_LOOP in h.worker.supervisor.ended())
    finally:
        await h.worker.stop()


async def test_misrouted_agent_control_gets_no_reply(fake_redis):
    h = Harness(fake_redis, FakeAdapter())
    await h.connect()
    await h.worker.start()
    try:
        with pytest.raises(RequestTimeout):
            await _command(h, "drain", agent_id="someone-else", timeout=0.2)
        assert not h.worker.draining
    finally:
        await h.worker.stop()


async def test_shutdown_while_draining_is_graceful_and_restart_is_available(fake_redis):
    h = Harness(fake_redis, FakeAdapter(default="ok"), shutdown_timeout=1)
    await h.connect()
    await h.worker.start()
    await _command(h, "drain")
    await h.worker.stop()  # Req 17.13: v1 graceful shutdown
    assert await _record(h) is None
    assert not h.queue.connected and not h.control.connected
    assert h.worker.supervisor.shutting_down

    # Req 17.16: a restarted Worker registers as Available.
    h2 = Harness(fake_redis, FakeAdapter(default="ok"))
    await h2.connect()
    await h2.worker.start()
    try:
        assert (await _record(h2)).status == "Available" and not h2.worker.draining
    finally:
        await h2.worker.stop()
