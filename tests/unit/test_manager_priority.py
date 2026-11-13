"""The Manager carries a Request's priority to dispatch (das-multi-tenant task 6.2; Phase 5 Req 7.4).

Every dispatch of a Job of a ``high`` Request, retries and the re-dispatch after a takeover included, is published with
the Priority_Header ``das-priority: high``; a ``normal`` Request's dispatches carry none. The payload is the same
either way."""

from __future__ import annotations

import asyncio
from typing import Any

from das.manager.driver import DriverOutcome, RequestDrivers
from das.manager.machine import MachineConfig
from das.state import ClientRequestSpec, StateStore
from das.transport.base import PRIORITY_HEADER
from tests.unit.test_manager_driver import ONE_JOB, TIMEOUT, TWO_JOBS, _true, make_env, steal_lease, wait_until

JOBS_PREFIX = "jobs/"


async def claim(env: Any, priority: str | None) -> tuple[str, Any]:
    """A Request of ``team-a`` with this priority, claimed by the Manager ``m1``."""
    spec = ClientRequestSpec(fingerprint="cbabecbd", tenant="team-a", priority=priority)
    rid = (await env.store.submit_request("hello", spec)).request_id
    assert rid is not None
    acquired = await env.store.acquire_lease(rid, "m1", env.drivers.lease_duration)
    assert acquired.ok and acquired.lease is not None
    return rid, env.drivers.on_claimed(acquired.lease)


def dispatches(env: Any) -> list[Any]:
    """Every dispatch the Manager put on the wire, in order."""
    return [r for r in env.broker.wire if r.destination.startswith(JOBS_PREFIX)]


async def test_every_dispatch_of_a_high_request_carries_the_header(state_store: StateStore, fake_redis: Any) -> None:
    env = await make_env(state_store, fake_redis)
    _, task = await claim(env, "high")
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED
    sent = dispatches(env)
    assert len(sent) == 2  # a, then b
    assert all(r.headers.get(PRIORITY_HEADER) == "high" for r in sent)


async def test_a_normal_request_carries_no_header(state_store: StateStore, fake_redis: Any) -> None:
    env = await make_env(state_store, fake_redis, plan_outcomes=(TWO_JOBS, TWO_JOBS), summary_outcomes=("x", "y"))
    for priority in (None, "normal"):
        _, task = await claim(env, priority)
        assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED
    sent = dispatches(env)
    assert len(sent) == 4 and all(PRIORITY_HEADER not in r.headers for r in sent)


async def test_a_request_written_before_the_upgrade_carries_no_header(state_store: StateStore, fake_redis: Any) -> None:
    env = await make_env(state_store, fake_redis)
    _, task = await env.submit_and_claim()  # the Phase 4 path: no tenant and no priority field
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED
    sent = dispatches(env)
    assert sent and all(PRIORITY_HEADER not in r.headers for r in sent)


async def test_retries_of_a_high_request_carry_the_header(state_store: StateStore, fake_redis: Any) -> None:
    env = await make_env(
        state_store, fake_redis, plan_outcomes=(ONE_JOB,), machine_config=MachineConfig(max_attempts=3)
    )
    env.worker.status = "Failed"  # every attempt fails: three dispatches of the same Job
    _, task = await claim(env, "high")
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.FAILED
    sent = dispatches(env)
    assert len(sent) == 3
    assert all(r.headers.get(PRIORITY_HEADER) == "high" for r in sent)


async def test_a_takeover_redispatch_carries_the_header(state_store: StateStore, fake_redis: Any) -> None:
    env = await make_env(state_store, fake_redis, plan_outcomes=(ONE_JOB,), lease_duration=1.0)
    env.worker.mode = "hold"  # the Job stays in flight
    rid, task = await claim(env, "high")
    await wait_until(lambda: _true(len(dispatches(env)) == 1))
    await steal_lease(fake_redis, state_store, rid)  # another Manager now holds the Lease
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.lease is not None and stored.lease.owner == "m2"
    task2 = RequestDrivers(env.drivers.deps, 30.0).on_claimed(stored.lease)
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.LEASE_LOST
    await wait_until(lambda: _true(len(dispatches(env)) >= 2))  # the new owner re-published the in-flight Job
    assert all(r.headers.get(PRIORITY_HEADER) == "high" for r in dispatches(env))
    task2.cancel()


async def test_the_payload_is_the_same_with_or_without_priority(state_store: StateStore, fake_redis: Any) -> None:
    """Only the header differs: the decoded dispatch payloads match field for field, ids aside."""
    env = await make_env(state_store, fake_redis, plan_outcomes=(TWO_JOBS, TWO_JOBS), summary_outcomes=("x", "y"))
    payloads: dict[str, list[dict[str, Any]]] = {}
    for priority in ("high", "normal"):
        env.worker.dispatches.clear()
        _, task = await claim(env, priority)
        assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED
        payloads[priority] = [{**m.payload.model_dump(mode="json"), "request_id": None} for m in env.worker.dispatches]
    assert payloads["high"] and payloads["high"] == payloads["normal"]
