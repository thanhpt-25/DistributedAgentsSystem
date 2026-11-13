"""The Manager carries a Job's Output_Schema to the Worker, and into its dead letter (das-smarter-agents task 1.2;
Req 1.3). Real driver, State_Store and in-memory broker, as in ``test_manager_driver``."""

from __future__ import annotations

import asyncio
import json

from das.manager.driver import DriverOutcome
from das.manager.machine import MachineConfig
from das.state import StateStore
from tests.unit.test_manager_driver import TIMEOUT, make_env

SCHEMA = {"type": "object", "properties": {"n": {"type": "integer"}}, "required": ["n"]}
PLAN = json.dumps(
    {
        "jobs": [
            {"job_id": "a", "instruction": "count", "capabilities": ["search"], "output_schema": SCHEMA},
            {"job_id": "b", "instruction": "write prose", "capabilities": ["write"]},
        ]
    }
)


async def test_only_the_structured_job_is_dispatched_with_a_schema(state_store: StateStore, fake_redis) -> None:
    env = await make_env(state_store, fake_redis, plan_outcomes=(PLAN,))
    rid, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED
    by_job = {m.payload.job_id: m.payload for m in env.worker.dispatches}
    assert by_job["a"].output_schema == SCHEMA
    assert by_job["b"].output_schema is None
    stored = await state_store.load_snapshot(rid)
    assert stored is not None and stored.plan is not None
    assert stored.plan.by_id()["a"].output_schema == SCHEMA  # the plan kept it too


async def test_the_unstructured_jobs_wire_bytes_have_no_schema_key(state_store: StateStore, fake_redis) -> None:
    env = await make_env(state_store, fake_redis, plan_outcomes=(PLAN,))
    _, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.COMPLETED
    sent = {r.destination: r.body for r in env.broker.wire if r.destination.startswith("jobs/")}
    assert b"output_schema" in sent["jobs/search"]
    assert b"output_schema" not in sent["jobs/write"]


async def test_the_dead_letter_of_a_structured_job_is_a_faithful_copy(state_store: StateStore, fake_redis) -> None:
    env = await make_env(
        state_store, fake_redis, plan_outcomes=(PLAN,), machine_config=MachineConfig(max_attempts=1)
    )
    env.worker.status = "Failed"
    _, task = await env.submit_and_claim()
    assert await asyncio.wait_for(task, TIMEOUT) is DriverOutcome.FAILED
    letters = {m.payload.job_id: m.payload for m in env.broker.pending_messages("dlq")}  # type: ignore[union-attr]
    assert set(letters) == {"a", "b"}  # both jobs fail with one attempt allowed
    assert letters["a"].output_schema == SCHEMA
    assert letters["b"].output_schema is None
    assert b"output_schema" not in next(b for b in env.broker.pending("dlq") if b"write prose" in b)
