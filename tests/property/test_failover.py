"""Property 3: whatever a Failover interrupts, no Request ends in two different terminal states and no Job_Result is
applied twice (das-scale-resilience task 8.4; Phase 7 Req 6.3, 1.5, 1.6).

A real Request (planner, two Jobs, summarizer, Lease, result ingest: the Manager harness of ``test_manager_driver``)
runs over a Redis whose connection fails at chosen commands, in the three ways a Failover shows itself:

* ``before``: the connection resets before the command reaches the server (nothing was applied);
* ``after``: the command was applied, then the connection resets and the reply is lost (the nasty case: the retry
  repeats a write that already happened);
* ``readonly``: the command was applied, then the server answers ``READONLY`` (a demoted master).

With at most three failures, every command is retried and the run must finish exactly like a run with no failure: the
same terminal state, the same Job states and transitions, the same results and the same token totals. A repeated
script that was not idempotent would show here as a duplicated transition, a doubled total or a second terminal state.
Beyond the three retries a command raises, which is checked separately."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import pytest
from fakeredis import FakeServer
from fakeredis.aioredis import FakeAsyncRedisConnection, FakeRedis
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import ReadOnlyError

from das.manager.driver import DriverOutcome
from das.state import StateStore, keys
from das.state.store import StoreStatus
from das.state.redisclient import RETRYABLE, FailoverRetry
from tests.unit.test_manager_driver import make_env

MODES = ("before", "after", "readonly")
RUN_TIMEOUT = 20.0
#: How a run may end. If the reply to the final write is lost, the retry finds the Lease already gone (that very script
#: finished the Request and released it), so the driver reports LEASE_LOST for a Request that is stored as Completed
#: exactly once. The stored state, compared with the baseline run, is what the Property is about.
FINISHED = (DriverOutcome.COMPLETED, DriverOutcome.LEASE_LOST)


@dataclass
class Chaos:
    """Which Redis commands fail, and how. A command is one packed send; its index counts from 0 for the whole run."""

    plan: dict[int, str] = field(default_factory=dict)
    count: int = 0
    fired: list[tuple[int, str]] = field(default_factory=list)


def chaos_connection(chaos: Chaos) -> type[FakeAsyncRedisConnection]:
    class ChaosConnection(FakeAsyncRedisConnection):
        _mode: str | None = None

        async def send_packed_command(self, command: Any, check_health: bool = True) -> None:
            index, chaos.count = chaos.count, chaos.count + 1
            self._mode = chaos.plan.get(index)
            if self._mode is not None:
                chaos.fired.append((index, self._mode))
            if self._mode == "before":
                self._mode = None
                raise RedisConnectionError("failover: connection reset before the command")
            await super().send_packed_command(command, check_health)

        async def read_response(self, **kwargs: Any) -> Any:
            response = await super().read_response(**kwargs)  # the server has applied the command
            mode, self._mode = self._mode, None
            if mode == "after":
                raise RedisConnectionError("failover: connection reset, reply lost")
            if mode == "readonly":
                raise ReadOnlyError("You can't write against a read only replica.")
            return response

    return ChaosConnection


async def _no_wait(seconds: float) -> None:
    return None


def chaos_client(server: FakeServer, chaos: Chaos, retries: list[str]) -> FakeRedis:
    client = FakeRedis(server=server, decode_responses=True, connection_class=chaos_connection(chaos))
    # fakeredis drops ``retry`` and ``retry_on_error`` from its constructor; the real builder passes them to the
    # connection kwargs, so that is where this puts them.
    client.connection_pool.connection_kwargs["retry"] = FailoverRetry(on_retry=retries.append, sleep=_no_wait)
    client.connection_pool.connection_kwargs["retry_on_error"] = list(RETRYABLE)
    return client


async def projection(store: StateStore, rid: str) -> dict[str, Any]:
    """What a run left behind, without ids or times, for comparing two runs."""
    stored = await store.load_snapshot(rid, include_transitions=True)
    assert stored is not None
    assert stored.final_result is not None
    transitions: dict[str, list[tuple[Any, Any]]] = {}
    for t in stored.transitions or []:
        transitions.setdefault(t.job_id, []).append((t.from_state, t.to_state))
    return {
        "status": stored.status,
        "summary": stored.final_result.summary,
        "jobs": {r.job_id: (r.state, r.input_tokens, r.output_tokens) for r in stored.final_result.jobs},
        "transitions": transitions,
        "results": sorted((job_id, attempt) for job_id, attempt in stored.results),
        "lease_released": stored.lease is None,
    }


async def expire_lease(clean: FakeRedis, rid: str) -> None:
    """Let the Lease run out, as it does after one lease duration (the Manager claim loop then takes it again)."""
    sec, _ = await clean.time()
    await clean.hset(keys.lease(rid), "expires_at", f"{int(sec) - 1}.000000")


async def claim(env: Any, clean: FakeRedis) -> tuple[str, Any, int]:
    """Create a Request and take its Lease. A retried ``acquire_lease`` whose first reply was lost answers HELD (the
    first one did run, so the Lease is held, by us): that costs one lease duration, which is the third value here."""
    rid = await env.store.create_request("hello")
    lost = 0
    while True:
        acquired = await env.store.acquire_lease(rid, "m1", env.drivers.lease_duration)
        if acquired.ok and acquired.lease is not None:
            return rid, env.drivers.on_claimed(acquired.lease), lost
        assert acquired.status is StoreStatus.HELD, acquired
        lost += 1
        assert lost <= 1, "a Lease the first try took must be claimable once it has expired"
        await expire_lease(clean, rid)


async def run_request(plan: dict[int, str]) -> tuple[DriverOutcome, dict[str, Any], Chaos, list[str]]:
    server = FakeServer()
    chaos, retries = Chaos(plan), []
    flaky = chaos_client(server, chaos, retries)
    clean = FakeRedis(server=server, decode_responses=True)
    try:
        env = await make_env(StateStore(flaky), flaky)
        rid, task, _ = await claim(env, clean)
        outcome = await asyncio.wait_for(task, RUN_TIMEOUT)
        return outcome, await projection(StateStore(clean), rid), chaos, retries
    finally:
        await flaky.aclose()
        await clean.aclose()


@pytest.fixture(scope="module")
def baseline() -> tuple[dict[str, Any], int]:
    outcome, projected, chaos, retries = asyncio.run(run_request({}))
    assert outcome is DriverOutcome.COMPLETED and retries == [] and chaos.fired == []
    return projected, chaos.count


# --------------------------------------------------------------------------- the baseline itself


def test_the_run_without_a_failure_completes_with_each_job_once(baseline: tuple[dict[str, Any], int]) -> None:
    projected, commands = baseline
    assert projected["status"].value == "Completed" and projected["lease_released"]
    assert set(projected["jobs"]) == {"a", "b"} and len(projected["results"]) == 2
    assert commands > 20, "the harness issues enough Redis commands for the sweep to mean something"


# --------------------------------------------------------------------------- one failure, at every command


@pytest.mark.parametrize("mode", MODES)
def test_a_failure_at_any_single_command_changes_nothing(baseline: tuple[dict[str, Any], int], mode: str) -> None:
    expected, commands = baseline
    fired_somewhere = retried = 0
    for index in range(commands + 5):
        outcome, projected, chaos, retries = asyncio.run(run_request({index: mode}))
        assert outcome in FINISHED, (index, mode)
        assert projected == expected, (index, mode)
        fired_somewhere += bool(chaos.fired)
        retried += bool(retries)
    assert fired_somewhere >= commands, "the sweep actually injected a failure at (nearly) every command"
    # A failure on a connection-setup command is swallowed by redis-py by design, so not every one is retried; but
    # almost every command of the run is a real one, and a failure there is retried and counted.
    assert retried >= commands // 2, f"only {retried} of {fired_somewhere} failures were retried and counted"


# --------------------------------------------------------------------------- up to three failures, anywhere


failure_plans = st.dictionaries(
    st.integers(min_value=0, max_value=70), st.sampled_from(MODES), min_size=1, max_size=3
)


@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(failure_plans)
def test_up_to_three_failures_anywhere_change_nothing(
    baseline: tuple[dict[str, Any], int], plan: dict[int, str]
) -> None:
    expected, _ = baseline
    outcome, projected, _, _ = asyncio.run(run_request(plan))
    assert outcome in FINISHED, plan
    assert projected == expected, plan


@pytest.mark.parametrize("mode", ["before", "after"])
def test_three_failures_in_a_row_on_one_command_are_survived(baseline: tuple[dict[str, Any], int], mode: str) -> None:
    expected, commands = baseline
    for start in range(0, commands, 7):
        plan = {start: mode, start + 1: mode, start + 2: mode}
        outcome, projected, _, _ = asyncio.run(run_request(plan))
        assert outcome in FINISHED and projected == expected, plan


# --------------------------------------------------------------------------- beyond the retries (Req 1.5)


async def test_a_command_that_fails_four_times_in_a_row_is_raised_not_retried_forever() -> None:
    server, chaos, retries = FakeServer(), Chaos(), []
    client = chaos_client(server, chaos, retries)
    try:
        await client.set("warm", "1")  # opens the connection, so the next sends are plain commands
        base = chaos.count
        chaos.plan = {base: "before", base + 1: "before", base + 2: "before", base + 3: "before"}
        with pytest.raises(RedisConnectionError):
            await client.set("k", "v")
        assert retries == ["connection"] * 3
    finally:
        await client.aclose()


# ------------------------------------------------------------------- what a lost reply to acquire costs (Req 1.6)


async def test_a_lost_reply_to_acquire_lease_leaves_the_lease_with_its_owner_until_it_expires() -> None:
    """The first ``acquire_lease`` ran, its reply was lost, and the retry meets the Lease it just took: HELD. Nothing
    is unsafe (the fencing epoch still guards every write) but the Request waits one lease duration, then any Manager
    takes the expired Lease and the epoch moves on. A re-entrant acquire could start a second driver for the same
    Request, which is worse than the delay, so the design accepts it (see the Failover section of the design).

    Which command of the acquire loses its reply is found by trying the next few: the one that ends HELD is the
    script."""
    held = 0
    for offset in range(8):
        server, chaos, retries = FakeServer(), Chaos(), []
        flaky = chaos_client(server, chaos, retries)
        clean = FakeRedis(server=server, decode_responses=True)
        try:
            store = StateStore(flaky)
            rid = await store.create_request("hello")
            chaos.plan = {chaos.count + offset: "after"}
            first = await store.acquire_lease(rid, "m1", 30)
            if first.ok:
                continue  # the failure hit another command of the call, which the retry made harmless
            assert first.status is StoreStatus.HELD and retries == ["connection"]
            held += 1
            assert (await clean.hget(keys.lease(rid), "owner")) == "m1"  # held, by the one that asked
            epoch = int(await clean.hget(keys.lease(rid), "epoch"))
            await expire_lease(clean, rid)
            second = await StateStore(clean).acquire_lease(rid, "m2", 30)
            assert second.ok and second.lease is not None and second.lease.epoch > epoch
        finally:
            await flaky.aclose()
            await clean.aclose()
    assert held >= 1, "no lost reply produced the HELD case this test documents"
