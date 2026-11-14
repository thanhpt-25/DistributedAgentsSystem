"""A Redis master stopped during a Request, on the real stack (das-scale-resilience task 11.2; Phase 7 Req 1.6-1.9).

Brings up ``deploy/docker-compose.yml`` with ``docker-compose.sentinel.yml`` (one master, one replica, three Sentinels,
every agent in Sentinel mode) and the stub LLM, under the project ``das-e2e-sentinel``. While a Job is running the
master is stopped: the Sentinels promote the replica, the agents retry through it, and the Request must still finish,
with every Job succeeding exactly once. Then the old master comes back and rejoins as a replica, and the stack takes a
further Request. The stack is always removed with ``docker compose down -v``; the module is skipped without Docker."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from tests.integration.e2e.stackkit import DEPLOY, ComposeStack, docker_available, running_stack

pytestmark = pytest.mark.integration

SENTINEL_FILE = DEPLOY / "docker-compose.sentinel.yml"
MASTER = "das"
AGENT_SERVICES = ("gateway", "manager", "worker-research", "worker-review")
FAILOVER_BUDGET = 60.0  # seconds from the master stopping to a new master named; 3 s to notice plus the election


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[ComposeStack]:
    if not docker_available():
        pytest.skip("Docker / docker compose not available; the Sentinel end-to-end test needs the Compose stack")
    with running_stack(
        "das-e2e-sentinel", [SENTINEL_FILE], ("sentinel",), tmp_path_factory.mktemp("failover")
    ) as running:
        yield running


def master_address(stack: ComposeStack, sentinel: str = "sentinel-1") -> tuple[str, str]:
    host, port = stack.redis_cli(sentinel, "SENTINEL", "get-master-addr-by-name", MASTER, port=26379).split()
    return host, port


def role(stack: ComposeStack, service: str) -> str:
    return stack.redis_cli(service, "ROLE").split()[0]


def failovers(stack: ComposeStack) -> float:
    return sum(stack.metric(service, "das_redis_failovers_total") for service in AGENT_SERVICES)


def wait(condition: Callable[[], Any], timeout: float, what: str) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = condition()
            if value:
                return value
        except AssertionError:
            pass
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for {what}")


# --------------------------------------------------------------------------- the topology


def test_the_stack_runs_on_one_master_one_replica_and_three_sentinels(stack: ComposeStack) -> None:
    """Req 1.9: the profile's topology, and every Sentinel agrees on it."""
    assert role(stack, "redis") == "master" and role(stack, "redis-replica") == "slave"
    for sentinel in ("sentinel-1", "sentinel-2", "sentinel-3"):
        info = stack.redis_cli(sentinel, "SENTINEL", "master", MASTER, port=26379).split("\n")
        fields = dict(zip(info[0::2], info[1::2], strict=False))
        counts = (fields["quorum"], fields["num-slaves"], fields["num-other-sentinels"])
        assert counts == ("2", "1", "2"), sentinel
        assert fields["flags"] == "master", f"{sentinel} sees {fields['flags']}"


def test_a_request_completes_on_the_sentinel_stack(stack: ComposeStack) -> None:
    body = stack.wait_terminal(stack.submit("Explain the CAP theorem and review a short snippet."), timeout=60)
    assert body["status"] == "Completed" and {j["state"] for j in body["jobs"]} == {"Succeeded"}


# --------------------------------------------------------------------------- the Failover (Req 1.6)


def test_stopping_the_master_during_a_request_lets_the_request_finish_with_each_job_succeeding_once(
    stack: ComposeStack,
) -> None:
    before = master_address(stack)
    rid = stack.submit("Failover probe [[delay=8]]")
    stack.wait_for(
        rid, lambda b: b["status"] == "Running" and any(j["state"] == "Dispatched" for j in b["jobs"]), timeout=30
    )

    stopped = time.monotonic()
    stack.compose("stop", "redis")
    after = wait(
        lambda: (m := master_address(stack)) != before and m, FAILOVER_BUDGET, "the Sentinels to name a new master"
    )
    print(f"\n[failover] new master named {time.monotonic() - stopped:.1f}s after the stop: {before} -> {after}")
    assert role(stack, "redis-replica") == "master", "the replica was promoted"

    body = stack.wait_terminal(rid, timeout=150)
    assert body["status"] == "Completed", body
    assert {j["state"] for j in body["jobs"]} == {"Succeeded"}
    succeeded = [t["job_id"] for t in body["transitions"] if t["to_state"] == "Succeeded"]
    assert sorted(succeeded) == sorted({j["job_id"] for j in body["jobs"]}), "no Job_Result was applied twice"
    assert body["final_result"]["failures"] == []


def test_the_agents_counted_the_failover_and_their_retries(stack: ComposeStack) -> None:
    """Req 1.8: das_redis_failovers_total moved on at least one agent, and commands were retried through it."""
    assert failovers(stack) >= 1
    retries = sum(stack.metric(service, "das_redis_retries_total") for service in AGENT_SERVICES)
    assert retries >= 1


def test_the_stack_takes_a_new_request_on_the_new_master(stack: ComposeStack) -> None:
    body = stack.wait_terminal(stack.submit("After the failover"), timeout=90)
    assert body["status"] == "Completed" and {j["state"] for j in body["jobs"]} == {"Succeeded"}


def test_the_old_master_rejoins_as_a_replica_and_the_stack_keeps_working(stack: ComposeStack) -> None:
    stack.compose("start", "redis")
    wait(lambda: role(stack, "redis") == "slave", FAILOVER_BUDGET, "the old master to rejoin as a replica")
    assert role(stack, "redis-replica") == "master"
    body = stack.wait_terminal(stack.submit("With the old master back"), timeout=90)
    assert body["status"] == "Completed"


def test_the_readiness_endpoints_are_healthy_again(stack: ComposeStack) -> None:
    """Req 1.7: with a master named, the Redis dependency is healthy on every agent."""
    for service in AGENT_SERVICES:
        status, _ = wait(
            lambda s=service: (r := stack.fetch(s, "http://127.0.0.1:8081/readyz")) and r[0] == 200 and r,
            60, f"{service} to be ready",
        )
        assert status == 200, service
