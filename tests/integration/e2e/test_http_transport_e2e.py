"""A Request over the HTTP transport, on the real stack (das-scale-resilience task 11.3; Phase 7 Req 2.1, 2.2, 2.11).

Brings up ``deploy/docker-compose.yml`` with ``docker-compose.http-transport.yml`` and the stub LLM, under the project
``das-e2e-http``: the Gateway serves the HTTP_Broker on its Ops_Port, the Manager and the Workers exchange Jobs and
Job_Results through it, control still travels over gRPC, and RabbitMQ is not started at all. A Request must run to a
Final_Result; the queues must be the broker's own streams in Redis; and the broker must refuse a wrong or missing token.
The stack is always removed with ``docker compose down -v``; the module is skipped without Docker."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from tests.integration.e2e.stackkit import DEPLOY, ComposeStack, docker_available, running_stack

pytestmark = pytest.mark.integration

HTTP_FILE = DEPLOY / "docker-compose.http-transport.yml"
TOKEN = "e2e-http-broker-token-0123456789"
PING = "http://127.0.0.1:8081/broker/v1/ping"  # the Gateway's Ops_Port, from inside its own container


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[ComposeStack]:
    if not docker_available():
        pytest.skip("Docker / docker compose not available; the HTTP transport end-to-end test needs the Compose stack")
    with running_stack(
        "das-e2e-http", [HTTP_FILE], (), tmp_path_factory.mktemp("http"), extra_env={"DAS_HTTP_BROKER_TOKEN": TOKEN}
    ) as running:
        yield running


def test_rabbitmq_is_not_part_of_the_stack(stack: ComposeStack) -> None:
    """Req 2.11: the stack needs no AMQP broker, and no agent is told where one is."""
    assert stack.containers("rabbitmq", all_states=True) == []
    for service in ("manager", "worker-research", "worker-review"):
        env = stack.exec(service, "env")
        assert "DAS__RABBITMQ__URL" not in env and "DAS__TRANSPORTS__JOBS=http" in env, service
        assert TOKEN not in stack.exec(service, "cat", "/etc/das/config.yaml"), "the token is never in a file"


def test_a_prompt_runs_to_a_final_result_with_jobs_carried_over_http(stack: ComposeStack) -> None:
    body = stack.wait_terminal(stack.submit("Explain the CAP theorem and review a short snippet."), timeout=90)
    assert body["status"] == "Completed", body
    assert {j["job_id"] for j in body["jobs"]} == {"j1", "j2", "j3"}
    assert {j["state"] for j in body["jobs"]} == {"Succeeded"}
    assert body["final_result"]["summary"] == "fake-llm final answer combining the job results."
    assert body["final_result"]["failures"] == []


def test_the_jobs_and_their_results_went_through_the_brokers_streams_in_redis(stack: ComposeStack) -> None:
    keys = set(stack.redis_cli("redis", "KEYS", "das:broker:*").split())
    assert {"das:broker:das.jobs.research", "das:broker:das.jobs.code-review", "das:broker:das.results"} <= keys, keys
    for key in ("das:broker:das.jobs.research", "das:broker:das.results"):
        groups = stack.redis_cli("redis", "XINFO", "GROUPS", key).split()
        assert "das" in groups, f"{key} has no consumer group: the broker was used"


def test_every_job_was_acknowledged_so_nothing_is_left_pending(stack: ComposeStack) -> None:
    for key in ("das:broker:das.jobs.research", "das:broker:das.jobs.code-review", "das:broker:das.results"):
        pending = stack.redis_cli("redis", "XPENDING", key, "das").split()[0]
        assert pending == "0", f"{key} holds {pending} unacknowledged messages"


def test_the_broker_answers_only_to_the_right_token(stack: ComposeStack) -> None:
    assert stack.fetch("gateway", PING, method="POST", token=TOKEN)[0] == 200
    assert stack.fetch("gateway", PING, method="POST", token="not-the-token-0123456789")[0] == 401
    assert stack.fetch("gateway", PING, method="POST")[0] == 401


def test_the_broker_is_on_the_ops_port_only_and_not_on_the_client_port(stack: ComposeStack) -> None:
    status, _ = stack.fetch("gateway", "http://127.0.0.1:8080/broker/v1/ping", method="POST", token=TOKEN)
    assert status == 404, "the Client API port does not serve the broker"


def test_several_requests_in_a_row_all_complete(stack: ComposeStack) -> None:
    ids = [stack.submit(f"Request number {n}") for n in range(5)]
    assert [stack.wait_terminal(rid, timeout=120)["status"] for rid in ids] == ["Completed"] * 5
