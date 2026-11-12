"""The Helm chart on a real kind cluster (das-multi-tenant task 11.1; Phase 5 Req 11.3).

The plan's spike G0b found what the chart has to get right on Kubernetes; these tests pin it: pods come up Ready and
register, a Request completes, agents advertise their pod IP so the Gateway can reach them over gRPC (admin drain), and
no Request is lost when a pod is deleted or killed or a Deployment rolls. A ``[[delay=N]]`` marker in the prompt makes
the stub LLM hold a Job for N seconds, so a Job is in flight when the pod goes."""

from __future__ import annotations

import time
from typing import Any

import httpx
import pytest

from tests.integration.k8s.cluster import MANAGERS, WORKERS, Cluster

pytestmark = pytest.mark.integration

TERMINAL = ("Completed", "Failed", "Cancelled")
DONE_WITHIN = 180  # seconds a Request may take, including a lease takeover after a force kill


def submit(cluster: Cluster, prompt: str) -> str:
    with cluster.client() as c:
        reply = c.post("/v1/requests", json={"prompt": prompt})
        assert reply.status_code == 202, reply.text
        return str(reply.json()["request_id"])


def view(cluster: Cluster, rid: str) -> dict[str, Any]:
    with cluster.client() as c:
        reply = c.get(f"/v1/requests/{rid}")
        reply.raise_for_status()
        return dict(reply.json())


def wait_for_status(
    cluster: Cluster, rid: str, accept: tuple[str, ...] = TERMINAL, timeout: float = DONE_WITHIN
) -> dict[str, Any]:
    box: dict[str, Any] = {}

    def done() -> bool:
        box["v"] = view(cluster, rid)
        return box["v"]["status"] in accept

    cluster.wait_for(done, timeout, f"request {rid} to reach {accept}", interval=0.5)
    return box["v"]  # type: ignore[no-any-return]


def wait_job_running(cluster: Cluster, rid: str) -> None:
    cluster.wait_for(
        lambda: any(j["state"] == "Dispatched" for j in view(cluster, rid)["jobs"]),
        60,
        f"a job of {rid} in flight",
        interval=0.5,
    )
    time.sleep(2)  # let the Worker take it up and the stub start holding it


def assert_completed(v: dict[str, Any]) -> None:
    assert v["status"] == "Completed", v
    assert v["jobs"] and all(j["state"] == "Succeeded" for j in v["jobs"]), v


# --------------------------------------------------------------------------- up and serving


def test_every_pod_is_ready_and_nothing_restarted(cluster: Cluster) -> None:
    state = ".status.containerStatuses[0]"
    row = "{.metadata.name} {.status.phase} {" + state + ".ready} {" + state + ".restartCount}"
    pods = cluster.kubectl(
        "get", "pods", "-l", "app.kubernetes.io/name=das", "-o", "jsonpath={range .items[*]}" + row + r"{'\n'}{end}"
    ).splitlines()
    assert len(pods) == 1 + MANAGERS + WORKERS, pods  # a Gateway, the Managers and the Workers
    for line in pods:
        name, phase, ready, restarts = line.split()
        assert (phase, ready, restarts) == ("Running", "true", "0"), line


def test_the_agents_register_their_pod_ip_as_the_grpc_endpoint(cluster: Cluster) -> None:
    by_id = {a["agent_id"]: a for a in cluster.agents()}
    research, review = cluster.pods("worker-research"), cluster.pods("worker-review")
    pods = cluster.pods("manager") + research + review
    assert sorted(by_id) == sorted(pods)  # the agent id is the pod name
    for pod in research:
        assert by_id[pod]["capabilities"] == ["research", "summarize"], pod
    for pod in review:
        assert by_id[pod]["capabilities"] == ["code-review"], pod
    for pod in pods:
        # The Gateway reaches an agent at this address for drain and cancel. A pod name is not reachable (G0b).
        endpoint = cluster.kubectl(
            "exec", "deploy/redis", "--", "redis-cli", "hget", f"das:agent:{pod}", "grpc_endpoint"
        )
        assert endpoint == f"{cluster.pod_ip(pod)}:50051", pod


def test_a_request_completes_end_to_end(cluster: Cluster) -> None:
    assert_completed(wait_for_status(cluster, submit(cluster, "Summarise the state of the cluster.")))


# --------------------------------------------------------------------------- the control path over gRPC


def test_an_admin_drain_reaches_a_worker_pod_and_undrain_returns_it(cluster: Cluster) -> None:
    target = cluster.pods("worker-review")[0]
    with cluster.client(cluster.credentials.admin_key) as admin:
        drained = admin.post(f"/v1/admin/agents/{target}/drain", json={}, timeout=40)
        assert drained.status_code == 202, drained.text  # 504 agent_timeout when the advertise host is a pod name
        cluster.wait_for(
            lambda: {a["agent_id"]: a["status"] for a in cluster.agents()}[target] == "Draining",
            15,
            "the Worker to drain",
        )
        assert admin.post(f"/v1/admin/agents/{target}/undrain", timeout=40).status_code == 202
    cluster.wait_for(
        lambda: {a["agent_id"]: a["status"] for a in cluster.agents()}[target] == "Available", 15, "undrain"
    )


# --------------------------------------------------------------------------- losing pods


def test_a_worker_pod_deleted_gracefully_during_a_job_loses_no_request(cluster: Cluster) -> None:
    rid = submit(cluster, "Graceful delete [[delay=15]]")
    wait_job_running(cluster, rid)
    victim = cluster.pods("worker-research")[0]
    cluster.kubectl("delete", "pod", victim, "--wait=false")
    assert_completed(wait_for_status(cluster, rid))
    cluster.wait_agents(MANAGERS, WORKERS, timeout=150)


def test_a_worker_pod_killed_during_a_job_loses_no_request(cluster: Cluster) -> None:
    rid = submit(cluster, "Force kill [[delay=15]]")
    wait_job_running(cluster, rid)
    victim = cluster.pods("worker-research")[0]
    cluster.kubectl("delete", "pod", victim, "--grace-period=0", "--force", "--wait=false")
    assert_completed(wait_for_status(cluster, rid))  # the unacknowledged dispatch goes back to the queue
    cluster.wait_agents(MANAGERS, WORKERS, timeout=150)


@pytest.mark.parametrize(
    "deployments", [["das-manager"], ["das-worker-research", "das-worker-review"]], ids=["managers", "workers"]
)
def test_a_rolling_restart_with_requests_in_flight_loses_none(cluster: Cluster, deployments: list[str]) -> None:
    rids = [submit(cluster, f"Rolling restart {i} [[delay=6]]") for i in range(6)]
    for deployment in deployments:
        cluster.kubectl("rollout", "restart", f"deploy/{deployment}")
    for rid in rids:
        assert_completed(wait_for_status(cluster, rid, timeout=240))
    for deployment in deployments:
        cluster.kubectl("rollout", "status", f"deploy/{deployment}", "--timeout=240s", timeout=260)
    cluster.wait_agents(MANAGERS, WORKERS, timeout=150)


def test_a_manager_pod_killed_with_requests_in_flight_loses_none(cluster: Cluster) -> None:
    rids = [submit(cluster, f"Manager kill {i} [[delay=8]]") for i in range(4)]
    time.sleep(3)
    victim = cluster.pods("manager")[0]
    cluster.kubectl("delete", "pod", victim, "--grace-period=0", "--force", "--wait=false")
    for rid in rids:
        assert_completed(wait_for_status(cluster, rid))  # another Manager takes over the Lease after lease_duration
    cluster.wait_agents(MANAGERS, WORKERS, timeout=150)


def test_the_gateway_keeps_serving_through_a_rollout(cluster: Cluster) -> None:
    cluster.kubectl("rollout", "restart", "deploy/das-gateway")
    cluster.kubectl("rollout", "status", "deploy/das-gateway", "--timeout=180s", timeout=200)
    cluster.forward()  # the port-forward was pinned to the replaced pod
    assert_completed(wait_for_status(cluster, submit(cluster, "After the Gateway rolled.")))
    with pytest.raises(httpx.HTTPStatusError):  # and a wrong key is still refused
        with cluster.client("not-a-key") as c:
            c.get("/v1/agents").raise_for_status()
