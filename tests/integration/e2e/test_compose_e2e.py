"""End-to-end tests against the Docker Compose stack (task 19.4).

Brings up ``deploy/docker-compose.yml`` plus ``docker-compose.e2e.yml`` under a
separate Compose project (``das-e2e``) so a developer's own ``das`` stack is
untouched. The LLM is ``llm_stub.py``, an Ollama-compatible fake server the
real ``ollama`` Provider_Adapter talks to, so no production code changes.

Covered:

- stack ready (all containers running, every agent registered, authenticated
  submit accepted) within 120 s of ``docker compose up`` (14.2)
- prompt -> Final_Result through Gateway, Manager, Workers (1.1, 1.2)
- submit/query/cancel each answer within 500 ms; cancel takes effect within 2 s (1.1, 1.2, 1.6)
- kill the lease-owning Manager; another takes the Lease within 10 s of its
  expiry and completes the Request (2.5)
- scale workers and managers up and down without restarting other agents (14.5, 14.6)
- an agent exits non-zero naming RabbitMQ or Redis when it is unreachable (14.7)

The stack is always torn down with ``docker compose down -v``. Measured timings
are printed at the end of the module (run with ``-s`` to see them). The module
is skipped when Docker or Compose is unavailable.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[3]
DEPLOY = ROOT / "deploy"
OVERRIDE = Path(__file__).with_name("docker-compose.e2e.yml")
PROJECT = "das-e2e"
API_KEY = "e2e-api-key-0123456789"
LEASE_DURATION = 10.0  # DAS__LIMITS__LEASE_DURATION in the override
HEARTBEAT_INTERVAL = 5.0  # limits.heartbeat_interval in deploy/config
WORKER_CONCURRENCY = 4  # worker.concurrency_limit in deploy/config/worker.yaml
TERMINAL = {"Completed", "Failed", "Cancelled"}
JOB_TERMINAL = {"Succeeded", "Failed", "Skipped", "Cancelled"}

TIMINGS: dict[str, Any] = {}


# --------------------------------------------------------------------------- helpers


def _docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        for cmd in (["docker", "info"], ["docker", "compose", "version"]):
            if subprocess.run(cmd, capture_output=True, timeout=20).returncode != 0:
                return False
    except (OSError, subprocess.TimeoutExpired):
        return False
    return True


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _parse_docker_time(value: str) -> float:
    # e.g. 2025-01-01T12:00:00.123456789Z -> trim to microseconds
    head, _, frac = value.rstrip("Z").partition(".")
    frac = (frac + "000000")[:6]
    return datetime.fromisoformat(f"{head}.{frac}+00:00").timestamp()


@dataclass
class Stack:
    env: dict[str, str]
    gateway_url: str
    http: httpx.Client = field(init=False)

    def __post_init__(self) -> None:
        self.http = httpx.Client(
            base_url=self.gateway_url, headers={"X-API-Key": API_KEY}, timeout=10
        )

    # ---- docker / compose
    def compose(self, *args: str, timeout: float = 300, check: bool = True) -> subprocess.CompletedProcess[str]:
        cmd = [
            "docker", "compose", "--project-directory", str(DEPLOY), "-p", PROJECT,
            "-f", str(DEPLOY / "docker-compose.yml"), "-f", str(OVERRIDE), *args,
        ]
        # stdin must not be inherited: an open, never-closing stdin makes
        # ``compose run`` hang before it creates the one-off container.
        proc = subprocess.run(
            cmd, env=self.env, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=timeout
        )
        if check and proc.returncode != 0:
            raise AssertionError(f"{' '.join(args)} failed ({proc.returncode}):\n{proc.stderr[-4000:]}")
        return proc

    def containers(self, service: str | None = None, *, all_states: bool = False) -> list[str]:
        cmd = ["docker", "ps", "-q", "--no-trunc", "--filter", f"label=com.docker.compose.project={PROJECT}"]
        if all_states:
            cmd.insert(2, "-a")
        if service:
            cmd += ["--filter", f"label=com.docker.compose.service={service}"]
        out = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout
        return [line for line in out.split() if line]

    def inspect(self, cid: str) -> dict[str, Any]:
        out = subprocess.run(["docker", "inspect", cid], capture_output=True, text=True, check=True).stdout
        return json.loads(out)[0]

    def redis_cli(self, *args: str) -> str:
        (cid,) = self.containers("redis")
        return subprocess.run(
            ["docker", "exec", cid, "redis-cli", "--raw", *args],
            capture_output=True, text=True, check=True, timeout=10,
        ).stdout

    def lease(self, rid: str) -> dict[str, str]:
        lines = self.redis_cli("HGETALL", f"das:lease:{rid}").split("\n")
        return dict(zip(lines[0::2], lines[1::2], strict=False)) if len(lines) > 1 else {}

    # ---- gateway
    def timed(self, method: str, path: str, **kw: Any) -> tuple[httpx.Response, float]:
        t = time.perf_counter()
        resp = self.http.request(method, path, **kw)
        return resp, time.perf_counter() - t

    def submit(self, prompt: str) -> str:
        resp, dt = self.timed("POST", "/v1/requests", json={"prompt": prompt})
        assert resp.status_code == 202, resp.text
        TIMINGS.setdefault("submit_s", []).append(round(dt, 4))
        assert dt < 0.5, f"submit took {dt:.3f}s"
        return resp.json()["request_id"]

    def query(self, rid: str) -> dict[str, Any]:
        resp, dt = self.timed("GET", f"/v1/requests/{rid}")
        assert resp.status_code == 200, resp.text
        TIMINGS.setdefault("query_s", []).append(round(dt, 4))
        assert dt < 0.5, f"query took {dt:.3f}s"
        return resp.json()

    def wait_for(
        self, rid: str, pred: Callable[[dict[str, Any]], bool], timeout: float, poll: float = 0.25
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        body = self.query(rid)
        while not pred(body):
            if time.monotonic() > deadline:
                raise AssertionError(f"timed out waiting on {rid}: {json.dumps(body)[:2000]}")
            time.sleep(poll)
            body = self.query(rid)
        return body

    def wait_terminal(self, rid: str, timeout: float = 90) -> dict[str, Any]:
        return self.wait_for(rid, lambda b: b["status"] in TERMINAL, timeout)

    def agents(self) -> list[dict[str, Any]]:
        resp = self.http.get("/v1/agents")
        assert resp.status_code == 200, resp.text
        return resp.json()

    def available(self, role: str) -> list[dict[str, Any]]:
        return [a for a in self.agents() if a["role"] == role and a["status"] == "Available"]


def _container_for_host(stack: Stack, host: str, service: str | None = None) -> str | None:
    return next((c for c in stack.containers(service) if c.startswith(host)), None)


def _wait_registered(
    stack: Stack, managers: int, workers: int, timeout: float, exclude: frozenset[str] = frozenset()
) -> None:
    """Every running manager/worker container has an Available registry entry.

    ``exclude`` lists agent IDs that must not count: a killed agent keeps an
    Available entry until its heartbeat goes stale, and a restarted container
    reuses its host name, so that stale entry would otherwise match it.
    """
    deadline = time.monotonic() + timeout
    while True:
        try:
            mgr = {c for c in stack.containers("manager")}
            wrk = {c for s in ("worker-research", "worker-review") for c in stack.containers(s)}
            live = {
                a["host"] for a in stack.agents() if a["status"] == "Available" and a["agent_id"] not in exclude
            }
            ok_m = len(mgr) == managers and all(any(c.startswith(h) for h in live) for c in mgr)
            ok_w = len(wrk) == workers and all(any(c.startswith(h) for h in live) for c in wrk)
            if ok_m and ok_w:
                return
        except (httpx.HTTPError, AssertionError):
            pass
        if time.monotonic() > deadline:
            raise AssertionError(f"agents not registered: {stack.agents()}")
        time.sleep(0.5)


# --------------------------------------------------------------------------- fixture


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Stack]:
    if not _docker_available():
        pytest.skip("Docker / docker compose not available; end-to-end tests need the Compose stack")

    port = _free_port()
    env = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": os.environ.get("HOME", ""),
        "RABBITMQ_USER": "das",
        "RABBITMQ_PASS": "e2e-rabbit-password",
        "DAS_GATEWAY_API_KEYS": API_KEY,
        "DAS_GATEWAY_PORT": str(port),
        "DAS_RABBITMQ_MGMT_PORT": str(_free_port()),
    }
    for name in ("DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG"):
        if name in os.environ:
            env[name] = os.environ[name]
    s = Stack(env=env, gateway_url=f"http://127.0.0.1:{port}")
    logs = tmp_path_factory.mktemp("e2e") / "compose.log"
    try:
        s.compose("down", "-v", "--remove-orphans", check=False)
        # Image build is not part of the 120 s bound (14.2: images available locally).
        s.compose("build", timeout=1500)
        s.compose("pull", "--ignore-buildable", "--quiet", timeout=900, check=False)

        t0 = time.monotonic()
        s.compose("up", "-d", timeout=180)
        deadline = t0 + 150
        # Every defined container running and every agent registered ...
        _wait_registered(s, managers=2, workers=2, timeout=max(1.0, deadline - time.monotonic()))
        services = ("rabbitmq", "redis", "gateway", "manager", "worker-research", "worker-review", "llm-stub")
        for svc in services:
            assert s.containers(svc), f"{svc} is not running"
        # ... and an authenticated Request accepted.
        while True:
            try:
                r = s.http.post("/v1/requests", json={"prompt": "readiness probe"})
                if r.status_code == 202:
                    break
            except httpx.HTTPError:
                pass
            if time.monotonic() > deadline:
                raise AssertionError("gateway did not accept a Request")
            time.sleep(0.5)
        TIMINGS["stack_ready_s"] = round(time.monotonic() - t0, 2)
        TIMINGS["readiness_probe_request"] = r.json()["request_id"]
        yield s
    finally:
        if log_dir := os.environ.get("DAS_E2E_LOG_DIR"):
            logs = Path(log_dir) / f"{PROJECT}.log"
        try:
            logs.parent.mkdir(parents=True, exist_ok=True)
            logs.write_text(s.compose("logs", "--no-color", "--timestamps", check=False, timeout=120).stdout)
        except Exception:  # noqa: BLE001 - diagnostics only
            pass
        s.http.close()
        s.compose("down", "-v", "--remove-orphans", check=False, timeout=300)
        print(f"\n[e2e] compose logs: {logs}")
        print("[e2e] timings:", json.dumps(_summarize(TIMINGS), indent=2))


def _summarize(t: dict[str, Any]) -> dict[str, Any]:
    out = dict(t)
    for key in ("submit_s", "query_s", "cancel_s"):
        vals = t.get(key)
        if vals:
            out[key] = {"n": len(vals), "max": max(vals), "median": sorted(vals)[len(vals) // 2]}
    return out


# --------------------------------------------------------------------------- tests


def test_stack_ready_within_120s(stack: Stack) -> None:
    """14.1, 14.2: every container running, 2 managers + 2 workers registered, submit accepted."""
    assert TIMINGS["stack_ready_s"] <= 120, TIMINGS["stack_ready_s"]
    workers = stack.available("worker")
    caps = {frozenset(w["capabilities"]) for w in workers}
    assert caps == {frozenset({"research", "summarize"}), frozenset({"code-review"})}
    assert len(stack.available("manager")) == 2


def test_prompt_to_final_result(stack: Stack) -> None:
    """1.1, 1.2: a prompt is planned, executed by workers, and summarized."""
    rid = stack.submit("Explain the CAP theorem and review a short snippet.")
    body = stack.wait_terminal(rid, timeout=60)
    assert body["status"] == "Completed", body
    final = body["final_result"]
    assert final["summary"] == "fake-llm final answer combining the job results."
    # One job per advertised Capability; summarize depends on the others.
    assert {j["job_id"] for j in body["jobs"]} == {"j1", "j2", "j3"}
    assert all(j["state"] == "Succeeded" for j in body["jobs"])
    reports = {r["job_id"]: r for r in final["jobs"]}
    assert set(reports) == {"j1", "j2", "j3"}
    assert all(r["worker_id"] and r["output_tokens"] > 0 for r in reports.values())
    assert final["failures"] == []
    # Transitions recorded per Job, ending in Succeeded (13.7, 13.9).
    last = {}
    for t in body["transitions"]:
        last[t["job_id"]] = t["to_state"]
    assert last == {"j1": "Succeeded", "j2": "Succeeded", "j3": "Succeeded"}
    # The readiness probe Request also completes.
    probe = stack.wait_terminal(TIMINGS["readiness_probe_request"], timeout=60)
    assert probe["status"] == "Completed"


def test_cancel_running_request_within_2s(stack: Stack) -> None:
    """1.6, 1.7: cancel answers within 500 ms and cancels the Request and open Jobs within 2 s."""
    rid = stack.submit("Long running job [[delay=30]]")
    stack.wait_for(
        rid,
        lambda b: b["status"] == "Running" and any(j["state"] == "Dispatched" for j in b["jobs"]),
        timeout=30,
    )
    t0 = time.monotonic()
    resp, dt = stack.timed("POST", f"/v1/requests/{rid}/cancel")
    assert resp.status_code == 202, resp.text
    TIMINGS.setdefault("cancel_s", []).append(round(dt, 4))
    assert dt < 0.5, f"cancel took {dt:.3f}s"
    body = stack.wait_for(
        rid,
        lambda b: b["status"] == "Cancelled" and all(j["state"] in JOB_TERMINAL for j in b["jobs"]),
        timeout=2.0,
        poll=0.05,
    )
    TIMINGS["cancel_effective_s"] = round(time.monotonic() - t0, 3)
    assert TIMINGS["cancel_effective_s"] <= 2.0
    assert all(j["state"] == "Cancelled" for j in body["jobs"]), body["jobs"]
    # Cancelling again is rejected as terminal (1.8), and the state stays Cancelled (1.7).
    again = stack.http.post(f"/v1/requests/{rid}/cancel")
    assert again.status_code == 409
    time.sleep(3)
    body = stack.query(rid)
    assert body["status"] == "Cancelled"
    assert all(j["state"] == "Cancelled" for j in body["jobs"])


def test_manager_kill_takeover_within_10s(stack: Stack) -> None:
    """2.5: kill the lease owner; another Manager takes over within 10 s of Lease expiry."""
    rid = stack.submit("Takeover job [[delay=15]]")
    stack.wait_for(
        rid,
        lambda b: b["status"] == "Running" and any(j["state"] == "Dispatched" for j in b["jobs"]),
        timeout=30,
    )
    lease = stack.lease(rid)
    owner = lease["owner"]
    agent = next(a for a in stack.agents() if a["agent_id"] == owner)
    cid = _container_for_host(stack, agent["host"], "manager")
    assert cid, f"no manager container for host {agent['host']}"

    subprocess.run(["docker", "kill", cid], check=True, capture_output=True)
    t_kill = time.monotonic()
    # The killed owner can no longer renew; its last written expiry is final.
    old_expiry = float(stack.lease(rid)["expires_at"])

    deadline = t_kill + LEASE_DURATION + 30
    new: dict[str, str] = {}
    while time.monotonic() < deadline:
        cur = stack.lease(rid)
        if cur.get("owner") and cur["owner"] != owner:
            new = cur
            break
        time.sleep(0.2)
    assert new, "no Manager took over the Lease"
    t_takeover = time.monotonic()
    # acquired_at = first observed expiry - duration (State_Store clock, same as old_expiry).
    acquired_at = float(new["expires_at"]) - LEASE_DURATION
    TIMINGS["takeover_after_expiry_s"] = round(acquired_at - old_expiry, 3)
    TIMINGS["takeover_after_kill_s"] = round(t_takeover - t_kill, 3)
    assert int(new["epoch"]) > int(lease["epoch"])
    assert acquired_at - old_expiry <= 10.0

    body = stack.wait_terminal(rid, timeout=90)
    assert body["status"] == "Completed", body
    assert all(j["state"] == "Succeeded" for j in body["jobs"])

    # Restore two running managers for the following tests.
    stack.compose("up", "-d", "--no-recreate", "--scale", "manager=2", "manager")
    _wait_registered(stack, managers=2, workers=2, timeout=60, exclude=frozenset({owner}))


def test_scale_up_and_down(stack: Stack) -> None:
    """14.5, 14.6: scaled-in agents register quickly; scaling never restarts other agents."""
    def started(cids: list[str]) -> dict[str, str]:
        return {c: stack.inspect(c)["State"]["StartedAt"] for c in cids}

    # Start from the baseline topology, so earlier tests cannot leak containers in.
    _wait_registered(stack, managers=2, workers=2, timeout=60)
    before = started(stack.containers())

    # ---- scale up: one more research worker and one more manager
    stack.compose(
        "up", "-d", "--no-recreate", "--scale", "worker-research=2", "--scale", "manager=3",
        "worker-research", "manager",
    )
    new_ids = [c for c in stack.containers() if c not in before]
    assert len(new_ids) == 2, new_ids
    reg_delay: dict[str, float] = {}
    deadline = time.monotonic() + 30
    while len(reg_delay) < len(new_ids) and time.monotonic() < deadline:
        live = [a for a in stack.agents() if a["status"] == "Available"]
        for c in new_ids:
            if c not in reg_delay and any(c.startswith(a["host"]) for a in live):
                reg_delay[c] = time.time() - _parse_docker_time(stack.inspect(c)["State"]["StartedAt"])
        time.sleep(0.25)
    TIMINGS["scale_up_registration_s"] = {c[:12]: round(v, 2) for c, v in reg_delay.items()}
    assert len(reg_delay) == len(new_ids), "a scaled-in agent did not register"
    assert all(v <= 2 * HEARTBEAT_INTERVAL for v in reg_delay.values()), reg_delay
    assert started(list(before)) == before, "an existing container was restarted"

    # The new research worker receives Jobs. Workers pull (basic.get), so the
    # broker does not round-robin: whichever worker polls first wins. Submit more
    # overlapping research Jobs than the old worker has slots
    # (worker.concurrency_limit), so the overflow must go to the new worker.
    new_worker = next(c for c in new_ids if c in stack.containers("worker-research"))
    new_worker_id = next(a["agent_id"] for a in stack.agents() if new_worker.startswith(a["host"]))
    rids = [stack.submit(f"Scale check {i} [[delay=3]]") for i in range(2 * WORKER_CONCURRENCY)]
    workers_used: set[str] = set()
    for rid in rids:
        body = stack.wait_terminal(rid, timeout=90)
        assert body["status"] == "Completed", body
        workers_used |= {r["worker_id"] for r in body["final_result"]["jobs"]}
    assert new_worker_id in workers_used, (new_worker_id, workers_used)

    # ---- scale down while a Request is in flight
    in_flight = stack.submit("In flight during scale down [[delay=4]]")
    stack.wait_for(in_flight, lambda b: b["status"] == "Running", timeout=30)
    agents_before = {a["agent_id"]: a for a in stack.agents()}
    up_ids = set(stack.containers())
    t0 = time.monotonic()
    stack.compose(
        "up", "-d", "--no-recreate", "--scale", "worker-research=1", "--scale", "manager=2",
        "worker-research", "manager", timeout=180,
    )
    TIMINGS["scale_down_s"] = round(time.monotonic() - t0, 2)
    removed = up_ids - set(stack.containers())
    assert len(removed) == 2, removed
    current = set(stack.containers())
    remaining_before = {c: v for c, v in before.items() if c in current}
    assert started(list(remaining_before)) == remaining_before, "a remaining container was restarted"

    body = stack.wait_terminal(in_flight, timeout=120)
    assert body["status"] == "Completed", body

    # Graceful shutdown deregistered the removed agents (11.3).
    removed_agents = [
        a["agent_id"]
        for a in agents_before.values()
        if a["status"] == "Available" and any(c.startswith(a["host"]) for c in removed)
    ]
    assert len(removed_agents) == 2, removed_agents
    listed = {a["agent_id"] for a in stack.agents()}
    assert not (set(removed_agents) & listed), "a removed agent was not deregistered"

    # Remaining agents keep serving new Requests.
    rid = stack.submit("After scale down")
    assert stack.wait_terminal(rid, timeout=60)["status"] == "Completed"


@pytest.mark.parametrize(
    ("dependency", "override"),
    [
        ("rabbitmq", {"DAS__RABBITMQ__URL": "amqp://das:unused-password@rabbitmq-missing.invalid:5672/"}),
        ("redis", {"DAS__REDIS__URL": "redis://redis-missing.invalid:6379/0"}),
    ],
)
def test_dependency_unreachable_exits_nonzero(stack: Stack, dependency: str, override: dict[str, str]) -> None:
    """14.7: retry for startup_timeout, then exit 4 naming the unreachable dependency."""
    timeout_s = 6
    env_args: list[str] = []
    for k, v in {
        **override,
        "DAS__LIMITS__STARTUP_TIMEOUT": str(timeout_s),
        "DAS__LIMITS__STARTUP_RETRY_INTERVAL": "1",
    }.items():
        env_args += ["-e", f"{k}={v}"]
    agents_before = {a["agent_id"] for a in stack.agents()}
    t0 = time.monotonic()
    proc = stack.compose(
        "run", "--rm", "--no-deps", "-T", *env_args, "worker-review", timeout=120, check=False
    )
    elapsed = time.monotonic() - t0
    TIMINGS[f"unreachable_{dependency}_exit_s"] = round(elapsed, 2)
    output = proc.stdout + proc.stderr
    assert proc.returncode == 4, output[-3000:]
    assert f"dependency '{dependency}' is unreachable" in output, output[-3000:]
    assert elapsed >= timeout_s - 0.5
    # The agent never registered: registration comes after every dependency connects.
    assert {a["agent_id"] for a in stack.agents()} == agents_before
