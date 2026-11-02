"""End-to-end tests for the Compose profile ``observability`` (task 19.2).

Brings up ``deploy/docker-compose.yml`` with ``--profile observability`` plus the
v1 e2e override (``docker-compose.e2e.yml``: ``llm-stub`` fake LLM behind the real
``ollama`` adapter) under its own Compose project (``das-e2e-obs``), so neither a
developer's ``das`` stack nor the v1 e2e project (``das-e2e``) is touched.
Variables are written to a temporary env file passed with ``--env-file``, which
Compose reads in place of ``deploy/.env``; a real ``deploy/.env`` cannot leak in.

Covered:

- every Prometheus scrape target (Gateway, both Managers, both Workers) is ``up``
  within 120 s of ``docker compose up`` (18.4)
- Grafana answers 401 to anonymous and default ``admin/admin`` calls, 200 to the
  configured admin (18.7)
- ``GRAFANA_ADMIN_PASSWORD`` absent or empty: the Grafana container exits non-zero
  naming the variable, before Grafana itself starts (18.11)
- tracing on: Jaeger's trace search for service ``das-gateway`` shows exactly one
  trace per Request (``/api/v3/traces?query.service_name=das-gateway``: the pinned
  Jaeger 2.22 answers 404 on the v1 ``/api/traces`` path), holding ``gateway.submit``,
  ``manager.request``, ``manager.plan`` and, for every dispatched Job, ``manager.dispatch``, ``worker.job`` and a child
  ``llm.completion``, within 30 s of Finished_At (18.12)
- ``--scale worker-research=2``: the new replica is an ``up`` target within 60 s
  of serving ``/readyz`` 200, without touching Prometheus (18.10)
- mixed fleet v1/v2 with tracing off (19.7), see below

Mixed fleet. No v1 build exists in this repository, so the v1 peer is simulated:
the agents are recreated with ``DAS__TRACING__ENABLED=false``, RabbitMQ's firehose
tracer copies every message published to ``das.jobs`` (job_dispatch) and the
default exchange (job_result to ``das.results``) into a mirror queue, and each
copied body is decoded with ``tests/fixtures/v1_codec.V1ReferenceCodec`` (a frozen
copy of the v1 codec, which a v1 Manager/Worker would use to decide ``malformed``).
Every body must decode, carry ``schema_version`` 1 and re-encode byte for byte;
State_Store must hold no ``malformed`` Dead_Letter_Record. Control messages go over
gRPC and cannot be tapped this way; their bytes are covered by the codec property
tests (Property for Req 7.2 / 19.7).

The stack is always torn down with ``docker compose down -v``. Timings are printed
at module end (``-s``). The module is skipped when Docker or Compose is
unavailable, or when 127.0.0.1:9090 / :3000 / :16686 (fixed by the profile) are
already in use.
"""

from __future__ import annotations

import base64
import json
import os
import socket
import subprocess
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import httpx
import pytest

from das.codec.envelope import JobDispatchMessage, JobResultMessage
from tests.fixtures.v1_codec import V1ParseError, V1ReferenceCodec
from tests.integration.e2e.test_compose_e2e import _docker_available, _free_port

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[3]
DEPLOY = ROOT / "deploy"
OVERRIDE = Path(__file__).with_name("docker-compose.e2e.yml")
PROJECT = "das-e2e-obs"
API_KEY = "e2e-obs-api-key-0123456789"
RABBIT_USER = "das"
RABBIT_PASS = "e2e-obs-rabbit-password"
GRAFANA_USER = "e2e-admin"
GRAFANA_PASS = "e2e-grafana-password"
PROMETHEUS = "http://127.0.0.1:9090"
GRAFANA = "http://127.0.0.1:3000"
JAEGER = "http://127.0.0.1:16686"
OPS_PORT = 8081
TERMINAL = {"Completed", "Failed", "Cancelled"}
AGENT_SERVICES = ("gateway", "manager", "worker-research", "worker-review")
# Prometheus job -> Compose services it discovers (deploy/prometheus/prometheus.yml).
SCRAPE_JOBS = {
    "gateway": ("gateway",),
    "manager": ("manager",),
    "worker": ("worker-research", "worker-review"),
}
MIRROR_QUEUE = "e2e.v1-peer"

TIMINGS: dict[str, Any] = {}


# --------------------------------------------------------------------------- helpers


def _port_in_use(port: int) -> bool:
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def _iso_to_epoch(value: str) -> float:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def _write_env(path: Path, values: dict[str, str]) -> Path:
    path.write_text("".join(f"{k}={v}\n" for k, v in values.items()))
    return path


@dataclass
class ObsStack:
    proc_env: dict[str, str]
    env_vars: dict[str, str]
    env_file: Path
    gateway_url: str
    mgmt_url: str
    http: httpx.Client = field(init=False)
    plain: httpx.Client = field(init=False)

    def __post_init__(self) -> None:
        self.http = httpx.Client(base_url=self.gateway_url, headers={"X-API-Key": API_KEY}, timeout=10)
        self.plain = httpx.Client(timeout=10)

    # ---- docker / compose
    def compose(
        self, *args: str, env_file: Path | None = None, timeout: float = 300, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        cmd = [
            "docker", "compose", "--project-directory", str(DEPLOY), "-p", PROJECT,
            "--env-file", str(env_file or self.env_file),
            "-f", str(DEPLOY / "docker-compose.yml"), "-f", str(OVERRIDE),
            "--profile", "observability", *args,
        ]
        proc = subprocess.run(
            cmd, env=self.proc_env, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=timeout
        )
        if check and proc.returncode != 0:
            raise AssertionError(f"{' '.join(args)} failed ({proc.returncode}):\n{proc.stderr[-4000:]}")
        return proc

    def set_env(self, **values: str) -> None:
        self.env_vars.update(values)
        _write_env(self.env_file, self.env_vars)

    def containers(self, service: str | None = None) -> list[str]:
        cmd = ["docker", "ps", "-q", "--no-trunc", "--filter", f"label=com.docker.compose.project={PROJECT}"]
        if service:
            cmd += ["--filter", f"label=com.docker.compose.service={service}"]
        out = subprocess.run(cmd, capture_output=True, text=True, check=True).stdout
        return [line for line in out.split() if line]

    def inspect(self, cid: str) -> dict[str, Any]:
        out = subprocess.run(["docker", "inspect", cid], capture_output=True, text=True, check=True).stdout
        return json.loads(out)[0]

    def ip(self, cid: str) -> str:
        nets = self.inspect(cid)["NetworkSettings"]["Networks"]
        return next(n["IPAddress"] for n in nets.values() if n.get("IPAddress"))

    def ready(self, cid: str) -> bool:
        """The agent's own Readiness_Probe answers 200 (urlopen raises on 503)."""
        probe = f"import urllib.request; urllib.request.urlopen('http://127.0.0.1:{OPS_PORT}/readyz', timeout=3)"
        proc = subprocess.run(
            ["docker", "exec", cid, "python", "-c", probe], capture_output=True, timeout=15
        )
        return proc.returncode == 0

    def exec_in(self, service: str, *args: str) -> str:
        (cid,) = self.containers(service)
        return subprocess.run(
            ["docker", "exec", cid, *args], capture_output=True, text=True, check=True, timeout=30
        ).stdout

    # ---- gateway
    def submit(self, prompt: str) -> str:
        resp = self.http.post("/v1/requests", json={"prompt": prompt})
        assert resp.status_code == 202, resp.text
        return resp.json()["request_id"]

    def wait_terminal(self, rid: str, timeout: float = 90) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while True:
            body = self.http.get(f"/v1/requests/{rid}").json()
            if body.get("status") in TERMINAL:
                return body
            if time.monotonic() > deadline:
                raise AssertionError(f"timed out waiting on {rid}: {json.dumps(body)[:2000]}")
            time.sleep(0.25)

    def finished_at(self, rid: str) -> float:
        resp = self.http.get("/v1/requests", params={"limit": "100"})
        assert resp.status_code == 200, resp.text
        item = next(i for i in resp.json()["items"] if i["request_id"] == rid)
        assert item["finished_at"], item
        return _iso_to_epoch(item["finished_at"])

    def agents(self) -> list[dict[str, Any]]:
        resp = self.http.get("/v1/agents")
        assert resp.status_code == 200, resp.text
        return resp.json()

    # ---- prometheus
    def targets(self) -> list[dict[str, Any]]:
        resp = self.plain.get(f"{PROMETHEUS}/api/v1/targets", params={"state": "active"})
        resp.raise_for_status()
        return resp.json()["data"]["activeTargets"]

    def expected_instances(self) -> dict[str, set[str]]:
        return {
            job: {f"{self.ip(c)}:{OPS_PORT}" for s in services for c in self.containers(s)}
            for job, services in SCRAPE_JOBS.items()
        }

    def up_instances(self) -> dict[str, set[str]]:
        up: dict[str, set[str]] = {job: set() for job in SCRAPE_JOBS}
        for t in self.targets():
            if t["health"] == "up" and t["labels"].get("job") in up:
                up[t["labels"]["job"]].add(t["labels"]["instance"])
        return up

    # ---- rabbitmq management
    def mgmt(self, method: str, path: str, **kw: Any) -> httpx.Response:
        resp = self.plain.request(method, f"{self.mgmt_url}/api{path}", auth=(RABBIT_USER, RABBIT_PASS), **kw)
        assert resp.status_code < 300, f"{method} {path}: {resp.status_code} {resp.text}"
        return resp


def _wait_agents_ready(stack: ObsStack, counts: dict[str, int], timeout: float) -> None:
    """The service replica counts match and every agent container serves /readyz 200."""
    deadline = time.monotonic() + timeout
    while True:
        cids = {svc: stack.containers(svc) for svc in counts}
        if all(len(cids[s]) == n for s, n in counts.items()) and all(
            stack.ready(c) for ids in cids.values() for c in ids
        ):
            return
        if time.monotonic() > deadline:
            raise AssertionError(f"agents not ready: { {s: len(c) for s, c in cids.items()} }")
        time.sleep(1)


def _wait_targets_up(stack: ObsStack, deadline: float) -> dict[str, set[str]]:
    """Every agent container is an ``up`` target of its Prometheus job."""
    last: dict[str, set[str]] = {}
    while True:
        try:
            expected = stack.expected_instances()
            last = stack.up_instances()
            if all(expected[j] and expected[j] <= last[j] for j in SCRAPE_JOBS):
                return expected
        except (httpx.HTTPError, KeyError):
            pass
        if time.monotonic() > deadline:
            raise AssertionError(f"targets not up: up={last}")
        time.sleep(1)


# --------------------------------------------------------------------------- fixture


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[ObsStack]:
    if not _docker_available():
        pytest.skip("Docker / docker compose not available; the observability e2e tests need the Compose stack")
    busy = [p for p in (9090, 3000, 16686) if _port_in_use(p)]
    if busy:
        pytest.skip(f"127.0.0.1 ports {busy} already in use; the observability profile publishes them")

    tmp = tmp_path_factory.mktemp("e2e-obs")
    port, mgmt_port = _free_port(), _free_port()
    env_vars = {
        "RABBITMQ_USER": RABBIT_USER,
        "RABBITMQ_PASS": RABBIT_PASS,
        "DAS_GATEWAY_API_KEYS": API_KEY,
        "DAS_GATEWAY_PORT": str(port),
        "DAS_RABBITMQ_MGMT_PORT": str(mgmt_port),
        "GRAFANA_ADMIN_USER": GRAFANA_USER,
        "GRAFANA_ADMIN_PASSWORD": GRAFANA_PASS,
        "DAS__TRACING__ENABLED": "true",
    }
    proc_env = {"PATH": os.environ.get("PATH", ""), "HOME": os.environ.get("HOME", "")}
    for name in ("DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG"):
        if name in os.environ:
            proc_env[name] = os.environ[name]
    s = ObsStack(
        proc_env=proc_env,
        env_vars=env_vars,
        env_file=_write_env(tmp / "env", env_vars),
        gateway_url=f"http://127.0.0.1:{port}",
        mgmt_url=f"http://127.0.0.1:{mgmt_port}",
    )
    logs = tmp / "compose.log"
    try:
        s.compose("down", "-v", "--remove-orphans", check=False)
        # Image build/pull is not part of the 120 s bound.
        s.compose("build", timeout=1500)
        s.compose("pull", "--ignore-buildable", "--quiet", timeout=900, check=False)

        t0 = time.monotonic()
        s.compose("up", "-d", timeout=180)
        deadline = t0 + 150
        _wait_agents_ready(
            s, {"gateway": 1, "manager": 2, "worker-research": 1, "worker-review": 1},
            timeout=max(1.0, deadline - time.monotonic()),
        )
        _wait_targets_up(s, deadline)
        TIMINGS["targets_up_s"] = round(time.monotonic() - t0, 2)
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
        s.plain.close()
        s.compose("down", "-v", "--remove-orphans", check=False, timeout=300)
        print(f"\n[e2e-obs] compose logs: {logs}")
        print("[e2e-obs] timings:", json.dumps(TIMINGS, indent=2))


# --------------------------------------------------------------------------- tests


def test_all_prometheus_targets_up_within_120s(stack: ObsStack) -> None:
    """18.4: Gateway, both Managers and both Workers are ``up`` targets within 120 s of ``up``."""
    assert TIMINGS["targets_up_s"] <= 120, TIMINGS["targets_up_s"]
    expected = stack.expected_instances()
    assert {j: len(v) for j, v in expected.items()} == {"gateway": 1, "manager": 2, "worker": 2}
    up = stack.up_instances()
    for job in SCRAPE_JOBS:
        assert expected[job] <= up[job], (job, expected[job], up[job])


def test_grafana_rejects_anonymous(stack: ObsStack) -> None:
    """18.7: anonymous access is off and admin/admin is not the login; the .env admin works."""
    deadline = time.monotonic() + 60
    while True:
        try:
            if stack.plain.get(f"{GRAFANA}/api/health").status_code == 200:
                break
        except httpx.HTTPError:
            pass
        assert time.monotonic() < deadline, "Grafana did not become healthy"
        time.sleep(1)
    for path in ("/api/search", "/api/datasources", "/api/dashboards/home"):
        assert stack.plain.get(f"{GRAFANA}{path}").status_code == 401, path
        assert stack.plain.get(f"{GRAFANA}{path}", auth=("admin", "admin")).status_code == 401, path
    resp = stack.plain.get(f"{GRAFANA}/api/datasources", auth=(GRAFANA_USER, GRAFANA_PASS))
    assert resp.status_code == 200, resp.text
    assert {d["type"] for d in resp.json()} >= {"prometheus", "jaeger"}


@pytest.mark.parametrize("mode", ["absent", "empty"])
def test_grafana_refuses_start_without_admin_password(
    stack: ObsStack, mode: str, tmp_path: Path
) -> None:
    """18.11: GRAFANA_ADMIN_PASSWORD missing or empty -> exit non-zero naming it; Grafana never starts."""
    values = dict(stack.env_vars)
    if mode == "absent":
        del values["GRAFANA_ADMIN_PASSWORD"]
    else:
        values["GRAFANA_ADMIN_PASSWORD"] = ""
    env_file = _write_env(tmp_path / "env", values)
    # ``run`` publishes no ports, so it cannot clash with the running grafana.
    proc = stack.compose("run", "--rm", "--no-deps", "-T", "grafana", env_file=env_file, timeout=120, check=False)
    output = proc.stdout + proc.stderr
    assert proc.returncode != 0, output[-3000:]
    assert "GRAFANA_ADMIN_PASSWORD" in output, output[-3000:]
    assert "GRAFANA_ADMIN_USER" not in output, output[-3000:]  # only the missing one is named
    assert "HTTP Server Listen" not in output, output[-3000:]  # Grafana's server never started


def _tag(span: dict[str, Any], key: str) -> Any:
    """An OTLP-JSON attribute value (``stringValue`` / ``intValue``) as a string."""
    for attr in span.get("attributes", []):
        if attr["key"] == key:
            return next(iter(attr["value"].values()), None)
    return None


def _jaeger_traces(stack: ObsStack, service: str, rid: str) -> dict[str, list[dict[str, Any]]]:
    """Traces holding a ``service`` Span tagged with ``rid``: trace ID -> every Span of the trace.

    Jaeger 2.22 serves only the api_v3 query API (the v1 ``/api/traces`` path is
    gone, 404), so this is ``/api/v3/traces`` with ``query.service_name`` and an
    attribute filter. A search returns whole traces, Spans of every service included.
    """
    now = time.time()
    fmt = "%Y-%m-%dT%H:%M:%SZ"
    resp = stack.plain.get(
        f"{JAEGER}/api/v3/traces",
        params={
            "query.service_name": service,
            "query.attributes": json.dumps({"das.request_id": rid}),
            "query.start_time_min": time.strftime(fmt, time.gmtime(now - 3600)),
            "query.start_time_max": time.strftime(fmt, time.gmtime(now + 600)),
        },
    )
    if resp.status_code != 200:  # 404 "No traces found" until the Spans arrive
        return {}
    traces: dict[str, list[dict[str, Any]]] = {}
    for rs in resp.json()["result"].get("resourceSpans", []):
        for ss in rs.get("scopeSpans", []):
            for sp in ss.get("spans", []):
                traces.setdefault(sp["traceId"], []).append(sp)
    return traces


def _missing_spans(spans: list[dict[str, Any]], rid: str, job_ids: set[str]) -> list[str]:
    by_op: dict[str, list[dict[str, Any]]] = {}
    for sp in spans:
        by_op.setdefault(sp["name"], []).append(sp)
    missing: list[str] = []
    submits = [sp for sp in by_op.get("gateway.submit", []) if _tag(sp, "das.request_id") == rid]
    if len(submits) != 1:
        missing.append(f"gateway.submit x{len(submits)}")
    for op in ("manager.request", "manager.plan"):
        if not any(_tag(sp, "das.request_id") == rid for sp in by_op.get(op, [])):
            missing.append(op)
    for jid in sorted(job_ids):
        if not any(_tag(sp, "das.job_id") == jid for sp in by_op.get("manager.dispatch", [])):
            missing.append(f"manager.dispatch[{jid}]")
        workers = [sp for sp in by_op.get("worker.job", []) if _tag(sp, "das.job_id") == jid]
        if not workers:
            missing.append(f"worker.job[{jid}]")
            continue
        worker_ids = {sp["spanId"] for sp in workers}
        if not any(sp.get("parentSpanId") in worker_ids for sp in by_op.get("llm.completion", [])):
            missing.append(f"llm.completion[{jid}]")
    return missing


def test_jaeger_one_trace_per_request(stack: ObsStack) -> None:
    """18.12: one trace per terminal Request with every required Span, within 30 s of Finished_At."""
    rids = [stack.submit(f"Trace check {i}") for i in range(2)]
    latencies: dict[str, float] = {}
    trace_of: dict[str, str] = {}
    for rid in rids:
        body = stack.wait_terminal(rid, timeout=90)
        assert body["status"] == "Completed", body
        dispatched = {t["job_id"] for t in body["transitions"] if t.get("to_state") == "Dispatched"}
        assert dispatched, body
        deadline = stack.finished_at(rid) + 30
        missing: list[str] = ["no trace"]
        trace_ids: set[str] = set()
        while True:
            traces = _jaeger_traces(stack, "das-gateway", rid)
            if len(traces) == 1:
                missing = _missing_spans(next(iter(traces.values())), rid, dispatched)
                # Spans of this Request from every das-* service, not only the Gateway's.
                trace_ids = {
                    tid
                    for svc in ("das-gateway", "das-manager", "das-worker")
                    for tid in _jaeger_traces(stack, svc, rid)
                }
            elif traces:
                missing = [f"{len(traces)} traces"]
            if not missing:
                latencies[rid] = round(time.time() - (deadline - 30), 2)
                break
            if time.time() > deadline:
                raise AssertionError(f"{rid}: trace incomplete 30 s after Finished_At: {missing}")
            time.sleep(1)
        assert trace_ids == set(traces), (rid, trace_ids, set(traces))
        trace_of[rid] = next(iter(traces))
    assert len(set(trace_of.values())) == len(rids), trace_of  # one trace per Request
    TIMINGS["trace_visible_after_finish_s"] = latencies


def test_scaled_worker_scraped_within_60s(stack: ObsStack) -> None:
    """18.10: a new replica is an ``up`` target within 60 s of /readyz 200, Prometheus untouched."""
    (prom,) = stack.containers("prometheus")
    prom_started = stack.inspect(prom)["State"]["StartedAt"]
    before = set(stack.containers("worker-research"))
    stack.compose("up", "-d", "--no-recreate", "--scale", "worker-research=2", "worker-research")
    new = set(stack.containers("worker-research")) - before
    assert len(new) == 1, new
    (cid,) = new
    deadline = time.monotonic() + 90
    while not stack.ready(cid):
        assert time.monotonic() < deadline, "scaled replica never served /readyz 200"
        time.sleep(0.5)
    t_ready = time.monotonic()
    instance = f"{stack.ip(cid)}:{OPS_PORT}"
    while instance not in stack.up_instances()["worker"]:
        assert time.monotonic() - t_ready < 75, f"{instance} not an up target"
        time.sleep(1)
    TIMINGS["scaled_target_up_after_ready_s"] = round(time.monotonic() - t_ready, 2)
    assert TIMINGS["scaled_target_up_after_ready_s"] <= 60
    assert len(stack.up_instances()["worker"]) == 3
    assert stack.inspect(prom)["State"]["StartedAt"] == prom_started, "Prometheus was restarted"


def test_mixed_fleet_v1_peer_tracing_disabled(stack: ObsStack) -> None:
    """19.7: tracing off -> every brokered Message is v1 bytes a v1 agent accepts; no ``malformed``."""
    stack.set_env(DAS__TRACING__ENABLED="false")
    stack.compose(
        "up", "-d", "--scale", "manager=2", "--scale", "worker-research=1", *AGENT_SERVICES, timeout=240
    )
    counts = {"gateway": 1, "manager": 2, "worker-research": 1, "worker-review": 1}
    _wait_agents_ready(stack, counts, timeout=120)
    for svc in AGENT_SERVICES:
        for cid in stack.containers(svc):
            env = stack.inspect(cid)["Config"]["Env"]
            assert "DAS__TRACING__ENABLED=false" in env, (svc, cid)

    # The simulated v1 peer: a copy of every publish (firehose) lands in a mirror queue.
    stack.mgmt("PUT", f"/queues/%2F/{MIRROR_QUEUE}", json={"durable": False, "auto_delete": False})
    stack.mgmt(
        "POST", f"/bindings/%2F/e/amq.rabbitmq.trace/q/{MIRROR_QUEUE}", json={"routing_key": "publish.#"}
    )
    stack.exec_in("rabbitmq", "rabbitmqctl", "-q", "trace_on")
    try:
        rids = [stack.submit(f"Mixed fleet check {i}") for i in range(2)]
        bodies = {rid: stack.wait_terminal(rid, timeout=90) for rid in rids}
        time.sleep(2)  # let the last job_result copies reach the mirror queue
    finally:
        stack.exec_in("rabbitmq", "rabbitmqctl", "-q", "trace_off")
    for rid, body in bodies.items():
        assert body["status"] == "Completed", body

    got = stack.mgmt(
        "POST", f"/queues/%2F/{MIRROR_QUEUE}/get",
        json={"count": 10000, "ackmode": "ack_requeue_false", "encoding": "base64", "truncate": 2_000_000},
    ).json()
    stack.mgmt("DELETE", f"/queues/%2F/{MIRROR_QUEUE}")

    codec = V1ReferenceCodec()
    seen: dict[tuple[str, str], int] = {}
    checked = 0
    for msg in got:
        exchange = (msg["properties"].get("headers") or {}).get("exchange_name", "")
        if exchange not in ("das.jobs", ""):
            continue
        raw = base64.b64decode(msg["payload"])
        try:
            decoded = codec.decode(raw)
        except V1ParseError as exc:  # a v1 agent would dead-letter this as ``malformed``
            raise AssertionError(f"v1 codec rejects a message to {exchange!r}: {exc}: {raw[:500]!r}") from exc
        assert json.loads(raw)["schema_version"] == 1
        assert codec.encode(decoded) == raw, "not byte-identical to v1 output"
        kind = "dispatch" if isinstance(decoded, JobDispatchMessage) else (
            "result" if isinstance(decoded, JobResultMessage) else type(decoded).__name__
        )
        key = (decoded.correlation_id, kind)  # type: ignore[attr-defined]
        seen[key] = seen.get(key, 0) + 1
        checked += 1
    for rid, body in bodies.items():
        jobs = len(body["jobs"])
        assert seen.get((rid, "dispatch"), 0) >= jobs, (rid, seen)
        assert seen.get((rid, "result"), 0) >= jobs, (rid, seen)
    TIMINGS["v1_peer_messages_checked"] = checked

    assert stack.exec_in("redis", "redis-cli", "--raw", "ZCARD", "das:dlq:by_reason:malformed").strip() == "0"
