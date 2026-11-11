"""End-to-end tests of the Phase 4 client API on the real Compose stack (das-client-api task 11.1).

Brings up ``deploy/docker-compose.yml`` plus the Phase 3 e2e override (the fake LLM, a 10 s Manager lease) plus
``docker-compose.client-api.e2e.yml`` under its own Compose project (``das-e2e-client``): real RabbitMQ and Redis,
**two** Gateways, two Managers, two Workers and a webhook sink. The Phase 3 stack (``das-e2e``) is never touched.
Variables go through a temporary env file passed with ``--env-file``, so a developer's ``deploy/.env`` cannot leak in.

Covered here (task 11.1):

- the Rate_Limit is one bucket shared by both Gateways (Phase 4 Req 2.3, 2.4)
- a new event reaches an open stream within 3 seconds of the write that created it (Req 7.9)
- ``das-client`` against the stack: shared idempotency across replicas, ``wait``, ``events``, paging (Req 9.6)
- a webhook is retried with doubling backoff, every attempt signed, and stops after the success (Req 8.3 to 8.6)
- a budget of the Planner's tokens plus one stops the Request after the Jobs in flight (Req 5.6 to 5.9)
- one open stream survives the kill of the Manager that owns the Request's lease (Req 7.3, 7.6)

Timings are measured with the host's own clock only (Docker Desktop's VM clock may differ from the host's), and
printed at the end of the module with ``-s``. The module is skipped when Docker or Compose is unavailable;
``DAS_REQUIRE_DOCKER=1`` turns that skip into a failure. ``DAS_E2E_CLIENT_OVERRIDE_EXTRA`` names one more Compose
file to layer on top, so a deliberately broken variant can be tried without editing the committed files.
"""

from __future__ import annotations

import asyncio
import http.client
import json
import math
import os
import shutil
import socket
import subprocess
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest

from tests.integration.e2e.client_api_constants import (
    GATEWAY_B_PORT_VAR,
    LIMITED_KEY,
    MAIN_KEY,
    PROJECT,
    SINK_HOST_PORT_VAR,
)

ROOT = Path(__file__).resolve().parents[3]
DEPLOY = ROOT / "deploy"
E2E = Path(__file__).resolve().parent
PHASE3_OVERRIDE = E2E / "docker-compose.e2e.yml"
OVERRIDE = E2E / "docker-compose.client-api.e2e.yml"
TERMINAL = {"Completed", "Failed", "Cancelled"}
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


@dataclass(frozen=True)
class Frame:
    """One frame of an event stream: its ``event:`` name, its text, and when it was read (host monotonic clock)."""

    name: str
    text: str
    arrival: float


class EventStream(threading.Thread):
    """Reads ``GET /v1/requests/{id}/events`` on a plain ``http.client`` connection, recording every frame.

    A raw reader on purpose: nothing between the socket and the timestamp can buffer or batch frames, so the
    arrival time is when the bytes really came through Docker's published port.
    """

    def __init__(self, port: int, request_id: str, key: str = MAIN_KEY) -> None:
        super().__init__(daemon=True)
        self.port, self.request_id, self.key = port, request_id, key
        self.frames: list[Frame] = []
        self.status: int | None = None
        self.error: BaseException | None = None

    def run(self) -> None:
        try:
            conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=90)
            conn.request("GET", f"/v1/requests/{self.request_id}/events", headers={"X-API-Key": self.key})
            response = conn.getresponse()
            self.status = response.status
            buffer = b""
            while chunk := response.read1(4096):
                arrived = time.monotonic()
                buffer += chunk
                while b"\n\n" in buffer:
                    raw, buffer = buffer.split(b"\n\n", 1)
                    text = raw.decode("utf-8")
                    name = next((ln[len("event: ") :] for ln in text.splitlines() if ln.startswith("event: ")), "")
                    self.frames.append(Frame(name, text, arrived))
            conn.close()
        except BaseException as exc:  # noqa: BLE001 - reported to the test through ``error``
            self.error = exc

    def wait_for(self, predicate: Callable[[Frame], bool], timeout: float) -> Frame | None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for frame in list(self.frames):
                if predicate(frame):
                    return frame
            time.sleep(0.01)
        return None

    def names(self) -> list[str]:
        return [f.name for f in self.frames]


@dataclass
class ClientStack:
    env: dict[str, str]
    env_file: Path
    ports: dict[str, int]  # "a" and "b" (the Gateways), "sink", "mgmt"
    extra_override: Path | None = None
    _clients: dict[tuple[str, str], httpx.Client] = field(default_factory=dict)

    # ---- docker / compose
    def compose(self, *args: str, timeout: float = 300, check: bool = True) -> subprocess.CompletedProcess[str]:
        files = ["-f", str(DEPLOY / "docker-compose.yml"), "-f", str(PHASE3_OVERRIDE), "-f", str(OVERRIDE)]
        if self.extra_override is not None:
            files += ["-f", str(self.extra_override)]
        cmd = [
            "docker", "compose", "--project-directory", str(DEPLOY), "-p", PROJECT, *files,
            "--env-file", str(self.env_file), *args,
        ]  # fmt: skip
        # stdin must not be inherited: an open stdin makes ``compose run`` hang.
        done = subprocess.run(
            cmd, env=self.env, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=timeout
        )
        if check and done.returncode != 0:
            raise AssertionError(f"{' '.join(args)} failed ({done.returncode}):\n{done.stderr[-4000:]}")
        return done

    def containers(self, service: str | None = None) -> list[str]:
        cmd = ["docker", "ps", "-q", "--no-trunc", "--filter", f"label=com.docker.compose.project={PROJECT}"]
        if service:
            cmd += ["--filter", f"label=com.docker.compose.service={service}"]
        return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout.split()

    def lease(self, request_id: str) -> dict[str, str]:
        """The Request's Lease hash (owner, epoch, expires_at), read from Redis; empty if there is none."""
        (redis,) = self.containers("redis")
        out = subprocess.run(
            ["docker", "exec", redis, "redis-cli", "--raw", "HGETALL", f"das:lease:{request_id}"],
            capture_output=True, text=True, check=True, timeout=10,
        ).stdout  # fmt: skip
        lines = out.split("\n")
        return dict(zip(lines[0::2], lines[1::2], strict=False)) if len(lines) > 1 else {}

    # ---- gateways
    def url(self, gateway: str) -> str:
        return f"http://127.0.0.1:{self.ports[gateway]}"

    def client(self, gateway: str, key: str = MAIN_KEY) -> httpx.Client:
        if (gateway, key) not in self._clients:
            self._clients[(gateway, key)] = httpx.Client(
                base_url=self.url(gateway), headers={"X-API-Key": key}, timeout=15
            )
        return self._clients[(gateway, key)]

    def close(self) -> None:
        for client in self._clients.values():
            client.close()

    def agents(self) -> list[dict[str, Any]]:
        response = self.client("a").get("/v1/agents")
        assert response.status_code == 200, response.text
        return response.json()

    def submit(self, gateway: str, prompt: str, **extra: Any) -> str:
        response = self.client(gateway).post("/v1/requests", json={"prompt": prompt, **extra})
        assert response.status_code == 202, response.text
        return response.json()["request_id"]

    def query(self, request_id: str, gateway: str = "a") -> dict[str, Any]:
        response = self.client(gateway).get(f"/v1/requests/{request_id}")
        assert response.status_code == 200, response.text
        return response.json()

    def wait_for(
        self, request_id: str, predicate: Callable[[dict[str, Any]], bool], timeout: float, gateway: str = "a"
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        body = self.query(request_id, gateway)
        while not predicate(body):
            if time.monotonic() > deadline:
                raise AssertionError(f"timed out waiting on {request_id}: {json.dumps(body)[:2000]}")
            time.sleep(0.25)
            body = self.query(request_id, gateway)
        return body

    def wait_terminal(self, request_id: str, timeout: float = 90, gateway: str = "a") -> dict[str, Any]:
        return self.wait_for(request_id, lambda b: b["status"] in TERMINAL, timeout, gateway)

    def sink(self, request_id: str | None = None) -> list[dict[str, Any]]:
        query = f"?request_id={request_id}" if request_id else ""
        response = httpx.get(f"http://127.0.0.1:{self.ports['sink']}/received{query}", timeout=10)
        assert response.status_code == 200, response.text
        return response.json()


# --------------------------------------------------------------------------- fixture


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[ClientStack]:
    if not _docker_available():
        pytest.skip("Docker / docker compose not available; end-to-end tests need the Compose stack")

    work = tmp_path_factory.mktemp("e2e-client")
    ports = {"a": _free_port(), "b": _free_port(), "sink": _free_port(), "mgmt": _free_port()}
    env_file = work / "e2e.env"
    env_file.write_text(
        "\n".join(
            [
                "RABBITMQ_USER=das",
                "RABBITMQ_PASS=e2e-rabbit-password",
                f"DAS_GATEWAY_API_KEYS={MAIN_KEY},{LIMITED_KEY}",
                f"DAS_GATEWAY_PORT={ports['a']}",
                f"{GATEWAY_B_PORT_VAR}={ports['b']}",
                f"{SINK_HOST_PORT_VAR}={ports['sink']}",
                f"DAS_RABBITMQ_MGMT_PORT={ports['mgmt']}",
            ]
        )
        + "\n"
    )
    passed_through = ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG")
    env = {k: os.environ[k] for k in passed_through if k in os.environ}
    extra = os.environ.get("DAS_E2E_CLIENT_OVERRIDE_EXTRA")
    s = ClientStack(env=env, env_file=env_file, ports=ports, extra_override=Path(extra) if extra else None)
    logs = work / "compose.log"
    try:
        s.compose("down", "-v", "--remove-orphans", check=False)
        # Image build is not part of the readiness bound (as in the Phase 3 e2e).
        s.compose("build", timeout=1500)
        s.compose("pull", "--ignore-buildable", "--quiet", timeout=900, check=False)

        t0 = time.monotonic()
        s.compose("up", "-d", timeout=180)
        deadline = t0 + 150
        while True:
            try:
                live = [a for a in s.agents() if a["status"] == "Available"]
                ready = (
                    sum(a["role"] == "manager" for a in live) == 2
                    and sum(a["role"] == "worker" for a in live) == 2
                    and all(s.client(g).get("/healthz").status_code == 200 for g in ("a", "b"))
                    and httpx.get(f"http://127.0.0.1:{ports['sink']}/healthz", timeout=5).status_code == 200
                )
                if ready:
                    break
            except (httpx.HTTPError, AssertionError):
                pass
            if time.monotonic() > deadline:
                raise AssertionError(f"stack not ready in 150 s: {s.compose('ps', check=False).stdout[-2000:]}")
            time.sleep(0.5)
        services = ("rabbitmq", "redis", "gateway", "gateway-b", "manager", "worker-research", "worker-review")
        for service in (*services, "webhook-sink"):
            assert s.containers(service), f"{service} is not running"
        TIMINGS["stack_ready_s"] = round(time.monotonic() - t0, 2)
        yield s
    finally:
        if log_dir := os.environ.get("DAS_E2E_LOG_DIR"):
            logs = Path(log_dir) / f"{PROJECT}.log"
        try:
            logs.parent.mkdir(parents=True, exist_ok=True)
            logs.write_text(s.compose("logs", "--no-color", "--timestamps", check=False, timeout=120).stdout)
        except Exception:  # noqa: BLE001 - diagnostics only
            pass
        s.close()
        s.compose("down", "-v", "--remove-orphans", check=False, timeout=300)
        print(f"\n[e2e-client] compose logs: {logs}")
        print("[e2e-client] timings:", json.dumps(TIMINGS, indent=2))


# --------------------------------------------------------------------------- tests


def test_stack_has_two_gateways_two_managers_two_workers_and_a_sink(stack: ClientStack) -> None:
    live = [a for a in stack.agents() if a["status"] == "Available"]
    assert sum(a["role"] == "manager" for a in live) == 2
    assert sum(a["role"] == "worker" for a in live) == 2
    assert {g: stack.client(g).get("/healthz").status_code for g in ("a", "b")} == {"a": 200, "b": 200}
    assert stack.ports["a"] != stack.ports["b"]
    assert TIMINGS["stack_ready_s"] <= 150


def test_two_gateways_share_one_rate_limit_bucket(stack: ClientStack) -> None:
    """Req 2.3, 2.4: the limited key (rate 1.0, burst 5) is admitted at most ``burst + rate * T`` times in total."""
    limited = {"a": stack.client("a", LIMITED_KEY), "b": stack.client("b", LIMITED_KEY)}
    calls: list[tuple[str, httpx.Response]] = []
    start = time.monotonic()
    for i in range(14):  # alternate, so both replicas see the key
        gateway = "ab"[i % 2]
        calls.append((gateway, limited[gateway].get("/v1/agents")))
    elapsed = time.monotonic() - start

    admitted = [(g, r) for g, r in calls if r.status_code == 200]
    rejected = [(g, r) for g, r in calls if r.status_code == 429]
    TIMINGS["bucket"] = {
        "elapsed_s": round(elapsed, 3),
        "admitted": len(admitted),
        "by_gateway": [g for g, _ in admitted],
    }
    assert len(admitted) + len(rejected) == 14, [r.status_code for _, r in calls]
    assert len(admitted) >= 5, "the burst of 5 was not admitted"
    # Two per-replica buckets would admit about 10 here. One shared bucket admits the burst plus what refilled.
    assert len(admitted) <= 5 + math.floor(elapsed * 1.0), f"{len(admitted)} admitted in {elapsed:.3f}s"
    assert {g for g, _ in admitted} == {"a", "b"}, "one replica answered everything: the bucket is not shared"
    for _, r in rejected:
        assert r.json() == {"error": "rate_limited"}
        assert int(r.headers["retry-after"]) >= 1

    wait = int(rejected[-1][1].headers["retry-after"])
    time.sleep(wait + 0.3)  # a token has come back, for whichever replica asks
    assert limited["b"].get("/v1/agents").status_code == 200


def test_a_new_event_reaches_an_open_stream_within_3_seconds(stack: ClientStack) -> None:
    """Req 7.9: measured from just before the write is requested (so never negative) to the frame being read."""
    latencies: list[float] = []
    for sample in range(3):
        rid = stack.submit("a", f"Long running job [[delay=20]] latency sample {sample}")
        stream = EventStream(stack.ports["b"], rid)  # the stream is on Gateway B, the write is requested on Gateway A
        stream.start()
        assert stream.wait_for(lambda f: f.name == "snapshot", 10), f"no snapshot: {stream.status} {stream.error!r}"
        stack.wait_for(rid, lambda b: any(j["state"] == "Dispatched" for j in b["jobs"]), timeout=30)

        sent = time.monotonic()
        cancelled = stack.client("a").post(f"/v1/requests/{rid}/cancel")
        assert cancelled.status_code == 202, cancelled.text
        frame = stream.wait_for(lambda f: f.name == "request.cancelled", 10)
        assert frame is not None, f"no request.cancelled frame: {stream.names()}"
        latencies.append(round(frame.arrival - sent, 3))

        stream.join(10)
        assert not stream.is_alive(), "the stream was not closed after the terminal event"
        assert stream.error is None, stream.error
        assert stream.names()[-1] == "request.cancelled"
        assert stream.names().count("request.cancelled") == 1
    TIMINGS["event_latency_s"] = latencies
    assert all(0 <= latency <= 3.0 for latency in latencies), latencies


def test_das_client_against_the_stack(stack: ClientStack) -> None:
    """Req 9.6, 4: idempotency is shared by the replicas; ``wait``, ``events`` and paging work on the real stack."""
    from das_client import AsyncDasClient, DasClient

    prompt = "Explain the CAP theorem in two sentences."
    key = f"e2e-{uuid.uuid4()}"
    with DasClient(stack.url("a"), MAIN_KEY) as a, DasClient(stack.url("b"), MAIN_KEY) as b:
        first = a.submit(prompt, idempotency_key=key)
        again = b.submit(prompt, idempotency_key=key)  # the same key through the other replica
        assert (first.replayed, again.replayed, again.request_id) == (False, True, first.request_id)

        view = a.wait(first.request_id, timeout=90)
        assert view.status == "Completed" and view.is_terminal
        assert view.final_result["summary"]
        assert all(job.state == "Succeeded" for job in view.jobs) and len(view.jobs) >= 2

        events = list(b.events(first.request_id))  # already finished: its snapshot, then the stream closes
        assert [e.type for e in events] == ["snapshot"]
        assert events[0].data["status"] == "Completed" and "final_result" not in events[0].data

        ids: list[str] = []
        cursor = None
        deadline = time.monotonic() + 30
        while True:
            page = _list_when_ready(b, limit=1, cursor=cursor, deadline=deadline)
            assert len(page.items) <= 1
            ids += [item.request_id for item in page.items]
            cursor = page.next_cursor
            if cursor is None:
                break
        assert first.request_id in ids and len(ids) == len(set(ids)), ids

    async def with_the_async_client() -> tuple[Any, Any, list[Any]]:
        async with AsyncDasClient(stack.url("b"), MAIN_KEY) as client:
            submitted = await client.submit("Name two uses of a message queue.")
            final = await client.wait(submitted.request_id, timeout=90)
            return submitted, final, [e async for e in client.events(submitted.request_id)]

    submitted, final, async_events = asyncio.run(with_the_async_client())
    assert final.status == "Completed" and final.final_result["summary"]
    assert [e.type for e in async_events] == ["snapshot"] and async_events[0].request_id == submitted.request_id


def _list_when_ready(client: Any, *, limit: int, cursor: str | None, deadline: float) -> Any:
    """``list_requests``, waiting out ``503 index_not_ready`` (the Request_Index is built after a Manager starts)."""
    from das_client import ServiceUnavailable

    while True:
        try:
            return client.list_requests(limit=limit, cursor=cursor)
        except ServiceUnavailable:
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


def _sink_records(stack: ClientStack, request_id: str, count: int, timeout: float) -> list[dict[str, Any]]:
    """The sink's records for one Request, once there are at least ``count`` of them."""
    deadline = time.monotonic() + timeout
    while True:
        records = stack.sink(request_id)
        if len(records) >= count:
            return records
        if time.monotonic() > deadline:
            raise AssertionError(f"only {len(records)} of {count} deliveries for {request_id} in {timeout}s: {records}")
        time.sleep(0.25)


def test_a_webhook_is_retried_with_backoff_and_every_attempt_is_signed(stack: ClientStack) -> None:
    """Req 8.3 to 8.6: the sink fails the first two attempts; the sender retries with doubling backoff, and stops."""
    rid = stack.submit("a", "Explain consensus in one sentence.")
    assert stack.wait_terminal(rid)["status"] == "Completed"

    records = _sink_records(stack, rid, count=3, timeout=40)
    assert len(records) == 3, records
    assert len({r["id"] for r in records}) == 1 and records[0]["id"].startswith(f"{rid}:"), records
    assert [r["attempt"] for r in records] == [1, 2, 3]
    assert [r["status"] for r in records] == [500, 500, 204]
    assert all(r["verified"] for r in records), "a delivery was not correctly signed"
    assert {r["event_type"] for r in records} == {"request.completed"} and {r["request_id"] for r in records} == {rid}

    # base_backoff is 1.0 and doubles with +-20% jitter: about 1 s, then about 2 s (the upper bounds allow for polling).
    first, second = (round(b["t"] - a["t"], 3) for a, b in zip(records, records[1:], strict=False))
    TIMINGS["webhook_gaps_s"] = [first, second]
    assert 0.75 <= first <= 2.2 and 1.5 <= second <= 3.4, (first, second)
    assert second > first, "the backoff did not grow"

    view = stack.wait_for(rid, lambda b: (b["webhook"] or {}).get("state") == "delivered", timeout=30)
    assert view["webhook"] == {"state": "delivered", "attempts": 3, "last_status": "204"}
    time.sleep(3)  # nothing is sent after a success
    assert len(stack.sink(rid)) == 3


def _find(obj: Any, key: str) -> Any:
    """The first value stored under ``key`` anywhere in a decoded JSON document, or ``None``."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        obj = list(obj.values())
    if isinstance(obj, list):
        for item in obj:
            if (found := _find(item, key)) is not None:
                return found
    return None


def test_a_budget_stops_the_request_after_the_planner_and_the_jobs_in_flight(stack: ClientStack) -> None:
    """Req 5.6 to 5.9: a limit of the Planner's tokens plus one is crossed by the first Job result.

    Both Jobs have no dependencies, so both are already in flight (in-flight Jobs finish, Req 5.7), and the Job that
    depends on them is skipped. The API shows the status, the error, each Job's state and the budget, and no
    ``final_result`` (it is only returned for a Completed Request, see the plan's Decision 1).
    """
    prompt = "Compare message queues with event logs. [[delay=2]]"  # the marker keeps both Jobs in flight
    baseline = stack.wait_terminal(stack.submit("a", prompt))
    assert baseline["status"] == "Completed" and baseline["budget"] is None
    plan_tokens = baseline["token_usage"]["by_operation"]["plan"]["total_tokens"]
    assert plan_tokens > 0
    limit = plan_tokens + 1

    rid = stack.submit("a", prompt, budget={"max_total_tokens": limit})
    stream = EventStream(stack.ports["b"], rid)
    stream.start()
    view = stack.wait_terminal(rid, timeout=60)
    stream.join(15)

    assert view["status"] == "Failed" and view["error"] == "budget_exceeded", view
    states = [job["state"] for job in view["jobs"]]
    assert sorted(states) == ["Skipped", "Succeeded", "Succeeded"], states
    assert states[-1] == "Skipped", "the Job that depends on the others is the one that was skipped"
    assert "final_result" not in view

    budget = view["budget"]
    used = view["token_usage"]["total"]["total_tokens"]
    assert budget["limits"]["max_total_tokens"] == limit and budget["exceeded"] is True
    assert budget["used"]["total_tokens"] == used >= limit
    operations = view["token_usage"]["by_operation"]
    assert operations["plan"]["total_tokens"] == plan_tokens
    assert "summarize" not in operations or operations["summarize"]["total_tokens"] == 0  # no Summarizer call
    # Req 5.8: usage is above the limit only by what the Jobs that were already in flight used.
    assert used == plan_tokens + operations["job"]["total_tokens"]

    assert not stream.is_alive() and stream.error is None, stream.error
    assert stream.names()[-1] == "request.failed", stream.names()
    terminal = json.loads(stream.frames[-1].text.split("data: ", 1)[1])
    assert _find(terminal, "budget")["exceeded"] is True
    TIMINGS["budget"] = {"limit": limit, "used": used, "plan_tokens": plan_tokens}


def _event_id(frame: Frame) -> tuple[int, int] | None:
    """The ``id:`` of a frame as a comparable pair (Redis stream ids are ``<ms>-<seq>``), or ``None`` without one."""
    line = next((ln for ln in frame.text.splitlines() if ln.startswith("id: ")), None)
    if line is None:
        return None
    millis, _, seq = line[len("id: ") :].partition("-")
    return int(millis), int(seq or 0)


def _wait_two_managers(stack: ClientStack, exclude: frozenset[str], timeout: float = 60) -> None:
    """Two running Manager containers, each with an Available registry entry that is not an excluded agent id."""
    deadline = time.monotonic() + timeout
    while True:
        try:
            live = {a["host"] for a in stack.agents() if a["status"] == "Available" and a["agent_id"] not in exclude}
            managers = stack.containers("manager")
            if len(managers) == 2 and all(any(c.startswith(h) for h in live) for c in managers):
                return
        except (httpx.HTTPError, AssertionError):
            pass
        if time.monotonic() > deadline:
            raise AssertionError(f"two Managers did not register in {timeout}s")
        time.sleep(0.5)


def test_an_open_stream_survives_the_kill_of_the_lease_owner(stack: ClientStack) -> None:
    """Req 7.3, 7.6: one connection, no reconnect; the Manager that owns the Request is killed mid-flight.

    The Jobs finish after the kill (``delay`` 15 s against a 10 s lease), so their ``Succeeded`` transitions and the
    terminal event are written by the Manager that takes over, and still arrive on the same open stream, once.
    """
    rid = stack.submit("a", "Takeover job [[delay=15]]")
    stack.wait_for(rid, lambda b: b["status"] == "Running" and any(j["state"] == "Dispatched" for j in b["jobs"]), 30)
    stream = EventStream(stack.ports["a"], rid)
    stream.start()
    assert stream.wait_for(lambda f: f.name == "snapshot", 10), f"no snapshot: {stream.status} {stream.error!r}"

    owner = stack.lease(rid)["owner"]
    agent = next(a for a in stack.agents() if a["agent_id"] == owner)
    victim = next((c for c in stack.containers("manager") if c.startswith(agent["host"])), None)
    assert victim, f"no Manager container for host {agent['host']}"
    try:
        subprocess.run(["docker", "kill", victim], check=True, capture_output=True)
        killed = time.monotonic()
        taken_over: float | None = None
        while time.monotonic() < killed + 10 + 30:
            current = stack.lease(rid).get("owner")
            if current and current != owner:
                taken_over = time.monotonic()
                break
            time.sleep(0.2)
        assert taken_over, "no Manager took over the Lease"
        TIMINGS["stream_takeover_after_kill_s"] = round(taken_over - killed, 3)

        view = stack.wait_terminal(rid, timeout=90)
        assert view["status"] == "Completed" and all(j["state"] == "Succeeded" for j in view["jobs"]), view
        stream.join(30)
    finally:
        stack.compose("up", "-d", "--no-recreate", "--scale", "manager=2", "manager")
        _wait_two_managers(stack, exclude=frozenset({owner}))

    assert not stream.is_alive(), "the stream was not closed after the terminal event"
    assert stream.error is None, stream.error
    names = stream.names()
    assert names[0] == "snapshot" and names.count("snapshot") == 1, names
    assert names.count("request.completed") == 1 and names[-1] == "request.completed", names
    assert not {"request.failed", "request.cancelled"} & set(names), names

    ids = [i for f in stream.frames if (i := _event_id(f)) is not None and f.name != "snapshot"]
    assert ids == sorted(set(ids)) and len(ids) == len(names) - 1, "event ids do not strictly increase"

    succeeded: dict[str, Frame] = {}
    for frame in stream.frames:
        if frame.name == "job.transition":
            data = json.loads(frame.text.split("data: ", 1)[1])
            if _find(data, "to") == "Succeeded":
                succeeded[_find(data, "job_id")] = frame
    assert set(succeeded) == {j["job_id"] for j in view["jobs"]}, (sorted(succeeded), view["jobs"])
    # Written by the new owner: the old one died with the Jobs still running.
    assert all(f.arrival >= taken_over - 1.0 for f in succeeded.values()), (taken_over, succeeded)
