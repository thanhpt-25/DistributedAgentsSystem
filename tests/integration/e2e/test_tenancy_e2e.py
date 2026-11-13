"""End-to-end tests of multi-tenancy on the real Compose stack (das-multi-tenant task 12.1).

Brings up ``deploy/docker-compose.yml`` plus the Phase 3 e2e override (the fake LLM, a 10 s Manager lease) plus
``docker-compose.tenancy.e2e.yml`` under its own Compose project (``das-e2e-tenant``): real RabbitMQ and Redis, **two**
Gateways with two tenants and an Operator, two Managers and two Workers. The other e2e stacks are never touched.
Variables go through a temporary env file passed with ``--env-file``, so a developer's ``deploy/.env`` cannot leak in.

Covered here (task 12.1), in the order the tests run (the stack is shared, and each test leaves it quiet):

- Scopes: what each key may and may not call, answered before anything is read or written (Phase 5 Req 2)
- Isolation across every route, on both Gateways: get, cancel, events, the listing and its cursors, the ``tenant``
  parameter, usage, agents, the Dead Letter routes (Req 3.2, 3.3, 3.4, 5.3)
- The Tenant_Quota: refused with its scope, tenant by tenant, and freed when a Request finishes (Req 3.7)
- Priority: a ``high`` Request overtakes a queued ``normal`` backlog on real RabbitMQ (Req 7.4)
- Usage: each tenant's report equals the token counts of its own Requests, which come from the fake LLM (Req 6.2, 6.3)
- The upgrade: data without tenant fields, the Tenant_Backfill, then reads (Req 9.2, 9.3)

The module is skipped when Docker or Compose is unavailable; ``DAS_REQUIRE_DOCKER=1`` turns that skip into a failure.
``DAS_E2E_TENANT_OVERRIDE_EXTRA`` names one more Compose file to layer on top, so a deliberately broken variant (see
``tenancy_variants.py``) can be tried without editing the committed files.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx
import pytest

from tests.integration.e2e.tenancy_constants import (
    API_KEYS,
    GATEWAY_B_PORT_VAR,
    PLAIN_KEY,
    PROJECT,
    ROOT_KEY,
    TEAM_A_ADMIN,
    TEAM_A_QUOTA,
    TEAM_A_READER,
    TEAM_A_WRITER,
    TEAM_B_ADMIN,
)
from tests.integration.e2e.test_client_api_e2e import _docker_available, _free_port

ROOT = Path(__file__).resolve().parents[3]
DEPLOY = ROOT / "deploy"
E2E = Path(__file__).resolve().parent
PHASE3_OVERRIDE = E2E / "docker-compose.e2e.yml"
OVERRIDE = E2E / "docker-compose.tenancy.e2e.yml"
TERMINAL = {"Completed", "Failed", "Cancelled"}
NOT_FOUND = {"error": "not_found"}
TIMINGS: dict[str, Any] = {}
GATEWAYS = ("a", "b")
TENANT_OF_KEY = {
    TEAM_A_ADMIN: "team-a", TEAM_A_READER: "team-a", TEAM_A_WRITER: "team-a", TEAM_B_ADMIN: "team-b",
    PLAIN_KEY: "default",
}  # fmt: skip


# --------------------------------------------------------------------------- the stack


@dataclass
class TenantStack:
    env: dict[str, str]
    env_file: Path
    ports: dict[str, int]  # "a" and "b" (the Gateways), "mgmt"
    extra_override: Path | None = None
    created: dict[str, list[str]] = field(default_factory=lambda: {"team-a": [], "team-b": [], "default": []})
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

    def redis(self, *args: str) -> str:
        """``redis-cli --raw`` in the stack's Redis container; its output, without the trailing newline."""
        (container,) = self.containers("redis")
        done = subprocess.run(
            ["docker", "exec", container, "redis-cli", "--raw", *args],
            capture_output=True, text=True, check=True, timeout=30,
        )  # fmt: skip
        return done.stdout.rstrip("\n")

    # ---- gateways
    def url(self, gateway: str) -> str:
        return f"http://127.0.0.1:{self.ports[gateway]}"

    def client(self, gateway: str, key: str) -> httpx.Client:
        if (gateway, key) not in self._clients:
            self._clients[(gateway, key)] = httpx.Client(
                base_url=self.url(gateway), headers={"X-API-Key": key}, timeout=20
            )
        return self._clients[(gateway, key)]

    def close(self) -> None:
        for client in self._clients.values():
            client.close()

    def agents(self) -> list[dict[str, Any]]:
        response = self.client("a", ROOT_KEY).get("/v1/agents")
        assert response.status_code == 200, response.text
        return response.json()

    def submit(self, gateway: str, key: str, prompt: str, **extra: Any) -> str:
        response = self.client(gateway, key).post("/v1/requests", json={"prompt": prompt, **extra})
        assert response.status_code == 202, response.text
        request_id = response.json()["request_id"]
        self.created[TENANT_OF_KEY[key]].append(request_id)
        return request_id

    def query(self, request_id: str, key: str = ROOT_KEY, gateway: str = "a") -> dict[str, Any]:
        response = self.client(gateway, key).get(f"/v1/requests/{request_id}")
        assert response.status_code == 200, response.text
        return response.json()

    def wait_terminal(self, request_id: str, timeout: float = 90, gateway: str = "a") -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        body = self.query(request_id, gateway=gateway)
        while body["status"] not in TERMINAL:
            if time.monotonic() > deadline:
                raise AssertionError(f"timed out waiting on {request_id}: {json.dumps(body)[:2000]}")
            time.sleep(0.25)
            body = self.query(request_id, gateway=gateway)
        return body

    def listed(self, gateway: str, key: str, **params: str) -> list[str]:
        """Every Request_ID of a listing, over all its pages (two to a page, so several pages are really walked)."""
        seen: list[str] = []
        cursor: str | None = None
        for _ in range(200):
            query = {"limit": "2", **params, **({"cursor": cursor} if cursor else {})}
            response = self.client(gateway, key).get("/v1/requests", params=query)
            assert response.status_code == 200, response.text
            body = response.json()
            seen += [item["request_id"] for item in body["items"]]
            cursor = body.get("next_cursor")
            if cursor is None:
                return seen
        raise AssertionError("a listing did not end")

    def all_created(self) -> list[str]:
        return [rid for ids in self.created.values() for rid in ids]


# --------------------------------------------------------------------------- fixture


@pytest.fixture(scope="module")
def stack(tmp_path_factory: pytest.TempPathFactory) -> Iterator[TenantStack]:
    if not _docker_available():
        pytest.skip("Docker / docker compose not available; end-to-end tests need the Compose stack")

    work = tmp_path_factory.mktemp("e2e-tenant")
    ports = {"a": _free_port(), "b": _free_port(), "mgmt": _free_port()}
    env_file = work / "e2e.env"
    env_file.write_text(
        "\n".join(
            [
                "RABBITMQ_USER=das",
                "RABBITMQ_PASS=e2e-rabbit-password",
                f"DAS_GATEWAY_API_KEYS={','.join(API_KEYS)}",
                f"DAS_GATEWAY_PORT={ports['a']}",
                f"{GATEWAY_B_PORT_VAR}={ports['b']}",
                f"DAS_RABBITMQ_MGMT_PORT={ports['mgmt']}",
            ]
        )
        + "\n"
    )
    passed_through = ("PATH", "HOME", "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG")
    env = {k: os.environ[k] for k in passed_through if k in os.environ}
    extra = os.environ.get("DAS_E2E_TENANT_OVERRIDE_EXTRA")
    s = TenantStack(env=env, env_file=env_file, ports=ports, extra_override=Path(extra) if extra else None)
    logs = work / "compose.log"
    try:
        s.compose("down", "-v", "--remove-orphans", check=False)
        # Image build is not part of the readiness bound (as in the other e2e stacks).
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
                    and all(s.client(g, ROOT_KEY).get("/healthz").status_code == 200 for g in GATEWAYS)
                    and s.redis("GET", "das:tenant:index:ready") == "1"  # the Tenant_Backfill has finished
                )
                if ready:
                    break
            except (httpx.HTTPError, AssertionError, subprocess.CalledProcessError, ValueError):
                pass
            if time.monotonic() > deadline:
                raise AssertionError(f"stack not ready in 150 s: {s.compose('ps', check=False).stdout[-2000:]}")
            time.sleep(0.5)
        for service in ("rabbitmq", "redis", "gateway", "gateway-b", "manager", "worker-research", "worker-review"):
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
        print(f"\n[e2e-tenant] compose logs: {logs}")
        print("[e2e-tenant] timings:", json.dumps(TIMINGS, indent=2))


def finished(stack: TenantStack, key: str, prompt: str, gateway: str = "a", **extra: Any) -> str:
    """Submit and wait for the terminal status: a quiet Request, so the next test starts from a quiet stack."""
    request_id = stack.submit(gateway, key, prompt, **extra)
    assert stack.wait_terminal(request_id)["status"] == "Completed"
    return request_id


# --------------------------------------------------------------------------- tests


def test_the_stack_has_two_gateways_two_managers_two_workers_and_the_tenant_index(stack: TenantStack) -> None:
    live = [a for a in stack.agents() if a["status"] == "Available"]
    assert sum(a["role"] == "manager" for a in live) == 2
    assert sum(a["role"] == "worker" for a in live) == 2
    assert {g: stack.client(g, ROOT_KEY).get("/healthz").status_code for g in GATEWAYS} == {"a": 200, "b": 200}
    assert stack.redis("GET", "das:tenant:index:ready") == "1"  # a tenant can be listed from the first call
    assert TIMINGS["stack_ready_s"] <= 150


@pytest.mark.parametrize("gateway", GATEWAYS)
def test_scopes_decide_before_anything_is_read_or_written(stack: TenantStack, gateway: str) -> None:
    """Req 2.2 to 2.5: each key may call what its Scopes allow, and a refused call touches nothing."""
    before = stack.redis("ZCARD", "das:requests:index")
    refused = [
        (TEAM_A_READER, "POST", "/v1/requests", "submit"),
        (TEAM_A_WRITER, "GET", "/v1/requests", "read"),
        (TEAM_A_WRITER, "GET", "/v1/usage", "read"),
        (TEAM_A_READER, "GET", "/v1/admin/dlq", "admin"),
        (TEAM_A_WRITER, "GET", "/v1/admin/dlq", "admin"),
        (PLAIN_KEY, "GET", "/v1/admin/dlq", "admin"),
        (TEAM_A_ADMIN, "GET", "/v1/admin/usage", "operator"),  # the admin Scope is not the Operator role
        (TEAM_B_ADMIN, "POST", "/v1/admin/agents/nobody/drain", "operator"),
        (PLAIN_KEY, "GET", "/v1/admin/usage", "operator"),
    ]
    for key, method, path, scope in refused:
        reply = stack.client(gateway, key).request(method, path, json={"prompt": "p"} if method == "POST" else None)
        assert (reply.status_code, reply.json()) == (403, {"error": "forbidden", "scope": scope}), (key, method, path)
    assert stack.redis("ZCARD", "das:requests:index") == before  # nothing was created by a refused call

    for headers in ({}, {"X-API-Key": "nobody-key-0123456789"}):
        reply = httpx.get(f"{stack.url(gateway)}/v1/requests", headers=headers, timeout=15)
        assert (reply.status_code, reply.json()) == (401, {"error": "unauthorized"})

    # And what each key may do.
    assert stack.client(gateway, TEAM_A_ADMIN).get("/v1/admin/dlq").json() == {"items": []}  # its own tenant's
    assert stack.client(gateway, ROOT_KEY).get("/v1/admin/usage").status_code == 200  # the Operator holds every role
    assert stack.client(gateway, TEAM_A_READER).get("/v1/usage").status_code == 200
    assert stack.client(gateway, PLAIN_KEY).get("/v1/requests").status_code == 200  # the Default_Tenant: submit + read


def test_a_key_without_a_tenant_is_the_default_tenant(stack: TenantStack) -> None:
    request_id = finished(stack, PLAIN_KEY, "a request of the default tenant", gateway="b")
    assert stack.client("a", PLAIN_KEY).get(f"/v1/requests/{request_id}").status_code == 200
    assert stack.redis("HGET", f"das:req:{request_id}", "tenant") in ("", "default")  # stored as, or read as, default


@pytest.mark.parametrize("gateway", GATEWAYS)
def test_tenants_cannot_see_each_others_requests_on_any_route(stack: TenantStack, gateway: str) -> None:
    """Req 3.2, 3.3, 3.4, 5.3: another tenant's Request is as good as missing, by every route, on either Gateway."""
    if len(stack.created["team-a"]) < 2:  # the first parametrization makes them; the second reuses them
        for i in range(2):
            finished(stack, TEAM_A_ADMIN, f"isolation request {i} of team a")
            finished(stack, TEAM_B_ADMIN, f"isolation request {i} of team b")
    a_ids, b_ids = stack.created["team-a"], stack.created["team-b"]
    ghost = uuid.uuid4().hex
    own, foreign = {TEAM_A_ADMIN: a_ids[0], TEAM_B_ADMIN: b_ids[0]}, {TEAM_A_ADMIN: b_ids[0], TEAM_B_ADMIN: a_ids[0]}

    for key in (TEAM_A_ADMIN, TEAM_B_ADMIN):
        client = stack.client(gateway, key)
        assert client.get(f"/v1/requests/{own[key]}").status_code == 200
        for method, suffix in (("GET", ""), ("POST", "/cancel"), ("GET", "/events")):
            other = client.request(method, f"/v1/requests/{foreign[key]}{suffix}")
            missing = client.request(method, f"/v1/requests/{ghost}{suffix}")
            assert (other.status_code, other.json()) == (missing.status_code, missing.json()) == (404, NOT_FOUND)
    for key in (PLAIN_KEY, TEAM_A_READER):  # the Default_Tenant and a read-only key of team-a: neither sees team-b's
        assert stack.client(gateway, key).get(f"/v1/requests/{b_ids[0]}").status_code == 404
    assert stack.client(gateway, TEAM_A_READER).get(f"/v1/requests/{a_ids[0]}").status_code == 200  # but its own, yes
    operator = stack.client(gateway, ROOT_KEY)
    assert [operator.get(f"/v1/requests/{r}").status_code for r in (a_ids[0], b_ids[0])] == [200, 200]
    assert stack.query(b_ids[0])["status"] == "Completed"  # the foreign cancel did nothing

    own_stream = stack.client(gateway, TEAM_A_ADMIN).get(f"/v1/requests/{a_ids[0]}/events", timeout=30)
    assert own_stream.status_code == 200 and "event: snapshot" in own_stream.text  # a finished Request: its snapshot
    assert '"status":"Completed"' in own_stream.text and a_ids[0] in own_stream.text  # and the stream then ends

    # The listing: each tenant sees its own Requests and no one else's, page by page.
    assert sorted(stack.listed(gateway, TEAM_A_ADMIN)) == sorted(a_ids)
    assert sorted(stack.listed(gateway, TEAM_B_ADMIN)) == sorted(b_ids)
    assert sorted(stack.listed(gateway, PLAIN_KEY)) == sorted(stack.created["default"])
    assert set(stack.listed(gateway, ROOT_KEY)) == set(stack.all_created())  # the Operator sees every tenant
    assert sorted(stack.listed(gateway, ROOT_KEY, tenant="team-b")) == sorted(b_ids)
    # The same by status: the tenant's by-status indexes are kept by the scripts as a Request moves.
    assert sorted(stack.listed(gateway, TEAM_A_ADMIN, status="Completed")) == sorted(a_ids)
    assert stack.listed(gateway, TEAM_A_ADMIN, status="Failed,Cancelled") == []
    assert sorted(stack.listed(gateway, TEAM_B_ADMIN, status="Completed")) == sorted(b_ids)

    # A cursor belongs to the scope that got it; the `tenant` parameter belongs to the Operator.
    first = stack.client(gateway, TEAM_A_ADMIN).get("/v1/requests", params={"limit": "1"})
    cursor = first.json()["next_cursor"]
    bad = {"error": "invalid_query", "parameter": "cursor"}
    for key in (TEAM_B_ADMIN, PLAIN_KEY, ROOT_KEY):
        reply = stack.client(gateway, key).get("/v1/requests", params={"limit": "1", "cursor": cursor})
        assert (reply.status_code, reply.json()) == (400, bad), key
    same = stack.client(gateway, TEAM_A_ADMIN).get("/v1/requests", params={"limit": "1", "cursor": cursor})
    assert same.status_code == 200
    for key in (TEAM_A_ADMIN, TEAM_B_ADMIN, PLAIN_KEY):
        reply = stack.client(gateway, key).get("/v1/requests", params={"tenant": "team-b"})
        assert (reply.status_code, reply.json()) == (400, {"error": "invalid_query", "parameter": "tenant"}), key

    # Usage, agents and the Dead Letter routes.
    report = stack.client(gateway, TEAM_A_ADMIN).get("/v1/usage").json()
    assert report["tenant"] == "team-a" and report["totals"]["requests"]["submitted"] == len(a_ids)
    assert stack.client(gateway, TEAM_B_ADMIN).get("/v1/usage").json()["totals"]["requests"]["submitted"] == len(b_ids)
    for key, sees_identity in ((TEAM_A_ADMIN, False), (TEAM_B_ADMIN, False), (PLAIN_KEY, True), (ROOT_KEY, True)):
        agents = stack.client(gateway, key).get("/v1/agents").json()
        assert agents and all(("host" in a and "agent_id" in a) == sees_identity for a in agents), key
    for key in (TEAM_A_ADMIN, TEAM_B_ADMIN, ROOT_KEY):
        assert stack.client(gateway, key).get("/v1/admin/dlq").json() == {"items": []}
        assert stack.client(gateway, key).get(f"/v1/admin/dlq/{'f' * 32}").json() == NOT_FOUND


def test_the_tenant_quota_refuses_with_its_scope_and_frees_when_a_request_finishes(stack: TenantStack) -> None:
    """Req 3.7: team-a may have two unfinished Requests in all, whichever key or Gateway; team-b is unaffected."""
    held = [
        stack.submit("a", TEAM_A_ADMIN, "[[delay=12]] holds one place of the quota"),
        stack.submit("b", TEAM_A_WRITER, "[[delay=12]] holds the other"),  # another key, another Gateway: one count
    ]
    index_before = stack.redis("ZCARD", "das:requests:index")
    over = {"error": "quota_exceeded", "limit": TEAM_A_QUOTA, "limit_scope": "tenant"}
    for gateway, key in (("a", TEAM_A_ADMIN), ("b", TEAM_A_WRITER), ("a", TEAM_A_WRITER)):
        reply = stack.client(gateway, key).post("/v1/requests", json={"prompt": "one too many"})
        assert (reply.status_code, reply.json()) == (429, over), (gateway, key)
    assert stack.redis("ZCARD", "das:requests:index") == index_before  # refused before any write

    finished(stack, TEAM_B_ADMIN, "team b has no quota to hit")  # the other tenant is unaffected
    finished(stack, PLAIN_KEY, "nor does the default tenant")

    # A Request leaving the count frees its place, on either Gateway.
    assert stack.client("a", TEAM_A_ADMIN).post(f"/v1/requests/{held[0]}/cancel").status_code == 202
    assert stack.wait_terminal(held[0])["status"] == "Cancelled"
    freed = stack.submit("b", TEAM_A_ADMIN, "[[delay=12]] a place was freed")
    assert stack.client("a", TEAM_A_ADMIN).post("/v1/requests", json={"prompt": "full again"}).status_code == 429
    for request_id in (held[1], freed):
        stack.client("a", TEAM_A_ADMIN).post(f"/v1/requests/{request_id}/cancel")
        stack.wait_terminal(request_id)


def test_a_high_request_overtakes_a_normal_backlog(stack: TenantStack) -> None:
    """Req 7.4 on real RabbitMQ: with a backlog of ``normal`` Jobs queued, a ``high`` Request submitted after a
    ``normal`` one is served first. Priority is a weighting, not strict (two ``high`` deliveries for each ``normal``),
    so the test asks for the order of the two late Requests and for the high one to beat at least half the backlog."""
    refused = stack.client("a", TEAM_B_ADMIN).post("/v1/requests", json={"prompt": "p", "priority": "high"})
    assert (refused.status_code, refused.json()) == (400, {"error": "priority_above_limit", "limit": "normal"})
    invalid = stack.client("a", TEAM_A_ADMIN).post("/v1/requests", json={"prompt": "p", "priority": "urgent"})
    assert (invalid.status_code, invalid.json()) == (400, {"error": "invalid_request", "parameter": "priority"})

    backlog = [stack.submit("a" if i % 2 == 0 else "b", TEAM_B_ADMIN, f"[[delay=3]] backlog {i}") for i in range(16)]
    time.sleep(2)  # let the Managers plan them and put their Jobs on the queues
    late_normal = stack.submit("a", TEAM_B_ADMIN, "[[delay=3]] a normal request submitted late")
    high = stack.submit("b", TEAM_A_ADMIN, "[[delay=3]] a high request submitted last", priority="high")
    assert stack.query(high)["priority"] == "high" and stack.query(late_normal)["priority"] == "normal"

    start, done = time.monotonic(), {}
    pending = {*backlog, late_normal, high}
    while pending:
        for request_id in list(pending):
            if stack.query(request_id)["status"] in TERMINAL:
                done[request_id] = time.monotonic() - start
                pending.discard(request_id)
        assert time.monotonic() - start < 240, f"still pending: {len(pending)}"
        time.sleep(0.2)
    beaten = sum(done[r] > done[high] for r in backlog)
    TIMINGS["priority"] = {
        "high_s": round(done[high], 2), "late_normal_s": round(done[late_normal], 2),
        "backlog_after_high": beaten, "backlog_last_s": round(max(done[r] for r in backlog), 2),
    }  # fmt: skip
    assert done[high] < done[late_normal], TIMINGS["priority"]  # submitted after it, served before it
    assert beaten >= len(backlog) // 2, TIMINGS["priority"]  # and ahead of at least half of the queue
    assert {stack.query(r)["status"] for r in (high, late_normal)} == {"Completed"}


def test_each_tenants_usage_report_equals_the_token_counts_of_its_own_requests(stack: TenantStack) -> None:
    """Req 6.2, 6.3: the rollups the scripts keep add up to what the fake LLM reported for each Request."""
    for request_id in stack.all_created():
        stack.wait_terminal(request_id)
    expected: dict[str, dict[str, int]] = {}
    for tenant, ids in stack.created.items():
        views = [stack.query(r) for r in ids]
        counts = [v["token_usage"]["total"] for v in views]
        expected[tenant] = {
            "submitted": len(ids),
            "input_tokens": sum(c["input_tokens"] for c in counts),
            "output_tokens": sum(c["output_tokens"] for c in counts),
            "total_tokens": sum(c["total_tokens"] for c in counts),
            "Completed": sum(v["status"] == "Completed" for v in views),
            "Cancelled": sum(v["status"] == "Cancelled" for v in views),
        }
    assert expected["team-a"]["input_tokens"] > 0 and expected["team-b"]["input_tokens"] > 0  # real counts, not zeros
    keys = {"team-a": TEAM_A_ADMIN, "team-b": TEAM_B_ADMIN, "default": PLAIN_KEY}
    for gateway in GATEWAYS:
        for tenant, key in keys.items():
            report = stack.client(gateway, key).get("/v1/usage").json()
            totals, want = report["totals"], expected[tenant]
            assert report["tenant"] == tenant and totals["estimated_cost"] is None  # no Price_Table configured
            assert totals["requests"]["submitted"] == want["submitted"], tenant
            assert totals["requests"]["finished"]["Completed"] == want["Completed"], tenant
            assert totals["requests"]["finished"]["Cancelled"] == want["Cancelled"], tenant
            assert (totals["input_tokens"], totals["output_tokens"], totals["total_tokens"]) == (
                want["input_tokens"], want["output_tokens"], want["total_tokens"],
            ), tenant  # fmt: skip
    everyone = stack.client("a", ROOT_KEY).get("/v1/admin/usage").json()["tenants"]  # the Operator: all, at once
    by_tenant = {r["tenant"]: r["totals"] for r in everyone}
    assert {"team-a", "team-b", "default"} <= set(by_tenant)
    for tenant, want in expected.items():
        assert by_tenant[tenant]["total_tokens"] == want["total_tokens"], tenant


def test_the_upgrade_backfills_data_written_without_tenant_fields(stack: TenantStack) -> None:
    """Req 9.2, 9.3: Requests with no ``tenant`` field and no tenant index, as a Phase 4 fleet leaves them, belong to
    the default tenant. Until the Tenant_Backfill sets its flag a tenant's listing is refused and the default tenant's
    reads the global index; once it has run, both read the tenant indexes and the old Requests are the default's."""
    old = set(stack.all_created())
    assert len(old) >= 20
    # Make the data what Phase 4 wrote: no tenant keys at all, and no `tenant` field on any Request.
    for key in stack.redis("--scan", "--pattern", "das:tenant:*").split():
        stack.redis("DEL", key)
    for request_id in stack.redis("ZRANGE", "das:requests:index", "0", "-1").split():
        stack.redis("HDEL", f"das:req:{request_id}", "tenant")
    assert stack.redis("GET", "das:tenant:index:ready") == ""

    for gateway in GATEWAYS:  # before the flag
        refused = stack.client(gateway, TEAM_A_ADMIN).get("/v1/requests")
        assert (refused.status_code, refused.json()) == (503, {"error": "index_not_ready"})
        assert set(stack.listed(gateway, PLAIN_KEY)) == old  # the default tenant: the global index, all of it
        assert set(stack.listed(gateway, ROOT_KEY)) == old
        assert stack.client(gateway, TEAM_A_ADMIN).get(f"/v1/requests/{stack.created['team-a'][0]}").status_code == 404

    stack.compose("restart", "manager", timeout=180)  # a Manager that starts with no flag runs the Tenant_Backfill
    deadline = time.monotonic() + 120
    while stack.redis("GET", "das:tenant:index:ready") != "1":
        assert time.monotonic() < deadline, "the Tenant_Backfill did not finish in 120 s"
        time.sleep(1)

    for gateway in GATEWAYS:  # after it
        assert stack.listed(gateway, TEAM_A_ADMIN) == []  # a configured tenant starts empty: nothing old is its own
        assert stack.listed(gateway, TEAM_B_ADMIN) == []
        assert set(stack.listed(gateway, PLAIN_KEY)) == old  # all of them are the default tenant's now
        assert set(stack.listed(gateway, ROOT_KEY)) == old
        assert stack.client(gateway, PLAIN_KEY).get(f"/v1/requests/{stack.created['team-a'][0]}").status_code == 200
    # New Requests are filed under their tenant at once, and usage from before the upgrade is not carried over.
    fresh = finished(stack, TEAM_A_ADMIN, "the first request of team a after the upgrade")
    assert stack.listed("a", TEAM_A_ADMIN) == [fresh]
    assert set(stack.listed("b", PLAIN_KEY)) == old
    assert stack.client("a", TEAM_A_ADMIN).get("/v1/usage").json()["totals"]["requests"]["submitted"] == 1
