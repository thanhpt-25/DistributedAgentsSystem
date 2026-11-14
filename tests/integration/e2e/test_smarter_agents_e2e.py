"""End-to-end tests of the Phase 6 features on the real Compose stack (das-smarter-agents task 7.3).

Brings up ``deploy/docker-compose.yml`` plus the Phase 3 e2e override plus ``docker-compose.smarter.e2e.yml`` under its
own Compose project (``das-e2e-smart``): real RabbitMQ and Redis, two Gateway-facing Managers, two Workers, and
``llm_stub_smart.py`` as an Ollama-compatible fake. The Phase 3 stack (``das-e2e``) is never touched.

Covered, each through the real Gateway, Manager, Worker and ``ollama`` adapter:

- structured output: a Job with an ``output_schema`` is answered in prose first, corrected once, and stored as canonical
  JSON (Phase 6 Req 1)
- the fallback model: the research Worker's Capability model does not exist (a 404), every research Job still
  succeeds on the fallback, and ``das_llm_fallbacks_total`` counts it (Req 2)
- replanning: a Job that fails for good makes the Manager replan; the Job that succeeded is kept and the Request
  completes with ``replans`` 1 (Req 3)
- a tool call: a Job that requires ``tool-calculator`` asks for the calculator, gets 42 and answers from it, and
  ``das_tool_calls_total`` counts the call (Req 5)
- without a marker a Request behaves as in Phase 5, with ``replans`` 0 (Req 7.1)

The module is skipped when Docker or Compose is unavailable; ``DAS_REQUIRE_DOCKER=1`` turns that skip into a failure.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from tests.integration.e2e import test_compose_e2e as phase3
from tests.integration.e2e.test_compose_e2e import API_KEY, DEPLOY, Stack

pytestmark = pytest.mark.integration

E2E = Path(__file__).resolve().parent
PROJECT = "das-e2e-smart"
OVERRIDES = [E2E / "docker-compose.e2e.yml", E2E / "docker-compose.smarter.e2e.yml"]
SCRAPE = "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8081/metrics').read().decode())"


class SmartStack(Stack):
    """The Phase 3 helper, pointed at this module's Compose project and override files."""

    def compose(self, *args: str, timeout: float = 300, check: bool = True) -> subprocess.CompletedProcess[str]:
        cmd = ["docker", "compose", "--project-directory", str(DEPLOY), "-p", PROJECT,
               "-f", str(DEPLOY / "docker-compose.yml"), *[a for o in OVERRIDES for a in ("-f", str(o))], *args]
        proc = subprocess.run(cmd, env=self.env, stdin=subprocess.DEVNULL, capture_output=True, text=True,
                              timeout=timeout)
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

    def job_result(self, rid: str, job_id: str, attempt: int = 1) -> dict[str, Any]:
        raw = self.redis_cli("HGET", f"das:req:{rid}:results", f"{job_id}:{attempt}").strip()
        assert raw, f"no stored result for {job_id}:{attempt} of {rid}"
        return json.loads(raw)

    def metric(self, service: str, name: str, **labels: str) -> float:
        """The summed value of ``name`` samples carrying ``labels`` on every container of ``service``."""
        total = 0.0
        for cid in self.containers(service):
            text = subprocess.run(["docker", "exec", cid, "python", "-c", SCRAPE], capture_output=True, text=True,
                                  timeout=20, check=True).stdout
            for line in text.splitlines():
                if not line.startswith(name + "{"):
                    continue
                found = dict(re.findall(r'(\w+)="([^"]*)"', line))
                if all(found.get(k) == v for k, v in labels.items()):
                    total += float(line.rsplit(" ", 1)[1])
        return total


@pytest.fixture(scope="module")
def smart(tmp_path_factory: pytest.TempPathFactory) -> Iterator[SmartStack]:
    if not phase3._docker_available():
        if os.environ.get("DAS_REQUIRE_DOCKER") == "1":
            pytest.fail("Docker / docker compose not available")
        pytest.skip("Docker / docker compose not available; end-to-end tests need the Compose stack")
    port = phase3._free_port()
    env = {
        "PATH": os.environ.get("PATH", ""), "HOME": os.environ.get("HOME", ""),
        "RABBITMQ_USER": "das", "RABBITMQ_PASS": "e2e-rabbit-password", "DAS_GATEWAY_API_KEYS": API_KEY,
        "DAS_GATEWAY_PORT": str(port), "DAS_RABBITMQ_MGMT_PORT": str(phase3._free_port()),
    }
    for name in ("DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG"):
        if name in os.environ:
            env[name] = os.environ[name]
    s = SmartStack(env=env, gateway_url=f"http://127.0.0.1:{port}")
    logs = tmp_path_factory.mktemp("e2e-smart") / "compose.log"
    try:
        s.compose("down", "-v", "--remove-orphans", check=False)
        s.compose("build", timeout=1500)
        s.compose("pull", "--ignore-buildable", "--quiet", timeout=900, check=False)
        s.compose("up", "-d", timeout=180)
        phase3._wait_registered(s, managers=2, workers=2, timeout=150)
        deadline = time.monotonic() + 60
        while True:
            try:
                if s.http.post("/v1/requests", json={"prompt": "readiness probe"}).status_code == 202:
                    break
            except httpx.HTTPError:
                pass
            if time.monotonic() > deadline:
                raise AssertionError("gateway did not accept a Request")
            time.sleep(0.5)
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


def jobs_by_id(body: dict[str, Any]) -> dict[str, str]:
    return {j["job_id"]: j["state"] for j in body["jobs"]}


# --------------------------------------------------------------------------- the stack


def test_the_research_worker_advertises_the_tool_capability_and_the_other_worker_does_not(smart: SmartStack) -> None:
    caps = {frozenset(w["capabilities"]) for w in smart.available("worker")}
    assert caps == {frozenset({"research", "summarize", "tool-calculator"}), frozenset({"code-review"})}


# --------------------------------------------------------------------------- Phase 5 behaviour is unchanged (Req 7.1)


def test_a_request_without_a_marker_behaves_as_in_phase_5(smart: SmartStack) -> None:
    rid = smart.submit("Explain the CAP theorem and review a short snippet.")
    body = smart.wait_terminal(rid, timeout=90)
    assert body["status"] == "Completed", body
    assert body["replans"] == 0
    assert set(jobs_by_id(body).values()) == {"Succeeded"}


# --------------------------------------------------------------------------- structured output (Req 1)


def test_a_structured_job_is_corrected_once_and_stored_as_canonical_json(smart: SmartStack) -> None:
    rid = smart.submit("What is six times seven? [[scenario=structured]]")
    body = smart.wait_terminal(rid, timeout=90)
    assert body["status"] == "Completed", body
    result = smart.job_result(rid, "j1")
    assert result["status"] == "Succeeded" and result["content"] == '{"answer":42}'
    # The first reply was prose, so the corrective completion's tokens are in the result as well.
    assert result["usage"]["output_tokens"] > 0


# --------------------------------------------------------------------------- tools (Req 5)


def test_a_job_that_requires_a_tool_capability_runs_the_calculator(smart: SmartStack) -> None:
    before = smart.metric("worker-research", "das_tool_calls_total", tool="calculator", outcome="ok")
    rid = smart.submit("Use the calculator. [[scenario=tool]]")
    body = smart.wait_terminal(rid, timeout=90)
    assert body["status"] == "Completed", body
    assert smart.job_result(rid, "j1")["content"] == "The result is 42"
    assert smart.metric("worker-research", "das_tool_calls_total", tool="calculator", outcome="ok") == before + 1


# --------------------------------------------------------------------------- the fallback model (Req 2)


def test_every_research_job_went_through_the_fallback_model(smart: SmartStack) -> None:
    """The earlier tests ran research Jobs on a Capability model that answers 404; they all still succeeded."""
    assert smart.metric("worker-research", "das_llm_fallbacks_total", model="missing-model") >= 3


# --------------------------------------------------------------------------- replanning (Req 3)


def test_a_failed_job_is_replanned_and_the_job_that_succeeded_is_kept(smart: SmartStack) -> None:
    rid = smart.submit("Review it and research it. [[scenario=replan]]")
    body = smart.wait_for(rid, lambda b: b["status"] in phase3.TERMINAL and b.get("replans", 0) >= 1, timeout=120)
    assert body["status"] == "Completed", body
    assert body["replans"] == 1
    states = jobs_by_id(body)
    assert states["j2"] == "Succeeded"  # kept exactly: not run again
    assert states["r1"] == "Succeeded" and states["r2"] == "Succeeded"
    assert "j1" not in states and "j3" not in states  # the failed and the skipped Jobs were replaced
    assert smart.job_result(rid, "j2")["attempt"] == 1
