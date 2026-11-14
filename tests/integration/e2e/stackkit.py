"""A Compose stack for the Phase 7 end-to-end tests (das-scale-resilience tasks 11.2, 11.3).

The Sentinel failover test and the HTTP transport test each bring up the real stack (``deploy/docker-compose.yml``, the
stub LLM override of ``docker-compose.e2e.yml``, and one Phase 7 file) under a project of their own, so a developer's
own ``das`` stack and the other e2e stacks are untouched. This module holds what they share: running Compose, finding
and exec-ing into a service's container, and submitting and waiting on Requests through the Gateway."""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[3]
DEPLOY = ROOT / "deploy"
E2E_OVERRIDE = Path(__file__).with_name("docker-compose.e2e.yml")
API_KEY = "e2e-api-key-0123456789"
TERMINAL = {"Completed", "Failed", "Cancelled"}


def docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        for cmd in (["docker", "info"], ["docker", "compose", "version"]):
            if subprocess.run(cmd, capture_output=True, timeout=20).returncode != 0:
                return False
    except (OSError, subprocess.TimeoutExpired):
        return False
    return True


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@dataclass
class ComposeStack:
    project: str
    files: list[Path]
    profiles: tuple[str, ...]
    env: dict[str, str]
    gateway_url: str
    http: httpx.Client = field(init=False)

    def __post_init__(self) -> None:
        self.http = httpx.Client(base_url=self.gateway_url, headers={"X-API-Key": API_KEY}, timeout=10)

    # ---- compose and containers
    def compose(self, *args: str, timeout: float = 300, check: bool = True) -> subprocess.CompletedProcess[str]:
        cmd = ["docker", "compose", "--project-directory", str(DEPLOY), "-p", self.project]
        for f in self.files:
            cmd += ["-f", str(f)]
        for p in self.profiles:
            cmd += ["--profile", p]
        proc = subprocess.run(
            cmd + list(args), env=self.env, stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=timeout
        )
        if check and proc.returncode != 0:
            raise AssertionError(f"{' '.join(args)} failed ({proc.returncode}):\n{proc.stderr[-4000:]}")
        return proc

    def containers(self, service: str, *, all_states: bool = False) -> list[str]:
        cmd = ["docker", "ps", "-q", "--no-trunc", "--filter", f"label=com.docker.compose.project={self.project}",
               "--filter", f"label=com.docker.compose.service={service}"]
        if all_states:
            cmd.insert(2, "-a")
        return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout.split()

    def exec(self, service: str, *cmd: str, timeout: float = 20) -> str:
        (cid, *_) = self.containers(service)
        out = subprocess.run(["docker", "exec", cid, *cmd], capture_output=True, text=True, timeout=timeout)
        assert out.returncode == 0, f"{' '.join(cmd)} in {service}: {out.stderr[-1000:]}"
        return out.stdout

    def redis_cli(self, service: str, *args: str, port: int | None = None) -> str:
        extra = ["-p", str(port)] if port else []
        return self.exec(service, "redis-cli", "--raw", *extra, *args)

    def fetch(self, service: str, url: str, *, method: str = "GET", token: str | None = None) -> tuple[int, str]:
        """An HTTP call from inside ``service``'s container (the images have Python, not curl): (status, body)."""
        script = (
            "import sys, urllib.request, urllib.error\n"
            "data = b'' if sys.argv[2] == 'POST' else None\n"
            "req = urllib.request.Request(sys.argv[1], method=sys.argv[2], data=data)\n"
            "if sys.argv[3]: req.add_header('Authorization', 'Bearer ' + sys.argv[3])\n"
            "try:\n r = urllib.request.urlopen(req, timeout=5); print(r.status); print(r.read().decode())\n"
            "except urllib.error.HTTPError as e: print(e.code); print(e.read().decode())\n"
        )
        status, _, body = self.exec(service, "python", "-c", script, url, method, token or "").partition("\n")
        return int(status), body

    def metric(self, service: str, name: str) -> float:
        """The sum of a counter's samples on one agent's Ops_Port (0 when it has none yet)."""
        _, body = self.fetch(service, "http://127.0.0.1:8081/metrics")
        total = 0.0
        for line in body.splitlines():
            if line.startswith(name) and not line.startswith("#") and line[len(name) : len(name) + 1] in ("{", " "):
                total += float(line.rsplit(" ", 1)[1])
        return total

    # ---- the Gateway
    def submit(self, prompt: str) -> str:
        resp = self.http.post("/v1/requests", json={"prompt": prompt})
        assert resp.status_code == 202, resp.text
        return resp.json()["request_id"]

    def query(self, rid: str) -> dict[str, Any]:
        resp = self.http.get(f"/v1/requests/{rid}")
        assert resp.status_code == 200, resp.text
        return resp.json()

    def wait_for(
        self, rid: str, pred: Callable[[dict[str, Any]], bool], timeout: float, poll: float = 0.25
    ) -> dict[str, Any]:
        """Poll a Request until ``pred`` holds; a failed poll (the Gateway cannot answer in a Failover) is retried."""
        deadline = time.monotonic() + timeout
        last: Any = None
        while time.monotonic() < deadline:
            try:
                last = self.query(rid)
                if pred(last):
                    return last
            except (httpx.HTTPError, AssertionError) as exc:
                last = repr(exc)
            time.sleep(poll)
        shown = json.dumps(last)[:2000] if isinstance(last, dict) else last
        raise AssertionError(f"timed out waiting on {rid}: {shown}")

    def wait_terminal(self, rid: str, timeout: float = 90) -> dict[str, Any]:
        return self.wait_for(rid, lambda b: b["status"] in TERMINAL, timeout)

    def available(self, role: str) -> list[dict[str, Any]]:
        resp = self.http.get("/v1/agents")
        assert resp.status_code == 200, resp.text
        return [a for a in resp.json() if a["role"] == role and a["status"] == "Available"]

    def wait_ready(self, managers: int, workers: int, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while True:
            try:
                if len(self.available("manager")) >= managers and len(self.available("worker")) >= workers:
                    r = self.http.post("/v1/requests", json={"prompt": "readiness probe"})
                    if r.status_code == 202:
                        return
            except (httpx.HTTPError, AssertionError):
                pass
            if time.monotonic() > deadline:
                raise AssertionError("the stack did not become ready")
            time.sleep(0.5)


@contextmanager
def running_stack(
    project: str,
    extra_files: list[Path],
    profiles: tuple[str, ...],
    tmp_path: Path,
    extra_env: dict[str, str] | None = None,
    ready_timeout: float = 240,
) -> Iterator[ComposeStack]:
    """Build, start and then remove a stack; the Compose logs go to ``DAS_E2E_LOG_DIR`` or ``tmp_path``."""
    port = free_port()
    env = {
        "PATH": os.environ.get("PATH", ""), "HOME": os.environ.get("HOME", ""),
        "RABBITMQ_USER": "das", "RABBITMQ_PASS": "e2e-rabbit-password",
        "DAS_GATEWAY_API_KEYS": API_KEY, "DAS_GATEWAY_PORT": str(port), "DAS_RABBITMQ_MGMT_PORT": str(free_port()),
        **(extra_env or {}),
    }
    for name in ("DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_CONFIG"):
        if name in os.environ:
            env[name] = os.environ[name]
    stack = ComposeStack(
        project, [DEPLOY / "docker-compose.yml", *extra_files, E2E_OVERRIDE], profiles, env, f"http://127.0.0.1:{port}"
    )
    log_dir = Path(os.environ["DAS_E2E_LOG_DIR"]) if os.environ.get("DAS_E2E_LOG_DIR") else tmp_path
    log = log_dir / f"{project}.log"
    try:
        stack.compose("down", "-v", "--remove-orphans", check=False)
        stack.compose("build", timeout=1500)
        stack.compose("pull", "--ignore-buildable", "--quiet", timeout=900, check=False)
        stack.compose("up", "-d", timeout=300)
        stack.wait_ready(managers=2, workers=2, timeout=ready_timeout)
        yield stack
    finally:
        try:
            log.parent.mkdir(parents=True, exist_ok=True)
            log.write_text(stack.compose("logs", "--no-color", "--timestamps", check=False, timeout=120).stdout)
        except Exception:  # noqa: BLE001 - diagnostics only
            pass
        stack.http.close()
        stack.compose("down", "-v", "--remove-orphans", check=False, timeout=300)
        print(f"\n[e2e] compose logs: {log}")
