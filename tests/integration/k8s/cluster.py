"""Driving a throwaway kind cluster from the tests (das-multi-tenant task 11.1; Phase 5 Req 11.3).

The cluster is created with its own kubeconfig file, so the user's ``~/.kube/config`` is never read or changed. The
three DAS images are built beforehand (``docker build -f deploy/docker/<role>.Dockerfile -t das-<role>:local .``) and
loaded into the node; Redis, RabbitMQ and the stub LLM come from ``tests/k8s/``; the chart is installed with
``tests/k8s/values-kind.yaml``. Every secret is random and exists only inside the cluster (plan section 9, G0b)."""

from __future__ import annotations

import json
import os
import platform
import secrets
import shutil
import socket
import subprocess
import tempfile
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[3]
K8S = ROOT / "tests" / "k8s"
CHART = ROOT / "deploy" / "helm" / "das"
LLM_STUB = ROOT / "tests" / "integration" / "e2e" / "llm_stub.py"

NAMESPACE = "das"
RELEASE = "das"  # the chart is named das, so objects are das-gateway, das-manager, das-worker-research, ...
CLUSTER = os.environ.get("DAS_K8S_CLUSTER_NAME", "das-e2e-k8s")
IMAGE_TAG = os.environ.get("DAS_K8S_IMAGE_TAG", "local")
DAS_IMAGES = [f"das-{role}:{IMAGE_TAG}" for role in ("gateway", "manager", "worker")]
INFRA_IMAGES = ["redis:7.4", "rabbitmq:4.1-management", "python:3.12-slim"]
INFRA_MANIFESTS = ("namespace.yaml",)  # applied first
WORKLOAD_MANIFESTS = ("redis.yaml", "rabbitmq.yaml", "llm-stub.yaml")
TOOLS = ("docker", "kind", "kubectl", "helm")

MANAGERS, WORKERS = 2, 3  # what values-kind.yaml deploys: two Managers, research x2 and review x1


class ClusterError(RuntimeError):
    pass


def missing_tools() -> list[str]:
    return [t for t in TOOLS if shutil.which(t) is None]


def docker_available() -> bool:
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=20).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def missing_images() -> list[str]:
    return [
        image
        for image in DAS_IMAGES
        if subprocess.run(["docker", "image", "inspect", image], capture_output=True).returncode != 0
    ]


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@dataclass(frozen=True)
class Credentials:
    """Random for every run; they live in the cluster Secret and in these test processes only."""

    api_key: str
    admin_key: str
    rabbit_user: str
    rabbit_password: str

    @classmethod
    def random(cls) -> Credentials:
        return cls(
            api_key="k8s-api-" + secrets.token_hex(12),
            admin_key="k8s-admin-" + secrets.token_hex(12),
            rabbit_user="das",
            rabbit_password=secrets.token_hex(12),
        )

    def secret_manifest(self) -> dict[str, Any]:
        return {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "das-secrets", "namespace": NAMESPACE},
            "stringData": {
                "API_KEYS": self.api_key,
                "ADMIN_API_KEYS": self.admin_key,
                "RABBITMQ_USER": self.rabbit_user,
                "RABBITMQ_PASS": self.rabbit_password,
                "RABBITMQ_URL": f"amqp://{self.rabbit_user}:{self.rabbit_password}@rabbitmq:5672/",
            },
        }


class Cluster:
    def __init__(self, kubeconfig: Path, credentials: Credentials) -> None:
        self.kubeconfig = kubeconfig
        self.credentials = credentials
        self._forward: subprocess.Popen[bytes] | None = None
        self.local_port = 0

    # ------------------------------------------------------------------ commands

    def _env(self) -> dict[str, str]:
        return {**os.environ, "KUBECONFIG": str(self.kubeconfig)}

    def run(self, cmd: Sequence[str], timeout: float = 300, check: bool = True, stdin: str | None = None) -> str:
        done = subprocess.run(list(cmd), env=self._env(), capture_output=True, text=True, timeout=timeout, input=stdin)
        if check and done.returncode != 0:
            raise ClusterError(f"{' '.join(cmd[:4])} failed ({done.returncode}): {(done.stderr or done.stdout)[-600:]}")
        return done.stdout.strip()

    def kubectl(self, *args: str, timeout: float = 300, check: bool = True, stdin: str | None = None) -> str:
        return self.run(["kubectl", "-n", NAMESPACE, *args], timeout=timeout, check=check, stdin=stdin)

    def helm(self, *args: str, timeout: float = 600) -> str:
        return self.run(["helm", "-n", NAMESPACE, *args], timeout=timeout)

    # ------------------------------------------------------------------ lifecycle

    @classmethod
    def create(cls, workdir: Path, credentials: Credentials) -> Cluster:
        cluster = cls(workdir / "kubeconfig", credentials)
        subprocess.run(
            ["kind", "delete", "cluster", "--name", CLUSTER], capture_output=True
        )  # a leftover of a failed run
        create = [
            "kind",
            "create",
            "cluster",
            "--name",
            CLUSTER,
            "--kubeconfig",
            str(cluster.kubeconfig),
            "--wait",
            "180s",
        ]
        if os.environ.get("DAS_KIND_NODE_IMAGE"):
            create += ["--image", os.environ["DAS_KIND_NODE_IMAGE"]]
        cluster.run(create, timeout=600)
        try:
            for image in [*DAS_IMAGES, *INFRA_IMAGES]:
                cluster.load_image(workdir, image)
            cluster.bring_up()
        except BaseException:
            cluster.destroy()
            raise
        return cluster

    def load_image(self, workdir: Path, image: str) -> None:
        """Put a local image into the node. ``kind load docker-image`` fails for a multi-platform image, so the node's
        platform is saved on its own and loaded as an archive (plan section 9, G0b)."""
        arch = {"x86_64": "amd64", "amd64": "amd64", "arm64": "arm64", "aarch64": "arm64"}.get(
            platform.machine().lower(), "amd64"
        )
        if subprocess.run(["docker", "image", "inspect", image], capture_output=True).returncode != 0:
            # The DAS images are built before the tests, but the infrastructure images come from a registry: a
            # developer machine has them from earlier Compose runs, a clean CI runner has never pulled them.
            pulled = subprocess.run(
                ["docker", "pull", "--platform", f"linux/{arch}", image], capture_output=True, text=True
            )
            if pulled.returncode != 0:
                raise ClusterError(f"docker pull {image} failed ({pulled.returncode}): {pulled.stderr[-600:]}")
        archive = workdir / (image.replace("/", "_").replace(":", "_") + ".tar")
        saved = subprocess.run(
            ["docker", "save", "--platform", f"linux/{arch}", "-o", str(archive), image], capture_output=True
        )
        if saved.returncode == 0:
            self.run(["kind", "load", "image-archive", str(archive), "--name", CLUSTER], timeout=600)
            archive.unlink(missing_ok=True)
        else:  # an older docker without --platform
            self.run(["kind", "load", "docker-image", image, "--name", CLUSTER], timeout=600)

    def bring_up(self) -> None:
        self.run(["kubectl", "apply", "-f", *[str(K8S / f) for f in INFRA_MANIFESTS]])
        self.kubectl("apply", "-f", "-", stdin=json.dumps(self.credentials.secret_manifest()))
        self.kubectl("create", "configmap", "llm-stub", f"--from-file=llm_stub.py={LLM_STUB}")
        self.kubectl("apply", *[a for f in WORKLOAD_MANIFESTS for a in ("-f", str(K8S / f))])
        for deployment in ("redis", "rabbitmq", "llm-stub"):
            self.kubectl("rollout", "status", f"deploy/{deployment}", "--timeout=240s", timeout=260)
        sets = ["--set", f"image.tag={IMAGE_TAG}"] if IMAGE_TAG != "local" else []
        self.helm(
            "install", RELEASE, str(CHART), "-f", str(K8S / "values-kind.yaml"), *sets, "--wait", "--timeout", "6m"
        )
        self.forward()
        self.wait_agents(MANAGERS, WORKERS, timeout=180)

    def destroy(self) -> None:
        self.stop_forward()
        if os.environ.get("DAS_K8S_KEEP") == "1":
            print(f"kind cluster {CLUSTER} kept; KUBECONFIG={self.kubeconfig}")
            return
        subprocess.run(["kind", "delete", "cluster", "--name", CLUSTER], capture_output=True, timeout=180)

    def collect_logs(self, directory: Path) -> None:
        """Every pod's logs and a description of every object, for a failed run."""
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "describe.txt").write_text(self.kubectl("describe", "pods", check=False))
        (directory / "events.txt").write_text(self.kubectl("get", "events", "--sort-by=.lastTimestamp", check=False))
        for pod in self.run(["kubectl", "-n", NAMESPACE, "get", "pods", "-o", "name"], check=False).split():
            name = pod.split("/")[-1]
            (directory / f"{name}.log").write_text(
                self.kubectl("logs", pod, "--all-containers", "--tail=2000", check=False)
            )

    # ------------------------------------------------------------------ the Gateway

    def forward(self) -> None:
        """``kubectl port-forward`` pins to one pod, so it must be restarted when the Gateway pod is replaced."""
        self.stop_forward()
        self.local_port = free_port()
        self._forward = subprocess.Popen(
            ["kubectl", "-n", NAMESPACE, "port-forward", f"svc/{RELEASE}-gateway", f"{self.local_port}:8080"],
            env=self._env(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )  # fmt: skip
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                if httpx.get(f"http://127.0.0.1:{self.local_port}/livez", timeout=2).status_code < 500:
                    return
            except httpx.HTTPError:
                time.sleep(0.5)
        raise ClusterError("the Gateway did not answer through the port-forward")

    def stop_forward(self) -> None:
        if self._forward is not None:
            self._forward.terminate()
            try:
                self._forward.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._forward.kill()
            self._forward = None

    def client(self, key: str | None = None) -> httpx.Client:
        return httpx.Client(
            base_url=f"http://127.0.0.1:{self.local_port}",
            headers={"X-API-Key": key or self.credentials.api_key},
            timeout=20,
        )

    # ------------------------------------------------------------------ the platform

    def pods(self, component: str, phase: str = "Running") -> list[str]:
        out = self.kubectl(
            "get", "pods", "-l", f"app.kubernetes.io/component={component}", f"--field-selector=status.phase={phase}",
            "-o", "jsonpath={.items[*].metadata.name}",
        )  # fmt: skip
        return out.split()

    def pod_ip(self, pod: str) -> str:
        return self.kubectl("get", "pod", pod, "-o", "jsonpath={.status.podIP}")

    def agents(self) -> list[dict[str, Any]]:
        with self.client() as c:
            reply = c.get("/v1/agents")
            reply.raise_for_status()
            return list(reply.json())

    def wait_agents(self, managers: int, workers: int, timeout: float = 120, exclude: Sequence[str] = ()) -> float:
        """Until exactly this many Available Managers and Workers are registered (not counting ``exclude``)."""
        return self.wait_for(
            lambda: self._available(managers, workers, exclude), timeout, f"{managers} managers and {workers} workers"
        )

    def _available(self, managers: int, workers: int, exclude: Sequence[str]) -> bool:
        live = [a for a in self.agents() if a["status"] == "Available" and a["agent_id"] not in exclude]
        return (
            sum(a["role"] == "manager" for a in live) == managers
            and sum(a["role"] == "worker" for a in live) == workers
        )

    @staticmethod
    def wait_for(predicate: Callable[[], bool], timeout: float, what: str, interval: float = 1.0) -> float:
        start = time.monotonic()
        last: Exception | None = None
        while time.monotonic() - start < timeout:
            try:
                if predicate():
                    return time.monotonic() - start
            except (httpx.HTTPError, ClusterError) as exc:
                last = exc
            time.sleep(interval)
        raise ClusterError(
            f"timed out after {timeout:g}s waiting for {what}" + (f" (last error: {last})" if last else "")
        )

    def workdir_factory(self) -> Path:  # pragma: no cover - convenience for ad-hoc debugging
        return Path(tempfile.mkdtemp(prefix="das-k8s-"))
