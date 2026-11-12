"""The kind cluster of the Kubernetes tests (das-multi-tenant task 11.1; Phase 5 Req 11.3).

The tests are opt-in: they need ``kind``, ``kubectl``, ``helm`` and the three DAS images, and bring up a whole cluster,
so they run only with ``DAS_K8S_E2E=1`` (the Kind_Job sets it). A skip whose reason names Docker becomes a failure under
``DAS_REQUIRE_DOCKER=1`` (tests/conftest.py), which is what the Kind_Job wants when Docker or the images are missing."""

from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.integration.k8s import cluster as k8s

OPT_IN = "DAS_K8S_E2E"
LOG_DIR_ENV = "DAS_K8S_LOG_DIR"


@pytest.fixture(scope="session")
def cluster(request: pytest.FixtureRequest) -> Iterator[k8s.Cluster]:
    if os.environ.get(OPT_IN) != "1":
        pytest.skip(f"set {OPT_IN}=1 to run the kind cluster tests (they need kind, kubectl, helm and the das images)")
    missing = k8s.missing_tools()
    if "docker" in missing:
        pytest.skip("Docker is not available: the kind cluster tests need it")
    if missing:
        pytest.skip(f"the kind cluster tests need {', '.join(missing)} on PATH")
    if not k8s.docker_available():
        pytest.skip("Docker is not reachable: the kind cluster tests need a running daemon")
    absent = k8s.missing_images()
    if absent:
        pytest.skip(
            "build the images first (docker build -f deploy/docker/<role>.Dockerfile "
            f"-t das-<role>:{k8s.IMAGE_TAG} .); "
            f"missing {', '.join(absent)}"
        )
    workdir = Path(tempfile.mkdtemp(prefix="das-k8s-"))
    try:
        instance = k8s.Cluster.create(workdir, k8s.Credentials.random())
        try:
            yield instance
        finally:
            if request.session.testsfailed:
                instance.collect_logs(Path(os.environ.get(LOG_DIR_ENV, str(workdir / "logs"))))
                print(f"cluster logs: {os.environ.get(LOG_DIR_ENV, str(workdir / 'logs'))}")
            instance.destroy()
    finally:
        if not request.session.testsfailed or LOG_DIR_ENV in os.environ:
            shutil.rmtree(workdir, ignore_errors=True)
