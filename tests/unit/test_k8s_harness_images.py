"""The kind harness puts a third-party image into the cluster even when the machine has never pulled it
(das-multi-tenant task 11.3; found by the first run of the Kind_Job on a clean GitHub runner).

On a developer machine ``redis:7.4`` is always there from earlier Compose runs, so ``kind load`` worked; a fresh runner
has no such image, ``docker save`` failed, the fallback ``kind load docker-image`` failed too, and every kind test
errored. ``Cluster.load_image`` now pulls an image that is not present, for the node's platform, before saving it."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest

from tests.integration.k8s import cluster as harness
from tests.integration.k8s.cluster import Cluster


class FakeDocker:
    """Stands in for ``subprocess.run``: records every command, and answers as a machine that has or has not pulled."""

    def __init__(self, present: bool, save_works: bool = True) -> None:
        self.present, self.save_works = present, save_works
        self.calls: list[list[str]] = []

    def __call__(self, cmd: list[str], **_: Any) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(cmd))
        verb = cmd[1] if cmd[0] == "docker" else cmd[0]
        code = 0
        if cmd[:3] == ["docker", "image", "inspect"]:
            code = 0 if self.present else 1
        if cmd[:2] == ["docker", "pull"]:
            self.present = True  # the pull worked
        if cmd[:2] == ["docker", "save"]:
            code = 0 if self.present and self.save_works else 1
        return subprocess.CompletedProcess(cmd, code, stdout="", stderr=f"{verb} failed" if code else "")

    def verbs(self) -> list[str]:
        return [" ".join(c[:3] if c[:2] == ["docker", "image"] else c[:2]) for c in self.calls]


@pytest.fixture
def cluster() -> Cluster:
    return Cluster(Path("kubeconfig"), None)  # type: ignore[arg-type]  # load_image does not use the credentials


def run_load(cluster: Cluster, docker: FakeDocker, monkeypatch: pytest.MonkeyPatch, tmp_path: Path, image: str) -> None:
    monkeypatch.setattr(harness.subprocess, "run", docker)
    cluster.load_image(tmp_path, image)


def test_an_image_the_machine_has_never_pulled_is_pulled_before_it_is_loaded(
    cluster: Cluster, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    docker = FakeDocker(present=False)
    run_load(cluster, docker, monkeypatch, tmp_path, "redis:7.4")
    verbs = docker.verbs()
    assert verbs.index("docker pull") < verbs.index("docker save") < verbs.index("kind load")  # in that order
    pull = next(c for c in docker.calls if c[:2] == ["docker", "pull"])
    assert pull[2] == "--platform" and pull[3].startswith("linux/") and pull[-1] == "redis:7.4"  # the node's platform


def test_an_image_that_is_already_local_is_not_pulled_again(
    cluster: Cluster, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    docker = FakeDocker(present=True)
    run_load(cluster, docker, monkeypatch, tmp_path, "das-gateway:local")  # a freshly built image has no registry
    assert "docker pull" not in docker.verbs()
    assert docker.verbs()[-1] == "kind load"


def test_an_older_docker_without_platform_save_still_falls_back_to_kind_load_docker_image(
    cluster: Cluster, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    docker = FakeDocker(present=True, save_works=False)
    run_load(cluster, docker, monkeypatch, tmp_path, "redis:7.4")
    assert docker.calls[-1][:3] == ["kind", "load", "docker-image"]


def test_a_failed_pull_is_an_error_that_names_the_image(
    cluster: Cluster, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class NoRegistry(FakeDocker):
        def __call__(self, cmd: list[str], **kw: Any) -> subprocess.CompletedProcess[str]:
            done = super().__call__(cmd, **kw)
            if cmd[:2] == ["docker", "pull"]:
                self.present = False
                return subprocess.CompletedProcess(cmd, 1, stdout="", stderr="manifest unknown")
            return done

    with pytest.raises(harness.ClusterError, match=r"redis:7\.4.*manifest unknown"):  # the image and why
        run_load(cluster, NoRegistry(present=False), monkeypatch, tmp_path, "redis:7.4")
