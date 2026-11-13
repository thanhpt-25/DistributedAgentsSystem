"""No Compose file and no Helm chart of the repository mounts a container runtime's socket or runs a privileged
container (das-smarter-agents task 6.4; Phase 6 Req 6.8).

The Code_Worker starts containers by talking to a container runtime. Handing that runtime's socket to a container, or
running one privileged, would let a break-out of the sandbox, or of any agent, take the host. So the repository's own
deployment files must never do either. The detectors are small pure functions, tested against bad samples first so a
green scan means something; they then run on every deployment file and on the rendered chart."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from tests.unit.test_chart_skeleton import ALL, CHART, MIN, needs_helm

ROOT = Path(__file__).resolve().parents[2]
SOCKET_WORDS = re.compile(
    r"docker\.sock|podman\.sock|containerd\.sock|crio\.sock|cri-dockerd|/var/run/docker|/run/docker|/run/podman|"
    r"/var/run/podman|DOCKER_HOST\s*[:=]\s*[\"']?(unix|tcp)://",
    re.IGNORECASE,
)
PRIVILEGE_WORDS = re.compile(
    r"privileged\s*:\s*[\"']?(true|yes|on|1)\b|hostPID\s*:\s*[\"']?true|hostIPC\s*:\s*[\"']?true|"
    r"hostNetwork\s*:\s*[\"']?true|network_mode\s*:\s*[\"']?host|\bpid\s*:\s*[\"']?host|\bipc\s*:\s*[\"']?host|"
    r"allowPrivilegeEscalation\s*:\s*[\"']?true|--privileged|seccomp[=:]\s*unconfined|apparmor[=:]\s*unconfined",
    re.IGNORECASE,
)
SOCKET_PATH = re.compile(r"(\.sock\b|^/var/run(/|$)|^/run(/|$))")


class ComposeLoader(yaml.SafeLoader):
    """SafeLoader that reads Compose's merge tags (``!override``, ``!reset``) as the plain value they wrap."""


def _plain(loader: yaml.SafeLoader, tag_suffix: str, node: yaml.Node) -> Any:
    if isinstance(node, yaml.SequenceNode):
        return loader.construct_sequence(node, deep=True)
    if isinstance(node, yaml.MappingNode):
        return loader.construct_mapping(node, deep=True)
    return loader.construct_scalar(node)  # type: ignore[arg-type]


ComposeLoader.add_multi_constructor("!", _plain)


def load_compose(text: str) -> dict[str, Any]:
    return yaml.load(text, Loader=ComposeLoader) or {}  # noqa: S506 - a safe loader subclass


def text_violations(text: str) -> list[str]:
    return [m.group(0) for m in (*SOCKET_WORDS.finditer(text), *PRIVILEGE_WORDS.finditer(text))]


def compose_violations(doc: dict[str, Any]) -> list[str]:
    found: list[str] = []
    for name, service in (doc.get("services") or {}).items():
        if service.get("privileged"):
            found.append(f"{name}: privileged")
        if service.get("network_mode") == "host" or service.get("pid") == "host" or service.get("ipc") == "host":
            found.append(f"{name}: host namespace")
        if service.get("cap_add"):
            found.append(f"{name}: cap_add")
        if service.get("devices"):
            found.append(f"{name}: devices")
        if any("unconfined" in str(o) for o in service.get("security_opt", [])):
            found.append(f"{name}: unconfined")
        for volume in service.get("volumes", []):
            source = volume.get("source", "") if isinstance(volume, dict) else str(volume).split(":", 1)[0]
            if SOCKET_PATH.search(str(source)):
                found.append(f"{name}: mounts {source}")
    return found


def pod_violations(spec: dict[str, Any], where: str = "pod") -> list[str]:
    found: list[str] = []
    for flag in ("hostNetwork", "hostPID", "hostIPC"):
        if spec.get(flag):
            found.append(f"{where}: {flag}")
    for volume in spec.get("volumes", []):
        if "hostPath" in volume:
            found.append(f"{where}: hostPath {volume['hostPath'].get('path')}")
    for container in [*spec.get("containers", []), *spec.get("initContainers", [])]:
        security = container.get("securityContext") or {}
        if security.get("privileged") or security.get("allowPrivilegeEscalation") is True:
            found.append(f"{where}/{container.get('name')}: privileged")
        if (security.get("capabilities") or {}).get("add"):
            found.append(f"{where}/{container.get('name')}: capabilities.add")
    return found


# --------------------------------------------------------------------------- the detectors themselves


@pytest.mark.parametrize(
    "bad",
    [
        "- /var/run/docker.sock:/var/run/docker.sock", "path: /run/containerd/containerd.sock", "privileged: true",
        "privileged: 'true'", "    privileged:  yes", "hostNetwork: true", "hostPID: true", "network_mode: host",
        "pid: host", "allowPrivilegeEscalation: true", "docker run --privileged", "DOCKER_HOST=unix:///var/run/x",
        "security_opt: [seccomp=unconfined]", "path: /var/run/podman/podman.sock",
    ],
)
def test_the_text_detector_flags_every_known_way_of_handing_over_the_host(bad: str) -> None:
    assert text_violations(bad)


@pytest.mark.parametrize(
    "fine",
    ["privileged: false", "hostNetwork: false", "allowPrivilegeEscalation: false", "network_mode: bridge",
     "image: python:3.12", "# the sandbox never mounts a thing", "readOnlyRootFilesystem: true"],
)
def test_the_text_detector_does_not_flag_ordinary_settings(fine: str) -> None:
    assert not text_violations(fine)


def test_the_compose_detector_flags_privileged_host_namespaces_sockets_and_extra_capabilities() -> None:
    bad = yaml.safe_load("""
services:
  a: {image: x, privileged: true}
  b: {image: x, network_mode: host}
  c: {image: x, volumes: ["/var/run/docker.sock:/var/run/docker.sock"]}
  d: {image: x, cap_add: [SYS_ADMIN]}
  e: {image: x, devices: ["/dev/kvm"]}
  f: {image: x, volumes: [{type: bind, source: /run/podman/podman.sock, target: /s}]}
  g: {image: x, pid: host}
  h: {image: x, security_opt: ["seccomp=unconfined"]}
""")
    assert len(compose_violations(bad)) == 8
    assert not compose_violations(yaml.safe_load("services: {a: {image: x, volumes: ['data:/var/lib/x']}}"))


def test_the_pod_detector_flags_privileged_host_namespaces_and_host_paths() -> None:
    bad = {
        "hostNetwork": True, "hostPID": True, "volumes": [{"name": "s", "hostPath": {"path": "/var/run/docker.sock"}}],
        "containers": [{"name": "c", "securityContext": {"privileged": True}},
                       {"name": "d", "securityContext": {"capabilities": {"add": ["SYS_ADMIN"]}}},
                       {"name": "e", "securityContext": {"allowPrivilegeEscalation": True}}],
    }
    assert len(pod_violations(bad)) == 6
    assert not pod_violations({"containers": [{"name": "c", "securityContext": {"allowPrivilegeEscalation": False}}]})


# --------------------------------------------------------------------------- the repository's own files (Req 6.8)

DEPLOYMENT_FILES = sorted(
    p
    for pattern in ("deploy/**/*", "tests/k8s/**/*", "tests/integration/e2e/**/*", ".github/workflows/*")
    for p in ROOT.glob(pattern)
    if p.is_file() and p.suffix in {".yml", ".yaml", ".tpl", ".Dockerfile", ".json"}
)
COMPOSE_FILES = [p for p in DEPLOYMENT_FILES if p.name.startswith("docker-compose")]


def test_the_scan_covers_the_files_it_is_meant_to() -> None:
    names = {p.name for p in DEPLOYMENT_FILES}
    assert {"docker-compose.yml", "docker-compose.e2e.yml", "workers.yaml", "values.yaml", "ci.yml"} <= names
    assert len(COMPOSE_FILES) >= 4


@pytest.mark.parametrize("path", DEPLOYMENT_FILES, ids=lambda p: str(p.relative_to(ROOT)))
def test_no_deployment_file_names_a_container_runtime_socket_or_grants_privilege(path: Path) -> None:
    assert text_violations(path.read_text(encoding="utf-8")) == []


@pytest.mark.parametrize("path", COMPOSE_FILES, ids=lambda p: str(p.relative_to(ROOT)))
def test_no_compose_service_is_privileged_shares_a_host_namespace_or_mounts_a_socket(path: Path) -> None:
    assert compose_violations(load_compose(path.read_text(encoding="utf-8"))) == []


def test_a_compose_override_with_a_merge_tag_is_still_scanned_not_skipped() -> None:
    doc = load_compose("services:\n  a:\n    volumes: !override\n      - /var/run/docker.sock:/s\n")
    assert compose_violations(doc) == ["a: mounts /var/run/docker.sock"]


def test_no_dockerfile_asks_for_a_privileged_runtime_or_the_socket() -> None:
    dockerfiles = sorted((ROOT / "deploy" / "docker").glob("*.Dockerfile"))
    assert dockerfiles
    for path in dockerfiles:
        assert text_violations(path.read_text(encoding="utf-8")) == [], path.name


# --------------------------------------------------------------------------- the rendered chart (Req 6.8)


def rendered(values: list[Path], sets: list[str] | None = None) -> list[dict[str, Any]]:
    cmd = ["helm", "template", "release", str(CHART)]
    for v in values:
        cmd += ["-f", str(v)]
    for s in sets or []:
        cmd += ["--set", s]
    out = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    assert out.returncode == 0, out.stderr
    return [d for d in yaml.safe_load_all(out.stdout) if d]


def pod_specs(docs: list[dict[str, Any]]) -> list[tuple[str, dict[str, Any]]]:
    specs = []
    for d in docs:
        template = (d.get("spec") or {}).get("template")
        if d["kind"] in {"Deployment", "StatefulSet", "DaemonSet", "Job"} and template:
            specs.append((f"{d['kind']}/{d['metadata']['name']}", template["spec"]))
    return specs


@needs_helm
@pytest.mark.parametrize("values", [[MIN], [ALL]], ids=["min", "all"])
def test_no_rendered_workload_is_privileged_or_mounts_a_host_path(values: list[Path]) -> None:
    docs = rendered(values)
    specs = pod_specs(docs)
    assert specs, "the chart renders workloads"
    for where, spec in specs:
        assert pod_violations(spec, where) == []
    assert text_violations(yaml.safe_dump_all(docs)) == []


def test_the_values_files_have_no_key_that_would_turn_privilege_on() -> None:
    key = re.compile(r"^\s*(privileged|hostNetwork|hostPID|hostIPC|hostPath)\s*:", re.MULTILINE)
    for path in (CHART / "values.yaml", MIN, ALL):
        assert not key.search(path.read_text(encoding="utf-8")), path.name
