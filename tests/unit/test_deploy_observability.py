"""Static smoke tests for the `observability` Compose profile and its docs.

Docker-free: parse deploy/docker-compose.yml, deploy/prometheus/prometheus.yml,
the provisioned Grafana dashboard and README.md, and scan the repository for
Kubernetes manifests or Helm charts.

Validates: Requirements 18.1, 18.2, 18.3, 18.5, 18.6, 18.7, 18.9, 20.6
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
import yaml

from das.observability.metrics import CATALOG_BY_NAME

ROOT = Path(__file__).resolve().parents[2]
DEPLOY = ROOT / "deploy"
DASHBOARD = DEPLOY / "grafana" / "dashboards" / "das-overview.json"

OBSERVABILITY_SERVICES = {"prometheus", "grafana", "jaeger"}

#: v1 default stack (v1 Req 14.1): service name -> replica count.
V1_DEFAULT_SERVICES = {
    "rabbitmq": 1,
    "redis": 1,
    "gateway": 1,
    "manager": 2,
    "worker-research": 1,
    "worker-review": 1,
}

HISTOGRAM_SUFFIXES = ("_bucket", "_sum", "_count")


@pytest.fixture(scope="module")
def compose() -> dict:
    return yaml.safe_load((DEPLOY / "docker-compose.yml").read_text())


@pytest.fixture(scope="module")
def prometheus() -> dict:
    return yaml.safe_load((DEPLOY / "prometheus" / "prometheus.yml").read_text())


@pytest.fixture(scope="module")
def dashboard() -> dict:
    return json.loads(DASHBOARD.read_text())


@pytest.fixture(scope="module")
def readme() -> str:
    return (ROOT / "README.md").read_text()


def _profile_services(compose: dict, profile: str) -> dict[str, dict]:
    return {
        name: svc
        for name, svc in compose["services"].items()
        if profile in (svc.get("profiles") or [])
    }


def _exprs(dashboard: dict) -> list[str]:
    return [
        t["expr"]
        for panel in dashboard["panels"]
        for t in panel.get("targets", [])
        if t.get("expr")
    ]


# ---------------------------------------------------------------------------
# Requirement 18.1: exactly three pinned containers in the observability profile
# ---------------------------------------------------------------------------

_EXACT_TAG = re.compile(r"^v?\d+\.\d+\.\d+$")


def test_observability_profile_has_exactly_three_services(compose: dict) -> None:
    services = _profile_services(compose, "observability")
    assert set(services) == OBSERVABILITY_SERVICES
    for svc in services.values():
        assert svc["profiles"] == ["observability"]


@pytest.mark.parametrize("name", sorted(OBSERVABILITY_SERVICES))
def test_observability_images_pinned_to_exact_version(compose: dict, name: str) -> None:
    image = compose["services"][name]["image"]
    assert "${" not in image, f"{name} image tag must not be interpolated: {image}"
    repo, sep, tag = image.rpartition(":")
    assert sep and repo and "/" not in tag, f"{name} image has no tag: {image}"
    assert tag != "latest"
    assert _EXACT_TAG.match(tag), f"{name} tag {tag!r} is not an exact major.minor.patch version"


@pytest.mark.parametrize("tag", ["latest", "3", "v3", "3.15", "13.2-ubuntu"])
def test_exact_tag_pattern_rejects_loose_tags(tag: str) -> None:
    assert not _EXACT_TAG.match(tag)


def test_trace_backend_receives_otlp_grpc_and_http(compose: dict) -> None:
    """Jaeger v2 all-in-one serves OTLP on 4317 (grpc) and 4318 (http/protobuf)."""
    image = compose["services"]["jaeger"]["image"]
    assert image.startswith("jaegertracing/jaeger:2.")


# ---------------------------------------------------------------------------
# Requirement 18.2: default service set is unchanged from v1
# ---------------------------------------------------------------------------


def test_default_services_match_v1(compose: dict) -> None:
    default = {n: s for n, s in compose["services"].items() if not s.get("profiles")}
    replicas = {n: int(s.get("deploy", {}).get("replicas", 1)) for n, s in default.items()}
    assert replicas == V1_DEFAULT_SERVICES


def test_default_services_do_not_depend_on_observability(compose: dict) -> None:
    for name in V1_DEFAULT_SERVICES:
        deps = compose["services"][name].get("depends_on") or {}
        assert not (set(deps) & OBSERVABILITY_SERVICES), f"{name} depends on {deps}"


# ---------------------------------------------------------------------------
# Requirement 18.7: Grafana credentials from deploy/.env, no anonymous, 127.0.0.1 ports
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(OBSERVABILITY_SERVICES))
def test_observability_ports_bind_loopback(compose: dict, name: str) -> None:
    ports = compose["services"][name].get("ports") or []
    for port in ports:
        assert isinstance(port, str) and port.startswith("127.0.0.1:"), f"{name}: {port!r}"


def test_otlp_ports_not_published(compose: dict) -> None:
    ports = " ".join(compose["services"]["jaeger"].get("ports") or [])
    assert "4317" not in ports and "4318" not in ports


def test_grafana_credentials_from_env_and_anonymous_disabled(compose: dict) -> None:
    env = compose["services"]["grafana"]["environment"]
    assert "${GRAFANA_ADMIN_USER" in env["GF_SECURITY_ADMIN_USER"]
    assert "${GRAFANA_ADMIN_PASSWORD" in env["GF_SECURITY_ADMIN_PASSWORD"]
    assert str(env["GF_AUTH_ANONYMOUS_ENABLED"]).lower() == "false"
    example = (DEPLOY / ".env.example").read_text()
    assert "GRAFANA_ADMIN_USER" in example and "GRAFANA_ADMIN_PASSWORD" in example


# ---------------------------------------------------------------------------
# Requirement 18.3: Prometheus scrape config
# ---------------------------------------------------------------------------


def _seconds(value: str) -> float:
    match = re.fullmatch(r"(\d+)(ms|s|m)", value)
    assert match, f"unexpected duration {value!r}"
    return int(match[1]) * {"ms": 0.001, "s": 1, "m": 60}[match[2]]


def test_prometheus_interval_and_timeout(prometheus: dict) -> None:
    glob = prometheus["global"]
    assert _seconds(glob["scrape_interval"]) == 15
    assert _seconds(glob["scrape_timeout"]) <= 10
    for job in prometheus["scrape_configs"]:
        if "scrape_interval" in job:
            assert _seconds(job["scrape_interval"]) == 15
        if "scrape_timeout" in job:
            assert _seconds(job["scrape_timeout"]) <= 10


def test_prometheus_discovers_every_agent_service_via_dns(
    prometheus: dict, compose: dict
) -> None:
    names: set[str] = set()
    for job in prometheus["scrape_configs"]:
        assert job.get("metrics_path", "/metrics") == "/metrics"
        assert "static_configs" not in job, f"{job['job_name']} must use dns_sd_configs"
        for sd in job["dns_sd_configs"]:
            assert sd["type"] == "A"
            assert sd["port"] == 8081
            names.update(sd["names"])
    agents = {n for n in V1_DEFAULT_SERVICES if n not in {"rabbitmq", "redis"}}
    assert names == agents
    assert agents <= set(compose["services"])


# ---------------------------------------------------------------------------
# Requirements 18.5, 18.6: provisioned dashboard panels and metric names
# ---------------------------------------------------------------------------

#: Req 18.5 panel -> metric its query must use.
REQUIRED_PANEL_METRICS = {
    "request rate by status": "das_requests_finished_total",
    "request duration p50/p95": "das_request_duration_seconds_bucket",
    "job transitions": "das_job_transitions_total",
    "job attempts": "das_job_attempts",
    "queue depth": "das_queue_depth",
    "llm call rate": "das_llm_calls_total",
    "llm duration p95": "das_llm_call_duration_seconds_bucket",
    "token rate": "das_llm_tokens_total",
    "cost rate": "das_llm_estimated_cost_total",
    "lease acquisitions": "das_lease_acquisitions_total",
    "lease losses": "das_lease_losses_total",
    "registry agents": "das_registry_agents",
    "dlq rate": "das_dlq_records_created_total",
}


def _base_metric(token: str) -> str:
    if token in CATALOG_BY_NAME:
        return token
    for suffix in HISTOGRAM_SUFFIXES:
        base = token.removesuffix(suffix)
        if base != token and CATALOG_BY_NAME.get(base) and CATALOG_BY_NAME[base].kind == "histogram":
            return base
    return token


def test_dashboard_has_thirteen_panels(dashboard: dict) -> None:
    panels = [p for p in dashboard["panels"] if p.get("type") != "row"]
    assert len(panels) == 13
    assert all(p.get("targets") for p in panels)


def test_dashboard_panels_cover_requirement_18_5(dashboard: dict) -> None:
    exprs = _exprs(dashboard)
    for panel, metric in REQUIRED_PANEL_METRICS.items():
        pattern = rf"\b{metric}(_bucket|_sum|_count)?\b"
        assert any(re.search(pattern, e) for e in exprs), f"no query for {panel}"
    assert any(
        'outcome!="success"' in e and e.count("das_llm_calls_total") == 2 for e in exprs
    ), "LLM error ratio panel missing"
    for e in exprs:
        if "rate(" in e:
            assert re.findall(r"\[(\w+)\]", e) and set(re.findall(r"\[(\w+)\]", e)) == {"5m"}, e


def test_dashboard_metric_names_are_in_catalog(dashboard: dict) -> None:
    exprs = _exprs(dashboard)
    assert exprs
    for expr in exprs:
        tokens = re.findall(r"\bdas_[a-z0-9_]+\b", expr)
        assert tokens, f"query references no DAS metric: {expr}"
        unknown = {t for t in tokens if _base_metric(t) not in CATALOG_BY_NAME}
        assert not unknown, f"{expr}: unknown metrics {unknown}"


def test_suffix_stripping_only_applies_to_histograms() -> None:
    assert _base_metric("das_request_duration_seconds_bucket") == "das_request_duration_seconds"
    assert _base_metric("das_job_attempts_count") == "das_job_attempts"
    # A counter's _count suffix is not a valid series name.
    assert _base_metric("das_llm_calls_total_count") not in CATALOG_BY_NAME
    assert _base_metric("das_not_a_metric") not in CATALOG_BY_NAME


def test_grafana_datasources_and_dashboard_provider() -> None:
    prov = DEPLOY / "grafana" / "provisioning"
    ds = yaml.safe_load((prov / "datasources" / "das.yaml").read_text())
    assert {d["type"] for d in ds["datasources"]} == {"prometheus", "jaeger"}
    providers = yaml.safe_load((prov / "dashboards" / "das.yaml").read_text())["providers"]
    assert any(p["options"]["path"] == "/var/lib/grafana/dashboards" for p in providers)


# ---------------------------------------------------------------------------
# Requirement 18.9: README documents the observability profile and operations
# ---------------------------------------------------------------------------

README_CHECKS: dict[str, list[str]] = {
    "start observability profile": [r"--profile\s+observability\b[^\n]*\bup\b"],
    "stop observability profile": [r"--profile\s+observability\b[^\n]*\b(down|stop)\b"],
    "Prometheus address": [r"Prometheus", r"(127\.0\.0\.1|localhost):9090"],
    "Grafana address": [r"Grafana", r"(127\.0\.0\.1|localhost):3000"],
    "Jaeger address": [r"Jaeger", r"(127\.0\.0\.1|localhost):16686"],
    "Grafana env vars": [r"deploy/\.env", r"GRAFANA_ADMIN_USER", r"GRAFANA_ADMIN_PASSWORD"],
    "tracing env vars": [r"DAS__TRACING__ENABLED"],
    "Ops_Port endpoints": [r"/metrics\b", r"/livez\b", r"/readyz\b", r"8081|ops[_ ]port"],
    "Admin_API": [r"/v1/admin/", r"DAS__ADMIN_API_KEYS|admin_api_keys"],
    "CLI drain": [r"\bdas\s+drain\b"],
    "Retention_Policy": [r"\bretention\b"],
    "Price_Table": [r"\bpricing\b", r"\bmodels\b"],
}


@pytest.mark.parametrize("section", sorted(README_CHECKS))
def test_readme_documents_requirement_18_9(readme: str, section: str) -> None:
    missing = [p for p in README_CHECKS[section] if not re.search(p, readme, re.IGNORECASE)]
    assert not missing, f"README.md lacks {section}: no match for {missing}"


# ---------------------------------------------------------------------------
# Requirement 20.6: Compose is the only deployment artifact
#
# SUPERSEDED by das-multi-tenant Req 10 and 11.4 (Phase 5): the repository now also carries a Helm chart. The tests
# below keep the rule that matters: Docker Compose stays the baseline, and Kubernetes artifacts exist only in their
# sanctioned places (deploy/helm/ for the chart, tests/k8s/ for the cluster test manifests), never scattered.
# ---------------------------------------------------------------------------

_SKIP_DIRS = {
    ".git", ".venv", "venv", "node_modules", ".hypothesis", ".pytest_cache",
    ".ruff_cache", ".mypy_cache", ".import_linter_cache", ".codegraph", "__pycache__",
    ".kiro", "build", "dist",
}
_K8S_KINDS = {
    "Deployment", "StatefulSet", "DaemonSet", "Service", "Pod", "ConfigMap", "Secret",
    "Ingress", "Job", "CronJob", "Namespace", "PersistentVolumeClaim", "HelmRelease",
    "Kustomization", "ReplicaSet", "ServiceAccount", "Role", "RoleBinding",
}


def _repo_files() -> list[Path]:
    out: list[Path] = []
    stack = [ROOT]
    while stack:
        d = stack.pop()
        for p in d.iterdir():
            if p.is_dir():
                if p.name not in _SKIP_DIRS and not p.name.endswith(".egg-info"):
                    stack.append(p)
            else:
                out.append(p)
    return out


def _is_k8s_manifest(path: Path) -> bool:
    try:
        docs = list(yaml.safe_load_all(path.read_text()))
    except (yaml.YAMLError, UnicodeDecodeError):
        return False
    return any(
        isinstance(doc, dict)
        and isinstance(doc.get("apiVersion"), str)
        and doc.get("kind") in _K8S_KINDS
        for doc in docs
    )


#: Where Kubernetes artifacts may live (das-multi-tenant Req 11.4). ``tests/integration/k8s`` holds Python tests only.
_CHART_DIR = "deploy/helm/"
_MANIFEST_DIRS = (_CHART_DIR, "tests/k8s/")
_K8S_DIR_NAMES = {"k8s", "kubernetes", "helm", "charts"}


def _relative(p: Path) -> str:
    return p.relative_to(ROOT).as_posix()


def test_docker_compose_is_still_the_baseline_deployment() -> None:
    compose = DEPLOY / "docker-compose.yml"
    assert compose.is_file()
    services = yaml.safe_load(compose.read_text())["services"]
    assert {"gateway", "manager", "rabbitmq", "redis"} <= set(services)


def test_kubernetes_artifacts_exist_only_in_their_sanctioned_places() -> None:
    files = _repo_files()
    helm_names = {"Chart.yaml", "Chart.yml", "values.yaml", "kustomization.yaml", "helmfile.yaml"}
    helm = [
        p for p in files
        if (p.name in helm_names or p.suffix == ".tpl") and not _relative(p).startswith(_CHART_DIR)
    ]
    assert not helm, f"Helm/Kustomize artifacts outside {_CHART_DIR}: {helm}"
    manifests = [
        p for p in files
        if p.suffix in {".yaml", ".yml"} and _is_k8s_manifest(p) and not _relative(p).startswith(_MANIFEST_DIRS)
    ]
    assert not manifests, f"Kubernetes manifests outside {_MANIFEST_DIRS}: {manifests}"
    allowed_dirs = (*_MANIFEST_DIRS, "tests/integration/k8s/")
    stray = sorted(
        {
            _relative(p.parent)
            for p in files
            if _K8S_DIR_NAMES & {part.lower() for part in p.relative_to(ROOT).parts[:-1]}
            and not _relative(p).startswith(allowed_dirs)
        }
    )
    assert not stray, f"directories named like Kubernetes artifacts outside {allowed_dirs}: {stray}"


def test_the_chart_is_where_the_rule_expects_it() -> None:
    assert (DEPLOY / "helm" / "das" / "Chart.yaml").is_file()


def test_k8s_detector_flags_a_manifest(tmp_path: Path) -> None:
    manifest = tmp_path / "deploy.yaml"
    manifest.write_text("apiVersion: apps/v1\nkind: Deployment\nmetadata: {name: x}\n")
    assert _is_k8s_manifest(manifest)
    # Grafana provisioning files carry apiVersion but no Kubernetes kind.
    assert not _is_k8s_manifest(DEPLOY / "grafana" / "provisioning" / "datasources" / "das.yaml")
