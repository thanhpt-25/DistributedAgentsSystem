"""The README documents Phase 5, and what it says is true (das-multi-tenant task 13.1).

Phase 5 Req 3.8, 6.7, 7.6, 9.3, 11.6 and 12.2.

A test that only looks for mentions cannot tell a right statement from a wrong one, so this file checks what can be
checked mechanically: the Scopes table says exactly what ``ROUTE_SCOPES`` says, the settings table carries the real
defaults, the tenants example loads through the real config loader with the real fingerprints, every error code and
state name the prose relies on is mentioned, the upgrade steps come in the right order, the job names for branch
protection are the workflow's, and ``CLAUDE.md`` names the files and the CI project of the tenancy e2e. Whether the
prose around them is right is checked by reading the code and by running the e2e stack (see the plan), not here."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from das.config import load_config
from das.config.schema import LabelLimitsConfig, TenantConfig, UsageConfig
from das.ops.keys import key_fingerprint
from das.tenancy.scopes import PUBLIC, ROUTE_SCOPES, Scope
from tests.unit.test_readme_client_api import README, blocks, mentioned, section

ROOT = Path(__file__).resolve().parents[2]
CLAUDE = (ROOT / "CLAUDE.md").read_text(encoding="utf-8")
CI_TEXT = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
CI = yaml.safe_load(CI_TEXT)
#: The keys behind the fingerprints of the README's tenants example (placeholders, like ``change-me-api-key``).
EXAMPLE_KEYS = ("change-me-api-key", "change-me-team-a-reader", "change-me-team-b-key")
ENV = {
    "DAS__REDIS__URL": "redis://redis:6379/0",
    "DAS__GRPC__ADVERTISE_HOST": "gateway",
    "DAS__API_KEYS": ",".join(EXAMPLE_KEYS),
}
ROUTE = re.compile(r"`(GET|POST) (/[^`\s]+)`")
SETTING_ROW = re.compile(r"^\| `(?P<path>[\w.\[\]]+)` \| (?P<default>[^|]*) \|", re.MULTILINE)


def normalized(path: str) -> str:
    return re.sub(r"\{\w+\}", "{id}", path)


# --------------------------------------------------------------------------- the sections exist


@pytest.mark.parametrize(
    "heading",
    [
        "Tenants and scopes", "Scopes", "What a tenant sees", "Tenant settings", "Rotating a key", "Priority",
        "Usage reports", "Kubernetes (Helm chart)", "Upgrading to Phase 5", "Not in Phase 5",
    ],
)  # fmt: skip
def test_the_section_exists_and_says_something(heading: str) -> None:
    assert len(section(heading).split()) > 20, f"README has no `{heading}` section, or it is empty"


# --------------------------------------------------------------------------- scopes


def test_the_scopes_table_is_exactly_the_route_table() -> None:
    """Every authenticated route is in the row of the Scope (or role) it needs, and no other route is."""
    expected: dict[str, set[tuple[str, str]]] = {}
    for (method, path), requirement in ROUTE_SCOPES.items():
        if requirement != PUBLIC:
            expected.setdefault(str(requirement), set()).add((method, normalized(path)))
    rows: dict[str, set[tuple[str, str]]] = {}
    for line in section("Scopes").splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if line.startswith("| `") or line.startswith("| Operator"):
            rows[cells[0].strip("`").lower()] = {(m, normalized(p)) for m, p in ROUTE.findall(cells[1])}
    assert rows == expected


def test_every_scope_and_the_operator_role_is_named() -> None:
    for scope in Scope:
        assert mentioned(scope.value), f"README never mentions the Scope `{scope.value}`"
    assert "Operator" in section("Scopes")


@pytest.mark.parametrize(
    "token",
    ["forbidden", "admin_disabled", "not_found", "invalid_query", "index_not_ready", "quota_exceeded",
     "priority_above_limit", "invalid_request", "limit_scope"],
)  # fmt: skip
def test_every_error_the_tenancy_prose_relies_on_is_mentioned(token: str) -> None:
    assert token in README, f"README never mentions `{token}`"


def test_the_cross_tenant_answers_are_described_as_the_ones_of_a_missing_resource() -> None:
    sees = section("What a tenant sees")
    assert '404 {"error": "not_found"}' in sees and "exactly as a Request that does not exist" in sees
    assert '"parameter": "cursor"' in sees and '"parameter": "tenant"' in sees
    assert 'limit_scope": "tenant"' in sees


# --------------------------------------------------------------------------- settings and the example


def test_the_tenant_settings_table_has_the_real_defaults() -> None:
    rows = {m["path"]: m["default"].strip() for m in SETTING_ROW.finditer(section("Tenant settings"))}
    tenant = TenantConfig.model_validate({"id": "x", "keys": [{"fingerprint": "00000000", "scopes": ["read"]}]})
    assert rows["tenants[].max_active_requests"] == str(tenant.max_active_requests)
    assert rows["tenants[].default_priority"] == f"`{tenant.default_priority}`"
    assert rows["tenants[].max_priority"] == f"`{tenant.max_priority}`"
    assert rows["usage.retention_days"] == str(UsageConfig().retention_days)
    assert rows["metrics.label_limits.tenant"] == str(LabelLimitsConfig().tenant)
    for field in ("id", "keys", "max_active_requests", "default_priority", "max_priority", "settings"):
        assert field in TenantConfig.model_fields and any(p.startswith(f"tenants[].{field}") for p in rows), field


def tenants_examples() -> list[str]:
    return [b for b in blocks("yaml") if re.search(r"^tenants:", b, re.MULTILINE)]


def test_there_is_a_tenants_example() -> None:
    assert tenants_examples(), "README shows no yaml example of `tenants`"


@pytest.mark.parametrize("example", tenants_examples() or ["tenants: []"], ids=lambda b: b.splitlines()[0])
def test_the_tenants_example_loads_through_the_config_loader_with_the_real_fingerprints(
    example: str, tmp_path: Path
) -> None:
    sample = yaml.safe_load((ROOT / "deploy" / "config" / "gateway.yaml").read_text())
    merged = {**sample, **yaml.safe_load(example)}
    path = tmp_path / "gateway.yaml"
    path.write_text(yaml.safe_dump(merged))
    config = load_config(path, ENV)  # a fingerprint that matches no configured key is refused here
    named = {k.fingerprint for t in config.tenants for k in t.keys}
    assert named == {key_fingerprint(k.encode()) for k in EXAMPLE_KEYS}
    assert {t.id for t in config.tenants} == {"team-a", "team-b"}


# --------------------------------------------------------------------------- priority, usage, upgrade


def test_priority_is_described_as_a_weighting_with_its_mechanism() -> None:
    text = section("Priority")
    assert "weighting, not a strict order" in text
    assert "about two for every `normal` one" in text and "never starved" in text  # Req 7.6
    assert "das-priority: high" in text and "AMQP priority 9" in text
    for name in ("default_priority", "max_priority", "priority_above_limit", "invalid_request"):
        assert name in text, name


def test_usage_reports_say_what_they_do_and_do_not_contain() -> None:
    text = section("Usage reports")
    assert "/v1/usage" in text and "/v1/admin/usage" in text
    assert "**Usage recorded before the upgrade is not in any report.**" in text  # Req 6.7
    assert "92 days" in text and "Price_Table" in text and "not by Prometheus" in text
    assert mentioned("das_requests_submitted_total") and mentioned("das_requests_finished_total")


def test_the_upgrade_steps_come_in_the_order_that_works() -> None:
    text = section("Upgrading to Phase 5")
    markers = ("Upgrade every Gateway", "Wait for the Tenant_Backfill", "Then add `tenants`")
    order = [text.index(marker) for marker in markers]
    assert order == sorted(order)
    assert "das:tenant:index:ready" in text and "tenant_index_backfilled" in text  # what to wait for, and its log line
    assert '503 {"error": "index_not_ready"}' in text  # what a tenant sees before the flag (Req 9.3)
    assert "usage from before the upgrade is not in any report" in text
    assert "idempotency records stay per key" in text and "no Phase 4 data" in text  # Req 3.8 and the rollback


def test_idempotency_is_per_key_and_a_rotation_starts_new_records() -> None:
    idempotency = section("Idempotent submits")
    assert "per API key, not per tenant" in idempotency
    assert "rotated key starts with no idempotency records" in idempotency
    rotating = section("Rotating a key")
    assert "overlap" in rotating and "starts with none" in rotating


def test_the_out_of_scope_list_is_the_requirements() -> None:
    text = section("Not in Phase 5")
    for phrase in (
        "Redis replication, Sentinel or Cluster", "key-management API", "per-tenant worker pools or queues",
        "per-tenant token or cost metrics in Prometheus",
    ):  # fmt: skip
        assert phrase in text, phrase


# --------------------------------------------------------------------------- Kubernetes and CI


def job_names() -> list[str]:
    names: list[str] = []
    for job in CI["jobs"].values():
        matrix = job.get("strategy", {}).get("matrix", {})
        for version in matrix.get("python", [None]):
            names.append(job["name"].replace("${{ matrix.python }}", str(version)))
    return names


def test_the_kubernetes_section_names_every_job_of_the_workflow_for_branch_protection() -> None:
    text = section("Kubernetes (Helm chart)")
    assert len(job_names()) == 8 and "Kind_Job" in job_names()
    for name in job_names():
        assert f"`{name}`" in text, f"the Kubernetes section does not name the status check `{name}`"
    assert "deploy/helm/das/README.md" in text and (ROOT / "deploy" / "helm" / "das" / "README.md").is_file()
    assert "helm install das deploy/helm/das" in text


# --------------------------------------------------------------------------- CLAUDE.md


def ci_projects() -> list[str]:
    loops = re.findall(r"for project in ([^;]+);", CI_TEXT)
    return list(dict.fromkeys(re.findall(r"das-e2e[\w-]*", " ".join(loops))))


def test_claude_md_describes_the_tenancy_e2e_and_the_architecture() -> None:
    for token in (
        "das-e2e-tenant", "test_tenancy_e2e.py", "docker-compose.tenancy.e2e.yml", "gateway.tenancy.yaml",
        "DAS_E2E_TENANT_OVERRIDE_EXTRA", "tenancy_variants", "tenancy_constants.py", "das/tenancy/",
        "das:tenant:index:ready", "das-priority: high", "das-multi-tenant",
    ):  # fmt: skip
        assert token in CLAUDE, f"CLAUDE.md never mentions `{token}`"
    assert "das-e2e-tenant" in ci_projects()
    for project in ci_projects():
        assert project in CLAUDE, f"CLAUDE.md does not name the e2e project {project} that CI tears down"


def test_the_files_claude_md_names_exist() -> None:
    for name in ("test_tenancy_e2e.py", "tenancy_variants.py", "tenancy_constants.py", "gateway.tenancy.yaml"):
        assert (ROOT / "tests" / "integration" / "e2e" / name).is_file(), name
    assert (ROOT / "tests" / "unit" / "test_e2e_tenancy_static.py").is_file()
    assert (ROOT / "das" / "tenancy").is_dir()
