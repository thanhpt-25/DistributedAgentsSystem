"""The chart README says what the chart does, and nothing it does not (das-multi-tenant task 10.5; Phase 5 Req 10.7).

Static, no ``helm`` needed: the README is read as text and compared with ``values.yaml``, the CI values files and the
templates, so a new value, a renamed secret key or a changed default fails here until the README follows."""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from tests.unit.test_chart_skeleton import ALL, CHART, MIN

README = CHART / "README.md"
VALUES = yaml.safe_load((CHART / "values.yaml").read_text())


def text() -> str:
    return README.read_text()


def ticked(name: str) -> bool:
    """Whether the README mentions ``name`` as code, ``name`` in backticks."""
    return f"`{name}`" in text()


# --------------------------------------------------------------------------- what it must cover


def test_the_readme_exists_and_names_the_prerequisites() -> None:
    body = text()
    for fact in ("Redis", "RabbitMQ", "management plugin", "KEDA", "Helm"):
        assert fact in body, f"the README does not mention {fact}"
    # Redis is one instance for now: the README must not suggest otherwise.
    assert re.search(r"single|one instance|no replication", body, re.IGNORECASE)
    assert "not deploy" in body or "does not deploy" in body  # the chart does not run Redis or RabbitMQ


def test_every_top_level_value_is_documented() -> None:
    for key in VALUES:
        assert ticked(key), f"values.yaml has `{key}` but the README does not document it"


def test_every_secret_key_of_the_defaults_is_documented() -> None:
    for role, mapping in VALUES["secretEnv"].items():
        for env, key in mapping.items():
            assert ticked(key), f"secretEnv.{role}.{env} reads the Secret key `{key}`, which the README does not list"
            assert ticked(env), f"the README does not mention the variable `{env}`"


def test_the_optional_features_are_documented_with_their_settings() -> None:
    for group in ("keda", "lifecycle.preStopDrain"):
        node = VALUES
        for part in group.split("."):
            node = node[part]
        for key in node:
            assert ticked(f"{group}.{key}"), f"the README does not document `{group}.{key}`"


def test_the_grace_period_rule_is_stated() -> None:
    body = text()
    assert "terminationGracePeriodSeconds" in body and "shutdown_timeout" in body
    refusal = r"less than\s+`limits\.shutdown_timeout`\s+plus\s+10\s+seconds"
    assert re.search(refusal, body), "the README does not say when the chart refuses (shutdown_timeout plus 10 s)"
    assert str(VALUES["terminationGracePeriodSeconds"]) in body  # the default


def test_the_delivery_limit_caveat_is_stated() -> None:
    body = text()
    assert "delivery limit" in body.lower()
    assert "dead-letter" in body.lower()
    assert "killed" in body.lower()


def test_tenant_configuration_is_explained_with_its_one_ordering_rule() -> None:
    body = text()
    for fact in ("config.gateway.tenants", "fingerprint", "tenant_index_backfilled", "existingSecret"):
        assert fact in body, f"the tenants section does not mention {fact}"


def test_the_two_secret_guards_are_documented() -> None:
    body = text()
    assert "credentials" in body and "refuse" in body.lower()  # a secret typed into values is refused, not rendered


def test_the_keda_minimum_and_the_prestop_key_advice_are_given() -> None:
    body = text()
    assert "no capable worker" in body  # why the KEDA minimum defaults to 1
    assert re.search(r"dedicated", body), "the README does not advise a dedicated admin key for the preStop hook"


# --------------------------------------------------------------------------- what it must not do or get wrong


def test_the_readme_holds_no_credentials() -> None:
    assert not re.search(r"://[^/\s<>@]+:[^/\s<>@]+@", text()), "an example URL carries credentials"


def test_every_file_the_readme_points_at_exists() -> None:
    for target in re.findall(r"`((?:ci|templates|\.\./|tests/|deploy/)[^`\s]*\.(?:yaml|yml|tpl|md|json))`", text()):
        base = CHART if target.startswith(("ci/", "templates/", "../")) else CHART.parents[2]
        assert (base / target).exists(), f"the README points at {target}, which does not exist"


def test_the_ci_values_files_are_the_ones_it_names() -> None:
    body = text()
    for values in (MIN, ALL):
        assert f"ci/{values.name}" in body


def test_the_default_keda_minimum_the_readme_states_is_the_real_one() -> None:
    minimum = VALUES["keda"]["minReplicaCount"]
    row = rf"`keda\.minReplicaCount`[^\n]*\bDefault {minimum}\b"
    assert re.search(row, text()), f"the README's keda.minReplicaCount row does not say Default {minimum}"
    assert re.search(rf"`keda\.minReplicaCount` at {minimum} or more", text())  # the advice in prose agrees


def test_every_readme_command_names_a_real_chart_path() -> None:
    for line in text().splitlines():
        match = re.search(r"helm (?:install|upgrade|lint|template)\b.*?\s(deploy/helm/das|\.)(?:\s|$)", line)
        if match and match.group(1) != ".":
            assert Path(CHART.parents[2], match.group(1)).is_dir()
