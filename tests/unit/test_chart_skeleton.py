"""The Helm chart skeleton (das-multi-tenant task 10.1; Phase 5 Req 10.4, 10.7).

Values, the strict values schema, the CI values files and the grace-period guard. The workload templates are task 10.2,
so these tests render nothing yet except the guard itself. They run the real ``helm`` binary and skip when it is not
installed (CI installs a pinned one, task 11.2)."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

CHART = Path(__file__).resolve().parents[2] / "deploy" / "helm" / "das"
MIN = CHART / "ci" / "values-min.yaml"
ALL = CHART / "ci" / "values-all.yaml"

#: The chart tests render with the real ``helm``. They are skipped on a machine without it, but not in CI (GitHub sets
#: ``CI=true``): there a missing binary must fail, or the chart would go untested without anyone noticing.
IN_CI = os.environ.get("CI", "").lower() == "true"
needs_helm = pytest.mark.skipif(
    shutil.which("helm") is None and not IN_CI, reason="the helm binary is not installed (required when CI=true)"
)


def test_helm_is_installed_when_running_in_ci() -> None:
    if not IN_CI:
        pytest.skip("only checked when CI=true")
    assert shutil.which("helm"), "CI=true but the helm binary is missing: the chart tests would all fail or skip"


def helm(
    *args: str,
    values: list[Path] | None = None,
    extra_values: dict[str, Any] | None = None,
    tmp: Path | None = None,
) -> subprocess.CompletedProcess[str]:
    cmd = ["helm", *args, str(CHART)]
    for v in values or []:
        cmd += ["-f", str(v)]
    if extra_values is not None:
        assert tmp is not None
        path = tmp / "extra.yaml"
        path.write_text(yaml.safe_dump(extra_values))
        cmd += ["-f", str(path)]
    return subprocess.run(cmd, capture_output=True, text=True, timeout=120)


def load(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- files


@pytest.mark.parametrize(
    "relative",
    [
        "Chart.yaml",
        "values.yaml",
        "values.schema.json",
        "templates/_helpers.tpl",
        "templates/validate.yaml",
        "ci/values-min.yaml",
        "ci/values-all.yaml",
    ],
)
def test_the_skeleton_files_exist(relative: str) -> None:
    assert (CHART / relative).is_file(), relative


def test_chart_yaml_describes_an_application_chart() -> None:
    chart = load(CHART / "Chart.yaml")
    assert chart["apiVersion"] == "v2" and chart["name"] == "das" and chart["type"] == "application"
    assert re.fullmatch(r"\d+\.\d+\.\d+", chart["version"])
    assert chart["appVersion"] and chart["description"]


# --------------------------------------------------------------------------- the values and their schema


def test_the_defaults_leave_every_optional_feature_off_and_the_ci_files_agree() -> None:
    default, everything = load(CHART / "values.yaml"), load(ALL)
    assert default["keda"]["enabled"] is False and default["lifecycle"]["preStopDrain"]["enabled"] is False
    assert everything["keda"]["enabled"] is True and everything["lifecycle"]["preStopDrain"]["enabled"] is True
    assert len(everything["workers"]) >= 2  # the range over workers is exercised with more than one entry


def test_the_defaults_carry_the_documented_numbers() -> None:
    v = load(CHART / "values.yaml")
    assert v["terminationGracePeriodSeconds"] == 45
    assert v["gateway"]["replicas"] == 2 and v["manager"]["replicas"] == 2
    assert v["config"]["common"]["limits"]["shutdown_timeout"] == 30
    assert v["existingSecret"] == ""  # no default: the operator must name a Secret


def test_the_schema_is_strict_everywhere() -> None:
    schema = json.loads((CHART / "values.schema.json").read_text(encoding="utf-8"))
    assert schema["$schema"].startswith("http://json-schema.org/") and schema["type"] == "object"
    assert schema["additionalProperties"] is False and "existingSecret" in schema["required"]
    for name in ("image", "gateway", "manager", "lifecycle", "keda", "secretEnv", "rabbitmq", "redis"):
        assert schema["properties"][name].get("additionalProperties") is False, name
    assert schema["properties"]["workers"]["items"]["additionalProperties"] is False
    assert schema["properties"]["workers"]["items"]["required"] == ["name", "capabilities"]


@needs_helm
def test_default_values_are_refused_until_a_secret_is_named() -> None:
    result = helm("lint")
    assert result.returncode != 0 and "existingSecret" in result.stdout + result.stderr


@needs_helm
@pytest.mark.parametrize("values", [MIN, ALL], ids=["min", "all"])
def test_the_ci_values_files_lint_and_template(values: Path) -> None:
    assert helm("lint", values=[values]).returncode == 0
    assert helm("template", "release", values=[values]).returncode == 0


BAD_VALUES = {
    "unknown top-level key": {"surprise": 1},
    "unknown nested key": {"gateway": {"surprise": 1}},
    "replicas as text": {"gateway": {"replicas": "two"}},
    "negative replicas": {"manager": {"replicas": -1}},
    "worker without capabilities": {"workers": [{"name": "w"}]},
    "worker with empty capabilities": {"workers": [{"name": "w", "capabilities": []}]},
    "grace period of zero": {"terminationGracePeriodSeconds": 0},
    "keda queue length zero": {"keda": {"queueLength": 0}},
    "secret name as number": {"existingSecret": 5},
}


@needs_helm
@pytest.mark.parametrize("bad", BAD_VALUES.values(), ids=BAD_VALUES.keys())
def test_the_schema_rejects_bad_values(bad: dict[str, Any], tmp_path: Path) -> None:
    result = helm("lint", values=[MIN], extra_values=bad, tmp=tmp_path)
    assert result.returncode != 0, result.stdout


# --------------------------------------------------------------------------- the grace-period guard


@needs_helm
@pytest.mark.parametrize(
    ("grace", "shutdown", "accepted"),
    [(45, 30, True), (40, 30, True), (39, 30, False), (45, 60, False), (70, 60, True), (69, 60, False)],
)
def test_the_grace_period_must_cover_the_shutdown_timeout_plus_ten_seconds(
    grace: int, shutdown: int, accepted: bool, tmp_path: Path
) -> None:
    extra = {
        "terminationGracePeriodSeconds": grace,
        "config": {"common": {"limits": {"shutdown_timeout": shutdown}}},
    }
    result = helm("template", "release", values=[MIN], extra_values=extra, tmp=tmp_path)
    if accepted:
        assert result.returncode == 0, result.stderr
    else:
        assert result.returncode != 0
        message = result.stderr + result.stdout
        assert "terminationGracePeriodSeconds" in message and "shutdown_timeout" in message
