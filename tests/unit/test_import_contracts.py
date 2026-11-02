"""Architecture tests for the import-linter contracts (Requirement 19.4).

Manager_Agent and Worker_Agent logic must reach metrics and tracing only
through ``das.observability``; importing ``prometheus_client`` or
``opentelemetry`` directly from ``das.manager`` / ``das.worker`` must break
the forbidden contract.

The violation tests run ``lint-imports`` against a throwaway copy of the
``das`` package plus ``pyproject.toml`` so the temporary violating module never
appears in the real source tree (other test runs may lint the repo in
parallel). The temporary copy is always removed in a ``finally`` block.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
LINT_IMPORTS = Path(sys.executable).parent / "lint-imports"

OBSERVABILITY_LIBS = {"prometheus_client", "opentelemetry"}

pytestmark = pytest.mark.skipif(
    not LINT_IMPORTS.exists(), reason="import-linter is not installed"
)


def _run_lint_imports(cwd: Path) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    # Make sure the package under ``cwd`` wins over any installed ``das``.
    env["PYTHONPATH"] = str(cwd)
    return subprocess.run(
        [str(LINT_IMPORTS), "--no-cache"],
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )


def test_contract_forbids_observability_libraries() -> None:
    config = tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["importlinter"]
    assert config.get("include_external_packages") is True
    assert any(
        c["type"] == "forbidden"
        and {"das.manager", "das.worker"} <= set(c["source_modules"])
        and OBSERVABILITY_LIBS <= set(c["forbidden_modules"])
        for c in config["contracts"]
    )


def test_lint_imports_passes_on_repo() -> None:
    result = _run_lint_imports(ROOT)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "0 broken" in result.stdout


@pytest.mark.parametrize("package", ["manager", "worker"])
@pytest.mark.parametrize(
    "import_line",
    [
        "import prometheus_client",
        "from opentelemetry import trace",
    ],
)
def test_lint_imports_fails_on_direct_observability_import(
    package: str, import_line: str
) -> None:
    workdir = Path(tempfile.mkdtemp(prefix="das-import-contract-"))
    try:
        shutil.copytree(
            ROOT / "das",
            workdir / "das",
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
        shutil.copy2(ROOT / "pyproject.toml", workdir / "pyproject.toml")
        violation = workdir / "das" / package / "_tmp_contract_violation.py"
        violation.write_text(f"{import_line}\n")

        result = _run_lint_imports(workdir)

        output = result.stdout + result.stderr
        assert result.returncode != 0, output
        assert f"das.{package}._tmp_contract_violation" in output
    finally:
        shutil.rmtree(workdir, ignore_errors=True)
    assert not workdir.exists()
