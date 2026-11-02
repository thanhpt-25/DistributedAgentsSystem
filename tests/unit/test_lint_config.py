"""Lint_Check configuration tests (Req 5.3, 5.7, 5.8).

Ruff reads source from stdin with ``--stdin-filename`` so path-based config
(``extend-exclude`` + ``force-exclude``) applies without writing files to the repo.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


def _ruff_cmd() -> list[str]:
    venv_ruff = Path(sys.executable).parent / ("ruff.exe" if os.name == "nt" else "ruff")
    if venv_ruff.is_file():
        return [str(venv_ruff)]
    found = shutil.which("ruff")
    if found:
        return [found]
    pytest.skip("ruff is not installed")


def _ruff_stdin(filename: str, source: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [*_ruff_cmd(), "check", "--no-cache", "--output-format", "concise", "--stdin-filename", filename, "-"],
        input=source,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )


def test_generated_code_is_excluded() -> None:
    """Req 5.3: an F401 violation inside das/transport/proto is not reported."""
    result = _ruff_stdin("das/transport/proto/_probe.py", "import os\n")
    output = result.stdout + result.stderr
    assert result.returncode == 0, output
    assert "F401" not in output
    assert "_probe.py:" not in output


def test_undefined_name_reports_path_line_and_code() -> None:
    """Req 5.7: an F821 violation fails with path, line number and rule code."""
    result = _ruff_stdin("das/_probe.py", "def f() -> int:\n    return undefined_name\n")
    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    assert "das/_probe.py:2:" in output
    assert "F821" in output


def test_syntax_error_reports_path_and_line() -> None:
    """Req 5.8: a Python 3.12 syntax error fails with path and line number."""
    result = _ruff_stdin("das/_probe.py", "x = 1\ndef f(:\n    pass\n")
    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    assert "das/_probe.py:2:" in output
