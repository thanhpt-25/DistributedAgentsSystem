"""Static checks on the ``das-client`` package metadata (das-client-api task 9.1; Phase 4 Req 9.1).

The client is a separate distribution that must run on Python 3.10 to 3.13 while the server is 3.12 only,
so it is *not* a uv workspace member (a member inherits the server's ``==3.12.*`` and uv then refuses
``--python 3.10``). The root dev group installs it as an editable path source instead; the client's own
suite runs in an isolated environment (see ``clients/python/README.md`` and the CI ``client`` job)."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]


def _toml(path: Path) -> dict[str, Any]:
    return tomllib.loads(path.read_text(encoding="utf-8"))


ROOT_PROJECT = _toml(ROOT / "pyproject.toml")
CLIENT = _toml(ROOT / "clients" / "python" / "pyproject.toml")


def test_client_is_a_separate_distribution_for_python_3_10_to_3_13() -> None:
    project = CLIENT["project"]
    assert project["name"] == "das-client"
    assert project["requires-python"] == ">=3.10,<3.14"


def test_only_runtime_dependency_is_the_servers_httpx_pin() -> None:
    server_pin = next(d for d in ROOT_PROJECT["project"]["dependencies"] if d.startswith("httpx=="))
    assert CLIENT["project"]["dependencies"] == [server_pin]  # exact pin, and nothing else


def test_lock_resolves_the_pinned_httpx() -> None:
    lock = _toml(ROOT / "uv.lock")
    pin = CLIENT["project"]["dependencies"][0].removeprefix("httpx==")
    assert {p["version"] for p in lock["package"] if p["name"] == "httpx"} == {pin}


def test_root_dev_group_installs_the_client_as_an_editable_path_source() -> None:
    assert "das-client" in ROOT_PROJECT["dependency-groups"]["dev"]
    assert ROOT_PROJECT["tool"]["uv"]["sources"]["das-client"] == {"path": "clients/python", "editable": True}
    assert "workspace" not in ROOT_PROJECT["tool"]["uv"]  # a member would pin the client to 3.12


def test_client_pytest_config_matches_the_servers_async_mode() -> None:
    options = CLIENT["tool"]["pytest"]["ini_options"]
    assert options["asyncio_mode"] == "auto"
    assert options["asyncio_default_fixture_loop_scope"] == "function"


def test_client_lints_for_python_3_10() -> None:
    ruff = CLIENT["tool"]["ruff"]
    assert ruff["target-version"] == "py310"  # so ruff flags nothing 3.10 cannot run
    assert ruff["line-length"] == ROOT_PROJECT["tool"]["ruff"]["line-length"]
    assert ruff["lint"]["select"] == ROOT_PROJECT["tool"]["ruff"]["lint"]["select"]
