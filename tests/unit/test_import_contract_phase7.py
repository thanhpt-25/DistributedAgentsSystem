"""The import contract keeps Phase 7's client libraries out of the Manager and the Worker (das-scale-resilience task
13.2; Phase 7 Req 5.3).

``das.manager`` and ``das.worker`` reach the network only through the Transport protocol and the State_Store, so neither
may import ``aiokafka`` or the Redis Sentinel client. The repository's own contract is checked for naming them, and then
proved to bite: a throwaway project with the same forbidden list and a Manager module that imports each library is run
through ``lint-imports`` and must be reported broken, while a module that imports nothing forbidden stays kept."""

from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
PYPROJECT = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
CONTRACT = next(
    c for c in PYPROJECT["tool"]["importlinter"]["contracts"] if "das.manager" in c.get("source_modules", [])
)
LINT = Path(sys.executable).parent / "lint-imports"


def test_the_contract_names_the_kafka_client_and_redis_which_holds_the_sentinel_client() -> None:
    """import-linter takes only the root of an external package (``redis.asyncio.sentinel`` is refused as a name), so
    ``redis`` is what forbids the Sentinel client; the tests below prove it does."""
    forbidden = set(CONTRACT["forbidden_modules"])
    assert {"aiokafka", "redis"} <= forbidden
    assert not [name for name in forbidden if name.startswith("redis.")], "a subpackage entry makes lint-imports fail"
    assert set(CONTRACT["source_modules"]) == {"das.manager", "das.worker"}


def project(tmp_path: Path, source: str, package: str = "manager") -> Path:
    """A project ``probe`` with one module under ``probe.<package>`` holding ``source``, and the real contract."""
    for part in ("probe", "probe/manager", "probe/worker"):  # the contract names both, so both must exist
        (tmp_path / part).mkdir(exist_ok=True)
        (tmp_path / part / "__init__.py").write_text("")
    (tmp_path / "probe" / package / "agent.py").write_text(source)
    forbidden = ", ".join(f'"{name}"' for name in CONTRACT["forbidden_modules"])
    config = tmp_path / "importlinter.toml"
    config.write_text(
        '[tool.importlinter]\nroot_packages = ["probe"]\ninclude_external_packages = true\n\n'
        '[[tool.importlinter.contracts]]\nname = "probe"\ntype = "forbidden"\n'
        f'source_modules = ["probe.manager", "probe.worker"]\nforbidden_modules = [{forbidden}]\n'
        "allow_indirect_imports = true\n"
    )
    return config


def lint(tmp_path: Path, config: Path) -> subprocess.CompletedProcess[str]:
    if not LINT.exists():
        pytest.skip("import-linter is not installed")
    env = {"PYTHONPATH": str(tmp_path), "PATH": "/usr/bin:/bin", "PYTHONDONTWRITEBYTECODE": "1"}
    return subprocess.run(
        [str(LINT), "--no-cache", "--config", str(config)],
        cwd=tmp_path, capture_output=True, text=True, timeout=120, env=env,
    )


@pytest.mark.parametrize("statement", [
    "import aiokafka",
    "from aiokafka import AIOKafkaConsumer",
    "from aiokafka.errors import KafkaError",
    "import redis.asyncio.sentinel",
    "from redis.asyncio.sentinel import Sentinel",
    "from redis.asyncio import Redis",
])
@pytest.mark.parametrize("package", ["manager", "worker"])
def test_a_manager_or_worker_module_importing_a_forbidden_client_breaks_the_contract(
    tmp_path: Path, statement: str, package: str
) -> None:
    result = lint(tmp_path, project(tmp_path, f"{statement}\n", package))
    assert result.returncode != 0, f"{statement!r} in probe.{package} was not refused:\n{result.stdout}"
    assert "broken" in result.stdout.lower()


def test_a_module_that_imports_nothing_forbidden_keeps_the_contract(tmp_path: Path) -> None:
    result = lint(tmp_path, project(tmp_path, "import asyncio\nfrom dataclasses import dataclass\n"))
    assert result.returncode == 0, result.stdout


def test_importing_the_adapters_modules_indirectly_is_allowed_as_before(tmp_path: Path) -> None:
    """The contract forbids direct imports only (allow_indirect_imports): going through a transport module is how the
    Manager and the Worker use Kafka, and it must stay possible."""
    config = project(tmp_path, "from probe import adapter\n")
    (tmp_path / "probe" / "adapter.py").write_text("import aiokafka\n")
    result = lint(tmp_path, config)
    assert result.returncode == 0, result.stdout
