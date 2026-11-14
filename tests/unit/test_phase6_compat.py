"""Phase 6 changes nothing for a deployment that sets none of its configuration (das-smarter-agents task 7.1; Phase 6
Req 7.1, 7.2).

Two checks. The sample configurations of Phase 5 load unchanged and leave every Phase 6 feature off. And the contract
baseline, compared with its content at the last Phase 5 commit, changed only by additions, with ``codec_corpus.jsonl``
byte for byte the same. The second one reads git history, so it is skipped where the history is not there (a shallow
clone)."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

from das.cli import _worker_capabilities
from das.config import load_config
from das.tools.registry import ToolRegistry

ROOT = Path(__file__).resolve().parents[2]
PHASE5_LAST_COMMIT = "b70a294"
BASELINE = "tests/fixtures/phase3_baseline"
ENV = {
    "DAS__RABBITMQ__URL": "amqp://guest:guest@rabbit:5672/",
    "DAS__RABBITMQ__MANAGEMENT_URL": "http://rabbit:15672",
    "DAS__REDIS__URL": "redis://redis:6379/0",
    "DAS__GRPC__ADVERTISE_HOST": "agent",
    "DAS__API_KEYS": "change-me-api-key",
    "DAS__WORKER__CAPABILITIES": "research,summarize",
    "DAS__LLM__PROVIDER": "ollama",
    "DAS__LLM__BASE_URL": "http://ollama:11434",
    "DAS__LLM__MODEL": "llama3",
}


# --------------------------------------------------------------------------- the sample configurations (Req 7.1)


@pytest.mark.parametrize("role", ["gateway", "manager", "worker"])
def test_a_phase_5_sample_configuration_loads_with_every_phase_6_feature_off(role: str) -> None:
    config = load_config(ROOT / "deploy" / "config" / f"{role}.yaml", ENV)
    assert config.limits.max_replans == 0
    assert config.tools.enabled == [] and config.sandbox.enabled is False
    if config.llm is not None:
        assert config.llm.capability_models == {} and config.llm.fallback_model is None


def test_a_worker_with_no_phase_6_configuration_advertises_exactly_its_own_capabilities() -> None:
    config = load_config(ROOT / "deploy" / "config" / "worker.yaml", ENV)
    assert config.worker is not None
    advertised = _worker_capabilities(config.worker.capabilities, ToolRegistry())
    assert advertised == frozenset(config.worker.capabilities)


# --------------------------------------------------------------------------- the baseline (Req 7.2)


def git_show(path: str) -> bytes | None:
    if shutil.which("git") is None:
        return None
    out = subprocess.run(["git", "show", f"{PHASE5_LAST_COMMIT}:{path}"], cwd=ROOT, capture_output=True, timeout=60)
    return out.stdout if out.returncode == 0 else None


def additions_only(old: Any, new: Any, where: str = "$") -> list[str]:
    """What ``old`` has that ``new`` lacks or changes: dicts may gain keys, lists may gain items, scalars must stay."""
    if isinstance(old, dict):
        if not isinstance(new, dict):
            return [f"{where}: was an object"]
        problems: list[str] = []
        for key, value in old.items():
            if key in new:
                problems += additions_only(value, new[key], f"{where}.{key}")
            else:
                problems.append(f"{where}.{key}: removed")
        return problems
    if isinstance(old, list):
        if not isinstance(new, list):
            return [f"{where}: was a list"]
        return [
            f"{where}[{index}]: removed or changed: {json.dumps(item)[:80]}"
            for index, item in enumerate(old)
            if not any(not additions_only(item, candidate, where) for candidate in new)
        ]
    return [] if old == new else [f"{where}: {old!r} became {new!r}"]


@pytest.fixture(scope="module")
def old_contract() -> dict[str, Any]:
    raw = git_show(f"{BASELINE}/contract.json")
    if raw is None:
        pytest.skip(f"the history of {PHASE5_LAST_COMMIT} is not available")
    return json.loads(raw)


#: Phase 7 made ``redis.url`` optional in the schema, because Sentinel mode has none (Phase 7 Req 1.2); standalone mode
#: still requires it, in ``cross_field_errors``. Every Phase 5 configuration is still valid, so this relaxation is the
#: one thing allowed to be more than an addition. Anything else that is not an addition still fails.
ALLOWED_RELAXATIONS = frozenset({
    "$.config_schema.$defs.RedisConfig.properties.url.type",
    "$.config_schema.$defs.RedisConfig.required",
})


def test_the_contract_baseline_changed_only_by_additions_since_phase_5(old_contract: dict[str, Any]) -> None:
    new = json.loads((ROOT / BASELINE / "contract.json").read_text(encoding="utf-8"))
    problems = [p for p in additions_only(old_contract, new) if p.split(":")[0] not in ALLOWED_RELAXATIONS]
    assert problems == [], "\n".join(problems[:20])


def test_the_codec_corpus_is_byte_for_byte_what_it_was_in_phase_5(old_contract: dict[str, Any]) -> None:
    old = git_show(f"{BASELINE}/codec_corpus.jsonl")
    assert old is not None
    assert (ROOT / BASELINE / "codec_corpus.jsonl").read_bytes() == old


# --------------------------------------------------------------------------- the checker itself


def test_the_additions_checker_accepts_new_keys_and_items_and_rejects_removals_and_changes() -> None:
    assert additions_only({"a": 1, "l": [1, 2]}, {"a": 1, "b": 2, "l": [0, 1, 2, 3]}) == []
    assert additions_only({"a": 1}, {}) == ["$.a: removed"]
    assert additions_only({"a": 1}, {"a": 2}) == ["$.a: 1 became 2"]
    assert additions_only({"l": [1, 2]}, {"l": [1]})
    assert additions_only({"l": [{"n": "x"}]}, {"l": [{"n": "x", "extra": 1}]}) == []
    assert additions_only({"l": [{"n": "x"}]}, {"l": [{"n": "y"}]})
