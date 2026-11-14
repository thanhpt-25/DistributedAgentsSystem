"""Phase 7 changes nothing for a deployment that sets none of its configuration (das-scale-resilience task 13.1; Phase 7
Req 5.1, 5.2).

Three checks. The sample configurations load unchanged and leave every Phase 7 feature off (Redis stays one instance
by URL, no HTTP or Kafka section, the broker is not served). The packages that define what Phase 6 sent, stored and
computed (the codec, the models, the Lua scripts and key helpers, the planner, the ops and client logic, the Manager
and the Worker) are byte for byte what they were at the last Phase 6 commit. And the contract baseline, compared with
its content at that commit, changed only by additions (``redis.url`` becoming optional in the schema is the one named
exception), with ``codec_corpus.jsonl`` byte for byte the same, and exactly two new metrics. The git checks are skipped
where the history is not there (a shallow clone)."""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

from das.config import load_config
from das.state.redisclient import FailoverAwarePool, build_redis_client
from tests.unit.test_phase6_compat import ALLOWED_RELAXATIONS, BASELINE, ENV, additions_only

ROOT = Path(__file__).resolve().parents[2]
PHASE6_LAST_COMMIT = "510b82b"
#: What defines Phase 6's messages, keys, scripts and behaviour: Phase 7 did not touch any of it.
UNCHANGED = (
    "das/codec", "das/models", "das/state/lua", "das/state/keys.py", "das/plan", "das/ops", "das/client", "das/manager",
    "das/worker", "das/llm", "das/registry", "das/tenancy", "das/tools", "das/sandbox", "das/structured", "das/eval",
)
NEW_METRICS = {"das_redis_failovers_total", "das_redis_retries_total"}


def git(*args: str) -> subprocess.CompletedProcess[bytes] | None:
    if shutil.which("git") is None:
        return None
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, timeout=60)


def git_show(path: str) -> bytes | None:
    out = git("show", f"{PHASE6_LAST_COMMIT}:{path}")
    return out.stdout if out is not None and out.returncode == 0 else None


def metric_names(catalog: Any) -> set[str]:
    return {m["name"] for m in catalog} if isinstance(catalog, list) else set(catalog)


# --------------------------------------------------------------------------- the sample configurations (Req 5.1)


@pytest.mark.parametrize("role", ["gateway", "manager", "worker"])
def test_a_sample_configuration_loads_with_every_phase_7_feature_off(role: str) -> None:
    config = load_config(ROOT / "deploy" / "config" / f"{role}.yaml", ENV)
    assert config.redis is not None and config.redis.mode == "standalone" and config.redis.url == ENV["DAS__REDIS__URL"]
    assert config.redis.sentinels is None and config.redis.master_name is None and config.redis.password is None
    assert config.http is None and config.kafka is None and config.http_broker.enabled is False
    assert config.transports is not None and (config.transports.jobs, config.transports.control) == ("rabbitmq", "grpc")


def test_a_standalone_redis_client_is_the_plain_one_not_a_sentinel_pool() -> None:
    config = load_config(ROOT / "deploy" / "config" / "manager.yaml", ENV)
    assert config.redis is not None
    client = build_redis_client(config.redis, socket_timeout=5, connect_timeout=2)
    assert not isinstance(client.connection_pool, FailoverAwarePool)
    assert client.connection_pool.connection_kwargs["host"] == "redis"
    assert client.connection_pool.connection_kwargs["decode_responses"] is True


# --------------------------------------------------------------------------- what did not change (Req 5.1)


def test_nothing_that_defines_phase_6s_messages_keys_scripts_or_logic_changed() -> None:
    out = git("diff", "--name-only", PHASE6_LAST_COMMIT, "--", *UNCHANGED)
    if out is None or out.returncode != 0:
        pytest.skip(f"the history of {PHASE6_LAST_COMMIT} is not available")
    changed = out.stdout.decode().split()
    assert changed == [], f"Phase 7 changed files that define Phase 6's behaviour: {changed}"


def test_every_listed_path_exists_so_the_check_above_is_not_vacuous() -> None:
    assert all((ROOT / path).exists() for path in UNCHANGED)


# --------------------------------------------------------------------------- the baseline (Req 5.2)


@pytest.fixture(scope="module")
def old_contract() -> dict[str, Any]:
    raw = git_show(f"{BASELINE}/contract.json")
    if raw is None:
        pytest.skip(f"the history of {PHASE6_LAST_COMMIT} is not available")
    return json.loads(raw)


@pytest.fixture(scope="module")
def new_contract() -> dict[str, Any]:
    return json.loads((ROOT / BASELINE / "contract.json").read_text(encoding="utf-8"))


def test_the_contract_baseline_changed_only_by_additions_since_phase_6(
    old_contract: dict[str, Any], new_contract: dict[str, Any]
) -> None:
    problems = [p for p in additions_only(old_contract, new_contract) if p.split(":")[0] not in ALLOWED_RELAXATIONS]
    assert problems == [], "\n".join(problems[:20])


def test_the_one_relaxation_is_the_redis_url_becoming_optional_in_the_schema(
    old_contract: dict[str, Any], new_contract: dict[str, Any]
) -> None:
    schema = new_contract["config_schema"]["$defs"]["RedisConfig"]
    assert "url" not in schema.get("required", []) and "mode" in schema["properties"]
    old_schema = old_contract["config_schema"]["$defs"]["RedisConfig"]
    assert "url" in old_schema["required"], "in Phase 6 it was required: this is the relaxation"


def test_the_codec_corpus_is_byte_for_byte_what_it_was_in_phase_6(old_contract: dict[str, Any]) -> None:
    old = git_show(f"{BASELINE}/codec_corpus.jsonl")
    assert old is not None
    assert (ROOT / BASELINE / "codec_corpus.jsonl").read_bytes() == old


def test_phase_7_added_exactly_two_metrics_and_removed_none(
    old_contract: dict[str, Any], new_contract: dict[str, Any]
) -> None:
    before, after = metric_names(old_contract["metrics"]), metric_names(new_contract["metrics"])
    assert before <= after and after - before == NEW_METRICS


def test_the_new_environment_variables_are_additions_to_the_published_contract(
    old_contract: dict[str, Any], new_contract: dict[str, Any]
) -> None:
    old_env, new_env = set(old_contract["env_vars"]), set(new_contract["env_vars"])
    assert old_env <= new_env
    for name in (
        "DAS__REDIS__MODE", "DAS__REDIS__SENTINELS", "DAS__HTTP__TOKEN", "DAS__HTTP_BROKER__TOKEN",
        "DAS__KAFKA__BOOTSTRAP_SERVERS", "DAS__KAFKA__SASL_PASSWORD",
    ):
        assert name in new_env - old_env, f"{name} is not a new, documented variable"
