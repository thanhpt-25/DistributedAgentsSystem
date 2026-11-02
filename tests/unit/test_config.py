"""Basic unit tests for Agent_Config loading (task 2.2)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from das.config import load_config, resolve
from das.config.loader import env_path
from das.errors import ConfigError

SECRET = "sk-test-0123456789abcdef"


def worker_cfg(**overrides: object) -> dict:
    cfg: dict = {
        "role": "worker",
        "host": "10.0.0.7",
        "transports": {"jobs": "rabbitmq", "control": "grpc"},
        "rabbitmq": {"url": "amqp://rabbit.internal:5672/"},
        "redis": {"url": "redis://redis.internal:6379/0"},
        "grpc": {"bind": "0.0.0.0:50051", "advertise_host": "10.0.0.7", "advertise_port": 50051},
        "llm": {"provider": "anthropic", "model": "claude-sonnet-4"},
        "worker": {"capabilities": ["research", "summarize"]},
    }
    cfg.update(overrides)
    return cfg


def write(tmp_path: Path, data: object, name: str = "config.yaml") -> Path:
    p = tmp_path / name
    p.write_text(yaml.safe_dump(data), encoding="utf-8")
    return p


ENV = {"DAS__LLM__API_KEY": SECRET}


def test_valid_worker_config_with_defaults(tmp_path: Path) -> None:
    cfg = load_config(write(tmp_path, worker_cfg()), ENV)
    assert cfg.role == "worker"
    assert cfg.limits.max_attempts == 3
    assert cfg.limits.lease_duration == 30
    assert cfg.limits.max_prompt_length == 32_000
    assert cfg.worker is not None and cfg.worker.concurrency_limit == 4
    assert cfg.llm is not None and cfg.llm.api_key is not None
    assert cfg.llm.api_key.get_secret_value() == SECRET
    assert cfg.agent_id and cfg.agent_id.startswith("worker-")
    assert SECRET in cfg.secret_values()


def test_env_overrides_file_and_file_overrides_default(tmp_path: Path) -> None:
    data = worker_cfg(limits={"max_attempts": 5, "max_jobs": 20})
    env = {**ENV, "DAS__LIMITS__MAX_ATTEMPTS": "7", "DAS__WORKER__CAPABILITIES": "a,b,c"}
    cfg = load_config(write(tmp_path, data), env)
    assert cfg.limits.max_attempts == 7  # env
    assert cfg.limits.max_jobs == 20  # file
    assert cfg.limits.planning_retries == 2  # default
    assert cfg.worker is not None and cfg.worker.capabilities == ["a", "b", "c"]


def test_resolve_is_pure() -> None:
    file_values = {"limits": {"max_jobs": 3}}
    out = resolve(file_values, {"DAS__LIMITS__MAX_JOBS": "4", "OTHER": "x"})
    assert out == {"limits": {"max_jobs": "4"}}
    assert file_values == {"limits": {"max_jobs": 3}}
    assert env_path("DAS__A____B") is None
    assert env_path("DAS__LLM__MODEL") == ("llm", "model")


def test_every_invalid_field_is_reported(tmp_path: Path) -> None:
    data = worker_cfg(
        limits={"max_attempts": 11, "base_backoff": 50, "max_backoff": 10, "job_timeout": 0},
        worker={"capabilities": []},
    )
    data["llm"] = {"provider": "nope"}
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, data), {})
    fields = set(ei.value.fields)
    assert {
        "limits.max_attempts",
        "limits.base_backoff",
        "limits.job_timeout",
        "worker.capabilities",
        "llm.provider",
        "llm.model",
    } <= fields
    msg = str(ei.value)
    assert "permitted range is 1 to 10" in msg
    assert ei.value.source is not None


def test_missing_role_and_unknown_role(tmp_path: Path) -> None:
    data = worker_cfg()
    del data["role"]
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, data), ENV)
    assert "role" in ei.value.fields
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, worker_cfg(role="boss")), ENV)
    assert "role" in ei.value.fields


def test_role_required_sections_and_no_address_defaults(tmp_path: Path) -> None:
    data = worker_cfg()
    for key in ("rabbitmq", "redis", "grpc", "worker"):
        del data[key]
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, data), ENV)
    assert {"rabbitmq", "redis", "grpc", "worker"} <= set(ei.value.fields)


def test_provider_credentials_required(tmp_path: Path) -> None:
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, worker_cfg()), {})
    assert "llm.api_key" in ei.value.fields
    ollama = worker_cfg(llm={"provider": "ollama", "model": "llama3"})
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, ollama), {})
    assert ei.value.fields == ("llm.base_url",)
    bedrock = worker_cfg(llm={"provider": "bedrock", "model": "m"})
    assert load_config(write(tmp_path, bedrock), {}).llm is not None


def test_secret_in_file_is_rejected_without_value(tmp_path: Path) -> None:
    data = worker_cfg()
    data["llm"] = {**data["llm"], "api_key": SECRET}
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, data), ENV)
    assert "llm.api_key" in ei.value.fields
    assert SECRET[:8] not in str(ei.value)
    assert SECRET not in repr(ei.value.errors)


def test_secrets_file_supplies_credentials(tmp_path: Path) -> None:
    secrets = tmp_path / "das.env"
    secrets.write_text(f"# creds\nexport DAS__LLM__API_KEY='{SECRET}'\n", encoding="utf-8")
    cfg = load_config(write(tmp_path, worker_cfg(secrets_file=str(secrets))), {})
    assert cfg.llm is not None and cfg.llm.api_key is not None
    assert cfg.llm.api_key.get_secret_value() == SECRET


def test_missing_secrets_file_names_reference(tmp_path: Path) -> None:
    ref = str(tmp_path / "nope.env")
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, worker_cfg(secrets_file=ref)), ENV)
    assert "secrets_file" in ei.value.fields
    assert ref in str(ei.value)


def test_file_errors_name_path_and_reason(tmp_path: Path) -> None:
    missing = tmp_path / "missing.yaml"
    with pytest.raises(ConfigError) as ei:
        load_config(missing, {})
    assert str(missing) in str(ei.value) and "does not exist" in str(ei.value)

    bad = tmp_path / "bad.yaml"
    bad.write_text(f"llm: {{api_key: {SECRET}\n", encoding="utf-8")
    with pytest.raises(ConfigError) as ei:
        load_config(bad, {})
    assert "cannot be parsed" in str(ei.value)
    assert SECRET[:8] not in str(ei.value)


def test_gateway_requires_api_keys(tmp_path: Path) -> None:
    data = {"role": "gateway", "redis": {"url": "redis://r:6379/0"}}
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, data), {})
    assert ei.value.fields == ("api_keys",)
    cfg = load_config(write(tmp_path, data), {"DAS__API_KEYS": "key-one-123,key-two-456"})
    assert [k.get_secret_value() for k in cfg.api_keys] == ["key-one-123", "key-two-456"]
