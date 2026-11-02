"""Unit tests for config file and secrets file errors (task 2.6).

Requirements 12.3, 12.6, 14.4.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
import yaml

from das.config import load_config
from das.config.secrets import read_secrets_file
from das.errors import ConfigError

SECRET = "sk-test-cafebabe0123456789"
ENV = {"DAS__LLM__API_KEY": SECRET}


def worker_cfg(**overrides: object) -> dict:
    cfg: dict = {
        "role": "worker",
        "host": "10.0.0.7",
        "transports": {"jobs": "rabbitmq", "control": "grpc"},
        "rabbitmq": {"url": "amqp://rabbit.internal:5672/"},
        "redis": {"url": "redis://redis.internal:6379/0"},
        "grpc": {"bind": "0.0.0.0:50051", "advertise_host": "10.0.0.7", "advertise_port": 50051},
        "llm": {"provider": "anthropic", "model": "claude-sonnet-4"},
        "worker": {"capabilities": ["research"]},
    }
    cfg.update(overrides)
    return cfg


def write(tmp_path: Path, data: object, name: str = "config.yaml") -> Path:
    p = tmp_path / name
    p.write_text(yaml.safe_dump(data), encoding="utf-8")
    return p


def assert_nonzero_exit(err: ConfigError) -> None:
    assert isinstance(err.exit_code, int) and err.exit_code != 0


# --------------------------------------------------------------------- Req 12.3


def test_missing_config_file(tmp_path: Path) -> None:
    path = tmp_path / "absent.yaml"
    with pytest.raises(ConfigError) as ei:
        load_config(path, ENV)
    err = ei.value
    assert err.source == str(path)
    assert "does not exist" in str(err)
    assert str(path) in str(err)
    assert_nonzero_exit(err)


@pytest.mark.skipif(
    sys.platform == "win32" or (hasattr(os, "geteuid") and os.geteuid() == 0),
    reason="file permissions are not enforced for root/Windows",
)
def test_unreadable_config_file(tmp_path: Path) -> None:
    path = write(tmp_path, worker_cfg())
    path.chmod(0o000)
    try:
        with pytest.raises(ConfigError) as ei:
            load_config(path, ENV)
    finally:
        path.chmod(0o600)
    err = ei.value
    assert err.source == str(path)
    assert "cannot be read" in str(err)
    assert_nonzero_exit(err)


def test_config_path_is_directory_is_unreadable(tmp_path: Path) -> None:
    with pytest.raises(ConfigError) as ei:
        load_config(tmp_path, ENV)
    assert ei.value.source == str(tmp_path)
    assert "cannot be read" in str(ei.value)
    assert_nonzero_exit(ei.value)


@pytest.mark.parametrize(
    ("content", "detail"),
    [
        (f"llm: {{api_key: {SECRET}\n".encode(), "invalid YAML"),
        (b"role: worker\n  bad: : indent\n", "invalid YAML"),
        (b"- a\n- b\n", "top level must be a mapping"),
        (b"\xff\xfe\x00role", "not valid UTF-8"),
    ],
    ids=["unclosed-flow", "bad-indent", "non-mapping", "non-utf8"],
)
def test_unparseable_config_file(tmp_path: Path, content: bytes, detail: str) -> None:
    path = tmp_path / "bad.yaml"
    path.write_bytes(content)
    with pytest.raises(ConfigError) as ei:
        load_config(path, ENV)
    msg = str(ei.value)
    assert ei.value.source == str(path)
    assert "cannot be parsed" in msg and detail in msg
    # File contents (possibly credentials) are never echoed back.
    assert SECRET[:10] not in msg
    assert_nonzero_exit(ei.value)


def test_empty_config_file_is_parsed_but_reports_missing_role(tmp_path: Path) -> None:
    path = tmp_path / "empty.yaml"
    path.write_text("", encoding="utf-8")
    with pytest.raises(ConfigError) as ei:
        load_config(path, {})
    assert "role" in ei.value.fields


# --------------------------------------------------------------------- Req 12.6


def test_missing_secrets_file_identifies_reference(tmp_path: Path) -> None:
    ref = str(tmp_path / "missing.env")
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, worker_cfg(secrets_file=ref)), {})
    err = ei.value
    assert "secrets_file" in err.fields
    assert ref in str(err)
    assert "does not exist" in str(err)
    assert_nonzero_exit(err)


def test_missing_secrets_file_via_env_reference(tmp_path: Path) -> None:
    ref = str(tmp_path / "env-ref.env")
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, worker_cfg()), {"DAS__SECRETS_FILE": ref})
    assert "secrets_file" in ei.value.fields
    assert ref in str(ei.value)


@pytest.mark.skipif(
    sys.platform == "win32" or (hasattr(os, "geteuid") and os.geteuid() == 0),
    reason="file permissions are not enforced for root/Windows",
)
def test_unreadable_secrets_file_omits_credential(tmp_path: Path) -> None:
    secrets = tmp_path / "das.env"
    secrets.write_text(f"DAS__LLM__API_KEY={SECRET}\n", encoding="utf-8")
    secrets.chmod(0o000)
    try:
        with pytest.raises(ConfigError) as ei:
            load_config(write(tmp_path, worker_cfg(secrets_file=str(secrets))), {})
    finally:
        secrets.chmod(0o600)
    msg = str(ei.value)
    assert "secrets_file" in ei.value.fields
    assert str(secrets) in msg and "cannot be read" in msg
    assert SECRET not in msg and SECRET not in repr(ei.value.errors)


def test_malformed_secrets_file_lines_never_include_values(tmp_path: Path) -> None:
    secrets = tmp_path / "das.env"
    secrets.write_text(
        f"{SECRET}\nDAS__LIMITS__MAX_JOBS={SECRET}\nDAS__LLM__API_KEY={SECRET}\n",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError) as ei:
        read_secrets_file(secrets)
    msg = str(ei.value)
    assert "line 1: expected NAME=VALUE" in msg
    assert "line 2: DAS__LIMITS__MAX_JOBS is not a credential setting" in msg
    assert SECRET not in msg and SECRET not in repr(ei.value.errors)


# --------------------------------------------------------------------- Req 14.4


@pytest.mark.parametrize("capabilities", [[], ""], ids=["empty-list", "empty-string"])
def test_worker_with_zero_capabilities(tmp_path: Path, capabilities: object) -> None:
    data = worker_cfg(worker={"capabilities": capabilities})
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, data), ENV)
    assert "worker.capabilities" in ei.value.fields
    assert "worker.capabilities" in str(ei.value)
    assert_nonzero_exit(ei.value)


def test_worker_zero_capabilities_from_env_override(tmp_path: Path) -> None:
    env = {**ENV, "DAS__WORKER__CAPABILITIES": ""}
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, worker_cfg()), env)
    assert "worker.capabilities" in ei.value.fields


def test_worker_without_worker_section(tmp_path: Path) -> None:
    data = worker_cfg()
    del data["worker"]
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, data), ENV)
    assert "worker" in ei.value.fields


@pytest.mark.parametrize("role", ["boss", "Worker", "", "registry"])
def test_unknown_role(tmp_path: Path, role: str) -> None:
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, worker_cfg(role=role)), ENV)
    assert "role" in ei.value.fields
    assert "role" in str(ei.value)
    assert_nonzero_exit(ei.value)


def test_unknown_role_from_env_overrides_valid_file_role(tmp_path: Path) -> None:
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, worker_cfg()), {**ENV, "DAS__ROLE": "supervisor"})
    assert "role" in ei.value.fields


def test_missing_role(tmp_path: Path) -> None:
    data = worker_cfg()
    del data["role"]
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, data), ENV)
    assert "role" in ei.value.fields
    assert_nonzero_exit(ei.value)
