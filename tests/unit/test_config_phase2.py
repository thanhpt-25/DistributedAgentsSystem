"""Unit tests for the Phase 2 Agent_Config fields (task 5.3).

Requirements 1.8, 8.6, 11.1, 11.10, 11.11, 19.1.
"""

from __future__ import annotations

import io
from pathlib import Path

import pytest
import yaml

import das.cli as cli
from das.config import load_config
from das.config.schema import (
    DEFAULT_ATTEMPTS_BUCKETS,
    DEFAULT_DURATION_BUCKETS,
    DEFAULT_HTTP_BUCKETS,
)
from das.errors import EXIT_CONFIG_ERROR, ConfigError
from das.observability import configure_logging

API_KEY = "client-key-0123456789"
ADMIN_KEY = "admin-key-abcdef0123456789"
ADMIN_KEY_2 = "admin-key-fedcba9876543210"
OTLP_HEADERS = "authorization=Bearer otlp-secret-token-42"
ENV = {"DAS__API_KEYS": API_KEY}


def gateway_cfg(**overrides: object) -> dict:
    cfg: dict = {"role": "gateway", "agent_id": "gateway-test", "redis": {"url": "redis://127.0.0.1:1/0"}}
    cfg.update(overrides)
    return cfg


def write(tmp_path: Path, data: object, name: str = "config.yaml") -> Path:
    p = tmp_path / name
    p.write_text(yaml.safe_dump(data), encoding="utf-8")
    return p


@pytest.fixture(autouse=True)
def _restore_logging():
    # cli.main() binds logging to the captured stdout; rebind afterwards.
    yield
    configure_logging("test", stream=io.StringIO())


@pytest.fixture
def no_startup(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Record whether startup got past config validation (it must not on exit 2)."""
    called: list[str] = []

    def fail_load_transports(config):  # pragma: no cover - must not run
        called.append("load_transports")
        raise AssertionError("startup continued after a config error")

    monkeypatch.setattr(cli, "load_transports", fail_load_transports)
    return called


def run_main(path: Path, env: dict[str, str]) -> int:
    return cli.main(["run", "--config", str(path)], env=env)


# --------------------------------------------------------------------------- defaults


def test_default_buckets_and_new_section_defaults(tmp_path: Path) -> None:
    cfg = load_config(write(tmp_path, gateway_cfg()), ENV)
    buckets = cfg.metrics.buckets
    assert tuple(buckets.http) == DEFAULT_HTTP_BUCKETS
    assert tuple(buckets.duration) == DEFAULT_DURATION_BUCKETS
    assert tuple(buckets.attempts) == DEFAULT_ATTEMPTS_BUCKETS
    assert DEFAULT_HTTP_BUCKETS == (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)
    assert DEFAULT_DURATION_BUCKETS == (0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600)
    assert DEFAULT_ATTEMPTS_BUCKETS == (1, 2, 3, 5, 10, 20)
    assert cfg.metrics.enabled is True
    assert cfg.tracing.enabled is False
    assert cfg.tracing.service_name == "das-gateway"
    assert cfg.admin_api_keys == []
    assert (cfg.http_port, cfg.health_port) == (8080, 8081)


# --------------------------------------------------------------------------- secrets in YAML (8.6, 11.10)


@pytest.mark.parametrize(
    ("overrides", "field", "secret"),
    [
        ({"admin_api_keys": [ADMIN_KEY]}, "admin_api_keys", ADMIN_KEY),
        ({"tracing": {"otlp_headers": OTLP_HEADERS}}, "tracing.otlp_headers", "otlp-secret-token-42"),
    ],
)
def test_new_secret_in_yaml_exits_2_without_value(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    no_startup: list[str],
    overrides: dict,
    field: str,
    secret: str,
) -> None:
    path = write(tmp_path, gateway_cfg(**overrides))

    with pytest.raises(ConfigError) as ei:
        load_config(path, ENV)
    assert field in ei.value.fields
    assert secret[:8] not in str(ei.value)

    assert run_main(path, ENV) == EXIT_CONFIG_ERROR
    out = capsys.readouterr()
    assert field in out.out + out.err
    assert secret[:8] not in out.out + out.err
    assert no_startup == []


# --------------------------------------------------------------------------- secrets file (11.11)


def test_missing_secrets_file_exits_2_naming_reference(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], no_startup: list[str]
) -> None:
    ref = str(tmp_path / "admin.env")
    env = {**ENV, "DAS__ADMIN_API_KEYS": ADMIN_KEY}
    path = write(tmp_path, gateway_cfg(secrets_file=ref))

    with pytest.raises(ConfigError) as ei:
        load_config(path, env)
    assert "secrets_file" in ei.value.fields
    assert ref in str(ei.value)
    assert ADMIN_KEY[:8] not in str(ei.value)

    assert run_main(path, env) == EXIT_CONFIG_ERROR
    out = capsys.readouterr()
    assert ref in out.out + out.err
    assert ADMIN_KEY[:8] not in out.out + out.err
    assert no_startup == []


def test_secrets_file_supplies_admin_keys_and_otlp_headers(tmp_path: Path) -> None:
    secrets = tmp_path / "das.env"
    secrets.write_text(
        f"DAS__ADMIN_API_KEYS={ADMIN_KEY},{ADMIN_KEY_2}\nDAS__TRACING__OTLP_HEADERS='{OTLP_HEADERS}'\n",
        encoding="utf-8",
    )
    cfg = load_config(write(tmp_path, gateway_cfg(secrets_file=str(secrets))), ENV)
    assert [k.get_secret_value() for k in cfg.admin_api_keys] == [ADMIN_KEY, ADMIN_KEY_2]
    assert cfg.tracing.header_pairs() == [("authorization", "Bearer otlp-secret-token-42")]


# --------------------------------------------------------------------------- health_port (1.8)


def test_health_port_equal_to_http_port_exits_2(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], no_startup: list[str]
) -> None:
    path = write(tmp_path, gateway_cfg(http_port=9000, health_port=9000))
    with pytest.raises(ConfigError) as ei:
        load_config(path, ENV)
    assert ei.value.fields == ("health_port",)

    assert run_main(path, ENV) == EXIT_CONFIG_ERROR
    out = capsys.readouterr()
    assert "health_port" in out.out + out.err
    assert no_startup == []


def test_health_port_equal_to_default_http_port_via_env(tmp_path: Path) -> None:
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, gateway_cfg()), {**ENV, "DAS__HEALTH_PORT": "8080"})
    assert ei.value.fields == ("health_port",)


@pytest.mark.parametrize("port", [0, 65536])
def test_health_port_out_of_range(tmp_path: Path, port: int) -> None:
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, gateway_cfg(health_port=port)), ENV)
    assert "health_port" in ei.value.fields


def test_distinct_health_port_is_accepted(tmp_path: Path) -> None:
    cfg = load_config(write(tmp_path, gateway_cfg(http_port=9000, health_port=9001)), ENV)
    assert (cfg.http_port, cfg.health_port) == (9000, 9001)


# --------------------------------------------------------------------------- DAS__ADMIN_API_KEYS (11.1, 11.10)


def test_admin_api_keys_env_trims_whitespace_and_skips_empty(tmp_path: Path) -> None:
    env = {**ENV, "DAS__ADMIN_API_KEYS": f"  {ADMIN_KEY} ,, \t{ADMIN_KEY_2}\t , "}
    cfg = load_config(write(tmp_path, gateway_cfg()), env)
    assert [k.get_secret_value() for k in cfg.admin_api_keys] == [ADMIN_KEY, ADMIN_KEY_2]


def test_invalid_admin_keys_reported_by_index_without_value(tmp_path: Path) -> None:
    short = "too-short-key"  # 13 chars, below the 16 minimum
    env = {**ENV, "DAS__ADMIN_API_KEYS": f"{ADMIN_KEY},{short},{API_KEY}"}
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, gateway_cfg()), env)
    assert set(ei.value.fields) == {"admin_api_keys[1]", "admin_api_keys[2]"}
    text = str(ei.value)
    for value in (ADMIN_KEY, short, API_KEY):
        assert value[:8] not in text


def test_too_many_admin_keys(tmp_path: Path) -> None:
    keys = ",".join(f"admin-key-{i:016d}" for i in range(33))
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, gateway_cfg()), {**ENV, "DAS__ADMIN_API_KEYS": keys})
    assert "admin_api_keys" in ei.value.fields


# --------------------------------------------------------------------------- precedence (19.1)


def test_tracing_enabled_precedence_env_over_file_over_default(tmp_path: Path) -> None:
    endpoint = {"otlp_endpoint": "http://otel-collector:4317"}

    # default
    assert load_config(write(tmp_path, gateway_cfg()), ENV).tracing.enabled is False
    # file over default
    file_on = write(tmp_path, gateway_cfg(tracing={"enabled": True, **endpoint}))
    assert load_config(file_on, ENV).tracing.enabled is True
    # env over file
    assert load_config(file_on, {**ENV, "DAS__TRACING__ENABLED": "false"}).tracing.enabled is False
    file_off = write(tmp_path, gateway_cfg(tracing={"enabled": False, **endpoint}))
    cfg = load_config(file_off, {**ENV, "DAS__TRACING__ENABLED": "true"})
    assert cfg.tracing.enabled is True
    # env-only nested field keeps sibling file values
    assert cfg.tracing.otlp_endpoint == "http://otel-collector:4317"


def test_tracing_enabled_by_env_without_endpoint_names_field(tmp_path: Path) -> None:
    with pytest.raises(ConfigError) as ei:
        load_config(write(tmp_path, gateway_cfg()), {**ENV, "DAS__TRACING__ENABLED": "true"})
    assert ei.value.fields == ("tracing.otlp_endpoint",)
