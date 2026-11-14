"""The ``http`` and ``http_broker`` configuration sections (das-scale-resilience task 9.4; Phase 7 Req 2.1, 2.2).

An agent that uses the HTTP transport names the broker and its token; the Gateway serves the broker only when asked,
with a token; neither token is ever read from the YAML file, and a Manager or Worker that uses HTTP for its jobs needs
no RabbitMQ."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from das.config import load_config
from das.errors import ConfigError

LLM_KEY = {"DAS__LLM__API_KEY": "sk-test-cafebabe0123456789"}
HTTP_TOKEN = {"DAS__HTTP__TOKEN": "agent-broker-token-0123456789"}


def worker(**sections: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "role": "worker", "host": "10.0.0.7", "transports": {"jobs": "http", "control": "grpc"},
        "redis": {"url": "redis://redis.internal:6379/0"},
        "http": {"url": "http://gateway.internal:8081"},
        "grpc": {"bind": "0.0.0.0:50051", "advertise_host": "10.0.0.7", "advertise_port": 50051},
        "llm": {"provider": "anthropic", "model": "big-model"}, "worker": {"capabilities": ["research"]},
    }
    cfg.update(sections)
    return {k: v for k, v in cfg.items() if v is not None}


def gateway(**sections: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {"role": "gateway", "host": "10.0.0.8", "redis": {"url": "redis://redis.internal:6379/0"}}
    cfg.update(sections)
    return cfg


def load(tmp_path: Path, cfg: dict[str, Any], env: dict[str, str] | None = None):  # noqa: ANN201
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    base: dict[str, str] = {}
    if cfg["role"] == "gateway":
        base["DAS__API_KEYS"] = "gateway-key-0000000001"
    if "llm" in cfg:  # a secret in the environment creates its section, so only give it to a config that has one
        base.update(LLM_KEY)
    if "http" in cfg:
        base.update(HTTP_TOKEN)
    return load_config(path, {**base, **(env or {})})


def refused(tmp_path: Path, cfg: dict[str, Any], env: dict[str, str] | None = None) -> ConfigError:
    with pytest.raises(ConfigError) as info:
        load(tmp_path, cfg, env)
    return info.value


# --------------------------------------------------------------------------- the http section (Req 2.1)


def test_the_http_section_is_absent_by_default(tmp_path: Path) -> None:
    cfg = worker(http=None, transports={"jobs": "rabbitmq", "control": "grpc"}, rabbitmq={"url": "amqp://r:5672/"})
    assert load(tmp_path, cfg).http is None


def test_a_worker_can_use_http_for_its_jobs_without_any_rabbitmq(tmp_path: Path) -> None:
    http = load(tmp_path, worker()).http
    assert http is not None and http.url == "http://gateway.internal:8081" and http.poll_wait == 20
    assert http.token is not None and http.token.get_secret_value() == "agent-broker-token-0123456789"


def test_a_manager_can_use_http_for_its_jobs_too(tmp_path: Path) -> None:
    cfg = worker(worker=None)
    cfg["role"] = "manager"
    assert load(tmp_path, cfg).http is not None


def test_using_http_without_the_http_section_is_refused_naming_the_section(tmp_path: Path) -> None:
    assert "http" in refused(tmp_path, worker(http=None)).fields


def test_using_http_without_a_token_is_refused_naming_the_token(tmp_path: Path) -> None:
    error = refused(tmp_path, worker(), {"DAS__HTTP__TOKEN": ""})
    assert "http.token" in error.fields


def test_the_http_transport_may_serve_the_control_channel_too(tmp_path: Path) -> None:
    assert load(tmp_path, worker(transports={"jobs": "http", "control": "http"})).http is not None


def test_rabbitmq_is_still_required_when_a_transport_uses_it(tmp_path: Path) -> None:
    cfg = worker(transports={"jobs": "rabbitmq", "control": "grpc"})
    assert "rabbitmq" in refused(tmp_path, cfg).fields


@pytest.mark.parametrize("url", ["", "gateway:8081", "ftp://gateway", "http://", "amqp://x"])
def test_the_broker_url_must_be_http_or_https(tmp_path: Path, url: str) -> None:
    assert "http.url" in refused(tmp_path, worker(http={"url": url})).fields


@pytest.mark.parametrize("wait", [0.5, 30.5, 0, -1])
def test_the_poll_wait_must_be_1_to_30_seconds(tmp_path: Path, wait: float) -> None:
    assert "http.poll_wait" in refused(tmp_path, worker(http={"url": "http://g:1", "poll_wait": wait})).fields


@pytest.mark.parametrize("wait", [1, 20, 30])
def test_a_poll_wait_inside_the_range_is_kept(tmp_path: Path, wait: float) -> None:
    http = load(tmp_path, worker(http={"url": "http://g:1", "poll_wait": wait})).http
    assert http is not None and http.poll_wait == wait


def test_a_token_in_the_configuration_file_is_refused(tmp_path: Path) -> None:
    error = refused(tmp_path, worker(http={"url": "http://g:1", "token": "from-the-file-0123456789"}))
    assert "http.token" in error.fields and "must not appear in the configuration file" in str(error)


# --------------------------------------------------------------------------- the http_broker section (Req 2.2)


def test_the_broker_is_off_by_default(tmp_path: Path) -> None:
    broker = load(tmp_path, gateway()).http_broker
    assert broker.enabled is False and broker.token is None and broker.visibility_timeout == 30


def test_a_gateway_serves_the_broker_when_enabled_with_a_token(tmp_path: Path) -> None:
    broker = load(tmp_path, gateway(http_broker={"enabled": True, "visibility_timeout": 60}),
                  {"DAS__HTTP_BROKER__TOKEN": "broker-token-0123456789"}).http_broker
    assert broker.enabled and broker.visibility_timeout == 60
    assert broker.token is not None and broker.token.get_secret_value() == "broker-token-0123456789"


def test_enabling_the_broker_without_a_token_is_refused_naming_the_token(tmp_path: Path) -> None:
    assert "http_broker.token" in refused(tmp_path, gateway(http_broker={"enabled": True})).fields


def test_a_token_with_the_broker_disabled_is_allowed_and_unused(tmp_path: Path) -> None:
    cfg = gateway(http_broker={"enabled": False})
    assert load(tmp_path, cfg, {"DAS__HTTP_BROKER__TOKEN": "broker-token-0123456789"}).http_broker.enabled is False


@pytest.mark.parametrize("role", ["manager", "worker"])
def test_only_a_gateway_can_serve_the_broker(tmp_path: Path, role: str) -> None:
    cfg = worker(http_broker={"enabled": True})
    cfg["role"] = role
    if role == "manager":
        cfg.pop("worker")
    error = refused(tmp_path, cfg, {"DAS__HTTP_BROKER__TOKEN": "broker-token-0123456789"})
    assert "http_broker.enabled" in error.fields


@pytest.mark.parametrize("seconds", [4, 601, 0])
def test_the_visibility_timeout_must_be_5_to_600_seconds(tmp_path: Path, seconds: float) -> None:
    cfg = gateway(http_broker={"enabled": True, "visibility_timeout": seconds})
    error = refused(tmp_path, cfg, {"DAS__HTTP_BROKER__TOKEN": "broker-token-0123456789"})
    assert "http_broker.visibility_timeout" in error.fields


@pytest.mark.parametrize("seconds", [5, 30, 600])
def test_a_visibility_timeout_inside_the_range_is_kept(tmp_path: Path, seconds: float) -> None:
    cfg = gateway(http_broker={"enabled": True, "visibility_timeout": seconds})
    broker = load(tmp_path, cfg, {"DAS__HTTP_BROKER__TOKEN": "broker-token-0123456789"}).http_broker
    assert broker.visibility_timeout == seconds


def test_the_broker_token_in_the_configuration_file_is_refused(tmp_path: Path) -> None:
    error = refused(tmp_path, gateway(http_broker={"enabled": True, "token": "from-the-file-0123456789"}))
    assert "http_broker.token" in error.fields and "must not appear in the configuration file" in str(error)


def test_both_tokens_are_redacted_from_logs(tmp_path: Path) -> None:
    env = {"DAS__HTTP_BROKER__TOKEN": "broker-token-0123456789"}
    broker = load(tmp_path, gateway(http_broker={"enabled": True}), env)
    assert "broker-token-0123456789" in broker.secret_values()
    assert "broker-token-0123456789" not in repr(broker)
    agent = load(tmp_path, worker())
    assert "agent-broker-token-0123456789" in agent.secret_values()
    assert "agent-broker-token-0123456789" not in repr(agent)


def test_a_token_that_is_too_short_to_be_a_secret_is_refused(tmp_path: Path) -> None:
    error = refused(tmp_path, gateway(http_broker={"enabled": True}), {"DAS__HTTP_BROKER__TOKEN": "short"})
    assert "http_broker.token" in error.fields
