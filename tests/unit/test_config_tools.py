"""The ``tools`` configuration section (das-smarter-agents task 5.2; Phase 6 Req 5.3, 5.9): which tools a Worker
enables, how many rounds a Job may use, how much of a result the model sees, and the allow-list that ``http_fetch``
cannot be enabled without."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from das.config import load_config
from das.errors import ConfigError

ENV = {"DAS__LLM__API_KEY": "sk-test-cafebabe0123456789"}


def worker_cfg(**tools: object) -> dict:
    cfg: dict = {
        "role": "worker",
        "host": "10.0.0.7",
        "transports": {"jobs": "rabbitmq", "control": "grpc"},
        "rabbitmq": {"url": "amqp://rabbit.internal:5672/"},
        "redis": {"url": "redis://redis.internal:6379/0"},
        "grpc": {"bind": "0.0.0.0:50051", "advertise_host": "10.0.0.7", "advertise_port": 50051},
        "llm": {"provider": "anthropic", "model": "big-model"},
        "worker": {"capabilities": ["research"]},
    }
    if tools:
        cfg["tools"] = tools
    return cfg


def load(tmp_path: Path, cfg: dict, env: dict[str, str] | None = None):  # noqa: ANN201
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    return load_config(path, ENV if env is None else {**ENV, **env})


def refused(tmp_path: Path, cfg: dict, env: dict[str, str] | None = None) -> tuple[str, ...]:
    with pytest.raises(ConfigError) as info:
        load(tmp_path, cfg, env)
    return tuple(info.value.fields)


# --------------------------------------------------------------------------- defaults (Req 5.3)


def test_nothing_is_enabled_by_default_and_every_limit_has_a_default(tmp_path: Path) -> None:
    tools = load(tmp_path, worker_cfg()).tools
    assert tools.enabled == [] and tools.max_rounds == 4 and tools.max_result_bytes == 16_384
    assert tools.http_fetch.allowed_hosts == [] and tools.http_fetch.allow_http is False
    assert tools.http_fetch.timeout == 10 and tools.http_fetch.max_bytes == 1_048_576


def test_tools_can_be_enabled_by_name(tmp_path: Path) -> None:
    tools = load(tmp_path, worker_cfg(enabled=["calculator", "current_time"])).tools
    assert tools.enabled == ["calculator", "current_time"]


def test_the_enabled_list_can_come_from_the_environment_as_text(tmp_path: Path) -> None:
    tools = load(tmp_path, worker_cfg(), {"DAS__TOOLS__ENABLED": "calculator, current_time"}).tools
    assert tools.enabled == ["calculator", "current_time"]


def test_an_unknown_tool_is_a_configuration_error_naming_the_field(tmp_path: Path) -> None:
    fields = refused(tmp_path, worker_cfg(enabled=["calculator", "rm_rf"]))
    assert any(f.startswith("tools.enabled") for f in fields)


def test_a_tool_listed_twice_is_refused(tmp_path: Path) -> None:
    fields = refused(tmp_path, worker_cfg(enabled=["calculator", "calculator"]))
    assert any(f.startswith("tools.enabled") for f in fields)


# --------------------------------------------------------------------------- ranges (Req 5.3)


@pytest.mark.parametrize("rounds", [1, 4, 10])
def test_max_rounds_accepts_one_to_ten(tmp_path: Path, rounds: int) -> None:
    assert load(tmp_path, worker_cfg(max_rounds=rounds)).tools.max_rounds == rounds


@pytest.mark.parametrize("rounds", [0, 11, -1])
def test_max_rounds_outside_one_to_ten_is_refused(tmp_path: Path, rounds: int) -> None:
    assert "tools.max_rounds" in refused(tmp_path, worker_cfg(max_rounds=rounds))


@pytest.mark.parametrize("bad", [255, 262_145])
def test_max_result_bytes_is_bounded(tmp_path: Path, bad: int) -> None:
    assert "tools.max_result_bytes" in refused(tmp_path, worker_cfg(max_result_bytes=bad))


@pytest.mark.parametrize(
    ("name", "bad"), [("timeout", 0.5), ("timeout", 61), ("max_bytes", 1023), ("max_bytes", 10_485_761)]
)
def test_the_http_fetch_limits_are_bounded(tmp_path: Path, name: str, bad: float) -> None:
    cfg = worker_cfg(enabled=["http_fetch"], http_fetch={"allowed_hosts": ["example.com"], name: bad})
    assert f"tools.http_fetch.{name}" in refused(tmp_path, cfg)


# --------------------------------------------------------------------------- http_fetch needs an allow-list (Req 5.9)


def test_http_fetch_cannot_be_enabled_without_an_allowed_host(tmp_path: Path) -> None:
    assert "tools.http_fetch.allowed_hosts" in refused(tmp_path, worker_cfg(enabled=["http_fetch"]))


def test_an_empty_allow_list_is_the_same_as_none(tmp_path: Path) -> None:
    cfg = worker_cfg(enabled=["http_fetch"], http_fetch={"allowed_hosts": []})
    assert "tools.http_fetch.allowed_hosts" in refused(tmp_path, cfg)


def test_http_fetch_with_an_allowed_host_loads(tmp_path: Path) -> None:
    cfg = worker_cfg(enabled=["http_fetch"], http_fetch={"allowed_hosts": ["docs.example.com", "example.org"]})
    assert load(tmp_path, cfg).tools.http_fetch.allowed_hosts == ["docs.example.com", "example.org"]


def test_the_allowed_hosts_can_come_from_the_environment(tmp_path: Path) -> None:
    env = {"DAS__TOOLS__ENABLED": "http_fetch", "DAS__TOOLS__HTTP_FETCH__ALLOWED_HOSTS": "a.example.com,b.example.com"}
    assert load(tmp_path, worker_cfg(), env).tools.http_fetch.allowed_hosts == ["a.example.com", "b.example.com"]


def test_hosts_are_not_required_when_http_fetch_is_not_enabled(tmp_path: Path) -> None:
    assert load(tmp_path, worker_cfg(enabled=["calculator"])).tools.http_fetch.allowed_hosts == []


@pytest.mark.parametrize(
    "bad", ["http://example.com", "*.example.com", "exa mple.com", "", "example.com:8080", "ex_ample.com", "-a.com"]
)
def test_an_allowed_host_must_be_a_plain_host_name(tmp_path: Path, bad: str) -> None:
    cfg = worker_cfg(enabled=["http_fetch"], http_fetch={"allowed_hosts": ["example.com", bad]})
    assert any(f.startswith("tools.http_fetch.allowed_hosts") for f in refused(tmp_path, cfg))


# --------------------------------------------------------------------------- only a Worker has tools


@pytest.mark.parametrize("role", ["manager", "gateway"])
def test_enabling_a_tool_on_another_role_is_refused(tmp_path: Path, role: str) -> None:
    cfg = worker_cfg(enabled=["calculator"])
    cfg["role"] = role
    cfg["rabbitmq"]["management_url"] = "http://rabbit.internal:15672"
    cfg["api_keys"] = ["gateway-key-0000000001"]
    assert "tools.enabled" in refused(tmp_path, cfg)


def test_the_section_is_harmless_on_another_role_while_nothing_is_enabled(tmp_path: Path) -> None:
    cfg = worker_cfg(max_rounds=2)
    cfg["role"] = "manager"
    cfg["rabbitmq"]["management_url"] = "http://rabbit.internal:15672"
    assert load(tmp_path, cfg).tools.max_rounds == 2
