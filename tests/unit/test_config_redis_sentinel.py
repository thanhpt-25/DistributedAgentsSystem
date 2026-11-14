"""The ``redis`` configuration section with Sentinel (das-scale-resilience task 8.1; Phase 7 Req 1.1 to 1.3): a
standalone Redis as before, a Sentinel-managed master by ``sentinels`` and ``master_name``, Redis Cluster refused with
its reason, and every refusal naming the field."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from das.config import load_config
from das.errors import ConfigError

ENV = {"DAS__LLM__API_KEY": "sk-test-cafebabe0123456789"}


def worker_cfg(redis: dict[str, Any]) -> dict[str, Any]:
    return {
        "role": "worker", "host": "10.0.0.7", "transports": {"jobs": "rabbitmq", "control": "grpc"},
        "rabbitmq": {"url": "amqp://rabbit.internal:5672/"}, "redis": redis,
        "grpc": {"bind": "0.0.0.0:50051", "advertise_host": "10.0.0.7", "advertise_port": 50051},
        "llm": {"provider": "anthropic", "model": "big-model"}, "worker": {"capabilities": ["research"]},
    }


def load(tmp_path: Path, redis: dict[str, Any], env: dict[str, str] | None = None):  # noqa: ANN201
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(worker_cfg(redis)), encoding="utf-8")
    return load_config(path, {**ENV, **(env or {})})


def refused(tmp_path: Path, redis: dict[str, Any], env: dict[str, str] | None = None) -> ConfigError:
    with pytest.raises(ConfigError) as info:
        load(tmp_path, redis, env)
    return info.value


SENTINEL: dict[str, Any] = {
    "mode": "sentinel", "sentinels": ["sentinel-1:26379", "sentinel-2:26379", "sentinel-3:26379"],
    "master_name": "das-master",
}


# --------------------------------------------------------------------------- standalone is as before (Req 1.1)


def test_a_configuration_with_only_a_url_is_standalone_and_unchanged(tmp_path: Path) -> None:
    redis = load(tmp_path, {"url": "redis://redis.internal:6379/0"}).redis
    assert redis is not None and redis.mode == "standalone" and redis.url == "redis://redis.internal:6379/0"
    assert redis.sentinels is None and redis.master_name is None


def test_standalone_may_name_its_mode(tmp_path: Path) -> None:
    redis = load(tmp_path, {"mode": "standalone", "url": "redis://r:6379/1"}).redis
    assert redis is not None and redis.mode == "standalone"


def test_standalone_without_a_url_is_refused(tmp_path: Path) -> None:
    assert "redis.url" in refused(tmp_path, {}).fields


@pytest.mark.parametrize("extra", [
    {"sentinels": ["s:26379"]}, {"master_name": "m"}, {"db": 3}, {"sentinel_tls": True},
])
def test_standalone_allows_only_a_url_and_names_the_extra_field(tmp_path: Path, extra: dict[str, Any]) -> None:
    error = refused(tmp_path, {"url": "redis://r:6379/0", **extra})
    assert f"redis.{next(iter(extra))}" in error.fields


# --------------------------------------------------------------------------- sentinel (Req 1.2)


def test_a_sentinel_configuration_loads(tmp_path: Path) -> None:
    redis = load(tmp_path, SENTINEL).redis
    assert redis is not None and redis.mode == "sentinel" and redis.url is None
    assert redis.sentinels == ["sentinel-1:26379", "sentinel-2:26379", "sentinel-3:26379"]
    assert redis.master_name == "das-master" and redis.db == 0 and redis.sentinel_tls is False


def test_a_sentinel_configuration_may_set_db_and_tls(tmp_path: Path) -> None:
    redis = load(tmp_path, {**SENTINEL, "db": 15, "sentinel_tls": True}).redis
    assert redis is not None and redis.db == 15 and redis.sentinel_tls is True


def test_the_sentinel_list_can_come_from_the_environment_as_text(tmp_path: Path) -> None:
    cfg = {"mode": "sentinel", "master_name": "m"}
    redis = load(tmp_path, cfg, {"DAS__REDIS__SENTINELS": "a:26379, b:26379"}).redis
    assert redis is not None and redis.sentinels == ["a:26379", "b:26379"]


def test_sentinel_without_sentinels_is_refused_naming_the_field(tmp_path: Path) -> None:
    assert "redis.sentinels" in refused(tmp_path, {"mode": "sentinel", "master_name": "m"}).fields


def test_sentinel_without_a_master_name_is_refused_naming_the_field(tmp_path: Path) -> None:
    assert "redis.master_name" in refused(tmp_path, {"mode": "sentinel", "sentinels": ["s:26379"]}).fields


def test_a_url_together_with_sentinels_is_refused_naming_the_url(tmp_path: Path) -> None:
    assert "redis.url" in refused(tmp_path, {**SENTINEL, "url": "redis://r:6379/0"}).fields


@pytest.mark.parametrize("count", [0, 11])
def test_one_to_ten_sentinels_are_allowed(tmp_path: Path, count: int) -> None:
    sentinels = [f"s{i}:26379" for i in range(count)]
    assert "redis.sentinels" in refused(tmp_path, {**SENTINEL, "sentinels": sentinels}).fields


@pytest.mark.parametrize("count", [1, 10])
def test_the_bounds_of_the_sentinel_list_are_accepted(tmp_path: Path, count: int) -> None:
    sentinels = [f"s{i}:26379" for i in range(count)]
    redis = load(tmp_path, {**SENTINEL, "sentinels": sentinels}).redis
    assert redis is not None and redis.sentinels is not None and len(redis.sentinels) == count


@pytest.mark.parametrize("bad", ["sentinel", "sentinel:", ":26379", "sentinel:0", "sentinel:70000", "sentinel:abc",
                                  "http://sentinel:26379", "a b:26379", "sentinel:26379:1", ""])
def test_a_sentinel_address_must_be_host_and_port(tmp_path: Path, bad: str) -> None:
    assert any(f.startswith("redis.sentinels") for f in refused(tmp_path, {**SENTINEL, "sentinels": [bad]}).fields)


@pytest.mark.parametrize("name", ["", "a b", "x" * 65, "master/1", "-x"])
def test_the_master_name_must_be_a_plain_name(tmp_path: Path, name: str) -> None:
    assert "redis.master_name" in refused(tmp_path, {**SENTINEL, "master_name": name}).fields


@pytest.mark.parametrize("db", [-1, 16, 100])
def test_db_must_be_between_0_and_15(tmp_path: Path, db: int) -> None:
    assert "redis.db" in refused(tmp_path, {**SENTINEL, "db": db}).fields


@pytest.mark.parametrize("db", [0, 7, 15])
def test_db_inside_the_range_is_kept(tmp_path: Path, db: int) -> None:
    redis = load(tmp_path, {**SENTINEL, "db": db}).redis
    assert redis is not None and redis.db == db


# ------------------------------------------------------------------- cluster is refused with its reason (Req 1.3)


def test_cluster_mode_is_refused_with_the_reason(tmp_path: Path) -> None:
    error = refused(tmp_path, {"mode": "cluster", "url": "redis://r:6379/0"})
    assert "redis.mode" in error.fields
    text = str(error)
    assert "Cluster" in text and "slots" in text and "not supported" in text


@pytest.mark.parametrize("mode", ["Cluster", "CLUSTER", "cluster "])
def test_cluster_is_refused_however_it_is_written(tmp_path: Path, mode: str) -> None:
    error = refused(tmp_path, {"mode": mode, "url": "redis://r:6379/0"})
    assert "redis.mode" in error.fields and "slots" in str(error)  # with the reason, not as an unknown mode


@pytest.mark.parametrize("mode", ["Sentinel", " sentinel ", "SENTINEL"])
def test_the_mode_is_read_without_regard_to_case_or_spaces(tmp_path: Path, mode: str) -> None:
    redis = load(tmp_path, {**SENTINEL, "mode": mode}).redis
    assert redis is not None and redis.mode == "sentinel"


@pytest.mark.parametrize("mode", ["replica", "", "sentinels", "master"])
def test_an_unknown_mode_is_refused_naming_the_field(tmp_path: Path, mode: str) -> None:
    assert "redis.mode" in refused(tmp_path, {"mode": mode, "url": "redis://r:6379/0"}).fields


# --------------------------------------------------------------------------- credentials (secrets only)


def test_the_passwords_come_from_the_environment_and_are_redacted(tmp_path: Path) -> None:
    env = {
        "DAS__REDIS__PASSWORD": "master-pass-0123456789",
        "DAS__REDIS__SENTINEL_PASSWORD": "sentinel-pass-0123456789",
    }
    config = load(tmp_path, SENTINEL, env)
    assert config.redis is not None and config.redis.password is not None
    assert config.redis.password.get_secret_value() == "master-pass-0123456789"
    assert "master-pass-0123456789" in config.secret_values()
    assert "sentinel-pass-0123456789" in config.secret_values()
    assert "master-pass-0123456789" not in repr(config)


@pytest.mark.parametrize("field", ["password", "sentinel_password"])
def test_a_password_in_the_configuration_file_is_refused(tmp_path: Path, field: str) -> None:
    error = refused(tmp_path, {**SENTINEL, field: "a-password-0123456789"})
    assert f"redis.{field}" in error.fields and "must not appear in the configuration file" in str(error)


def test_a_password_in_standalone_mode_is_allowed_from_the_environment(tmp_path: Path) -> None:
    config = load(tmp_path, {"url": "redis://r:6379/0"}, {"DAS__REDIS__PASSWORD": "pw-0123456789"})
    assert config.redis is not None and config.redis.password is not None
