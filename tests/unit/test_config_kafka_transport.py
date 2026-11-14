"""The ``kafka`` configuration section (das-scale-resilience task 10.3; Phase 7 Req 3.1, 3.2, 3.9).

An agent that uses the Kafka transport names the cluster; the protocol rules say what a SASL or TLS cluster needs and
refuse credentials the protocol would never use; the passwords come only from the environment or the secrets file; and
a Manager or Worker on Kafka needs no RabbitMQ."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from das.config import load_config
from das.errors import ConfigError
from das.transport.kafka import KafkaTransport

LLM_KEY = {"DAS__LLM__API_KEY": "sk-test-cafebabe0123456789"}
PASSWORD = "sasl-password-0123456789"
KEY_PASSWORD = "tls-key-password-0123"


def worker(**sections: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "role": "worker", "host": "10.0.0.7", "transports": {"jobs": "kafka", "control": "grpc"},
        "redis": {"url": "redis://redis.internal:6379/0"},
        "kafka": {"bootstrap_servers": ["kafka-1:9092", "kafka-2:9092"]},
        "grpc": {"bind": "0.0.0.0:50051", "advertise_host": "10.0.0.7", "advertise_port": 50051},
        "llm": {"provider": "anthropic", "model": "big-model"}, "worker": {"capabilities": ["research"]},
    }
    cfg.update(sections)
    return {k: v for k, v in cfg.items() if v is not None}


def load(tmp_path: Path, cfg: dict[str, Any], env: dict[str, str] | None = None):  # noqa: ANN201
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    base = dict(LLM_KEY) if "llm" in cfg else {}  # a secret in the environment creates its section
    return load_config(path, {**base, **(env or {})})


def refused(tmp_path: Path, cfg: dict[str, Any], env: dict[str, str] | None = None) -> ConfigError:
    with pytest.raises(ConfigError) as info:
        load(tmp_path, cfg, env)
    return info.value


def with_kafka(**fields: Any) -> dict[str, Any]:
    return worker(kafka={"bootstrap_servers": ["kafka-1:9092"], **fields})


# ----------------------------------------------------------------------- the section and its defaults (Req 3.1, 3.2)


def test_the_section_is_absent_by_default(tmp_path: Path) -> None:
    cfg = worker(kafka=None, transports={"jobs": "rabbitmq", "control": "grpc"}, rabbitmq={"url": "amqp://r:5672/"})
    assert load(tmp_path, cfg).kafka is None


def test_a_worker_can_use_kafka_without_any_rabbitmq_and_gets_the_documented_defaults(tmp_path: Path) -> None:
    kafka = load(tmp_path, worker()).kafka
    assert kafka is not None and kafka.bootstrap_servers == ["kafka-1:9092", "kafka-2:9092"]
    assert (kafka.security_protocol, kafka.sasl_mechanism) == ("PLAINTEXT", "PLAIN")
    assert (kafka.partitions, kafka.replication_factor, kafka.auto_create_topics, kafka.retry_delay) == (3, 1, True, 1)
    assert kafka.sasl_password is None and kafka.ssl_key_password is None


def test_using_kafka_without_the_section_is_refused_naming_it(tmp_path: Path) -> None:
    assert "kafka" in refused(tmp_path, worker(kafka=None)).fields


def test_rabbitmq_is_still_required_when_a_transport_uses_it(tmp_path: Path) -> None:
    cfg = worker(transports={"jobs": "rabbitmq", "control": "grpc"})
    assert "rabbitmq" in refused(tmp_path, cfg).fields


def test_the_bootstrap_servers_may_come_as_one_comma_separated_value(tmp_path: Path) -> None:
    cfg = worker(kafka={"bootstrap_servers": "a:9092, b:9092"})
    assert load(tmp_path, cfg).kafka.bootstrap_servers == ["a:9092", "b:9092"]  # type: ignore[union-attr]


@pytest.mark.parametrize("servers", [
    [], ["host"], ["host:0"], ["host:65536"], ["host:09092"], [":9092"], ["bad host:9092"], ["h:1"] * 11, "",
])
def test_bad_bootstrap_servers_are_refused_naming_the_field(tmp_path: Path, servers: Any) -> None:
    assert "kafka.bootstrap_servers" in refused(tmp_path, worker(kafka={"bootstrap_servers": servers})).fields


@pytest.mark.parametrize("fields", [
    {"partitions": 0}, {"partitions": 101}, {"replication_factor": 0}, {"replication_factor": 6},
    {"retry_delay": -1}, {"retry_delay": 61},
])
def test_a_value_outside_its_range_is_refused_naming_the_field(tmp_path: Path, fields: dict[str, Any]) -> None:
    assert f"kafka.{next(iter(fields))}" in refused(tmp_path, with_kafka(**fields)).fields


@pytest.mark.parametrize("fields", [
    {"partitions": 1}, {"partitions": 100}, {"replication_factor": 1}, {"replication_factor": 5},
    {"retry_delay": 0}, {"retry_delay": 60}, {"auto_create_topics": False},
])
def test_a_value_on_the_edge_of_its_range_is_kept(tmp_path: Path, fields: dict[str, Any]) -> None:
    kafka = load(tmp_path, with_kafka(**fields)).kafka
    assert kafka is not None and all(getattr(kafka, name) == value for name, value in fields.items())


def test_an_unknown_kafka_setting_is_refused(tmp_path: Path) -> None:
    assert "kafka.bogus" in refused(tmp_path, with_kafka(bogus=1)).fields


# --------------------------------------------------------------------------- security (Req 3.9)


@pytest.mark.parametrize(("given", "stored"), [("plaintext", "PLAINTEXT"), (" Ssl ", "SSL"), ("sasl_ssl", "SASL_SSL")])
def test_the_security_protocol_is_case_insensitive(tmp_path: Path, given: str, stored: str) -> None:
    fields: dict[str, Any] = {"security_protocol": given}
    if stored.startswith("SASL"):
        fields["sasl_username"] = "das"
    env = {"DAS__KAFKA__SASL_PASSWORD": PASSWORD} if stored.startswith("SASL") else None
    assert load(tmp_path, with_kafka(**fields), env).kafka.security_protocol == stored  # type: ignore[union-attr]


def test_an_unknown_security_protocol_is_refused(tmp_path: Path) -> None:
    assert "kafka.security_protocol" in refused(tmp_path, with_kafka(security_protocol="KERBEROS")).fields


@pytest.mark.parametrize("protocol", ["SASL_PLAINTEXT", "SASL_SSL"])
def test_a_sasl_protocol_needs_its_username_and_password(tmp_path: Path, protocol: str) -> None:
    error = refused(tmp_path, with_kafka(security_protocol=protocol))
    assert {"kafka.sasl_username", "kafka.sasl_password"} <= set(error.fields)


def test_a_sasl_cluster_is_configured_with_the_password_from_the_environment(tmp_path: Path) -> None:
    cfg = with_kafka(security_protocol="SASL_SSL", sasl_mechanism="scram-sha-512", sasl_username="das")
    kafka = load(tmp_path, cfg, {"DAS__KAFKA__SASL_PASSWORD": PASSWORD}).kafka
    assert kafka is not None and kafka.sasl_mechanism == "SCRAM-SHA-512" and kafka.sasl_username == "das"
    assert kafka.sasl_password is not None and kafka.sasl_password.get_secret_value() == PASSWORD


def test_an_unknown_sasl_mechanism_is_refused(tmp_path: Path) -> None:
    cfg = with_kafka(security_protocol="SASL_SSL", sasl_username="das", sasl_mechanism="GSSAPI")
    assert "kafka.sasl_mechanism" in refused(tmp_path, cfg, {"DAS__KAFKA__SASL_PASSWORD": PASSWORD}).fields


@pytest.mark.parametrize("protocol", ["PLAINTEXT", "SSL"])
def test_credentials_beside_a_protocol_that_does_not_use_them_are_refused(tmp_path: Path, protocol: str) -> None:
    error = refused(tmp_path, with_kafka(security_protocol=protocol, sasl_username="das"),
                    {"DAS__KAFKA__SASL_PASSWORD": PASSWORD})
    assert {"kafka.sasl_username", "kafka.sasl_password"} <= set(error.fields)


@pytest.mark.parametrize("protocol", ["PLAINTEXT", "SASL_PLAINTEXT"])
def test_tls_files_beside_a_protocol_without_tls_are_refused(tmp_path: Path, protocol: str) -> None:
    fields: dict[str, Any] = {"security_protocol": protocol, "ssl_cafile": "/etc/ca.pem"}
    env: dict[str, str] = {}
    if protocol == "SASL_PLAINTEXT":
        fields["sasl_username"] = "das"
        env["DAS__KAFKA__SASL_PASSWORD"] = PASSWORD
    assert "kafka.ssl_cafile" in refused(tmp_path, with_kafka(**fields), env).fields


def test_mutual_tls_takes_a_certificate_with_its_key_and_the_key_password_from_the_environment(tmp_path: Path) -> None:
    cfg = with_kafka(
        security_protocol="SSL", ssl_cafile="/etc/ca.pem", ssl_certfile="/etc/c.pem", ssl_keyfile="/etc/k.pem"
    )
    kafka = load(tmp_path, cfg, {"DAS__KAFKA__SSL_KEY_PASSWORD": KEY_PASSWORD}).kafka
    assert kafka is not None and kafka.ssl_certfile == "/etc/c.pem" and kafka.ssl_keyfile == "/etc/k.pem"
    assert kafka.ssl_key_password is not None and kafka.ssl_key_password.get_secret_value() == KEY_PASSWORD


@pytest.mark.parametrize("only", ["ssl_certfile", "ssl_keyfile"])
def test_a_certificate_without_its_key_or_the_reverse_is_refused(tmp_path: Path, only: str) -> None:
    assert "kafka.ssl_keyfile" in refused(tmp_path, with_kafka(security_protocol="SSL", **{only: "/etc/x.pem"})).fields


@pytest.mark.parametrize(("field", "value"), [("sasl_password", PASSWORD), ("ssl_key_password", KEY_PASSWORD)])
def test_a_password_in_the_configuration_file_is_refused(tmp_path: Path, field: str, value: str) -> None:
    error = refused(tmp_path, with_kafka(**{field: value}))
    assert f"kafka.{field}" in error.fields and "must not appear in the configuration file" in str(error)
    assert value not in str(error)


def test_both_passwords_are_redacted_from_logs(tmp_path: Path) -> None:
    cfg = with_kafka(security_protocol="SASL_SSL", sasl_username="das", ssl_cafile="/etc/ca.pem")
    env = {"DAS__KAFKA__SASL_PASSWORD": PASSWORD, "DAS__KAFKA__SSL_KEY_PASSWORD": KEY_PASSWORD}
    config = load(tmp_path, cfg, env)
    assert {PASSWORD, KEY_PASSWORD} <= set(config.secret_values())
    assert PASSWORD not in repr(config) and KEY_PASSWORD not in repr(config)


# --------------------------------------------------------------------------- the adapter from the configuration


def test_the_adapter_is_built_from_the_section_and_the_limits(tmp_path: Path) -> None:
    cfg = with_kafka(
        security_protocol="SASL_SSL", sasl_username="das", partitions=6, replication_factor=3,
        auto_create_topics=False, retry_delay=2.5, ssl_cafile="/etc/ca.pem",
    )
    config = load(tmp_path, cfg, {"DAS__KAFKA__SASL_PASSWORD": PASSWORD})
    transport = KafkaTransport.from_config(config)
    s = transport.settings
    assert (s.bootstrap_servers, s.security_protocol, s.sasl_username) == (("kafka-1:9092",), "SASL_SSL", "das")
    assert s.sasl_password == PASSWORD and s.ssl_cafile == "/etc/ca.pem"
    assert (s.partitions, s.replication_factor, s.auto_create_topics, s.retry_delay) == (6, 3, False, 2.5)
    assert s.delivery_limit == config.limits.max_attempts - 1
    assert transport.request_reply_timeout == config.limits.request_reply_timeout


def test_the_adapter_refuses_a_configuration_without_the_section(tmp_path: Path) -> None:
    config = load(tmp_path, worker(kafka=None, transports={"jobs": "grpc", "control": "grpc"}))
    with pytest.raises(ValueError, match="no kafka section"):
        KafkaTransport.from_config(config)
