"""Unit tests for the Transport loader (Requirements 8.4, 8.5, 15.1, 15.2, 15.3, 15.5)."""

from __future__ import annotations

import tomllib
from importlib.metadata import EntryPoint
from pathlib import Path

import pytest

from das.errors import EXIT_TRANSPORT_ERROR, TransportIncomplete, TransportNotInstalled, exit_code_for
from das.transport.loader import ENTRY_POINT_GROUP, available_transports, load_transport

_MODULE = __name__


class CompleteAdapter:
    async def publish(self, destination, message, headers=None): ...
    async def subscribe(self, destination, handler, options=None): ...
    async def acknowledge(self, delivery, outcome=None): ...
    async def request_reply(self, destination, message, timeout=None): ...


class PartialAdapter:
    async def publish(self, destination, message, headers=None): ...
    async def subscribe(self, destination, handler, options=None): ...


def _ep(name: str, target: str) -> EntryPoint:
    return EntryPoint(name=name, value=target, group=ENTRY_POINT_GROUP)


def _source(*eps: EntryPoint):
    return lambda: list(eps)


def test_loads_registered_adapter_without_code_changes() -> None:
    # A third-party http/kafka adapter only needs an entry point (15.1, 15.2).
    source = _source(_ep("http", f"{_MODULE}:CompleteAdapter"),
                     _ep("kafka", f"{_MODULE}:CompleteAdapter"))
    assert load_transport("http", source) is CompleteAdapter
    assert load_transport("kafka", source) is CompleteAdapter
    assert available_transports(source) == ["http", "kafka"]


def test_unknown_name_raises_not_installed_naming_it() -> None:
    with pytest.raises(TransportNotInstalled) as info:
        load_transport("kafka", _source(_ep("grpc", f"{_MODULE}:CompleteAdapter")))
    assert info.value.name == "kafka"
    assert "kafka" in str(info.value)
    assert exit_code_for(info.value) == EXIT_TRANSPORT_ERROR != 0


def test_unimportable_target_raises_not_installed() -> None:
    source = _source(_ep("ghost", "das.transport.does_not_exist:Ghost"),
                     _ep("noattr", f"{_MODULE}:NoSuchClass"))
    for name in ("ghost", "noattr"):
        with pytest.raises(TransportNotInstalled) as info:
            load_transport(name, source)
        assert name in str(info.value)


def test_incomplete_adapter_names_missing_operations() -> None:
    with pytest.raises(TransportIncomplete) as info:
        load_transport("partial", _source(_ep("partial", f"{_MODULE}:PartialAdapter")))
    assert info.value.name == "partial"
    assert info.value.missing == ("acknowledge", "request_reply")
    assert "partial" in str(info.value) and "request_reply" in str(info.value)
    assert exit_code_for(info.value) == EXIT_TRANSPORT_ERROR


def test_pyproject_registers_the_shipped_transports() -> None:
    # v1 shipped rabbitmq and grpc (15.3); Phase 7 adds http (wave 9) and kafka (wave 10).
    pyproject = Path(__file__).resolve().parents[2] / "pyproject.toml"
    data = tomllib.loads(pyproject.read_text())
    eps = data["project"]["entry-points"][ENTRY_POINT_GROUP]
    assert eps == {
        "rabbitmq": "das.transport.rabbitmq:RabbitMQTransport",
        "grpc": "das.transport.grpc_rpc:GrpcTransport",
        "http": "das.transport.http:HttpTransport",
        "kafka": "das.transport.kafka:KafkaTransport",
    }
