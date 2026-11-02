# Feature: distributed-agent-system, Property 36: Configuration validation reports every bad field
"""Property 36: Configuration validation reports every bad field.

*For any* valid configuration with a random subset of fields removed or set out
of range, ``load_config`` raises ``ConfigError`` listing exactly those field
paths with a reason each. Fields include required fields, ranges for retry and
backoff settings, ``base_backoff > max_backoff``, LLM retry count and timeout,
unknown provider, missing model, and missing credentials. It raises before any
connection attempt.

**Validates: Requirements 12.2, 6.8, 10.7, 10.8**
"""

from __future__ import annotations

import copy
import socket
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
import yaml
from hypothesis import given
from hypothesis import strategies as st

from das.config.loader import load_config
from das.config.schema import PROVIDERS
from das.config.secrets import env_name
from das.errors import ConfigError

# A valid worker configuration: the role with the most required sections.
_BASE_FILE: dict[str, Any] = {
    "role": "worker",
    "agent_id": "worker-1",
    "host": "host-1",
    "transports": {"jobs": "rabbitmq", "control": "grpc"},
    "rabbitmq": {"url": "amqp://localhost:5672/"},
    "redis": {"url": "redis://localhost:6379/0"},
    "grpc": {"bind": "0.0.0.0:50051", "advertise_host": "worker-1", "advertise_port": 50051},
    "llm": {"provider": "openai", "model": "gpt-test"},
    "worker": {"capabilities": ["research"]},
}
_BASE_ENV: dict[str, str] = {"DAS__LLM__API_KEY": "sk-test-credential"}

_REMOVE = object()


@dataclass(frozen=True)
class Mutation:
    """Set ``path`` to ``value`` (or remove it), in the file or the env; expect ``field``."""

    path: str
    value: Any
    field: str
    in_env: bool = False
    range_error: bool = False


def _out_of_range(lo: float, hi: float, *, integer: bool) -> st.SearchStrategy[Any]:
    if integer:
        return st.integers(max_value=int(lo) - 1) | st.integers(min_value=int(hi) + 1)
    finite = {"allow_nan": False, "allow_infinity": False}
    return st.floats(max_value=lo, exclude_max=True, **finite) | st.floats(
        min_value=hi, exclude_min=True, **finite
    )


# (path, lo, hi, integer) for fields with a documented range.
_RANGED: tuple[tuple[str, float, float, bool], ...] = (
    ("limits.max_attempts", 1, 10, True),
    ("limits.planning_retries", 0, 5, True),
    ("limits.lease_duration", 5, 300, False),
    ("limits.capability_wait", 1, 3600, False),
    ("limits.max_jobs", 1, 100, True),
    ("llm.retry_count", 0, 10, True),
    ("llm.timeout", 1, 600, False),
    ("llm.temperature", 0, 2, False),
    ("worker.concurrency_limit", 1, 100, True),
    ("grpc.advertise_port", 1, 65535, True),
    ("health_port", 1, 65535, True),
)

# Required fields that are reported by path when removed.
_REQUIRED = ("rabbitmq.url", "redis.url", "grpc.bind", "grpc.advertise_host")


@st.composite
def _range_mutation(draw: st.DrawFn, path: str, lo: float, hi: float, integer: bool) -> Mutation:
    value = draw(_out_of_range(lo, hi, integer=integer))
    in_env = draw(st.booleans())
    return Mutation(path, str(value) if in_env else value, path, in_env, range_error=True)


@st.composite
def _backoff_mutations(draw: st.DrawFn) -> list[Mutation]:
    """At most one interacting backoff case: either bound out of range, or base > max."""
    choice = draw(st.sampled_from(["none", "base", "max", "both", "cross"]))
    base = _range_mutation("limits.base_backoff", 0.1, 60, False)
    top = _range_mutation("limits.max_backoff", 1, 600, False)
    if choice == "base":
        return [draw(base)]
    if choice == "max":
        return [draw(top)]
    if choice == "both":
        return [draw(base), draw(top)]
    if choice == "cross":
        finite = {"allow_nan": False, "allow_infinity": False}
        max_b = draw(st.floats(min_value=1, max_value=59, **finite))
        base_b = draw(st.floats(min_value=max_b, max_value=60, exclude_min=True, **finite))
        return [
            Mutation("limits.max_backoff", max_b, "limits.max_backoff_ok"),
            Mutation("limits.base_backoff", base_b, "limits.base_backoff"),
        ]
    return []


@st.composite
def _provider_mutations(draw: st.DrawFn) -> list[Mutation]:
    """Unknown provider, or a known provider missing its required credential/address."""
    choice = draw(st.sampled_from(["none", "unknown", "no_key", "anthropic_no_key", "ollama"]))
    if choice == "unknown":
        name = draw(st.from_regex(r"\A[a-z][a-z0-9]{0,11}\Z").filter(lambda p: p not in PROVIDERS))
        return [Mutation("llm.provider", name, "llm.provider", draw(st.booleans()))]
    if choice == "no_key":
        return [Mutation("llm.api_key", _REMOVE, "llm.api_key", in_env=True)]
    if choice == "anthropic_no_key":
        return [
            Mutation("llm.provider", "anthropic", "llm.provider_ok"),
            Mutation("llm.api_key", _REMOVE, "llm.api_key", in_env=True),
        ]
    if choice == "ollama":
        return [Mutation("llm.provider", "ollama", "llm.base_url")]
    return []


@st.composite
def _structure_mutations(draw: st.DrawFn) -> list[Mutation]:
    """Missing role, a missing role-required section, or a missing field within it."""
    choice = draw(st.sampled_from(["none", "role", "section", "field"]))
    if choice == "role":
        return [Mutation("role", _REMOVE, "role")]
    if choice == "section":
        return [Mutation("transports", _REMOVE, "transports")]
    if choice == "field":
        return [Mutation("transports.jobs", _REMOVE, "transports.jobs")]
    return []


@st.composite
def mutated_configs(draw: st.DrawFn) -> list[Mutation]:
    """A random subset of independent mutations, each producing exactly one error."""
    mutations: list[Mutation] = []
    for path, lo, hi, integer in _RANGED:
        if draw(st.booleans()):
            mutations.append(draw(_range_mutation(path, lo, hi, integer)))
    for path in _REQUIRED:
        if draw(st.booleans()):
            mutations.append(Mutation(path, _REMOVE, path))
    model = draw(st.sampled_from(["none", "remove", "empty"]))
    if model != "none":
        mutations.append(Mutation("llm.model", _REMOVE if model == "remove" else "", "llm.model"))
    if draw(st.booleans()):
        mutations.append(Mutation("worker.capabilities", [], "worker.capabilities"))
    mutations += draw(_backoff_mutations())
    mutations += draw(_provider_mutations())
    mutations += draw(_structure_mutations())
    return mutations


def _apply(file_values: dict[str, Any], env: dict[str, str], m: Mutation) -> None:
    if m.in_env:
        name = env_name(m.path)
        if m.value is _REMOVE:
            env.pop(name, None)
        else:
            env[name] = m.value
        return
    *parents, leaf = m.path.split(".")
    node = file_values
    for part in parents:
        node = node.setdefault(part, {})
    if m.value is _REMOVE:
        node.pop(leaf, None)
    else:
        node[leaf] = m.value


def _expected(mutations: list[Mutation]) -> set[str]:
    # "_ok" markers are helper mutations that set up a case without being errors.
    return {m.field for m in mutations if not m.field.endswith("_ok")}


def _no_network(*_args: Any, **_kwargs: Any) -> None:
    raise AssertionError("load_config attempted a network connection")


def _load(file_values: dict[str, Any], env: dict[str, str]) -> Any:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "agent.yaml"
        path.write_text(yaml.safe_dump(file_values), encoding="utf-8")
        with (
            mock.patch.object(socket.socket, "connect", _no_network),
            mock.patch.object(socket, "create_connection", _no_network),
        ):
            return load_config(path, env)


def test_base_config_is_valid() -> None:
    config = _load(copy.deepcopy(_BASE_FILE), dict(_BASE_ENV))
    assert config.role == "worker"


@given(mutated_configs())
def test_config_validation_reports_every_bad_field(mutations: list[Mutation]) -> None:
    file_values = copy.deepcopy(_BASE_FILE)
    env = dict(_BASE_ENV)
    for m in mutations:
        _apply(file_values, env, m)
    expected = _expected(mutations)

    if not expected:
        _load(file_values, env)
        return

    with pytest.raises(ConfigError) as info:
        _load(file_values, env)
    err = info.value

    assert set(err.fields) == expected
    assert len(err.fields) == len(expected), f"duplicate fields reported: {err.fields}"
    for fe in err.errors:
        assert fe.reason.strip(), f"no reason for {fe.field}"
    range_fields = {m.field for m in mutations if m.range_error}
    for fe in err.errors:
        if fe.field in range_fields:
            assert "range" in fe.reason, f"{fe.field}: {fe.reason!r} lacks the permitted range"
    # The credential value never leaks into the error.
    assert _BASE_ENV["DAS__LLM__API_KEY"] not in str(err)
