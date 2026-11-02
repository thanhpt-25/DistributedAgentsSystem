# Feature: distributed-agent-system, Property 37: Credentials in config files are never echoed
"""Property 37: Credentials in config files are never echoed.

For any credential value placed in a secret field of the configuration file,
startup fails with an error naming the field, and the error text contains no
substring of the value of length >= 8 (or the full value if shorter).

**Validates: Requirements 12.5**
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import pytest
import yaml
from hypothesis import assume, given
from hypothesis import strategies as st

from das.config import load_config
from das.config.schema import SECRET_FIELDS
from das.errors import ConfigError

_WINDOW = 8

_BASES: dict[str, dict[str, Any]] = {
    "worker": {
        "role": "worker",
        "host": "10.0.0.7",
        "transports": {"jobs": "rabbitmq", "control": "grpc"},
        "rabbitmq": {"url": "amqp://rabbit.internal:5672/"},
        "redis": {"url": "redis://redis.internal:6379/0"},
        "grpc": {"bind": "0.0.0.0:50051", "advertise_host": "10.0.0.7", "advertise_port": 50051},
        "llm": {"provider": "anthropic", "model": "claude-sonnet-4"},
        "worker": {"capabilities": ["research"]},
    },
    "gateway": {
        "role": "gateway",
        "redis": {"url": "redis://redis.internal:6379/0"},
        "llm": {"provider": "openai", "model": "gpt-4o"},
    },
}

# Credential-shaped values (API keys, AWS keys) plus arbitrary Unicode text.
_key_like = st.from_regex(r"\A(sk-|AKIA|key_)?[A-Za-z0-9+/_=-]{4,60}\Z")
_any_text = st.text(
    alphabet=st.characters(blacklist_categories=("Cs",)), min_size=4, max_size=60
).filter(lambda s: s.strip() != "")
_credentials = _key_like | _any_text


@st.composite
def _cases(draw: st.DrawFn) -> tuple[str, dict[str, Any], list[str]]:
    """A base config, the secret fields set in it, and every credential value used."""
    role = draw(st.sampled_from(sorted(_BASES)))
    fields = draw(st.lists(st.sampled_from(SECRET_FIELDS), min_size=1, unique=True))
    placed: dict[str, Any] = {}
    values: list[str] = []
    for field in fields:
        if field == "api_keys":
            keys = draw(st.lists(_credentials, min_size=1, max_size=3))
            values.extend(keys)
            placed[field] = keys if draw(st.booleans()) else ",".join(keys)
        else:
            value = draw(_credentials)
            values.append(value)
            placed[field] = value
    return role, placed, values


def _config(role: str, placed: dict[str, Any]) -> dict[str, Any]:
    data = yaml.safe_load(yaml.safe_dump(_BASES[role]))  # deep copy
    for field, value in placed.items():
        node = data
        *parents, leaf = field.split(".")
        for part in parents:
            node = node.setdefault(part, {})
        node[leaf] = value
    return data


def _error_text(path: Path, data: dict[str, Any]) -> tuple[ConfigError, str]:
    path.write_text(yaml.safe_dump(data, allow_unicode=True), encoding="utf-8")
    with pytest.raises(ConfigError) as ei:
        load_config(path, {})
    exc = ei.value
    text = "\n".join([str(exc), repr(exc), *(str(e) for e in exc.errors)])
    return exc, text


def _fragments(value: str) -> set[str]:
    if len(value) < _WINDOW:
        return {value}
    return {value[i : i + _WINDOW] for i in range(len(value) - _WINDOW + 1)}


@given(_cases())
def test_credentials_in_config_file_are_never_echoed(
    case: tuple[str, dict[str, Any], list[str]],
) -> None:
    role, placed, values = case
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "config.yaml"

        # Baseline: the same structure with sentinel credentials. Fragments that
        # already occur in the fixed error wording are coincidences, not echoes.
        sentinel = {
            f: (["\x00"] if isinstance(v, list) else "\x00") for f, v in placed.items()
        }
        _, baseline = _error_text(path, _config(role, sentinel))
        frags = set().union(*(_fragments(v) for v in values))
        assume(not any(f in baseline for f in frags))

        exc, text = _error_text(path, _config(role, placed))

    for field in placed:
        assert field in exc.fields, f"error does not name {field}: {exc.fields}"
    leaked = sorted(f for f in frags if f in text)
    assert not leaked, f"credential fragments echoed in error: {leaked!r}"
