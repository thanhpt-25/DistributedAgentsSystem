# Feature: das-observability-operations, Property 16: Validator cấu hình mới báo đúng các trường lỗi
"""Property 16: the new-config validator reports exactly the mutated fields.

*For any* valid generated configuration with an arbitrary subset of the new
fields mutated (bucket lists, label limits, queue sample interval, retention
fields, Price_Table entries and currency), ``validate_config`` reports exactly
the mutated field paths (Price_Table paths carry the entry index). Error
messages never contain a secret value. An unmutated configuration is always
accepted.

**Validates: Requirements 2.12, 5.8, 10.2, 19.2**
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from das.config.schema import MAX_BUCKETS, validate_config
from das.errors import ConfigError
from das.ops.cost import MAX_PRICE_ENTRIES
from das.ops.iso4217 import ISO_4217_CODES

_SECRETS = {
    "api": "api-key-secret-value-0001",
    "admin": "admin-key-secret-value-0001",
    "llm": "sk-llm-secret-value-0001",
    "otlp": "otlp-header-secret-value-0001",
}

_FINITE = {"allow_nan": False, "allow_infinity": False}

_REMOVE = object()


def _entry(i: int) -> dict[str, Any]:
    return {
        "provider": "openai",
        "model": f"model-{i}",
        "input_per_million": 1.5,
        "output_per_million": 2.25,
    }


def _base(n_entries: int) -> dict[str, Any]:
    """A valid worker configuration (the role with most required sections) with secrets."""
    return {
        "role": "worker",
        "agent_id": "worker-1",
        "host": "host-1",
        "transports": {"jobs": "rabbitmq", "control": "grpc"},
        "rabbitmq": {"url": "amqp://localhost:5672/"},
        "redis": {"url": "redis://localhost:6379/0"},
        "grpc": {"bind": "0.0.0.0:50051", "advertise_host": "worker-1", "advertise_port": 50051},
        "llm": {"provider": "openai", "model": "gpt-test", "api_key": _SECRETS["llm"]},
        "worker": {"capabilities": ["research"]},
        "api_keys": [_SECRETS["api"]],
        "admin_api_keys": [_SECRETS["admin"]],
        "tracing": {"otlp_headers": f"authorization={_SECRETS['otlp']}"},
        "metrics": {
            "queue_sample_interval": 15,
            "label_limits": {"capability": 50, "queue": 50, "provider": 50, "model": 50},
            "buckets": {"http": [0.1, 1, 10], "duration": [1, 10, 100], "attempts": [1, 2, 3]},
        },
        "retention": {
            "enabled": True,
            "terminal_request_ttl": 604_800,
            "inbox_ttl": 3600,
            "dlq_record_ttl": 2_592_000,
            "sweep_interval": 60,
            "batch_size": 500,
        },
        "pricing": {"currency": "USD", "models": [_entry(i) for i in range(n_entries)]},
    }


@dataclass(frozen=True)
class Mutation:
    """Set ``path`` (a list of keys/indices) to ``value`` (or remove it); expect ``field``."""

    path: tuple[Any, ...]
    value: Any
    field: str
    range_error: bool = False


def _apply(data: dict[str, Any], m: Mutation) -> None:
    node: Any = data
    for part in m.path[:-1]:
        node = node[part]
    if m.value is _REMOVE:
        del node[m.path[-1]]
    else:
        node[m.path[-1]] = m.value


def _out_of_range(lo: float, hi: float, *, integer: bool) -> st.SearchStrategy[Any]:
    if integer:
        return st.integers(max_value=int(lo) - 1) | st.integers(min_value=int(hi) + 1)
    return st.floats(max_value=lo, exclude_max=True, **_FINITE) | st.floats(
        min_value=hi, exclude_min=True, **_FINITE
    )


# --------------------------------------------------------------------------- metrics

_positive = st.floats(min_value=1e-6, max_value=1e6, **_FINITE)


@st.composite
def _bad_buckets(draw: st.DrawFn) -> list[float]:
    kind = draw(st.sampled_from(["empty", "too_many", "non_positive", "not_increasing"]))
    if kind == "empty":
        return []
    if kind == "too_many":
        n = draw(st.integers(min_value=MAX_BUCKETS + 1, max_value=MAX_BUCKETS + 20))
        return [float(i + 1) for i in range(n)]
    values = sorted(draw(st.sets(_positive, min_size=1, max_size=10)))
    if kind == "non_positive":
        bad = draw(st.floats(max_value=0, **_FINITE))
        values.insert(draw(st.integers(min_value=0, max_value=len(values))), bad)
        return values
    # not strictly increasing: repeat an element or swap two
    if len(values) == 1 or draw(st.booleans()):
        i = draw(st.integers(min_value=0, max_value=len(values) - 1))
        values.insert(i, values[i])
    else:
        i = draw(st.integers(min_value=0, max_value=len(values) - 2))
        values[i], values[i + 1] = values[i + 1], values[i]
    return values


_bad_label_limit = (
    _out_of_range(1, 500, integer=True)
    | st.floats(min_value=1.01, max_value=499.99, **_FINITE).filter(lambda x: not x.is_integer())
    | st.text(alphabet="abcxyz", min_size=1, max_size=5)
)


@st.composite
def _metrics_mutations(draw: st.DrawFn) -> list[Mutation]:
    out: list[Mutation] = []
    for name in ("http", "duration", "attempts"):
        if draw(st.booleans()):
            out.append(
                Mutation(
                    ("metrics", "buckets", name), draw(_bad_buckets()), f"metrics.buckets.{name}"
                )
            )
    for name in ("capability", "queue", "provider", "model"):
        if draw(st.booleans()):
            value = draw(_bad_label_limit)
            out.append(
                Mutation(
                    ("metrics", "label_limits", name),
                    value,
                    f"metrics.label_limits.{name}",
                    range_error=isinstance(value, int),
                )
            )
    if draw(st.booleans()):
        out.append(
            Mutation(
                ("metrics", "queue_sample_interval"),
                draw(_out_of_range(5, 300, integer=False)),
                "metrics.queue_sample_interval",
                range_error=True,
            )
        )
    return out


# --------------------------------------------------------------------------- retention

_RETENTION: tuple[tuple[str, float, float, bool], ...] = (
    ("terminal_request_ttl", 3600, 31_536_000, True),
    ("inbox_ttl", 60, 604_800, True),
    ("dlq_record_ttl", 3600, 31_536_000, True),
    ("sweep_interval", 10, 3600, False),
    ("batch_size", 1, 10_000, True),
)


@st.composite
def _retention_mutations(draw: st.DrawFn) -> list[Mutation]:
    out: list[Mutation] = []
    for name, lo, hi, integer in _RETENTION:
        if draw(st.booleans()):
            out.append(
                Mutation(
                    ("retention", name),
                    draw(_out_of_range(lo, hi, integer=integer)),
                    f"retention.{name}",
                    range_error=True,
                )
            )
    return out


# --------------------------------------------------------------------------- pricing

_MILLION = Decimal(1_000_000)


@st.composite
def _too_precise(draw: st.DrawFn) -> Decimal:
    """A price within 0..1e6 with more than 6 significant decimal places."""
    k = draw(st.integers(min_value=7, max_value=10))
    n = draw(st.integers(min_value=1, max_value=10**9).filter(lambda x: x % 10 != 0))
    return Decimal(n).scaleb(-k)


_negative_price = st.decimals(max_value=Decimal("-0.000001"), places=6, **_FINITE)
_huge_price = st.decimals(min_value=_MILLION + Decimal("0.000001"), places=6, **_FINITE)


@st.composite
def _entry_mutation(draw: st.DrawFn, i: int) -> Mutation | None:
    """One mutation of entry ``i`` (entry 0 keeps its provider/model: it is the dup target)."""
    kinds = ["none", "missing", "price_negative", "price_huge", "price_precise"]
    if i > 0:
        kinds += ["provider_empty", "provider_long", "model_empty", "model_long", "duplicate"]
    kind = draw(st.sampled_from(kinds))
    base = ("pricing", "models", i)
    prefix = f"pricing.models[{i}]"
    price = draw(st.sampled_from(["input_per_million", "output_per_million"]))
    if kind == "none":
        return None
    if kind == "missing":
        if i == 0:
            name = price
        else:
            name = draw(
                st.sampled_from(["provider", "model", "input_per_million", "output_per_million"])
            )
        return Mutation((*base, name), _REMOVE, f"{prefix}.{name}")
    if kind == "price_negative":
        return Mutation((*base, price), draw(_negative_price), f"{prefix}.{price}", True)
    if kind == "price_huge":
        return Mutation((*base, price), draw(_huge_price), f"{prefix}.{price}", True)
    if kind == "price_precise":
        return Mutation((*base, price), draw(_too_precise()), f"{prefix}.{price}")
    if kind == "provider_empty":
        return Mutation((*base, "provider"), "", f"{prefix}.provider")
    if kind == "provider_long":
        n = draw(st.integers(min_value=65, max_value=100))
        return Mutation((*base, "provider"), "p" * n, f"{prefix}.provider")
    if kind == "model_empty":
        return Mutation((*base, "model"), "", f"{prefix}.model")
    if kind == "model_long":
        n = draw(st.integers(min_value=129, max_value=160))
        return Mutation((*base, "model"), f"model-{i}-" + "m" * n, f"{prefix}.model")
    # duplicate of entry 0's provider/model pair
    return Mutation((*base, "model"), "model-0", f"{prefix}.model")


_bad_currency = (
    st.from_regex(r"\A[A-Z]{3}\Z").filter(lambda c: c not in ISO_4217_CODES)
    | st.sampled_from(sorted(ISO_4217_CODES)).map(str.lower)
    | st.from_regex(r"\A[A-Z]{1,2}\Z|\A[A-Z]{4,6}\Z")
)


@st.composite
def _pricing_case(draw: st.DrawFn) -> tuple[int, list[Mutation]]:
    """Number of base entries plus the Price_Table mutations to apply."""
    out: list[Mutation] = []
    if draw(st.booleans()):
        out.append(Mutation(("pricing", "currency"), draw(_bad_currency), "pricing.currency"))
    if draw(st.integers(min_value=0, max_value=9)) == 0:
        # More than 200 entries, otherwise valid and unique.
        n = draw(st.integers(min_value=MAX_PRICE_ENTRIES + 1, max_value=MAX_PRICE_ENTRIES + 10))
        out.append(Mutation(("pricing", "models"), [_entry(i) for i in range(n)], "pricing.models"))
        return 0, out
    n = draw(st.integers(min_value=0, max_value=6))
    for i in range(n):
        m = draw(_entry_mutation(i))
        if m is not None:
            out.append(m)
    return n, out


@st.composite
def mutated_configs(draw: st.DrawFn) -> tuple[dict[str, Any], list[Mutation]]:
    n_entries, pricing = draw(_pricing_case())
    mutations = draw(_metrics_mutations()) + draw(_retention_mutations()) + pricing
    data = _base(n_entries)
    for m in mutations:
        _apply(data, m)
    return data, mutations


# --------------------------------------------------------------------------- tests


@pytest.mark.parametrize("n_entries", [0, 1, MAX_PRICE_ENTRIES])
def test_unmutated_config_is_accepted(n_entries: int) -> None:
    config = validate_config(copy.deepcopy(_base(n_entries)))
    assert config.pricing is not None
    assert len(config.pricing.models) == n_entries


@settings(max_examples=200, deadline=None)
@given(mutated_configs())
def test_validator_reports_exactly_the_mutated_new_fields(
    case: tuple[dict[str, Any], list[Mutation]],
) -> None:
    data, mutations = case
    expected = {m.field for m in mutations}

    if not expected:
        validate_config(data)
        return

    with pytest.raises(ConfigError) as info:
        validate_config(data)
    err = info.value

    assert set(err.fields) == expected
    assert len(err.fields) == len(expected), f"duplicate fields reported: {err.fields}"
    range_fields = {m.field for m in mutations if m.range_error}
    for fe in err.errors:
        assert fe.reason.strip(), f"no reason for {fe.field}"
        if fe.field in range_fields:
            assert "range" in fe.reason, f"{fe.field}: {fe.reason!r} lacks the permitted range"
    text = str(err) + " ".join(str(fe) for fe in err.errors)
    for secret in _SECRETS.values():
        assert secret not in text
