# Feature: das-observability-operations, Property 7: Decode khoan dung với `trace_context`
"""Property 7: lenient decode of ``trace_context``.

For any valid v2 encoding whose ``trace_context`` field is replaced by an
arbitrary value (non-object, missing ``traceparent``, ``traceparent`` mutated in
version / length / case / all-zero ids, ``tracestate`` non-string, longer than
512 characters or with more than 32 members):

- ``decode`` does not raise ``ParseError``.
- Every other envelope and payload field equals the original.
- The resulting ``trace_context`` equals what an independent W3C reference
  checker (regex oracle, below) says: a valid ``traceparent`` is kept, a valid
  ``tracestate`` is kept, anything else is dropped.

For any ``schema_version`` outside {1, 2}, ``decode`` raises
``ParseError(field="schema_version")``.

**Validates: Requirements 6.11, 7.1, 7.7, 7.8, 7.10**
"""

from __future__ import annotations

import json
import re
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from das.codec.envelope import TraceContextModel
from das.codec.message_codec import MessageCodec, ParseError, canonical_json
from tests.strategies import messages, traceparents, tracestates

_codec = MessageCodec()

# --------------------------------------------------------------------------- W3C oracle
# Written independently of das.observability.trace_context, straight from the spec.
_TP_V00 = re.compile(r"\A00-(?!0{32})[0-9a-f]{32}-(?!0{16})[0-9a-f]{16}-[0-9a-f]{2}\Z")
_TP_FUTURE = re.compile(
    r"\A(?!00)(?!ff)[0-9a-f]{2}-(?!0{32})[0-9a-f]{32}-(?!0{16})[0-9a-f]{16}-[0-9a-f]{2}"
    r"(?:-[\s\S]*)?\Z"
)
_TS_MEMBER = re.compile(
    r"\A(?:[a-z][a-z0-9_\-*/]{0,255}|[a-z0-9][a-z0-9_\-*/]{0,240}@[a-z][a-z0-9_\-*/]{0,13})"
    r"=[\x20-\x2b\x2d-\x3c\x3e-\x7e]{0,255}[\x21-\x2b\x2d-\x3c\x3e-\x7e]\Z"
)


def _oracle_traceparent(value: object) -> bool:
    return isinstance(value, str) and bool(_TP_V00.match(value) or _TP_FUTURE.match(value))


def _oracle_tracestate(value: object) -> bool:
    if not isinstance(value, str) or len(value) > 512:
        return False
    members = [m.strip(" \t") for m in value.split(",")]
    members = [m for m in members if m]
    if not members or len(members) > 32:
        return False
    if not all(_TS_MEMBER.match(m) for m in members):
        return False
    keys = [m.split("=", 1)[0] for m in members]
    return len(set(keys)) == len(keys)


def _oracle(raw: object) -> TraceContextModel | None:
    if not isinstance(raw, dict) or not _oracle_traceparent(raw.get("traceparent")):
        return None
    state = raw.get("tracestate")
    return TraceContextModel(
        traceparent=raw["traceparent"],
        tracestate=state if _oracle_tracestate(state) else None,
    )


# --------------------------------------------------------------------------- generators
_json_scalars = st.none() | st.booleans() | st.integers() | st.floats(allow_nan=False, allow_infinity=False) | st.text(
    max_size=20
)
_json_values = st.recursive(
    _json_scalars,
    lambda inner: st.lists(inner, max_size=3) | st.dictionaries(st.text(max_size=8), inner, max_size=3),
    max_leaves=6,
)
_HEX = "0123456789abcdef"


@st.composite
def _mutated_traceparents(draw: st.DrawFn) -> object:
    tp: str = draw(traceparents)
    kind = draw(
        st.sampled_from(
            ["valid", "ff", "future", "future_bad", "short", "long", "upper", "zero_trace",
             "zero_parent", "non_string", "garbage"]
        )
    )
    if kind == "ff":
        return "ff" + tp[2:]
    if kind == "future":
        version = draw(st.text(alphabet=_HEX, min_size=2, max_size=2))
        suffix = draw(st.sampled_from(["", "-", "-abc", "-01-xyz"]))
        return version + tp[2:] + suffix
    if kind == "future_bad":
        return draw(st.sampled_from(["01", "7f", "fe"])) + tp[2:] + draw(
            st.sampled_from(["x", "0", "a-b"])
        )
    if kind == "short":
        return tp[: draw(st.integers(0, len(tp) - 1))]
    if kind == "long":
        return tp + draw(st.text(alphabet=_HEX + "-", min_size=1, max_size=5))
    if kind == "upper":
        return tp.upper() if any(c.isalpha() for c in tp) else tp[:3] + "A" + tp[4:]
    if kind == "zero_trace":
        return tp[:3] + "0" * 32 + tp[35:]
    if kind == "zero_parent":
        return tp[:36] + "0" * 16 + tp[52:]
    if kind == "non_string":
        return draw(st.none() | st.integers() | st.lists(st.text(max_size=5), max_size=2))
    if kind == "garbage":
        return draw(st.text(max_size=60))
    return tp


@st.composite
def _mutated_tracestates(draw: st.DrawFn) -> object:
    kind = draw(
        st.sampled_from(["valid", "too_long", "too_many", "duplicate", "non_string", "garbage"])
    )
    if kind == "too_long":
        return "k=" + "v" * draw(st.integers(511, 600))
    if kind == "too_many":
        n = draw(st.integers(33, 40))
        return ",".join(f"k{i}=v" for i in range(n))
    if kind == "duplicate":
        ts: str = draw(tracestates())
        return ts + "," + ts.split(",")[0]
    if kind == "non_string":
        return draw(st.integers() | st.booleans() | st.lists(st.text(max_size=5), max_size=2)
                    | st.dictionaries(st.text(max_size=5), st.text(max_size=5), max_size=2))
    if kind == "garbage":
        return draw(st.text(max_size=40))
    return draw(tracestates())


@st.composite
def _trace_context_values(draw: st.DrawFn) -> object:
    kind = draw(st.sampled_from(["non_object", "missing_tp", "object", "object_extra"]))
    if kind == "non_object":
        return draw(_json_scalars | st.lists(_json_values, max_size=3))
    obj: dict[str, Any] = {}
    if kind != "missing_tp":
        obj["traceparent"] = draw(_mutated_traceparents())
    if draw(st.booleans()):
        obj["tracestate"] = draw(_mutated_tracestates() | st.none())
    if kind == "object_extra":
        obj[draw(st.text(min_size=1, max_size=8).filter(
            lambda k: k not in ("traceparent", "tracestate")))] = draw(_json_values)
    return obj


# --------------------------------------------------------------------------- properties
@given(messages(), _trace_context_values())
def test_decode_is_lenient_about_trace_context(message: Any, trace_value: object) -> None:
    """**Validates: Requirements 6.11, 7.1, 7.7, 7.8**"""
    obj = json.loads(_codec.encode(message))
    obj["schema_version"] = 2
    obj["trace_context"] = trace_value
    data = canonical_json(obj).encode("utf-8")

    decoded = _codec.decode(data)  # must not raise ParseError (Req 7.7, 7.8)

    assert type(decoded) is type(message)
    exclude = {"trace_context", "schema_version"}
    assert decoded.model_dump(exclude=exclude) == message.model_dump(exclude=exclude)
    expected = _oracle(trace_value)
    assert decoded.trace_context == expected
    # Absent/invalid Trace_Context behaves exactly like a Message without one (Req 6.11).
    assert decoded.schema_version == (2 if expected is not None else 1)


_bad_versions = st.integers().filter(lambda v: v not in (1, 2)) | st.sampled_from([True, False])


@given(messages(), _bad_versions, st.booleans())
def test_unsupported_schema_version_is_rejected(
    message: Any, version: object, with_trace: bool
) -> None:
    """**Validates: Requirements 7.10**"""
    obj = json.loads(_codec.encode(message))
    obj["schema_version"] = version
    if with_trace:
        obj["trace_context"] = {"traceparent": "00-" + "1" * 32 + "-" + "2" * 16 + "-01"}
    with pytest.raises(ParseError) as exc_info:
        _codec.decode(canonical_json(obj).encode("utf-8"))
    assert exc_info.value.field == "schema_version"
