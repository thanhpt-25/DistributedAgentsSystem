# Feature: distributed-agent-system, Property 5: Codec rejects malformed input atomically
"""Property 5: Codec rejects malformed input atomically.

*For any* byte string that is not a valid encoded Message, ``decode`` raises
``ParseError`` with a reason and offending field and never returns a Message
object. Generated inputs include random bytes, valid encodings with a required
field removed, an unknown type, an unsupported schema_version, or a payload
field of the wrong type.

**Validates: Requirements 9.4**
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pytest
from hypothesis import given, note
from hypothesis import strategies as st

from das.codec.envelope import ENVELOPE_FIELDS, MESSAGE_CLASSES, PAYLOAD_MODELS
from das.codec.message_codec import ROOT_FIELD, MessageCodec, ParseError
from tests.strategies import messages

_CODEC = MessageCodec()


@dataclass(frozen=True)
class Malformed:
    kind: str
    data: bytes
    # Exact offending field, or a "payload.<name>" prefix for payload mutations.
    field: str | None
    field_is_prefix: bool = False


def _to_bytes(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False).encode("utf-8")


@st.composite
def _valid_objects(draw: st.DrawFn) -> dict[str, Any]:
    """A valid Message as its decoded JSON object (fresh copy per draw)."""
    return json.loads(_CODEC.encode(draw(messages())))


@st.composite
def _random_bytes(draw: st.DrawFn) -> Malformed:
    return Malformed("random_bytes", draw(st.binary(max_size=512)), None)


@st.composite
def _truncated(draw: st.DrawFn) -> Malformed:
    data = _CODEC.encode(draw(messages()))
    cut = draw(st.integers(min_value=0, max_value=len(data) - 1))
    return Malformed("truncated", data[:cut], ROOT_FIELD)


@st.composite
def _missing_envelope_field(draw: st.DrawFn) -> Malformed:
    obj = draw(_valid_objects())
    name = draw(st.sampled_from(sorted(ENVELOPE_FIELDS)))
    del obj[name]
    return Malformed("missing_envelope_field", _to_bytes(obj), name)


@st.composite
def _missing_payload_field(draw: st.DrawFn) -> Malformed:
    obj = draw(_valid_objects())
    model = PAYLOAD_MODELS[obj["type"]]
    required = sorted(n for n, f in model.model_fields.items() if f.is_required())
    name = draw(st.sampled_from(required))
    del obj["payload"][name]
    return Malformed("missing_payload_field", _to_bytes(obj), f"payload.{name}")


@st.composite
def _unknown_type(draw: st.DrawFn) -> Malformed:
    obj = draw(_valid_objects())
    obj["type"] = draw(
        st.text(max_size=30).filter(lambda t: t not in MESSAGE_CLASSES)
        | st.integers()
        | st.none()
        | st.booleans()
    )
    return Malformed("unknown_type", _to_bytes(obj), "type")


@st.composite
def _unsupported_version(draw: st.DrawFn) -> Malformed:
    obj = draw(_valid_objects())
    obj["schema_version"] = draw(
        st.integers().filter(lambda v: v not in MessageCodec.SUPPORTED_VERSIONS)
        | st.text(max_size=5)
        | st.booleans()
        | st.floats(allow_nan=False, allow_infinity=False)
    )
    return Malformed("unsupported_schema_version", _to_bytes(obj), "schema_version")


# JSON values of a type different from the original, never None (some fields are optional).
_WRONG_BY_TYPE: dict[type, st.SearchStrategy[Any]] = {
    str: st.integers() | st.lists(st.integers(), max_size=3) | st.just({"x": 1}),
    int: st.text(max_size=10) | st.lists(st.integers(), max_size=3) | st.just({"x": 1}),
    list: st.text(max_size=10) | st.integers() | st.just({"x": 1}),
    dict: st.text(max_size=10) | st.integers() | st.lists(st.integers(), max_size=3),
    type(None): st.integers() | st.lists(st.integers(), max_size=3) | st.just({"x": 1}),
}


@st.composite
def _wrong_payload_type(draw: st.DrawFn) -> Malformed:
    obj = draw(_valid_objects())
    payload = obj["payload"]
    name = draw(st.sampled_from(sorted(payload)))
    payload[name] = draw(_WRONG_BY_TYPE[type(payload[name])])
    return Malformed("wrong_payload_type", _to_bytes(obj), f"payload.{name}", True)


malformed_inputs = st.one_of(
    _random_bytes(),
    _truncated(),
    _missing_envelope_field(),
    _missing_payload_field(),
    _unknown_type(),
    _unsupported_version(),
    _wrong_payload_type(),
)


@given(case=malformed_inputs)
def test_decode_rejects_malformed_input_atomically(case: Malformed) -> None:
    note(f"kind={case.kind} data={case.data!r}")
    result: object = None
    with pytest.raises(ParseError) as excinfo:
        result = _CODEC.decode(case.data)

    # Atomic: nothing was returned, not even a partially populated Message.
    assert result is None
    err = excinfo.value
    assert isinstance(err.reason, str) and err.reason
    assert isinstance(err.field, str) and err.field

    if case.field is not None:
        if case.field_is_prefix:
            assert err.field == case.field or err.field.startswith(case.field + "."), err.field
        else:
            assert err.field == case.field, err.field
