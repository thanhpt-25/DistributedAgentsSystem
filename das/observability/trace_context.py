"""Pure W3C Trace Context value object (Requirement 7.1).

This module performs no I/O and deliberately does not import ``opentelemetry`` so
the codec and the pure core can validate Trace_Context without the SDK.

Rules (https://www.w3.org/TR/trace-context/):

- ``traceparent`` = ``version-traceid-parentid-flags``: lowercase hex, version is
  2 chars and not ``ff``, trace-id 32 chars not all zero, parent-id 16 chars not
  all zero, flags 2 chars. Version ``00`` is exactly 55 chars. A higher (future)
  version may carry extra fields after a ``-`` at position 55.
- ``tracestate``: at most 512 chars, at most 32 list-members, each member
  ``key=value`` per the W3C grammar, keys unique. Empty members (only optional
  whitespace between commas) are allowed and not counted.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

__all__ = ["TraceContext", "is_valid_traceparent", "is_valid_tracestate"]

_TRACEPARENT_RE = re.compile(
    r"(?P<version>[0-9a-f]{2})-(?P<trace_id>[0-9a-f]{32})-"
    r"(?P<parent_id>[0-9a-f]{16})-(?P<flags>[0-9a-f]{2})"
)
_TRACEPARENT_V00_LEN = 55

_TRACESTATE_MAX_LEN = 512
_TRACESTATE_MAX_MEMBERS = 32

_KEY_CHARS = r"[a-z0-9_\-*/]"
_KEY_RE = re.compile(
    rf"[a-z]{_KEY_CHARS}{{0,255}}"  # simple-key
    rf"|[a-z0-9]{_KEY_CHARS}{{0,240}}@[a-z]{_KEY_CHARS}{{0,13}}"  # multi-tenant-key
)
# value = 0*255(chr) nblk-chr; chr = %x20 / nblk-chr; nblk-chr = %x21-7E except ',' '='
_VALUE_RE = re.compile(r"[\x20-\x2b\x2d-\x3c\x3e-\x7e]{0,255}[\x21-\x2b\x2d-\x3c\x3e-\x7e]")
_OWS = " \t"


def is_valid_traceparent(value: object) -> bool:
    """True when ``value`` is a ``str`` that is a valid W3C ``traceparent``."""
    if not isinstance(value, str):
        return False
    match = _TRACEPARENT_RE.match(value)
    if match is None:
        return False
    version = match["version"]
    if version == "ff":
        return False
    if version == "00":
        if len(value) != _TRACEPARENT_V00_LEN:
            return False
    elif len(value) > _TRACEPARENT_V00_LEN and value[_TRACEPARENT_V00_LEN] != "-":
        return False
    return match["trace_id"] != "0" * 32 and match["parent_id"] != "0" * 16


def is_valid_tracestate(value: object) -> bool:
    """True when ``value`` is a ``str`` that is a valid W3C ``tracestate``."""
    if not isinstance(value, str) or len(value) > _TRACESTATE_MAX_LEN:
        return False
    keys: set[str] = set()
    for raw in value.split(","):
        member = raw.strip(_OWS)
        if not member:
            continue
        key, sep, val = member.partition("=")
        if not sep or _KEY_RE.fullmatch(key) is None or _VALUE_RE.fullmatch(val) is None:
            return False
        if key in keys:
            return False
        keys.add(key)
        if len(keys) > _TRACESTATE_MAX_MEMBERS:
            return False
    return bool(keys)


@dataclass(frozen=True)
class TraceContext:
    """A W3C Trace_Context; build validated instances with :meth:`parse`."""

    traceparent: str
    tracestate: str | None = None

    @staticmethod
    def parse(traceparent: object, tracestate: object = None) -> TraceContext | None:
        """Validate and build a Trace_Context.

        Returns ``None`` when ``traceparent`` is invalid. An invalid or non-string
        ``tracestate`` is dropped while a valid ``traceparent`` is kept (Req 7.7, 7.8).
        """
        if not is_valid_traceparent(traceparent):
            return None
        assert isinstance(traceparent, str)
        state = tracestate if is_valid_tracestate(tracestate) else None
        assert state is None or isinstance(state, str)
        return TraceContext(traceparent=traceparent, tracestate=state)

    @property
    def trace_id(self) -> str:
        return self.traceparent[3:35]

    @property
    def parent_id(self) -> str:
        return self.traceparent[36:52]

    @property
    def flags(self) -> int:
        return int(self.traceparent[53:55], 16)

    @property
    def sampled(self) -> bool:
        """The W3C ``sampled`` trace-flag (bit 0)."""
        return bool(self.flags & 0x01)
