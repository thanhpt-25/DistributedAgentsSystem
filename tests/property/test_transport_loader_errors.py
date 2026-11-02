# Feature: distributed-agent-system, Property 32: Transport loader errors
"""Property 32: Transport loader errors.

*For any* adapter name not registered under ``das.transports``,
``load_transport`` raises an error whose message contains that name. *For any*
registered adapter class missing a non-empty subset of {publish, subscribe,
acknowledge, request_reply}, it raises an error naming the adapter and exactly
that subset.

**Validates: Requirements 8.5, 15.5**
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from hypothesis import given
from hypothesis import strategies as st

from das.errors import EXIT_TRANSPORT_ERROR, TransportIncomplete, TransportNotInstalled, exit_code_for
from das.transport.base import TRANSPORT_OPERATIONS
from das.transport.loader import load_transport


@dataclass(frozen=True)
class _FakeEntryPoint:
    """Duck-typed stand-in for importlib.metadata.EntryPoint (name + load())."""

    name: str
    target: type

    def load(self) -> type:
        return self.target


async def _op(self, *args, **kwargs) -> None:  # pragma: no cover - never called
    return None


def _adapter(ops: frozenset[str]) -> type:
    """Build an adapter class implementing exactly ``ops``."""
    return type("GeneratedAdapter", (), {op: _op for op in ops})


_COMPLETE = _adapter(frozenset(TRANSPORT_OPERATIONS))

# Entry-point style names: lowercase identifiers, possibly with - or _.
adapter_names = st.from_regex(r"[a-z][a-z0-9_\-]{0,20}", fullmatch=True)

operation_subsets = st.frozensets(st.sampled_from(TRANSPORT_OPERATIONS))


@given(
    name=adapter_names,
    registered=st.lists(adapter_names, max_size=5, unique=True),
)
def test_unregistered_name_raises_not_installed_naming_it(name: str, registered: list[str]) -> None:
    registered = [r for r in registered if r != name]
    eps = [_FakeEntryPoint(r, _COMPLETE) for r in registered]

    with pytest.raises(TransportNotInstalled) as info:
        load_transport(name, lambda: list(eps))

    assert info.value.name == name
    assert name in str(info.value)
    assert exit_code_for(info.value) == EXIT_TRANSPORT_ERROR != 0


@given(
    name=adapter_names,
    present=operation_subsets.filter(lambda s: len(s) < len(TRANSPORT_OPERATIONS)),
    others=st.lists(adapter_names, max_size=3, unique=True),
)
def test_incomplete_adapter_names_exactly_missing_operations(
    name: str, present: frozenset[str], others: list[str]
) -> None:
    expected_missing = tuple(op for op in TRANSPORT_OPERATIONS if op not in present)
    eps = [_FakeEntryPoint(o, _COMPLETE) for o in others if o != name]
    eps.append(_FakeEntryPoint(name, _adapter(present)))

    with pytest.raises(TransportIncomplete) as info:
        load_transport(name, lambda: list(eps))

    err = info.value
    assert err.name == name
    assert tuple(err.missing) == expected_missing
    message = str(err)
    assert name in message
    for op in expected_missing:
        assert op in message
    # Operations the adapter does implement are not reported as missing.
    reported = set(err.missing)
    assert reported.isdisjoint(present)
    assert exit_code_for(err) == EXIT_TRANSPORT_ERROR != 0


@given(name=adapter_names)
def test_complete_adapter_loads(name: str) -> None:
    # Control: with all four operations present, no error is raised.
    assert load_transport(name, lambda: [_FakeEntryPoint(name, _COMPLETE)]) is _COMPLETE
