"""Transport loader: resolves a Transport adapter by name (Requirements 8.4, 8.5, 15).

Adapters register under the Python entry-point group ``das.transports``. v1
ships ``rabbitmq`` and ``grpc`` only; ``http`` and ``kafka`` are not shipped, so
selecting them fails with :class:`~das.errors.TransportNotInstalled` (Req 15.3).
A third-party package can add either by declaring an entry point in that group,
with no change to Manager or Worker code (Req 15.1, 15.2).

Agent startup calls :func:`load_transport` before registry registration and
before any subscribe or publish (Req 8.4), so a load failure aborts startup
with exit code 3 (see :mod:`das.errors`) and nothing is sent.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from importlib.metadata import EntryPoint, entry_points
from typing import Final

from das.errors import TransportIncomplete, TransportNotInstalled
from das.transport.base import missing_operations

#: Entry-point group under which Transport adapters register.
ENTRY_POINT_GROUP: Final = "das.transports"

#: Returns the entry points registered in :data:`ENTRY_POINT_GROUP`.
EntryPointSource = Callable[[], Iterable[EntryPoint]]


def _installed_entry_points() -> Iterable[EntryPoint]:
    return entry_points(group=ENTRY_POINT_GROUP)


def available_transports(source: EntryPointSource | None = None) -> list[str]:
    """Return the sorted names of installed Transport adapters."""
    eps = (source or _installed_entry_points)()
    return sorted({ep.name for ep in eps})


def load_transport(name: str, source: EntryPointSource | None = None) -> type:
    """Return the adapter class registered as ``name``.

    Raises:
        TransportNotInstalled: no entry point named ``name`` exists, or its
            target module cannot be imported (Req 8.5, 15.3).
        TransportIncomplete: the adapter lacks one or more of publish,
            subscribe, acknowledge, request_reply (Req 15.5).
    """
    eps = (source or _installed_entry_points)()
    match = next((ep for ep in eps if ep.name == name), None)
    if match is None:
        raise TransportNotInstalled(name)
    try:
        adapter = match.load()
    except (ImportError, AttributeError) as exc:
        # Declared but not importable: the implementation is effectively absent.
        raise TransportNotInstalled(name) from exc
    missing = missing_operations(adapter)
    if missing:
        raise TransportIncomplete(name, missing)
    return adapter


__all__ = [
    "ENTRY_POINT_GROUP",
    "EntryPointSource",
    "available_transports",
    "load_transport",
]
