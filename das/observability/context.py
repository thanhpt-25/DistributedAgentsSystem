"""Request/Job context binding for logs and outbound Messages (Req 13.2-13.5).

Handlers bind the inbound Message's ``correlation_id`` as the ``request_id`` so
that every log entry and every outbound Message produced while handling it
carries the same Request_ID. Bindings are ``contextvars`` so they follow asyncio
tasks and are restored when the ``with`` block exits.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

request_id_var: ContextVar[str | None] = ContextVar("das_request_id", default=None)
job_id_var: ContextVar[str | None] = ContextVar("das_job_id", default=None)


def get_request_id() -> str | None:
    """The currently bound Request_ID, or ``None``."""
    return request_id_var.get()


def get_job_id() -> str | None:
    """The currently bound Job_ID, or ``None``."""
    return job_id_var.get()


def current_context() -> dict[str, str]:
    """Bound identifiers for log entries; omits unbound ones."""
    ctx: dict[str, str] = {}
    request_id = request_id_var.get()
    if request_id:
        ctx["request_id"] = request_id
    job_id = job_id_var.get()
    if job_id:
        ctx["job_id"] = job_id
    return ctx


@contextmanager
def bind_request(request_id: str) -> Iterator[None]:
    """Bind ``request_id`` for the duration of the block.

    A Job binding from an enclosing scope is cleared, since it may belong to a
    different Request.
    """
    rid_token = request_id_var.set(request_id)
    jid_token = job_id_var.set(None)
    try:
        yield
    finally:
        job_id_var.reset(jid_token)
        request_id_var.reset(rid_token)


@contextmanager
def bind_job(request_id: str, job_id: str) -> Iterator[None]:
    """Bind both the owning ``request_id`` and ``job_id`` (Req 13.3)."""
    rid_token = request_id_var.set(request_id)
    jid_token = job_id_var.set(job_id)
    try:
        yield
    finally:
        job_id_var.reset(jid_token)
        request_id_var.reset(rid_token)


@contextmanager
def bind_correlation_id(correlation_id: str | None) -> Iterator[bool]:
    """Bind an inbound Message's correlation ID as the Request_ID (Req 13.5).

    Yields ``True`` when a non-empty correlation ID was bound. An empty or
    missing correlation ID binds nothing (and clears any outer binding, so the
    handler doesn't inherit an unrelated Request_ID) and yields ``False``; the
    caller decides whether that warrants the Req 13.6 warning.
    """
    value = correlation_id or None
    rid_token = request_id_var.set(value)
    jid_token = job_id_var.set(None)
    try:
        yield value is not None
    finally:
        job_id_var.reset(jid_token)
        request_id_var.reset(rid_token)
