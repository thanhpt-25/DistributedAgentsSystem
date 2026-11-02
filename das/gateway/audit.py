"""Audit_Entry emission for Admin_API calls (Requirement 11.8).

:class:`AuditMiddleware` is a pure ASGI middleware that only acts on HTTP calls
under the admin prefix (``/v1/admin``). It buffers the response produced by the
handler or by the authorization dependency, emits exactly one ``audit`` log
entry at info level, and only then forwards the response to the client.

Fields: ``at`` (UTC, millisecond precision), ``action`` (``"<METHOD> <route
template>"``), ``target`` (the endpoint's path parameter, e.g. Dead_Letter_ID or
agent ID, or null), ``http_status``, ``error_code`` (the ``error`` of the JSON
body, or null) and ``key_fingerprint`` (set by
:func:`das.gateway.auth.admin_key_auth` when the presented key matched, else null).
"""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Final

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from das.gateway.auth import AUDIT_SCOPE_KEY
from das.observability import get_logger
from das.observability.logging import format_timestamp

log = get_logger(__name__)

ADMIN_PREFIX: Final = "/v1/admin"
#: Route template used when no route matched the path.
UNMATCHED_ROUTE: Final = "_unmatched"


def _now() -> datetime:
    return datetime.now(UTC)


def is_admin_path(path: str, prefix: str = ADMIN_PREFIX) -> bool:
    return path == prefix or path.startswith(prefix + "/")


def _error_code(body: bytes) -> str | None:
    try:
        parsed = json.loads(body)
    except (ValueError, UnicodeDecodeError):
        return None
    if isinstance(parsed, dict):
        code = parsed.get("error")
        if isinstance(code, str):
            return code
    return None


def _route_template(scope: Scope) -> str:
    route = scope.get("route")
    path = getattr(route, "path", None)
    return path if isinstance(path, str) else UNMATCHED_ROUTE


def _target(scope: Scope) -> str | None:
    params = scope.get("path_params") or {}
    for value in params.values():
        return str(value)
    return None


class AuditMiddleware:
    """Emit one Audit_Entry per Admin_API call, before the response is sent."""

    def __init__(
        self,
        app: ASGIApp,
        prefix: str = ADMIN_PREFIX,
        now: Callable[[], datetime] = _now,
    ) -> None:
        self.app = app
        self.prefix = prefix
        self.now = now

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or not is_admin_path(scope.get("path", ""), self.prefix):
            await self.app(scope, receive, send)
            return

        ctx: dict[str, Any] = {}
        scope[AUDIT_SCOPE_KEY] = ctx
        start: Message | None = None
        chunks: list[bytes] = []
        sent = False

        def emit(http_status: int, error_code: str | None) -> None:
            log.info(
                "audit",
                at=format_timestamp(self.now()),
                action=f"{scope.get('method', '')} {_route_template(scope)}",
                target=_target(scope),
                http_status=http_status,
                error_code=error_code,
                key_fingerprint=ctx.get("key_fingerprint"),
            )

        async def flush() -> None:
            nonlocal sent
            assert start is not None
            body = b"".join(chunks)
            emit(int(start["status"]), _error_code(body))
            sent = True
            await send(start)
            await send({"type": "http.response.body", "body": body, "more_body": False})

        async def audited_send(message: Message) -> None:
            nonlocal start
            if sent:
                await send(message)
            elif message["type"] == "http.response.start":
                start = message
            elif message["type"] == "http.response.body":
                chunks.append(message.get("body", b""))
                if not message.get("more_body", False):
                    await flush()
            else:
                await send(message)

        try:
            await self.app(scope, receive, audited_send)
        except Exception:
            if not sent:
                # The outer ServerErrorMiddleware turns this into a plain 500.
                emit(500, None)
                sent = True
            raise
        if not sent and start is not None:
            await flush()


__all__ = ["ADMIN_PREFIX", "UNMATCHED_ROUTE", "AuditMiddleware", "is_admin_path"]
