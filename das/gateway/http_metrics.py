"""HTTP metrics for the Gateway_API ``http_port`` app (Requirement 2.10).

:class:`HttpMetricsMiddleware` is a pure ASGI middleware. For every HTTP call it
records one ``das_http_requests_total{route, method, status_code}`` increment and
one ``das_http_request_duration_seconds{route, method}`` observation, measured with
``clock.monotonic()`` from receipt of the call until the last body chunk is sent.

``route`` is the matched route template (``scope["route"]``, e.g.
``/v1/requests/{request_id}``), never the concrete URL path, and ``_unmatched``
when no route matched. ``method`` is normalized by :class:`GatewayMetrics`.

The middleware is only installed on the ``http_port`` app built by
:func:`das.gateway.app.create_app`; the Ops server is a separate app and is never
wrapped with it.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from fastapi.routing import iter_route_contexts
from starlette.routing import BaseRoute
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from das.clock import Clock, SystemClock
from das.observability.metrics import GatewayMetrics


def route_template(route: object) -> str | None:
    """The path template of a matched route, or ``None``."""
    template = getattr(route, "path_format", None) or getattr(route, "path", None)
    return template if isinstance(template, str) and template else None


def _flatten(routes: Iterable[BaseRoute]) -> Iterator[object]:
    """Every concrete route, including those of included routers.

    FastAPI keeps included routers as a single wrapper entry in ``app.routes`` and,
    when serving, writes the router's original route into ``scope["route"]``; the
    templates are therefore read from those same original routes.
    """
    for context in iter_route_contexts(list(routes)):
        yield context.original_route


def route_templates(routes: Iterable[BaseRoute]) -> tuple[str, ...]:
    """Distinct route templates of ``routes`` in registration order (Req 5.2)."""
    seen: dict[str, None] = {}
    for route in _flatten(routes):
        template = route_template(route)
        if template is not None:
            seen.setdefault(template, None)
    return tuple(seen)


class HttpMetricsMiddleware:
    """Record ``das_http_*`` metrics for each HTTP call on the wrapped app."""

    def __init__(self, app: ASGIApp, metrics: GatewayMetrics, clock: Clock | None = None) -> None:
        self.app = app
        self.metrics = metrics
        self.clock = clock or SystemClock()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        start = self.clock.monotonic()
        status_code: int | None = None
        recorded = False

        def record(code: int) -> None:
            nonlocal recorded
            if recorded:
                return
            recorded = True
            # The router writes the matched route into the shared scope dict.
            self.metrics.http_request(
                route_template(scope.get("route")),
                str(scope.get("method", "")),
                code,
                self.clock.monotonic() - start,
            )

        async def measured_send(message: Message) -> None:
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = int(message["status"])
            await send(message)
            if (
                message["type"] == "http.response.body"
                and not message.get("more_body", False)
                and status_code is not None
            ):
                record(status_code)

        try:
            await self.app(scope, receive, measured_send)
        except Exception:
            # The outer ServerErrorMiddleware turns this into a 500 response.
            record(500 if status_code is None else status_code)
            raise
        if status_code is not None:
            record(status_code)


__all__ = ["HttpMetricsMiddleware", "route_template", "route_templates"]
