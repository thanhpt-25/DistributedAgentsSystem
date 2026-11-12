"""The routes of a FastAPI application, flattened (das-multi-tenant task 4.3).

Recent FastAPI keeps a router that was included into an application as one wrapper object, so ``app.routes`` holds a
couple of wrappers rather than the routes themselves, and a walk that looks only at ``app.routes`` sees none of them
and passes without checking anything. :func:`method_templates` reaches the real routes, whose ``path_format`` already
includes the router's prefix, and refuses an empty result."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

#: A Gateway has far more than this; the floor exists so a walk that finds nothing fails loudly.
MIN_ROUTES = 10


def _flatten(routes: list[Any]) -> Iterator[Any]:
    for route in routes:
        inner = getattr(route, "original_router", None)
        if inner is not None:
            yield from _flatten(list(inner.routes))
        else:
            yield route


def method_templates(app: Any, prefix: str = "/v1") -> set[tuple[str, str]]:
    """Every ``(METHOD, path template)`` the application serves under ``prefix`` (HEAD and OPTIONS left out)."""
    found = {
        (method, route.path_format)
        for route in _flatten(list(app.routes))
        if hasattr(route, "path_format")
        for method in getattr(route, "methods", None) or ()
        if method not in ("HEAD", "OPTIONS") and route.path_format.startswith(prefix)
    }
    assert len(found) >= MIN_ROUTES, f"found only {len(found)} routes: the walk missed the included routers"
    return found
