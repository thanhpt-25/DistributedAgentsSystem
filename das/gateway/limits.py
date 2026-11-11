"""Per-key rate limit dependency for the ``/v1`` and admin routers (Phase 4 Req 2.1, 2.2, 2.5, 2.6).

Runs after the key dependency (:func:`das.gateway.auth.api_key_auth` or
:func:`das.gateway.auth.admin_key_auth`), which has already stored the caller's
:class:`das.client.settings.ClientSettings` on ``request.state``. A rejected key therefore
never consumes a token. ``/healthz`` sits outside both routers and is never limited (Req 2.6).
The bucket lives in Redis and is read by Redis ``TIME``, so every Gateway replica shares it (Req 2.4).
"""

from __future__ import annotations

import math
from collections.abc import Awaitable, Callable
from typing import Any, Final

from fastapi import HTTPException, Request, status

from das.client.settings import ClientSettings
from das.observability.metrics import GatewayMetrics
from das.state.store import StateStore

RATE_LIMITED_BODY: Final = {"error": "rate_limited"}
RETRY_AFTER_HEADER: Final = "Retry-After"

Bounded = Callable[..., Awaitable[Any]]


def retry_after_seconds(retry_after: float) -> int:
    """Whole seconds for the ``Retry-After`` header: rounded up, never below 1 (Req 2.2)."""
    return max(1, math.ceil(retry_after))


def rate_limit(
    *,
    store: StateStore,
    bounded: Bounded,
    metrics: Callable[[], GatewayMetrics],
) -> Callable[[Request], Awaitable[None]]:
    """Dependency taking one token per call; 429 with ``Retry-After`` when the bucket is empty.

    A store failure surfaces as ``StoreUnavailable`` (503) through ``bounded``: the check fails
    closed rather than letting the call through (design D7). ``rate == 0`` disables the limit and
    skips Redis altogether (Req 2.1).
    """

    async def enforce(request: Request) -> None:
        settings: ClientSettings = request.state.client_settings
        if settings.rate == 0:
            return
        retry_after = await bounded(store.rate_check(settings.fingerprint, settings.rate, settings.burst))
        if retry_after is None:
            return
        metrics().client_rejected("rate_limited")
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail=dict(RATE_LIMITED_BODY),
            headers={RETRY_AFTER_HEADER: str(retry_after_seconds(retry_after))},
        )

    return enforce


__all__ = ["RATE_LIMITED_BODY", "RETRY_AFTER_HEADER", "rate_limit", "retry_after_seconds"]
