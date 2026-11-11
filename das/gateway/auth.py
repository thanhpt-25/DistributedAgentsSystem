"""API_Key and Admin_API_Key authorization for the Gateway_API (Requirements 1.3, 11).

Two FastAPI dependency builders, both declared at router level so they run before
any handler code touches the State_Store or publishes a control command:

- :func:`api_key_auth` for the ``/v1`` router: accepts an API_Key or an
  Admin_API_Key with identical behavior (Req 11.6); anything else is 401.
- :func:`admin_key_auth` for the ``/v1/admin`` router: ``NONE`` → 401
  ``unauthorized``, ``API`` → 403 ``forbidden`` (or ``admin_disabled`` when no
  Admin_API_Key is configured), ``ADMIN`` → handler (Req 11.2-11.5).

Every presented key is compared against every configured key in constant time via
:func:`das.ops.keys.classify_key` (Req 11.7). Key values are never logged or
echoed; only the Key_Fingerprint is recorded for the Audit_Entry (Req 11.8).
"""

from __future__ import annotations

import hmac
from collections.abc import Awaitable, Callable, Iterable
from typing import Any, Final

from fastapi import HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader
from pydantic import SecretStr

from das.client.settings import resolve_settings
from das.config.schema import ClientLimitsConfig
from das.ops.keys import KeyClass, classify_key, key_fingerprint

API_KEY_HEADER: Final = "X-API-Key"
UNAUTHORIZED_BODY: Final = {"error": "unauthorized"}
FORBIDDEN_BODY: Final = {"error": "forbidden"}
ADMIN_DISABLED_BODY: Final = {"error": "admin_disabled"}

#: ASGI scope key holding the per-call audit context shared with
#: :class:`das.gateway.audit.AuditMiddleware`.
AUDIT_SCOPE_KEY: Final = "das.audit"

_header_scheme = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)


def _to_bytes(key: SecretStr | str) -> bytes:
    raw = key.get_secret_value() if isinstance(key, SecretStr) else key
    return raw.encode("utf-8")


def _configured(keys: Iterable[SecretStr | str]) -> tuple[bytes, ...]:
    """Encoded non-empty keys; empty entries never match anything."""
    return tuple(b for b in map(_to_bytes, keys) if b)


def is_valid_key(candidate: str | None, keys: Iterable[bytes]) -> bool:
    """Constant-time check of ``candidate`` against every configured key.

    Every key is compared (no early exit on a match), so timing does not reveal
    which key matched or how many keys exist before it.
    """
    if not candidate:
        return False
    presented = candidate.encode("utf-8")
    matched = False
    for key in keys:
        matched |= hmac.compare_digest(presented, key)
    return matched


def _reject(code: int, body: dict[str, str]) -> HTTPException:
    headers = {"WWW-Authenticate": "ApiKey"} if code == status.HTTP_401_UNAUTHORIZED else None
    return HTTPException(status_code=code, detail=dict(body), headers=headers)


def _identify(request: Request, api_key: str, client_limits: ClientLimitsConfig) -> None:
    """Record a matched key's Key_Fingerprint and Client_Settings, once per call.

    The audit context (Req 11.8) and the rate limit (Phase 4 Req 1.2, 2.1) read the same
    fingerprint, so they cannot disagree. Only the 8-hex fingerprint is kept, never the key.
    """
    fingerprint = key_fingerprint(api_key.encode("utf-8"))
    audit_context(request)["key_fingerprint"] = fingerprint
    request.state.client_settings = resolve_settings(fingerprint, client_limits)


def api_key_auth(
    keys: Iterable[SecretStr | str],
    admin_keys: Iterable[SecretStr | str] = (),
    client_limits: ClientLimitsConfig | None = None,
) -> Callable[..., Awaitable[None]]:
    """Dependency for the ``/v1`` router: API_Key or Admin_API_Key, same response.

    Empty configured keys are ignored; with no usable keys every call is rejected. A matched
    key leaves its Client_Settings on ``request.state.client_settings`` for the rate limit.
    """
    api = _configured(keys)
    admin = _configured(admin_keys)
    limits = client_limits if client_limits is not None else ClientLimitsConfig()

    async def require_api_key(
        request: Request,
        api_key: str | None = Security(_header_scheme),
    ) -> None:
        kind, _ = classify_key(api_key, api, admin)
        if kind is KeyClass.NONE or not api_key:
            raise _reject(status.HTTP_401_UNAUTHORIZED, UNAUTHORIZED_BODY)
        _identify(request, api_key, limits)

    return require_api_key


def audit_context(request: Request) -> dict[str, Any]:
    """Mutable audit context for this call (created by the middleware if present)."""
    ctx = request.scope.get(AUDIT_SCOPE_KEY)
    if not isinstance(ctx, dict):
        ctx = {}
        request.scope[AUDIT_SCOPE_KEY] = ctx
    return ctx


def admin_key_auth(
    keys: Iterable[SecretStr | str],
    admin_keys: Iterable[SecretStr | str],
    client_limits: ClientLimitsConfig | None = None,
) -> Callable[..., Awaitable[None]]:
    """Dependency for the ``/v1/admin`` router (Req 11.2-11.5).

    Records the Key_Fingerprint of a matched key in the audit context, so the
    Audit_Entry carries it even when the call is rejected with 403 (Req 11.8). It also
    leaves the key's Client_Settings on ``request.state`` for the rate limit.
    """
    api = _configured(keys)
    admin = _configured(admin_keys)
    limits = client_limits if client_limits is not None else ClientLimitsConfig()

    async def require_admin_key(
        request: Request,
        api_key: str | None = Security(_header_scheme),
    ) -> None:
        kind, _ = classify_key(api_key, api, admin)
        if kind is not KeyClass.NONE and api_key:
            _identify(request, api_key, limits)
        if kind is KeyClass.NONE:
            raise _reject(status.HTTP_401_UNAUTHORIZED, UNAUTHORIZED_BODY)
        if kind is KeyClass.API:
            body = FORBIDDEN_BODY if admin else ADMIN_DISABLED_BODY
            raise _reject(status.HTTP_403_FORBIDDEN, body)

    return require_admin_key


__all__ = [
    "ADMIN_DISABLED_BODY",
    "API_KEY_HEADER",
    "AUDIT_SCOPE_KEY",
    "FORBIDDEN_BODY",
    "UNAUTHORIZED_BODY",
    "admin_key_auth",
    "api_key_auth",
    "audit_context",
    "is_valid_key",
]
