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
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from typing import Any, Final

from fastapi import HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader
from pydantic import SecretStr

from das.client.settings import resolve_settings
from das.config.schema import ClientLimitsConfig, TenantConfig
from das.tenancy.directory import KeyDirectory, StaticKeyDirectory
from das.tenancy.scopes import Principal, Scope

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


def _identify(
    request: Request,
    principal: Principal,
    client_limits: ClientLimitsConfig,
    tenants: Mapping[str, TenantConfig],
) -> None:
    """Record who the caller is, once per call: the Principal, and the Client_Settings that go with it.

    The audit context (Req 11.8) and the rate limit (Phase 4 Req 1.2, 2.1) read the same fingerprint, so they cannot
    disagree. The settings are layered default, then the tenant's, then the key's own override (Phase 5 Req 4.1).
    Only the 8-hex fingerprint is kept, never the key.
    """
    audit_context(request)["key_fingerprint"] = principal.fingerprint
    request.state.principal = principal
    layer = tenants.get(principal.tenant)
    request.state.client_settings = resolve_settings(principal.fingerprint, client_limits, layer)


def _directory(
    keys: Iterable[SecretStr | str],
    admin_keys: Iterable[SecretStr | str],
    tenants: Sequence[TenantConfig],
    directory: KeyDirectory | None,
) -> KeyDirectory:
    """The injected directory, or the one that reads the configured keys and tenants (Phase 5 Req 12.3)."""
    return directory if directory is not None else StaticKeyDirectory(keys, admin_keys, tenants)


def api_key_auth(
    keys: Iterable[SecretStr | str],
    admin_keys: Iterable[SecretStr | str] = (),
    client_limits: ClientLimitsConfig | None = None,
    *,
    tenants: Sequence[TenantConfig] = (),
    directory: KeyDirectory | None = None,
) -> Callable[..., Awaitable[None]]:
    """Dependency for the ``/v1`` router: any known key, one Principal, the same 401 for every other.

    A matched key leaves its Principal on ``request.state.principal`` and its Client_Settings on
    ``request.state.client_settings`` for the rate limit. Without ``tenants`` every API_Key is a Default_Tenant key
    with Scopes submit and read, as in Phase 4 (Req 1.5). Empty configured keys are ignored; with no usable keys every
    call is rejected. ``directory`` replaces the static one (Req 12.3).
    """
    keyring = _directory(keys, admin_keys, tenants, directory)
    limits = client_limits if client_limits is not None else ClientLimitsConfig()
    by_id = {t.id: t for t in tenants}

    async def require_api_key(
        request: Request,
        api_key: str | None = Security(_header_scheme),
    ) -> None:
        principal = keyring.resolve(api_key)
        if principal is None:
            raise _reject(status.HTTP_401_UNAUTHORIZED, UNAUTHORIZED_BODY)
        _identify(request, principal, limits, by_id)

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
    *,
    tenants: Sequence[TenantConfig] = (),
    directory: KeyDirectory | None = None,
) -> Callable[..., Awaitable[None]]:
    """Dependency for the ``/v1/admin`` router (Req 11.2-11.5).

    Records the Principal and its Key_Fingerprint, so the Audit_Entry carries them even when the call is rejected with
    403 (Req 11.8), and leaves the key's Client_Settings on ``request.state`` for the rate limit. Only an Operator (an
    Admin_API_Key) passes here; a key of a tenant is refused with 403 ``forbidden``, or ``admin_disabled`` when nothing
    in the configuration can administer: no Admin_API_Key and no tenant key holding the ``admin`` Scope (Phase 4 shape).
    """
    keyring = _directory(keys, admin_keys, tenants, directory)
    limits = client_limits if client_limits is not None else ClientLimitsConfig()
    by_id = {t.id: t for t in tenants}
    can_administer = (
        directory is not None
        or bool(_configured(admin_keys))
        or any(Scope.ADMIN.value in key.scopes for t in tenants for key in t.keys)
    )

    async def require_admin_key(
        request: Request,
        api_key: str | None = Security(_header_scheme),
    ) -> None:
        principal = keyring.resolve(api_key)
        if principal is None:
            raise _reject(status.HTTP_401_UNAUTHORIZED, UNAUTHORIZED_BODY)
        _identify(request, principal, limits, by_id)
        if not principal.operator:
            raise _reject(status.HTTP_403_FORBIDDEN, FORBIDDEN_BODY if can_administer else ADMIN_DISABLED_BODY)

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
