"""Authentication for the Gateway_API: a presented key becomes a Principal (Requirements 1.3, 11; Phase 5 Req 1, 2.1).

Two FastAPI dependency builders, both declared at router level so they run before
any handler code touches the State_Store or publishes a control command:

- :func:`api_key_auth` for the ``/v1`` router: any known key (an API_Key of any tenant or an
  Admin_API_Key) becomes a Principal, left on ``request.state.principal``; anything else is 401.
- :func:`admin_key_auth` for the ``/v1/admin`` router: the same, authentication only.

What a Principal may do is decided next by :func:`das.gateway.authz.authorize_route` against the route
table: 403 ``forbidden`` naming the Scope or role, or ``admin_disabled`` when nothing in the
configuration can administer (:func:`can_administer`, the Phase 4 answer).

Every presented key is compared against every configured key in constant time via
:func:`das.ops.keys.classify_key` (Req 11.7). Key values are never logged or
echoed; only the Key_Fingerprint, the tenant and the operator flag are recorded for the
Audit_Entry (Req 11.8; Phase 5 Req 2.7).
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
    context = audit_context(request)
    context["key_fingerprint"] = principal.fingerprint
    context["tenant"] = principal.tenant  # Phase 5 Req 2.7
    context["operator"] = principal.operator
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
    """Dependency for the ``/v1/admin`` router (Req 11.2-11.5): authentication only.

    An unknown or missing key is 401. A known key leaves its Principal, Key_Fingerprint and Client_Settings on the
    request (the fingerprint also in the audit context, so the Audit_Entry carries it even for a refused call, Req
    11.8). What the key may do is decided next by :func:`das.gateway.authz.authorize_route` against the route table:
    the dead-letter routes need Scope ``admin`` (and show a tenant only its own records), the rest the Operator role.
    """
    keyring = _directory(keys, admin_keys, tenants, directory)
    limits = client_limits if client_limits is not None else ClientLimitsConfig()
    by_id = {t.id: t for t in tenants}

    async def require_admin_key(
        request: Request,
        api_key: str | None = Security(_header_scheme),
    ) -> None:
        principal = keyring.resolve(api_key)
        if principal is None:
            raise _reject(status.HTTP_401_UNAUTHORIZED, UNAUTHORIZED_BODY)
        _identify(request, principal, limits, by_id)

    return require_admin_key


def can_administer(
    admin_keys: Iterable[SecretStr | str],
    tenants: Sequence[TenantConfig] = (),
    directory: KeyDirectory | None = None,
) -> bool:
    """Whether anything in the configuration can administer: an Admin_API_Key, a tenant key holding the ``admin``
    Scope, or an injected directory. When nothing can, an admin route answers ``admin_disabled`` instead of
    ``forbidden`` (the Phase 4 answer, Phase 5 Req 2.3)."""
    return (
        directory is not None
        or bool(_configured(admin_keys))
        or any(Scope.ADMIN.value in key.scopes for t in tenants for key in t.keys)
    )


__all__ = [
    "ADMIN_DISABLED_BODY",
    "API_KEY_HEADER",
    "AUDIT_SCOPE_KEY",
    "FORBIDDEN_BODY",
    "UNAUTHORIZED_BODY",
    "admin_key_auth",
    "api_key_auth",
    "audit_context",
    "can_administer",
    "is_valid_key",
]
