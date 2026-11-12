"""Where tenants and keys come from (Phase 5 Req 1.2, 1.3, 1.5, 1.7, 12.3). Pure: no I/O, no logging.

:class:`KeyDirectory` is the one interface that turns a presented key into a :class:`Principal`. Today's only
implementation, :class:`StaticKeyDirectory`, reads the configured keys and tenants; a directory that reads keys from a
store could replace it without a route handler noticing (Req 12.3).
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Protocol, runtime_checkable

from pydantic import SecretStr

from das.config.schema import TenantConfig
from das.ops.keys import KeyClass, classify_key, key_fingerprint
from das.tenancy.scopes import DEFAULT_TENANT, Principal, Scope

__all__ = ["KeyDirectory", "StaticKeyDirectory"]

#: What a key that no tenant claims may do in the Default_Tenant (Phase 4 behaviour).
_UNCLAIMED_SCOPES = frozenset({Scope.SUBMIT, Scope.READ})
_ALL_SCOPES = frozenset(Scope)


@runtime_checkable
class KeyDirectory(Protocol):
    def resolve(self, key: str | None) -> Principal | None:
        """The Principal of a presented key, or ``None`` when the key is unknown or missing."""
        ...


def _encoded(keys: Iterable[SecretStr | str]) -> tuple[bytes, ...]:
    """Encoded non-empty keys; an empty entry never matches anything."""
    raw = (k.get_secret_value() if isinstance(k, SecretStr) else k for k in keys)
    return tuple(b for b in (r.encode("utf-8") for r in raw) if b)


class StaticKeyDirectory:
    """Principals from ``api_keys``, ``admin_api_keys`` and ``tenants``.

    The matched key is found by :func:`das.ops.keys.classify_key`, which compares the candidate with every configured
    key and exits early on none (constant time, v2 Req 11.7). Only after a match is the key's fingerprint looked up in
    the tenants, so the tenant table is never searched with unauthenticated input.
    """

    def __init__(
        self,
        api_keys: Iterable[SecretStr | str],
        admin_keys: Iterable[SecretStr | str],
        tenants: Sequence[TenantConfig],
    ) -> None:
        self._api = _encoded(api_keys)
        self._admin = _encoded(admin_keys)
        self._claims: dict[str, tuple[str, frozenset[Scope]]] = {
            key.fingerprint: (tenant.id, frozenset(Scope(s) for s in key.scopes))
            for tenant in tenants
            for key in tenant.keys
        }

    def resolve(self, key: str | None) -> Principal | None:
        kind, index = classify_key(key, self._api, self._admin)
        if index is None:
            return None
        if kind is KeyClass.ADMIN:
            return Principal(DEFAULT_TENANT, _ALL_SCOPES, True, key_fingerprint(self._admin[index]))
        fingerprint = key_fingerprint(self._api[index])
        tenant, scopes = self._claims.get(fingerprint, (DEFAULT_TENANT, _UNCLAIMED_SCOPES))
        return Principal(tenant, scopes, False, fingerprint)
