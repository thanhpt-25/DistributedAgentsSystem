"""The key directory (das-multi-tenant task 1.2; Phase 5 Req 1.2, 1.3, 1.5, 1.7, 12.3).

``StaticKeyDirectory`` turns a presented key into a Principal from the configured keys and tenants, in constant time
over every configured key. Property 5 (task 1.5) covers it over generated configurations; these are the examples."""

from __future__ import annotations

import hmac as real_hmac
from typing import Any

import pytest
from pydantic import SecretStr

from das.config.schema import TenantConfig
from das.ops.keys import key_fingerprint
from das.tenancy.directory import KeyDirectory, StaticKeyDirectory
from das.tenancy.scopes import Principal, Scope

TEAM_A_RW = "team-a-reader-writer-0123456789"
TEAM_A_ADMIN = "team-a-admin-key-0123456789"
TEAM_B_READ = "team-b-reader-key-0123456789"
LEGACY = "legacy-unclaimed-key-0123456789"
PLATFORM_ADMIN = "platform-admin-key-0123456789"


def fp(key: str) -> str:
    return key_fingerprint(key.encode())


def tenant(tenant_id: str, *keys: tuple[str, list[str]]) -> TenantConfig:
    return TenantConfig.model_validate(
        {"id": tenant_id, "keys": [{"fingerprint": fp(k), "scopes": scopes} for k, scopes in keys]}
    )


TENANTS = [
    tenant("team-a", (TEAM_A_RW, ["submit", "read"]), (TEAM_A_ADMIN, ["submit", "read", "admin"])),
    tenant("team-b", (TEAM_B_READ, ["read"])),
]


def directory(*, tenants: list[TenantConfig] | None = None) -> StaticKeyDirectory:
    return StaticKeyDirectory(
        api_keys=[TEAM_A_RW, TEAM_A_ADMIN, TEAM_B_READ, LEGACY],
        admin_keys=[PLATFORM_ADMIN],
        tenants=TENANTS if tenants is None else tenants,
    )


def principal(candidate: str | None, d: StaticKeyDirectory | None = None) -> Principal:
    found = (d or directory()).resolve(candidate)
    assert found is not None
    return found


# --------------------------------------------------------------------------- resolution


def test_a_tenant_key_resolves_to_its_tenant_and_scopes() -> None:
    assert principal(TEAM_A_RW) == Principal("team-a", frozenset({Scope.SUBMIT, Scope.READ}), False, fp(TEAM_A_RW))
    assert principal(TEAM_A_ADMIN) == Principal(
        "team-a", frozenset({Scope.SUBMIT, Scope.READ, Scope.ADMIN}), False, fp(TEAM_A_ADMIN)
    )
    assert principal(TEAM_B_READ) == Principal("team-b", frozenset({Scope.READ}), False, fp(TEAM_B_READ))


def test_a_key_no_tenant_claims_belongs_to_the_default_tenant_with_submit_and_read() -> None:
    assert principal(LEGACY) == Principal("default", frozenset({Scope.SUBMIT, Scope.READ}), False, fp(LEGACY))


def test_an_admin_key_is_an_operator_in_the_default_tenant_with_every_scope() -> None:
    assert principal(PLATFORM_ADMIN) == Principal(
        "default", frozenset({Scope.SUBMIT, Scope.READ, Scope.ADMIN}), True, fp(PLATFORM_ADMIN)
    )


def test_without_tenants_every_api_key_is_a_default_tenant_key() -> None:
    d = directory(tenants=[])
    for key in (TEAM_A_RW, TEAM_A_ADMIN, TEAM_B_READ, LEGACY):
        assert principal(key, d) == Principal("default", frozenset({Scope.SUBMIT, Scope.READ}), False, fp(key))


@pytest.mark.parametrize(
    "candidate", [None, "", " ", "unknown-key-0123456789", TEAM_A_RW + "x", TEAM_A_RW[:-1], TEAM_A_RW.upper()]
)
def test_an_unknown_or_missing_key_resolves_to_nothing(candidate: str | None) -> None:
    assert directory().resolve(candidate) is None


def test_the_principals_fingerprint_is_that_of_the_matched_key() -> None:
    assert principal(TEAM_B_READ).fingerprint == key_fingerprint(TEAM_B_READ.encode())


def test_resolving_is_deterministic() -> None:
    d = directory()
    assert [d.resolve(LEGACY) for _ in range(3)] == [d.resolve(LEGACY)] * 3


def test_two_tenants_never_share_a_principal() -> None:
    assert principal(TEAM_A_RW).tenant != principal(TEAM_B_READ).tenant


# --------------------------------------------------------------------------- configured keys


def test_secret_strings_and_plain_strings_are_both_accepted() -> None:
    d = StaticKeyDirectory(api_keys=[SecretStr(LEGACY)], admin_keys=[SecretStr(PLATFORM_ADMIN)], tenants=[])
    assert d.resolve(LEGACY) is not None and d.resolve(PLATFORM_ADMIN) is not None


def test_empty_configured_keys_never_match_even_an_empty_candidate() -> None:
    d = StaticKeyDirectory(api_keys=["", LEGACY], admin_keys=[""], tenants=[])
    assert d.resolve("") is None and d.resolve(None) is None
    assert d.resolve(LEGACY) is not None


def test_with_no_usable_keys_everything_is_refused() -> None:
    assert StaticKeyDirectory(api_keys=[], admin_keys=[], tenants=[]).resolve(LEGACY) is None


def test_a_key_listed_in_both_lists_is_an_operator() -> None:
    d = StaticKeyDirectory(api_keys=[LEGACY], admin_keys=[LEGACY], tenants=[])
    assert principal(LEGACY, d).operator is True


def test_a_tenant_fingerprint_that_matches_no_key_claims_nothing() -> None:
    """Config validation rejects such a tenant (task 2.2); the directory must still not invent a Principal."""
    ghost = TenantConfig.model_validate({"id": "ghost", "keys": [{"fingerprint": "deadbeef", "scopes": ["read"]}]})
    d = StaticKeyDirectory(api_keys=[LEGACY], admin_keys=[], tenants=[ghost])
    assert principal(LEGACY, d).tenant == "default"


# --------------------------------------------------------------------------- constant time and the interface


@pytest.mark.parametrize("candidate", [TEAM_A_RW, PLATFORM_ADMIN, "unknown-key-0123456789", None, ""])
def test_every_configured_key_is_compared_whatever_the_candidate(
    monkeypatch: pytest.MonkeyPatch, candidate: str | None
) -> None:
    calls: list[tuple[Any, Any]] = []
    original = real_hmac.compare_digest

    def counting(a: Any, b: Any) -> bool:
        calls.append((a, b))
        return original(a, b)

    d = directory()
    monkeypatch.setattr("das.ops.keys.hmac.compare_digest", counting)
    d.resolve(candidate)
    assert len(calls) == 5  # 4 api keys + 1 admin key: no early exit on a match, and none for an empty candidate


def test_the_static_directory_implements_the_key_directory_interface() -> None:
    assert isinstance(directory(), KeyDirectory)

    class Other:
        def resolve(self, key: str | None) -> Principal | None:
            return None

    assert isinstance(Other(), KeyDirectory)  # any object with resolve() can replace it (Req 12.3)
