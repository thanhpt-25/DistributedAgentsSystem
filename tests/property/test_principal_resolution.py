# Feature: das-multi-tenant, Property 5: Principal resolution
"""Property 5: Principal resolution.

For any valid configuration and presented key, ``StaticKeyDirectory.resolve`` returns exactly one Principal or none:
an ``admin_api_keys`` key is an Operator in ``default`` with every Scope; an ``api_keys`` key listed by a tenant gets
that tenant and those Scopes; any other ``api_keys`` key is ``default`` with ``{submit, read}``; an unknown key gives
none. With no ``tenants``, every ``api_keys`` key is a ``default`` key. Configurations with a fingerprint matching no
key, listed twice, listed for an admin key, or shared by two keys are rejected by ``tenant_errors``, and the error
never contains a key value.

**Validates: Requirements 1.2, 1.3, 1.4, 1.5, 1.7**
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from hypothesis import assume, given
from hypothesis import strategies as st

from das.config.schema import AgentConfig, tenant_errors
from das.ops.keys import key_fingerprint
from das.tenancy.directory import StaticKeyDirectory
from das.tenancy.scopes import DEFAULT_TENANT, Scope

keys_ = st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789-", min_size=12, max_size=24)
scope_sets = st.sets(st.sampled_from(["submit", "read", "admin"]), min_size=1).map(sorted)
tenant_ids = st.lists(st.sampled_from(["a", "b", "c", "team-x", "team-y", "ops"]), unique=True, max_size=4)


def fp(key: str) -> str:
    return key_fingerprint(key.encode())


@dataclass(frozen=True)
class World:
    api: list[str]
    admin: list[str]
    tenants: list[dict[str, Any]]
    claimed: dict[str, tuple[str, frozenset[str]]]  # fingerprint -> (tenant, scopes)

    def config(self, **extra: Any) -> AgentConfig:
        data = {
            "role": "gateway",
            "agent_id": "gw",
            "redis": {"url": "redis://127.0.0.1:1/0"},
            "api_keys": self.api,
            "admin_api_keys": self.admin,
            "tenants": self.tenants,
        }
        data.update(extra)
        return AgentConfig.model_validate(data)


@st.composite
def worlds(draw: st.DrawFn) -> World:
    all_keys = draw(st.lists(keys_, min_size=1, max_size=8, unique=True))
    assume(len({fp(k) for k in all_keys}) == len(all_keys))  # a valid configuration: one fingerprint per key
    n_admin = draw(st.integers(0, min(2, len(all_keys) - 1)))
    admin, api = all_keys[:n_admin], all_keys[n_admin:]
    ids = draw(tenant_ids)
    pool = list(api)
    tenants: list[dict[str, Any]] = []
    claimed: dict[str, tuple[str, frozenset[str]]] = {}
    for tid in ids:
        take = draw(st.lists(st.sampled_from(pool), unique=True, max_size=3)) if pool else []
        if not take:
            continue
        entries = []
        for key in take:
            pool.remove(key)
            scopes = draw(scope_sets)
            entries.append({"fingerprint": fp(key), "scopes": scopes})
            claimed[fp(key)] = (tid, frozenset(scopes))
        tenants.append({"id": tid, "keys": entries})
    return World(api, admin, tenants, claimed)


def directory(world: World) -> StaticKeyDirectory:
    return StaticKeyDirectory(world.api, world.admin, world.config().tenants)


@given(worlds(), st.lists(keys_, max_size=3))
def test_every_key_resolves_to_the_principal_the_configuration_names(world: World, strangers: list[str]) -> None:
    assert tenant_errors(world.config()) == []
    d = directory(world)
    for key in world.admin:
        p = d.resolve(key)
        assert p is not None
        assert (p.tenant, p.scopes, p.operator, p.fingerprint) == (DEFAULT_TENANT, frozenset(Scope), True, fp(key))
    for key in world.api:
        p = d.resolve(key)
        assert p is not None and not p.operator and p.fingerprint == fp(key)
        tenant, scopes = world.claimed.get(fp(key), (DEFAULT_TENANT, frozenset({"submit", "read"})))
        assert (p.tenant, {s.value for s in p.scopes}) == (tenant, set(scopes))
    known = set(world.api) | set(world.admin)
    for key in [*strangers, "", None]:
        if key not in known:
            assert d.resolve(key) is None


@given(worlds())
def test_the_result_is_a_pure_function_of_the_configuration_and_the_key(world: World) -> None:
    d1, d2 = directory(world), directory(world)
    for key in [*world.api, *world.admin, "unknown-key-0000", None]:
        assert d1.resolve(key) == d1.resolve(key) == d2.resolve(key)


@given(worlds())
def test_without_tenants_every_api_key_is_a_default_key(world: World) -> None:
    d = StaticKeyDirectory(world.api, world.admin, [])
    for key in world.api:
        p = d.resolve(key)
        assert p is not None and (p.tenant, {s.value for s in p.scopes}) == (DEFAULT_TENANT, {"submit", "read"})
        assert not p.operator


VIOLATIONS = ["unknown", "twice_in_tenant", "twice_across_tenants", "admin_key", "duplicate_id", "reserved_id"]


@given(worlds(), st.sampled_from(VIOLATIONS))
def test_a_violation_is_rejected_and_the_error_never_contains_a_key_or_a_fingerprint(
    world: World, violation: str
) -> None:
    assume(world.tenants and world.api)
    tenants = [dict(t, keys=list(t["keys"])) for t in world.tenants]
    first = tenants[0]["keys"][0]
    if violation == "unknown":
        first["fingerprint"] = "0" * 8
        assume("0" * 8 not in {fp(k) for k in world.api})
    elif violation == "twice_in_tenant":
        tenants[0]["keys"].append(dict(first))
    elif violation == "twice_across_tenants":
        assume(len(tenants) > 1)
        tenants[1]["keys"].append(dict(first))
    elif violation == "admin_key":
        assume(world.admin)
        tenants[0]["keys"].append({"fingerprint": fp(world.admin[0]), "scopes": ["read"]})
    elif violation == "duplicate_id":
        assume(len(tenants) > 1)
        tenants[1]["id"] = tenants[0]["id"]
    else:
        tenants[0]["id"] = "default"
    errors = tenant_errors(world.config(tenants=tenants))
    assert errors, violation
    text = " ".join(f"{e.field} {e.reason}" for e in errors)
    for secret in [*world.api, *world.admin, *(fp(k) for k in [*world.api, *world.admin])]:
        assert secret not in text
