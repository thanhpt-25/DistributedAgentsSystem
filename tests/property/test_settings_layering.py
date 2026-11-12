# Feature: das-multi-tenant, Property 6: Settings layering
"""Property 6: Settings layering.

For any platform default, tenant settings and key override, each resolved field equals the value of the most specific
layer that sets it (override, then tenant, then default); ``None`` is unset and ``0`` is a value. ``resolve_budget``
applied to the resolved default keeps Phase 4 Property 4: it never returns a limit above the default.

**Validates: Requirements 4.1, 4.4, 4.5**
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from hypothesis import given
from hypothesis import strategies as st

from das.client.budget import Budget, BudgetError, resolve_budget
from das.client.settings import resolve_settings
from das.config.schema import ClientLimitsConfig, TenantConfig
from das.tenancy.settings import layered

FP = "abcdef01"
NAMES = ("rate", "burst", "max_active_requests", "max_streams", "default_budget", "webhook_url", "webhook_secret_name")
VALUES: dict[str, list[Any]] = {
    "rate": [0, 0.5, 5, 60, 1000],
    "burst": [1, 10, 120, 5000],
    "max_active_requests": [0, 1, 100, 5000],
    "max_streams": [0, 1, 10, 100],
    "default_budget": [{"max_total_tokens": 100}, {"max_total_tokens": 10_000}, {"max_total_tokens": 1_000_000}],
    "webhook_url": ["https://a.example/x", "https://b.example/y"],
    "webhook_secret_name": ["alpha", "beta"],
}


def layer(draw_name: str) -> st.SearchStrategy[Any]:
    return st.none() | st.sampled_from(VALUES[draw_name])


@st.composite
def stacks(draw: st.DrawFn) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    default = {n: draw(st.sampled_from(VALUES[n])) for n in NAMES if n not in ("webhook_url", "webhook_secret_name")}
    default |= {n: draw(layer(n)) for n in ("webhook_url", "webhook_secret_name")}
    tenant = {n: draw(layer(n)) for n in NAMES}
    override = {n: draw(layer(n)) for n in NAMES}
    return default, tenant, override


def build(
    default: dict[str, Any], tenant: dict[str, Any], override: dict[str, Any]
) -> tuple[ClientLimitsConfig, TenantConfig]:
    limits = ClientLimitsConfig.model_validate(
        {"default": default, "overrides": [{"fingerprint": FP, **{k: v for k, v in override.items() if v is not None}}]}
    )
    t = TenantConfig.model_validate(
        {
            "id": "team-a",
            "keys": [{"fingerprint": FP, "scopes": ["read"]}],
            "settings": {k: v for k, v in tenant.items() if v is not None},
        }
    )
    return limits, t


def expected(name: str, default: dict[str, Any], tenant: dict[str, Any], override: dict[str, Any]) -> Any:
    for source in (override, tenant, default):
        if source.get(name) is not None:
            return source[name]
    return None


@given(stacks())
def test_each_field_comes_from_the_most_specific_layer_that_sets_it(stack: tuple[dict[str, Any], ...]) -> None:
    default, tenant, override = stack
    limits, t = build(default, tenant, override)
    got = layered(limits, FP, t)
    for name in NAMES:
        want = expected(name, default, tenant, override)
        have = getattr(got, name)
        if name == "default_budget":
            assert (have.max_total_tokens if have is not None else None) == (want or {}).get("max_total_tokens")
        elif name == "rate":
            assert have == float(want)
        elif name == "webhook_secret_name":
            assert have == (want.lower() if want is not None else None)
        else:
            assert have == want, name


@given(stacks())
def test_a_zero_is_a_value_and_beats_a_less_specific_layer(stack: tuple[dict[str, Any], ...]) -> None:
    default, tenant, override = stack
    tenant = {**tenant, "max_streams": 0}
    override = {**override, "max_streams": None}
    limits, t = build(default, tenant, override)
    assert layered(limits, FP, t).max_streams == 0


@given(stacks())
def test_without_a_tenant_the_result_is_the_phase_4_layering(stack: tuple[dict[str, Any], ...]) -> None:
    default, _, override = stack
    limits, _ = build(default, {}, override)
    assert layered(limits, FP, None) == limits.merged(limits.override_for(FP))
    assert layered(limits, "00000000", None) == limits.default  # no override for this key


@given(
    stacks(),
    st.none() | st.integers(1, 2_000_000),
    st.none() | st.decimals(Decimal("0.01"), Decimal("1000"), places=2),
)
def test_the_resolved_default_budget_still_caps_a_client_budget(
    stack: tuple[dict[str, Any], ...], tokens: int | None, cost: Decimal | None
) -> None:
    default, tenant, override = stack
    limits, t = build(default, tenant, override)
    cap = resolve_settings(FP, limits, t).default_budget
    client = Budget(max_total_tokens=tokens, max_cost=cost) if tokens is not None or cost is not None else None
    try:
        resolved = resolve_budget(client, cap)
    except BudgetError:
        assert client is not None and cap is not None
        return
    if cap is not None and cap.max_total_tokens is not None and resolved is not None:
        assert resolved.max_total_tokens is not None and resolved.max_total_tokens <= cap.max_total_tokens
