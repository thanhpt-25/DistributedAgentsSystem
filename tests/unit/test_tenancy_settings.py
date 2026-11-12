"""Settings layering (das-multi-tenant task 1.3; Phase 5 Req 1.6, 4.1, 4.2, 4.4).

Each Client_Settings field comes from the most specific layer that sets it: the key's override, then the tenant's
settings, then ``client_limits.default``. ``None`` means "not set" (so ``0`` is a value: it disables a limit)."""

from __future__ import annotations

from typing import Any

import pytest

from das.client.budget import Budget
from das.client.settings import ClientSettings, resolve_settings
from das.config.schema import (
    BudgetConfig,
    ClientLimitsConfig,
    ClientOverride,
    ClientSettingsConfig,
    TenantConfig,
    TenantSettings,
)
from das.tenancy.settings import layered

FP = "cbabecbd"


def tenant_with(**settings: Any) -> TenantConfig:
    return TenantConfig.model_validate(
        {"id": "team-a", "keys": [{"fingerprint": FP, "scopes": ["read"]}], "settings": settings}
    )


def limits(default: dict[str, Any] | None = None, override: dict[str, Any] | None = None) -> ClientLimitsConfig:
    return ClientLimitsConfig(
        default=ClientSettingsConfig(**(default or {})),
        overrides=[ClientOverride(fingerprint=FP, **override)] if override is not None else [],
    )


# --------------------------------------------------------------------------- the three layers


def test_without_a_tenant_the_result_is_the_phase_4_one() -> None:
    lim = limits({"rate": 10}, {"burst": 5})
    assert resolve_settings(FP, lim) == resolve_settings(FP, lim, None)
    assert layered(lim, FP, None) == lim.settings_for(FP)


def test_a_tenant_layer_sits_between_the_default_and_the_override() -> None:
    lim = limits({"rate": 10, "burst": 20, "max_streams": 3}, {"rate": 1})
    s = resolve_settings(FP, lim, tenant_with(rate=50, burst=60))
    assert (s.rate, s.burst, s.max_streams) == (1.0, 60, 3)  # override > tenant > default, field by field


@pytest.mark.parametrize(
    ("default", "tenant_value", "override", "expected"),
    [
        (60, None, None, 60),  # default only
        (60, 5, None, 5),  # tenant over default
        (60, None, 7, 7),  # override over default
        (60, 5, 7, 7),  # override over tenant
        (60, 0, None, 0),  # 0 disables: it is a value of the tenant layer, not "unset"
        (60, 5, 0, 0),  # and of the override layer
    ],
)
def test_each_field_comes_from_the_most_specific_layer_that_sets_it(
    default: int, tenant_value: int | None, override: int | None, expected: int
) -> None:
    tenant_settings = {} if tenant_value is None else {"max_streams": tenant_value}
    override_settings = {} if override is None else {"max_streams": override}
    lim = limits({"max_streams": default}, override_settings)
    assert resolve_settings(FP, lim, tenant_with(**tenant_settings)).max_streams == expected


def test_an_override_applies_to_its_key_whatever_the_tenant() -> None:
    lim = limits({}, {"rate": 2})
    assert resolve_settings(FP, lim, None).rate == 2.0
    assert resolve_settings(FP, lim, tenant_with(rate=99)).rate == 2.0
    other = TenantConfig.model_validate({"id": "team-z", "keys": [{"fingerprint": FP, "scopes": ["read"]}]})
    assert resolve_settings(FP, lim, other).rate == 2.0


def test_a_tenant_with_no_settings_changes_nothing() -> None:
    lim = limits({"rate": 10}, {"burst": 5})
    plain = TenantConfig.model_validate({"id": "team-a", "keys": [{"fingerprint": FP, "scopes": ["read"]}]})
    assert resolve_settings(FP, lim, plain) == resolve_settings(FP, lim)


# --------------------------------------------------------------------------- budget and webhook


def test_the_default_budget_comes_from_the_most_specific_layer() -> None:
    lim = limits({"default_budget": BudgetConfig(max_total_tokens=1000)})
    assert resolve_settings(FP, lim).default_budget == Budget(max_total_tokens=1000)
    tenant = tenant_with(default_budget={"max_total_tokens": 200})
    assert resolve_settings(FP, lim, tenant).default_budget == Budget(max_total_tokens=200)
    lim2 = limits(
        {"default_budget": BudgetConfig(max_total_tokens=1000)}, {"default_budget": BudgetConfig(max_total_tokens=5)}
    )
    assert resolve_settings(FP, lim2, tenant).default_budget == Budget(max_total_tokens=5)


def test_a_tenant_budget_acts_as_the_cap_a_key_default_would() -> None:
    """``resolve_budget`` (v4 Property 4) then caps a client budget by it."""
    s = resolve_settings(FP, limits(), tenant_with(default_budget={"max_total_tokens": 300}))
    assert s.default_budget == Budget(max_total_tokens=300)


def test_the_webhook_target_layers_like_the_other_fields() -> None:
    lim = limits({"webhook_url": "https://default.example.com/h", "webhook_secret_name": "platform"})
    t = tenant_with(webhook_url="https://team.example.com/h", webhook_secret_name="team_a")
    s = resolve_settings(FP, lim, t)
    assert (s.webhook_url, s.webhook_secret_name) == ("https://team.example.com/h", "team_a")
    lim2 = limits({"webhook_url": "https://default.example.com/h"}, {"webhook_secret_name": "k"})
    s2 = resolve_settings(FP, lim2, t)
    assert (s2.webhook_url, s2.webhook_secret_name) == ("https://team.example.com/h", "k")


# --------------------------------------------------------------------------- shape


def test_the_resolved_settings_keep_their_phase_4_shape_and_carry_the_fingerprint() -> None:
    s = resolve_settings(FP, limits(), tenant_with(rate=5))
    assert isinstance(s, ClientSettings) and s.fingerprint == FP
    assert set(ClientSettings.__dataclass_fields__) == {
        "fingerprint", "rate", "burst", "max_active_requests", "max_streams",
        "default_budget", "webhook_url", "webhook_secret_name",
    }  # fmt: skip


def test_layering_covers_every_field_of_the_settings_model() -> None:
    names = set(ClientSettingsConfig.model_fields)
    assert names == set(TenantSettings.model_fields) == set(ClientOverride.model_fields) - {"fingerprint"}
    merged = layered(limits(), FP, tenant_with(rate=1.0, burst=2, max_active_requests=3, max_streams=4))
    assert (merged.rate, merged.burst, merged.max_active_requests, merged.max_streams) == (1.0, 2, 3, 4)
