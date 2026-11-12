"""Tenant configuration schema (das-multi-tenant task 2.1; Phase 5 Req 1.1, 4.2, 4.3, 6.6, 7.1, 7.2, 8.1).

Schema only: what a Tenant_Config may contain. The cross-field rules (fingerprints that must match a configured key,
duplicates, the reserved id) are task 2.2."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from das.config.schema import (
    AgentConfig,
    ClientOverride,
    LabelLimitsConfig,
    TenantConfig,
    TenantKeyConfig,
    TenantSettings,
    UsageConfig,
)
from tests.unit.test_config_client_api import FP, gateway_cfg


def tenant(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {"id": "team-a", "keys": [{"fingerprint": FP, "scopes": ["submit", "read"]}]}
    data.update(overrides)
    return data


# --------------------------------------------------------------------------- defaults


def test_a_config_without_tenants_has_the_phase_4_shape() -> None:
    config = AgentConfig.model_validate(gateway_cfg())
    assert config.tenants == []
    assert config.usage == UsageConfig() and config.usage.retention_days == 400
    assert config.metrics.label_limits.tenant == 100


def test_a_minimal_tenant_gets_the_documented_defaults() -> None:
    t = TenantConfig.model_validate(tenant())
    assert t.id == "team-a"
    assert [(k.fingerprint, k.scopes) for k in t.keys] == [(FP, ["submit", "read"])]
    assert (t.max_active_requests, t.default_priority, t.max_priority) == (0, "normal", "normal")
    assert t.settings == TenantSettings()
    assert all(getattr(t.settings, name) is None for name in TenantSettings.model_fields)


def test_a_full_tenant_round_trips_through_the_agent_config() -> None:
    config = AgentConfig.model_validate(
        gateway_cfg(
            tenants=[
                tenant(
                    keys=[
                        {"fingerprint": FP, "scopes": ["submit", "read"]},
                        {"fingerprint": "5e6f7a8b", "scopes": ["submit", "read", "admin"]},
                    ],
                    settings={"rate": 120, "burst": 240, "default_budget": {"max_total_tokens": 200000}},
                    max_active_requests=200,
                    default_priority="normal",
                    max_priority="high",
                )
            ]
        )
    )
    (t,) = config.tenants
    assert (t.settings.rate, t.settings.burst) == (120, 240)
    assert t.settings.default_budget is not None and t.settings.default_budget.max_total_tokens == 200000
    assert (t.max_active_requests, t.max_priority) == (200, "high")
    assert t.keys[1].scopes == ["submit", "read", "admin"]


def test_the_models_are_immutable() -> None:
    t = TenantConfig.model_validate(tenant())
    with pytest.raises(ValidationError):
        t.id = "other"  # type: ignore[misc]


def test_unknown_fields_are_refused() -> None:
    with pytest.raises(ValidationError):
        TenantConfig.model_validate(tenant(surprise=1))
    with pytest.raises(ValidationError):
        TenantKeyConfig.model_validate({"fingerprint": FP, "scopes": ["read"], "key": "x"})


# --------------------------------------------------------------------------- tenant id


@pytest.mark.parametrize("good", ["a", "team-a", "0", "a1-b2", "a" * 32])
def test_valid_tenant_ids(good: str) -> None:
    assert TenantConfig.model_validate(tenant(id=good)).id == good


@pytest.mark.parametrize("bad", ["", "A", "Team", "-a", "a" * 33, "a_b", "a b", "a.b", "é", "team:a", "a/b"])
def test_invalid_tenant_ids(bad: str) -> None:
    with pytest.raises(ValidationError):
        TenantConfig.model_validate(tenant(id=bad))


# --------------------------------------------------------------------------- keys and scopes


def test_a_tenant_needs_between_one_and_fifty_keys() -> None:
    def keys(n: int) -> list[dict[str, Any]]:
        return [{"fingerprint": f"{i:08x}", "scopes": ["read"]} for i in range(n)]

    assert len(TenantConfig.model_validate(tenant(keys=keys(50))).keys) == 50
    for n in (0, 51):
        with pytest.raises(ValidationError):
            TenantConfig.model_validate(tenant(keys=keys(n)))


@pytest.mark.parametrize("bad", ["", "1A2B3C4D", "1a2b3c4", "1a2b3c4d5", "gggggggg", "1a2b-c4d"])
def test_a_fingerprint_is_eight_lower_case_hex_characters(bad: str) -> None:
    with pytest.raises(ValidationError):
        TenantKeyConfig.model_validate({"fingerprint": bad, "scopes": ["read"]})


@pytest.mark.parametrize(
    "scopes", [["submit"], ["read"], ["admin"], ["submit", "read"], ["read", "admin", "submit"]]
)
def test_any_non_empty_set_of_the_three_scopes_is_valid(scopes: list[str]) -> None:
    assert TenantKeyConfig.model_validate({"fingerprint": FP, "scopes": scopes}).scopes == scopes


@pytest.mark.parametrize("bad", [[], ["operator"], ["root"], ["read", "read"], ["READ"], ["submit", "write"], "read"])
def test_the_scope_list_is_non_empty_known_and_without_duplicates(bad: Any) -> None:
    with pytest.raises(ValidationError):
        TenantKeyConfig.model_validate({"fingerprint": FP, "scopes": bad})


# --------------------------------------------------------------------------- quota and priority


@pytest.mark.parametrize("value", [0, 1, 1_000_000])
def test_the_tenant_quota_range_accepts_0_to_a_million(value: int) -> None:
    assert TenantConfig.model_validate(tenant(max_active_requests=value)).max_active_requests == value


@pytest.mark.parametrize("value", [-1, 1_000_001, 1.5, "many"])
def test_the_tenant_quota_range_refuses_the_rest(value: Any) -> None:
    with pytest.raises(ValidationError):
        TenantConfig.model_validate(tenant(max_active_requests=value))


@pytest.mark.parametrize(("default", "maximum"), [("normal", "normal"), ("normal", "high"), ("high", "high")])
def test_a_default_priority_not_above_the_maximum_is_valid(default: str, maximum: str) -> None:
    t = TenantConfig.model_validate(tenant(default_priority=default, max_priority=maximum))
    assert (t.default_priority, t.max_priority) == (default, maximum)


def test_a_default_priority_above_the_maximum_is_refused_naming_the_field() -> None:
    with pytest.raises(ValidationError, match="default_priority"):
        TenantConfig.model_validate(tenant(default_priority="high", max_priority="normal"))


@pytest.mark.parametrize("bad", ["low", "urgent", "", "HIGH", 1, None])
def test_priority_values_are_normal_or_high(bad: Any) -> None:
    for field in ("default_priority", "max_priority"):
        with pytest.raises(ValidationError):
            TenantConfig.model_validate(tenant(**{field: bad}))


# --------------------------------------------------------------------------- settings layer


def test_tenant_settings_have_exactly_the_override_fields_without_the_fingerprint() -> None:
    assert set(TenantSettings.model_fields) == set(ClientOverride.model_fields) - {"fingerprint"}


@pytest.mark.parametrize(
    ("field", "good", "bad"),
    [
        ("rate", [0, 0.001, 100_000], [-1, 0.0005, 100_001, float("nan")]),
        ("burst", [1, 1_000_000], [0, 1_000_001]),
        ("max_active_requests", [0, 1_000_000], [-1, 1_000_001]),
        ("max_streams", [0, 10_000], [-1, 10_001]),
    ],
)
def test_tenant_settings_use_the_ranges_of_a_key_override(field: str, good: list[Any], bad: list[Any]) -> None:
    for value in good:
        assert getattr(TenantSettings.model_validate({field: value}), field) == value
    for value in bad:
        with pytest.raises(ValidationError):
            TenantSettings.model_validate({field: value})


def test_tenant_settings_accept_a_budget_and_a_webhook_target() -> None:
    s = TenantSettings.model_validate(
        {
            "default_budget": {"max_total_tokens": 5},
            "webhook_url": "https://hooks.example.com/das",
            "webhook_secret_name": "Team_A",
        }
    )
    assert s.default_budget is not None and s.default_budget.max_total_tokens == 5
    assert s.webhook_secret_name == "team_a"  # lower-cased as for a key override; letters, digits and "_" only
    with pytest.raises(ValidationError):
        TenantSettings.model_validate({"webhook_secret_name": "team-a"})  # the Phase 4 secret-name rule is reused


def test_tenant_settings_refuse_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        TenantSettings.model_validate({"fingerprint": FP})


# --------------------------------------------------------------------------- the list, usage and label limit


def test_at_most_one_hundred_tenants() -> None:
    def many(n: int) -> list[dict[str, Any]]:
        return [{"id": f"t{i}", "keys": [{"fingerprint": f"{i:08x}", "scopes": ["read"]}]} for i in range(n)]

    assert len(AgentConfig.model_validate(gateway_cfg(tenants=many(100))).tenants) == 100
    with pytest.raises(ValidationError):
        AgentConfig.model_validate(gateway_cfg(tenants=many(101)))


@pytest.mark.parametrize(("days", "ok"), [(29, False), (30, True), (400, True), (3650, True), (3651, False)])
def test_usage_retention_days_range(days: int, ok: bool) -> None:
    if ok:
        assert UsageConfig.model_validate({"retention_days": days}).retention_days == days
    else:
        with pytest.raises(ValidationError):
            UsageConfig.model_validate({"retention_days": days})


@pytest.mark.parametrize(("limit", "ok"), [(0, False), (1, True), (100, True), (500, True), (501, False)])
def test_the_tenant_label_limit_range_and_its_place_under_metrics(limit: int, ok: bool) -> None:
    assert LabelLimitsConfig().tenant == 100
    data = gateway_cfg(metrics={"label_limits": {"tenant": limit}})
    if ok:
        assert AgentConfig.model_validate(data).metrics.label_limits.tenant == limit
    else:
        with pytest.raises(ValidationError):
            AgentConfig.model_validate(data)
