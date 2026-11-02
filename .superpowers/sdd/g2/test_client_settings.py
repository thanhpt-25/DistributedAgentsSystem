"""Effective Client_Settings (Phase 4 Req 1.2, 1.3)."""
from __future__ import annotations

from decimal import Decimal

from das.client.budget import Budget
from das.client.settings import ClientSettings, resolve_settings
from das.config.schema import BudgetConfig, ClientLimitsConfig, ClientOverride, ClientSettingsConfig

FP = "cbabecbd"


def test_default_settings() -> None:
    assert resolve_settings(FP, ClientLimitsConfig()) == ClientSettings(
        fingerprint=FP,
        rate=60.0,
        burst=120,
        max_active_requests=100,
        max_streams=10,
        default_budget=None,
        webhook_url=None,
        webhook_secret_name=None,
    )


def test_override_fields_win_and_others_inherit() -> None:
    limits = ClientLimitsConfig(
        default=ClientSettingsConfig(rate=10, max_streams=3, default_budget=BudgetConfig(max_total_tokens=500)),
        overrides=[ClientOverride(fingerprint=FP, rate=0, webhook_url="https://h.example.com/x",
                                  webhook_secret_name="partner_a")],
    )
    s = resolve_settings(FP, limits)
    assert (s.rate, s.burst, s.max_streams) == (0.0, 120, 3)
    assert s.default_budget == Budget(max_total_tokens=500)
    assert (s.webhook_url, s.webhook_secret_name) == ("https://h.example.com/x", "partner_a")


def test_other_fingerprint_gets_the_default() -> None:
    limits = ClientLimitsConfig(
        default=ClientSettingsConfig(burst=7), overrides=[ClientOverride(fingerprint=FP, burst=99)]
    )
    assert resolve_settings("ffffffff", limits).burst == 7


def test_budget_conversion() -> None:
    limits = ClientLimitsConfig(
        default=ClientSettingsConfig(default_budget=BudgetConfig(max_cost=Decimal("1.25")))
    )
    assert resolve_settings(FP, limits).default_budget == Budget(max_cost=Decimal("1.25"))


def test_empty_budget_is_none() -> None:
    limits = ClientLimitsConfig(default=ClientSettingsConfig(default_budget=BudgetConfig()))
    assert resolve_settings(FP, limits).default_budget is None
