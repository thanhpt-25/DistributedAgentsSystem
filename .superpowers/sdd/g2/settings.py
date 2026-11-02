"""Effective Client_Settings for one Key_Fingerprint (Phase 4 Req 1.2, 1.3). Pure."""
from __future__ import annotations

from dataclasses import dataclass

from das.client.budget import Budget
from das.config.schema import BudgetConfig, ClientLimitsConfig


@dataclass(frozen=True, slots=True)
class ClientSettings:
    """Limits, default Budget and Webhook_Target that apply to one API key.

    ``rate``, ``max_active_requests`` or ``max_streams`` equal to 0 disables that limit.
    """

    fingerprint: str
    rate: float
    burst: int
    max_active_requests: int
    max_streams: int
    default_budget: Budget | None
    webhook_url: str | None
    webhook_secret_name: str | None


def _budget(config: BudgetConfig | None) -> Budget | None:
    if config is None:
        return None
    budget = Budget(max_total_tokens=config.max_total_tokens, max_cost=config.max_cost)
    return None if budget.is_empty() else budget


def resolve_settings(fingerprint: str, limits: ClientLimitsConfig) -> ClientSettings:
    """``client_limits.default`` merged with the override for ``fingerprint``, if any (Req 1.3)."""
    s = limits.settings_for(fingerprint)
    return ClientSettings(
        fingerprint=fingerprint,
        rate=s.rate,
        burst=s.burst,
        max_active_requests=s.max_active_requests,
        max_streams=s.max_streams,
        default_budget=_budget(s.default_budget),
        webhook_url=s.webhook_url,
        webhook_secret_name=s.webhook_secret_name,
    )
