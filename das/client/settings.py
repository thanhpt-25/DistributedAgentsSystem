"""Effective Client_Settings for one Key_Fingerprint (Phase 4 Req 1.2, 1.3). Pure."""
from __future__ import annotations

from dataclasses import dataclass

from das.client.budget import Budget
from das.config.schema import BudgetConfig, ClientLimitsConfig, PriorityClass, TenantConfig
from das.tenancy.settings import layered


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
    #: The tenant of the key and its Tenant_Quota (0 = none), distinct from the per-key ``max_active_requests``
    #: above (Phase 5 Req 3.7, 4.3).
    tenant: str = "default"
    tenant_max_active: int = 0
    #: What a submit with no ``priority`` gets, and the highest it may ask for (Phase 5 Req 7.1, 7.2). The
    #: Default_Tenant has no TenantConfig and may use ``high``.
    default_priority: PriorityClass = "normal"
    max_priority: PriorityClass = "high"


def _budget(config: BudgetConfig | None) -> Budget | None:
    if config is None:
        return None
    budget = Budget(max_total_tokens=config.max_total_tokens, max_cost=config.max_cost)
    return None if budget.is_empty() else budget


def resolve_settings(
    fingerprint: str, limits: ClientLimitsConfig, tenant: TenantConfig | None = None
) -> ClientSettings:
    """``client_limits.default``, the tenant's settings and the override for ``fingerprint``, each field from the
    most specific layer that sets it (Phase 4 Req 1.3; Phase 5 Req 4.1). Without a tenant: the Phase 4 result."""
    s = layered(limits, fingerprint, tenant)
    return ClientSettings(
        fingerprint=fingerprint,
        rate=s.rate,
        burst=s.burst,
        max_active_requests=s.max_active_requests,
        max_streams=s.max_streams,
        default_budget=_budget(s.default_budget),
        webhook_url=s.webhook_url,
        webhook_secret_name=s.webhook_secret_name,
        tenant=tenant.id if tenant is not None else "default",
        tenant_max_active=tenant.max_active_requests if tenant is not None else 0,
        default_priority=tenant.default_priority if tenant is not None else "normal",
        max_priority=tenant.max_priority if tenant is not None else "high",
    )
