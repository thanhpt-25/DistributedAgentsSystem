"""Client_Settings layering (Phase 5 Req 1.6, 4.1, 4.2). Pure.

Each field comes from the most specific layer that sets it: the key's override (matched by fingerprint, whatever the
tenant), then the tenant's ``settings``, then ``client_limits.default``. A layer leaves a field unset with ``None``;
``0`` is a value (it disables a limit), so it overrides a less specific layer.
"""

from __future__ import annotations

from das.config.schema import ClientLimitsConfig, ClientSettingsConfig, TenantConfig

__all__ = ["layered"]


def layered(limits: ClientLimitsConfig, fingerprint: str, tenant: TenantConfig | None) -> ClientSettingsConfig:
    """``limits.default`` overlaid with the tenant's settings, then with the key's override."""
    update: dict[str, object] = {}
    for layer in (tenant.settings if tenant is not None else None, limits.override_for(fingerprint)):
        if layer is None:
            continue
        for name in ClientSettingsConfig.model_fields:
            value = getattr(layer, name)
            if value is not None:
                update[name] = value
    return limits.default.model_copy(update=update) if update else limits.default
