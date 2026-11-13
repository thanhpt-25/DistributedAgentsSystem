"""Deliberately broken variants of the tenancy e2e stack (das-multi-tenant task 12.1).

An end-to-end test that passes proves little until it has been seen to fail. Each variant here breaks one thing the
tenancy tests are meant to notice, as a Compose override layered on top of the committed ones:

- a *config* variant swaps the Gateway config for an edited copy (tenants removed, a Scope too wide, a quota off);
- a *code* variant bind-mounts a patched copy of one source file over the installed one in the images (a tenant check
  removed, a script that skips the twin update, priority dropped), so nothing is rebuilt and ``das/`` does not change.

Use one with::

    uv run python -m tests.integration.e2e.tenancy_variants --list
    DAS_E2E_TENANT_OVERRIDE_EXTRA="$(uv run python -m tests.integration.e2e.tenancy_variants wrong-scope)" \\
        DAS_REQUIRE_DOCKER=1 uv run pytest tests/integration/e2e/test_tenancy_e2e.py

The unmodified stack is the control: every test passes there. ``fails`` names the tests each variant must make fail;
``tests/unit/test_e2e_tenancy_static.py`` checks that every patch still applies to the current source exactly once, so a
variant cannot quietly stop breaking anything after a refactor.
"""

from __future__ import annotations

import argparse
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
E2E = Path(__file__).resolve().parent
GATEWAY_CONFIG = E2E / "gateway.tenancy.yaml"
#: Where the images install the ``das`` package (``uv sync --no-editable`` into /opt/venv).
SITE_PACKAGES = "/opt/venv/lib/python3.12/site-packages"
GATEWAYS = ("gateway", "gateway-b")
EVERY_SERVICE = (*GATEWAYS, "manager")

# The tests a variant must break, by name (parametrized ones by their prefix).
SCOPES = "test_scopes_decide_before_anything_is_read_or_written"
ISOLATION = "test_tenants_cannot_see_each_others_requests_on_any_route"
QUOTA = "test_the_tenant_quota_refuses_with_its_scope_and_frees_when_a_request_finishes"
PRIORITY = "test_a_high_request_overtakes_a_normal_backlog"
USAGE = "test_each_tenants_usage_report_equals_the_token_counts_of_its_own_requests"
UPGRADE = "test_the_upgrade_backfills_data_written_without_tenant_fields"


@dataclass(frozen=True)
class Patch:
    """Replace ``old`` (which must occur exactly once) by ``new`` in the source file ``path`` (relative to the repo)."""

    path: str
    old: str
    new: str


@dataclass(frozen=True)
class Variant:
    name: str
    description: str
    fails: tuple[str, ...]
    patches: tuple[Patch, ...] = ()
    services: tuple[str, ...] = GATEWAYS  # the services that get the patched files
    config: tuple[tuple[str, str], ...] = ()  # (regex, replacement) edits of the Gateway config, each matching once


VARIANTS: dict[str, Variant] = {
    v.name: v
    for v in (
        # ---- configuration: the same code, a wrong setting
        Variant(
            "no-tenants",
            "the tenants block is gone: every key is a Default_Tenant key",
            (SCOPES, ISOLATION, QUOTA, PRIORITY, USAGE, UPGRADE),
            config=((r"tenants:\n.*?(?=\nevents:)", ""),),
        ),
        Variant(
            "all-scopes",
            "every key of team-a holds every Scope",
            (SCOPES,),
            config=(
                (r"scopes: \[read\]", "scopes: [submit, read, admin]"),
                (r"scopes: \[submit\]", "scopes: [submit, read, admin]"),
            ),
        ),
        Variant(
            "no-quota",
            "team-a has no Tenant_Quota",
            (QUOTA,),
            config=((r"max_active_requests: 2 +# the Tenant_Quota", "max_active_requests: 0"),),
        ),
        Variant(
            "priority-capped",
            "team-a may not ask for high",
            (PRIORITY,),
            config=((r"max_priority: high", "max_priority: normal"),),
        ),
        # ---- code: one check or one write removed
        Variant(
            "tenant-filter-removed",
            "GET /v1/requests/{id} no longer compares tenants",
            (ISOLATION,),
            patches=(
                Patch(
                    "das/gateway/app.py",
                    "if snap is None or not can_see(request.state.principal, snap.tenant):\n"
                    "            return _error(status.HTTP_404_NOT_FOUND, NOT_FOUND)\n        return request_view",
                    "if snap is None:\n            return _error(status.HTTP_404_NOT_FOUND, NOT_FOUND)\n"
                    "        return request_view",
                ),
            ),
        ),
        Variant(
            "foreign-cancel-allowed",
            "cancel no longer compares tenants",
            (ISOLATION,),
            patches=(
                Patch(
                    "das/gateway/app.py",
                    "if owner is not None and not can_see(request.state.principal, owner):",
                    "if False:",
                ),
            ),
        ),
        Variant(
            "listing-reads-every-tenant",
            "the listing reads the global index for a tenant too",
            (ISOLATION,),
            patches=(
                Patch(
                    "das/gateway/listing.py",
                    "                    tenant=scope,\n",
                    "                    tenant=None,\n",
                ),
            ),
        ),
        Variant(
            "wrong-scope",
            "listing needs only the submit Scope",
            (SCOPES,),
            patches=(
                Patch(
                    "das/tenancy/scopes.py",
                    '("GET", "/v1/requests"): Scope.READ,',
                    '("GET", "/v1/requests"): Scope.SUBMIT,',
                ),
            ),
        ),
        Variant(
            "quota-ignored",
            "the Gateway never hands the Tenant_Quota to the store",
            (QUOTA,),
            patches=(
                Patch("das/gateway/submit.py", "tenant_max_active=settings.tenant_max_active,", "tenant_max_active=0,"),
            ),
        ),
        Variant(
            "priority-not-stored",
            "the Gateway stores every Request as normal",
            (PRIORITY,),
            patches=(
                Patch(
                    "das/gateway/submit.py",
                    "priority=priority,  # the store keeps it only",
                    "priority='normal',  # the store keeps it only",
                ),
            ),
        ),
        Variant(
            "priority-dropped",
            "the Manager dispatches a high Request without the Priority_Header",
            (PRIORITY,),
            services=("manager",),
            patches=(
                Patch(
                    "das/manager/driver.py",
                    "headers = {PRIORITY_HEADER: PRIORITY_HIGH} if self._priority == PRIORITY_HIGH else None",
                    "headers = None",
                ),
            ),
        ),
        Variant(
            "status-twin-skipped",
            "the scripts stop moving a Request between the tenant's by-status indexes",
            (ISOLATION,),  # not the usage test: totals come from the rollups, not from the by-status indexes
            services=EVERY_SERVICE,
            patches=(
                Patch(
                    "das/state/lua/_common.lua",
                    "      local twin = tenant_twin(to_key, t)\n"
                    "      if twin then redis.call('ZADD', twin, score, rid) end",
                    "      local twin = tenant_twin(to_key, t)\n      if twin then end",
                ),
            ),
        ),
        Variant(
            "active-set-skipped",
            "the scripts stop adding a Request to its tenant's active set",
            (QUOTA,),
            services=EVERY_SERVICE,
            patches=(
                Patch(
                    "das/state/lua/_common.lua",
                    "  redis.call('SADD', tenant_active_key(req_key, tenant_of(req_key)), rid)\n",
                    "",
                ),
            ),
        ),
    )
}


def patched_source(patch: Patch, text: str) -> str:
    """``text`` with the patch applied; the patch must match exactly once, or the variant would break nothing."""
    count = text.count(patch.old)
    if count != 1:
        raise ValueError(f"{patch.path}: expected one match of {patch.old[:60]!r}, found {count}")
    return text.replace(patch.old, patch.new, 1)


def edited_config(variant: Variant, text: str) -> str:
    for pattern, replacement in variant.config:
        text, count = re.subn(pattern, replacement, text, flags=re.DOTALL)
        if count < 1:
            raise ValueError(f"config edit {pattern!r} matched nothing")
    return text


def build(variant: Variant, out_dir: Path) -> Path:
    """Write the patched files and the Compose override of ``variant`` under ``out_dir``; returns the override."""
    out_dir.mkdir(parents=True, exist_ok=True)
    code_mounts: list[str] = []
    for patch in variant.patches:
        target = out_dir / "src" / patch.path
        target.parent.mkdir(parents=True, exist_ok=True)
        # Two patches of one file are applied one after the other.
        current = target.read_text(encoding="utf-8") if target.exists() else (ROOT / patch.path).read_text("utf-8")
        target.write_text(patched_source(patch, current), encoding="utf-8")
        mount = f"{target}:{SITE_PACKAGES}/{patch.path}:ro"
        if mount not in code_mounts:
            code_mounts.append(mount)
    config_mounts: list[str] = []
    if variant.config:
        config = out_dir / "gateway.tenancy.yaml"
        config.write_text(edited_config(variant, GATEWAY_CONFIG.read_text(encoding="utf-8")), encoding="utf-8")
        config_mounts = [f"{config}:/etc/das/config.yaml:ro"]
    lines = [f"# Broken variant {variant.name!r}: {variant.description}.", "services:"]
    for service in dict.fromkeys([*variant.services, *(GATEWAYS if config_mounts else ())]):
        mounts = [
            *(code_mounts if service in variant.services else []),
            *(config_mounts if service in GATEWAYS else []),
        ]
        lines += [f"  {service}:", "    volumes:", *[f"      - {mount}" for mount in mounts]]
    override = out_dir / "docker-compose.variant.yml"
    override.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return override


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n")[0])
    parser.add_argument("name", nargs="?", help="the variant to build; its Compose override path is printed")
    parser.add_argument("--list", action="store_true", help="list the variants and what each must break")
    parser.add_argument("--out", type=Path, help="where to write the files (default: a new temporary directory)")
    args = parser.parse_args(argv)
    if args.list or not args.name:
        for variant in VARIANTS.values():
            print(f"{variant.name:28} {variant.description}\n{'':28} must fail: {', '.join(variant.fails)}")
        return 0
    if args.name not in VARIANTS:
        parser.error(f"unknown variant {args.name!r}; try --list")
    out = args.out or Path(tempfile.mkdtemp(prefix=f"das-variant-{args.name}-"))
    out.mkdir(parents=True, exist_ok=True)
    print(build(VARIANTS[args.name], out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
