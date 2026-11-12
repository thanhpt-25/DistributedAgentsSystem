"""Cross-field tenant validation (das-multi-tenant task 2.2; Phase 5 Req 1.1, 1.4).

Every rule names a field path and never a key, a secret or a fingerprint value. A rejected configuration is a
ConfigError, which the CLI maps to exit code 2."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from das.config.schema import AgentConfig, tenant_errors
from das.ops.keys import key_fingerprint
from tests.unit.test_config_client_api import (
    ADMIN_FP,
    ADMIN_KEY,
    API_KEY,
    COLLIDING,
    FP,
    SECRET,
    errors,
    gateway_cfg,
    load,
    worker_cfg,
)
from tests.unit.test_config_tenants import tenant

API_KEY_2 = "second-key-abcdef0123456789"
FP_2 = key_fingerprint(API_KEY_2.encode())
BOTH = {"DAS__API_KEYS": f"{API_KEY},{API_KEY_2}"}
HOOK = "https://h.example/x"


def keyed(fingerprint: str, scopes: list[str] | None = None) -> dict[str, Any]:
    return {"fingerprint": fingerprint, "scopes": scopes or ["submit", "read"]}


# --------------------------------------------------------------------------- valid configurations


def test_no_tenants_is_valid(tmp_path: Path) -> None:
    assert load(tmp_path, gateway_cfg()).tenants == []


def test_a_tenant_claiming_a_configured_api_key_is_valid(tmp_path: Path) -> None:
    cfg = load(tmp_path, gateway_cfg(tenants=[tenant(keys=[keyed(FP)])]))
    assert [t.id for t in cfg.tenants] == ["team-a"]


def test_two_tenants_with_their_own_keys_are_valid(tmp_path: Path) -> None:
    cfg = load(
        tmp_path,
        gateway_cfg(tenants=[tenant(id="a", keys=[keyed(FP)]), tenant(id="b", keys=[keyed(FP_2)])]),
        BOTH,
    )
    assert [t.id for t in cfg.tenants] == ["a", "b"]


def test_a_configured_key_no_tenant_claims_is_valid(tmp_path: Path) -> None:
    load(tmp_path, gateway_cfg(tenants=[tenant(keys=[keyed(FP)])]), BOTH)


def test_a_webhook_secret_name_that_is_configured_is_valid(tmp_path: Path) -> None:
    settings = {"webhook_url": HOOK, "webhook_secret_name": "partner"}
    data = gateway_cfg(tenants=[tenant(keys=[keyed(FP)], settings=settings)])
    load(tmp_path, data, {**BOTH, "DAS__WEBHOOK_SECRETS__PARTNER": SECRET})


# --------------------------------------------------------------------------- Req 1.4, one rule at a time


def test_a_fingerprint_matching_no_configured_key_is_refused(tmp_path: Path) -> None:
    found = errors(tmp_path, gateway_cfg(tenants=[tenant(keys=[keyed("00000000")])]))
    assert list(found) == ["tenants[0].keys[0].fingerprint"]
    assert "no configured" in found["tenants[0].keys[0].fingerprint"]


def test_a_fingerprint_listed_twice_in_one_tenant_is_refused(tmp_path: Path) -> None:
    found = errors(tmp_path, gateway_cfg(tenants=[tenant(keys=[keyed(FP), keyed(FP, ["read"])])]))
    assert list(found) == ["tenants[0].keys[1].fingerprint"]
    assert "tenants[0].keys[0]" in found["tenants[0].keys[1].fingerprint"]


def test_a_fingerprint_listed_in_two_tenants_is_refused(tmp_path: Path) -> None:
    found = errors(
        tmp_path, gateway_cfg(tenants=[tenant(id="a", keys=[keyed(FP)]), tenant(id="b", keys=[keyed(FP)])])
    )
    assert list(found) == ["tenants[1].keys[0].fingerprint"]
    assert "tenants[0].keys[0]" in found["tenants[1].keys[0].fingerprint"]


def test_a_fingerprint_of_an_admin_key_is_refused(tmp_path: Path) -> None:
    env = {"DAS__API_KEYS": API_KEY, "DAS__ADMIN_API_KEYS": ADMIN_KEY}
    found = errors(tmp_path, gateway_cfg(tenants=[tenant(keys=[keyed(ADMIN_FP)])]), env)
    assert list(found) == ["tenants[0].keys[0].fingerprint"]
    assert "Admin_API_Key" in found["tenants[0].keys[0].fingerprint"]


def test_two_keys_with_one_fingerprint_are_refused_even_with_tenants(tmp_path: Path) -> None:
    env = {"DAS__API_KEYS": ",".join(COLLIDING)}
    found = errors(tmp_path, gateway_cfg(tenants=[tenant(keys=[keyed(FP)])]), env)
    assert "api_keys[1]" in found


def test_a_duplicate_tenant_id_is_refused(tmp_path: Path) -> None:
    data = gateway_cfg(tenants=[tenant(id="a", keys=[keyed(FP)]), tenant(id="a", keys=[keyed(FP_2)])])
    found = errors(tmp_path, data, BOTH)
    assert list(found) == ["tenants[1].id"]
    assert "tenants[0].id" in found["tenants[1].id"]


def test_the_reserved_id_default_is_refused(tmp_path: Path) -> None:
    found = errors(tmp_path, gateway_cfg(tenants=[tenant(id="default", keys=[keyed(FP)])]))
    assert list(found) == ["tenants[0].id"]
    assert "reserved" in found["tenants[0].id"]


def test_a_webhook_secret_name_that_is_not_configured_is_refused(tmp_path: Path) -> None:
    settings = {"webhook_url": HOOK, "webhook_secret_name": "nope"}
    found = errors(tmp_path, gateway_cfg(tenants=[tenant(keys=[keyed(FP)], settings=settings)]))
    assert list(found) == ["tenants[0].settings.webhook_secret_name"]


def test_a_webhook_url_without_a_secret_name_is_refused_for_the_tenant(tmp_path: Path) -> None:
    data = gateway_cfg(tenants=[tenant(keys=[keyed(FP)], settings={"webhook_url": HOOK})])
    assert list(errors(tmp_path, data)) == ["tenants[0].settings.webhook_secret_name"]


def test_the_reserved_id_is_the_default_tenant_of_the_other_layers() -> None:
    from das.config.schema import DEFAULT_TENANT_ID
    from das.state import keys
    from das.tenancy.scopes import DEFAULT_TENANT

    assert DEFAULT_TENANT_ID == keys.DEFAULT_TENANT == DEFAULT_TENANT


def test_every_violation_is_reported_in_one_pass(tmp_path: Path) -> None:
    data = gateway_cfg(tenants=[tenant(id="default", keys=[keyed("00000000"), keyed("00000000")])])
    assert sorted(errors(tmp_path, data)) == [
        "tenants[0].id",
        "tenants[0].keys[0].fingerprint",
        "tenants[0].keys[1].fingerprint",
    ]


# --------------------------------------------------------------------------- secrecy and roles


def test_no_error_contains_a_key_a_secret_or_a_fingerprint(tmp_path: Path) -> None:
    env = {"DAS__API_KEYS": API_KEY, "DAS__ADMIN_API_KEYS": ADMIN_KEY, "DAS__WEBHOOK_SECRETS__PARTNER": SECRET}
    settings = {"webhook_url": HOOK, "webhook_secret_name": "nope"}
    data = gateway_cfg(
        tenants=[
            tenant(id="a", keys=[keyed(ADMIN_FP), keyed("00000000")], settings=settings),
            tenant(id="b", keys=[keyed(FP)]),
            tenant(id="c", keys=[keyed(FP)]),
        ]
    )
    found = errors(tmp_path, data, env)
    assert found
    text = " ".join(f"{path} {reason}" for path, reason in found.items())
    for secret in (API_KEY, ADMIN_KEY, SECRET, FP, ADMIN_FP, "00000000"):
        assert secret not in text


def test_tenants_are_not_checked_on_other_roles(tmp_path: Path) -> None:
    cfg = load(tmp_path, worker_cfg(tenants=[tenant(keys=[keyed("00000000")])]), {})
    assert len(cfg.tenants) == 1


def test_the_validator_is_pure_and_returns_field_errors_only() -> None:
    config = AgentConfig.model_validate(gateway_cfg(tenants=[tenant(keys=[keyed("00000000")])]))
    assert [e.field for e in tenant_errors(config)] == ["tenants[0].keys[0].fingerprint"]
    assert tenant_errors(config) == tenant_errors(config)


@pytest.mark.parametrize("tenants", [[], None])
def test_an_absent_or_empty_tenants_list_has_no_errors(tenants: Any) -> None:
    data = gateway_cfg() if tenants is None else gateway_cfg(tenants=tenants)
    assert tenant_errors(AgentConfig.model_validate(data)) == []
