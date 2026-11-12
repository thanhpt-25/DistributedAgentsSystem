"""The sample Gateway config shows how to configure tenants, and what it shows is true (das-multi-tenant task 2.3;
Phase 5 Req 1.1, 1.4, 1.5, 1.6, 4.1).

``deploy/config/gateway.yaml`` carries a commented ``tenants`` example. Without it the sample is a Phase 4
configuration (no tenants: every API key is a Default_Tenant key); switched on, the example must load, be accepted by
the tenant checks, and resolve the sample key to the tenant it names with the settings layers in the order documented.
Every rule of Req 1.4 is also proven never to echo a key, even when an operator pastes it where a fingerprint goes."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from das.client.settings import resolve_settings
from das.config import load_config
from das.config.schema import AgentConfig, tenant_errors
from das.errors import ConfigError
from das.ops.keys import key_fingerprint
from das.tenancy.directory import StaticKeyDirectory
from das.tenancy.scopes import DEFAULT_TENANT
from tests.unit.test_deploy_client_api import (
    BLOCK,
    ENV,
    GATEWAY_YAML,
    SAMPLE_KEY,
    SECRET_VALUE,
    SECRET_VAR,
    uncommented,
)
from tests.unit.test_config_client_api import errors as config_errors
from tests.unit.test_config_client_api import gateway_cfg
from tests.unit.test_config_tenants import tenant

SAMPLE_FP = key_fingerprint(SAMPLE_KEY.encode())
SENTINEL = "SENTINEL-0123456789abcdef"  # a key that must appear in no message
SENTINEL_FP = key_fingerprint(SENTINEL.encode())
ENV_SENTINEL = {"DAS__API_KEYS": SENTINEL}


def tenants_example(tmp_path: Path) -> Path:
    """The sample with only its tenants example switched on."""
    text = GATEWAY_YAML.read_text()
    blocks = [m for m in BLOCK.finditer(text) if "tenants" in m.group(0).splitlines()[0]]
    assert len(blocks) == 1, (
        "gateway.yaml needs exactly one `# BEGIN EXAMPLE tenants` ... `# END EXAMPLE tenants` block"
    )
    path = tmp_path / "gateway.yaml"
    only = text[: blocks[0].start()] + uncommented(blocks[0].group(0)) + text[blocks[0].end() :]
    path.write_text(only)
    return path


# --------------------------------------------------------------------------- the sample


def test_the_sample_without_its_example_is_a_phase_4_configuration() -> None:
    config = load_config(GATEWAY_YAML, ENV)
    assert config.tenants == []
    principal = StaticKeyDirectory(config.api_keys, config.admin_api_keys, config.tenants).resolve(SAMPLE_KEY)
    assert principal is not None and principal.tenant == DEFAULT_TENANT and not principal.operator


def test_the_sample_says_what_happens_without_tenants() -> None:
    text = GATEWAY_YAML.read_text()
    for fact in ("tenants", "default tenant", "submit", "read"):
        assert fact in text, f"gateway.yaml does not mention {fact!r} near its tenants example"


def test_the_tenants_example_loads_and_passes_the_tenant_checks(tmp_path: Path) -> None:
    config = load_config(tenants_example(tmp_path), ENV)
    assert [t.id for t in config.tenants] == ["team-a"]
    assert tenant_errors(config) == []
    assert [k.fingerprint for k in config.tenants[0].keys] == [SAMPLE_FP], (
        f"the example must name the fingerprint of {SAMPLE_KEY!r}: {SAMPLE_FP}"
    )


def test_the_example_key_resolves_to_the_tenant_it_names(tmp_path: Path) -> None:
    config = load_config(tenants_example(tmp_path), ENV)
    principal = StaticKeyDirectory(config.api_keys, config.admin_api_keys, config.tenants).resolve(SAMPLE_KEY)
    assert principal is not None
    assert (principal.tenant, sorted(s.value for s in principal.scopes)) == ("team-a", ["read", "submit"])


def test_the_example_shows_the_layers_in_order_and_everything_still_loads_together(tmp_path: Path) -> None:
    """Every example block on at once: a key override beats the tenant's settings, which beat the defaults."""
    path = tmp_path / "gateway.yaml"
    path.write_text(uncommented(GATEWAY_YAML.read_text()))
    config = load_config(path, {**ENV, SECRET_VAR: SECRET_VALUE})
    assert tenant_errors(config) == []
    team = config.tenants[0]
    effective = resolve_settings(SAMPLE_FP, config.client_limits, team)
    assert team.settings.max_active_requests is not None
    assert effective.max_active_requests == team.settings.max_active_requests  # from the tenant, not the default 100
    assert (effective.rate, effective.burst) == (600.0, 1200)  # the key's own override wins over the tenant's
    assert effective.max_streams == config.client_limits.default.max_streams  # left out everywhere: the default


def test_the_tenants_example_is_valid_yaml_inside_its_comments() -> None:
    body = yaml.safe_load(uncommented(GATEWAY_YAML.read_text()))
    assert isinstance(body["tenants"], list) and body["tenants"][0]["id"] == "team-a"


# --------------------------------------------------------------------------- Req 1.4: no rule echoes a key


def entry(fingerprint: str, scopes: list[str] | None = None) -> dict[str, Any]:
    return {"fingerprint": fingerprint, "scopes": scopes or ["submit", "read"]}


RULES: dict[str, dict[str, Any]] = {
    "fingerprint matches no key": {"tenants": [tenant(keys=[entry("0" * 8)])]},
    "fingerprint listed twice in a tenant": {
        "tenants": [tenant(keys=[entry(SENTINEL_FP), entry(SENTINEL_FP, ["read"])])]
    },
    "fingerprint listed in two tenants": {
        "tenants": [tenant(id="a", keys=[entry(SENTINEL_FP)]), tenant(id="b", keys=[entry(SENTINEL_FP)])]
    },
    "duplicate id": {"tenants": [tenant(id="a", keys=[entry(SENTINEL_FP)]), tenant(id="a", keys=[entry("0" * 8)])]},
    "reserved id": {"tenants": [tenant(id="default", keys=[entry(SENTINEL_FP)])]},
    "invalid id": {"tenants": [tenant(id=SENTINEL, keys=[entry(SENTINEL_FP)])]},
    "the key pasted where the fingerprint goes": {"tenants": [tenant(keys=[entry(SENTINEL)])]},
    "an invalid scope": {"tenants": [tenant(keys=[entry(SENTINEL_FP, [SENTINEL])])]},
    "unknown webhook secret": {
        "tenants": [
            tenant(
                keys=[entry(SENTINEL_FP)],
                settings={"webhook_url": "https://h.example/x", "webhook_secret_name": "nope"},
            )
        ]
    },
}


@pytest.mark.parametrize("rule", RULES)
def test_every_rule_refuses_the_configuration_without_echoing_the_key(tmp_path: Path, rule: str) -> None:
    found = config_errors(tmp_path, gateway_cfg(**RULES[rule]), ENV_SENTINEL)
    assert found, rule
    text = " ".join(f"{path} {reason}" for path, reason in found.items())
    assert SENTINEL not in text and SENTINEL_FP not in text, f"{rule}: {text}"


def test_a_key_collision_is_refused_without_echoing_either_key(tmp_path: Path) -> None:
    from tests.unit.test_config_client_api import COLLIDING

    found = config_errors(tmp_path, gateway_cfg(), {"DAS__API_KEYS": ",".join(COLLIDING)})
    text = " ".join(f"{path} {reason}" for path, reason in found.items())
    assert found and not any(key in text for key in COLLIDING)


def test_the_load_error_names_a_field_and_carries_the_config_exit_code(tmp_path: Path) -> None:
    from das.errors import EXIT_CONFIG_ERROR

    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(gateway_cfg(**RULES["reserved id"])))
    with pytest.raises(ConfigError) as caught:
        load_config(path, ENV_SENTINEL)
    assert caught.value.exit_code == EXIT_CONFIG_ERROR
    assert [e.field for e in caught.value.errors] == ["tenants[0].id"]


def test_an_empty_tenants_list_is_the_same_configuration_as_none(tmp_path: Path) -> None:
    absent = AgentConfig.model_validate(gateway_cfg())
    empty = AgentConfig.model_validate(gateway_cfg(tenants=[]))
    assert absent == empty and tenant_errors(empty) == []
