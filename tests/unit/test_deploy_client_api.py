"""The deployment files show the Phase 4 client API settings, and what they show is true
(das-client-api task 10.2; Phase 4 Req 1, 11.3).

``deploy/config/gateway.yaml`` writes out the defaults of ``client_limits``, ``events`` and ``webhook`` so an
operator can see and change them; a test proves the sample equals the schema defaults, so it changes no behaviour
and cannot drift. A commented example (a default budget, an override, a webhook target) is proven to load when it
is uncommented. ``deploy/.env.example`` and Compose carry the webhook secret by name only: an empty one is fatal
(the Gateway refuses to start), so the example keeps it commented out."""

from __future__ import annotations

import re
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml

from das.config import load_config
from das.config.schema import BudgetConfig, ClientLimitsConfig, ClientSettingsConfig, EventsConfig, WebhookConfig
from das.errors import ConfigError
from das.ops.keys import key_fingerprint
from tests.unit.test_deploy_static import DEPLOY, _env_example, _passed_by_name, _services_of_role

GATEWAY_YAML = DEPLOY / "config" / "gateway.yaml"
ENV_EXAMPLE = DEPLOY / ".env.example"
SECRET_VAR = "DAS__WEBHOOK_SECRETS__PARTNER"
SAMPLE_KEY = "change-me-api-key"  # the placeholder of .env.example and the README
SECRET_VALUE = "a-test-webhook-secret-0123456789"
ENV = {
    "DAS__REDIS__URL": "redis://redis:6379/0",
    "DAS__GRPC__ADVERTISE_HOST": "gateway",
    "DAS__API_KEYS": SAMPLE_KEY,
}
BLOCK = re.compile(r"^[ \t]*# BEGIN EXAMPLE[^\n]*\n(?P<body>.*?)^[ \t]*# END EXAMPLE[^\n]*\n", re.MULTILINE | re.DOTALL)


@pytest.fixture(scope="module")
def compose() -> dict:
    return yaml.safe_load((DEPLOY / "docker-compose.yml").read_text())


def uncommented(text: str) -> str:
    """The YAML with every ``# BEGIN EXAMPLE`` ... ``# END EXAMPLE`` block switched on (one ``# `` removed per line)."""

    def switch_on(match: re.Match[str]) -> str:
        lines = []
        for line in match["body"].splitlines():
            stripped = line.lstrip()
            indent = line[: len(line) - len(stripped)]
            if stripped.startswith("# "):
                lines.append(indent + stripped[2:])
            elif stripped == "#":
                lines.append("")
            else:
                lines.append(line)  # a line that is not a comment stays as it is
        return "\n".join(lines) + "\n"

    return BLOCK.sub(switch_on, text)


def shown(model: Any, skip: frozenset[str] = frozenset()) -> set[str]:
    """Fields of ``model`` that have a value to show (a default other than ``None``)."""
    return {name for name, field in model.model_fields.items() if field.default is not None and name not in skip}


def with_examples(tmp_path: Path) -> Path:
    path = tmp_path / "gateway.yaml"
    path.write_text(uncommented(GATEWAY_YAML.read_text()))
    return path


# --------------------------------------------------------------------------- gateway.yaml


def test_the_sample_gateway_config_loads() -> None:
    assert load_config(GATEWAY_YAML, ENV).role == "gateway"


def test_the_sample_sections_are_the_schema_defaults() -> None:
    """Written out for the reader, behaviour-neutral for the Gateway."""
    config = load_config(GATEWAY_YAML, ENV)
    assert config.client_limits == ClientLimitsConfig()
    assert config.events == EventsConfig()
    assert config.webhook == WebhookConfig()


def test_the_sample_shows_every_setting_that_has_a_value() -> None:
    raw = yaml.safe_load(GATEWAY_YAML.read_text())
    # ``overrides`` has no default entries; it is documented by the example block instead.
    assert shown(ClientLimitsConfig, frozenset({"overrides"})) <= set(raw["client_limits"])
    assert shown(ClientSettingsConfig) <= set(raw["client_limits"]["default"])
    assert shown(EventsConfig) <= set(raw["events"])
    assert shown(WebhookConfig) <= set(raw["webhook"])


def test_the_example_blocks_load_when_switched_on(tmp_path: Path) -> None:
    assert BLOCK.search(GATEWAY_YAML.read_text()), "gateway.yaml has no `# BEGIN EXAMPLE` ... `# END EXAMPLE` block"
    config = load_config(with_examples(tmp_path), {**ENV, SECRET_VAR: SECRET_VALUE})

    default = config.client_limits.default
    assert default.default_budget == BudgetConfig(max_total_tokens=200_000, max_cost=Decimal("5.00"))
    fingerprint = key_fingerprint(SAMPLE_KEY.encode())
    override = config.client_limits.override_for(fingerprint)
    assert override is not None, f"the example override must name the fingerprint of {SAMPLE_KEY!r}: {fingerprint}"
    effective = config.client_limits.settings_for(fingerprint)
    assert (effective.rate, effective.burst) == (600.0, 1200)
    assert effective.webhook_url == "https://hooks.example.com/das" and effective.webhook_secret_name == "partner"
    assert effective.max_streams == default.max_streams  # a field the override leaves out is inherited


def test_the_example_webhook_needs_its_secret(tmp_path: Path) -> None:
    """Why the secret comes from the environment: with the example on and no secret, the config is refused."""
    with pytest.raises(ConfigError, match="webhook_secret_name: names no configured webhook secret"):
        load_config(with_examples(tmp_path), ENV)


def test_an_empty_webhook_secret_stops_the_gateway() -> None:
    """What Compose passes for ``DAS__WEBHOOK_SECRETS__PARTNER=`` in ``.env``: why the line stays commented out."""
    with pytest.raises(ConfigError, match=r"webhook_secrets\.partner: must be at least 16 characters"):
        load_config(GATEWAY_YAML, {**ENV, SECRET_VAR: ""})
    assert load_config(GATEWAY_YAML, ENV).webhook_secrets == {}  # unset is fine


# --------------------------------------------------------------------------- .env.example and Compose


def test_env_example_keeps_the_webhook_secret_commented_out_and_explains_it() -> None:
    text = ENV_EXAMPLE.read_text()
    assert SECRET_VAR not in _env_example(), "an uncommented (empty) webhook secret stops the Gateway from starting"
    assert re.search(rf"^# {SECRET_VAR}=\S", text, re.MULTILINE), f"no commented `# {SECRET_VAR}=...` example"
    for fact in ("16 characters", "empty"):
        assert fact in text, f".env.example does not say {fact!r} about the webhook secret"


def test_gateway_passes_the_webhook_secret_by_name_only(compose: dict) -> None:
    gateways = _services_of_role(compose, "gateway")
    assert gateways
    for name, service in gateways.items():
        env: Any = service.get("environment")
        assert _passed_by_name(env, SECRET_VAR), f"{name}: {SECRET_VAR} must be declared by name only, got {env!r}"


@pytest.mark.parametrize("role", ["manager", "worker"])
def test_only_the_gateway_gets_webhook_secrets(compose: dict, role: str) -> None:
    for name, service in _services_of_role(compose, role).items():
        assert SECRET_VAR not in (service.get("environment") or {}), f"{name} must not receive webhook secrets"
