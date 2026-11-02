"""Phase 4 client API config: client_limits, events, webhook and webhook secrets (Req 1)."""
from __future__ import annotations

import io
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml

import das.cli as cli
from das.config import load_config
from das.config.schema import AgentConfig
from das.errors import EXIT_CONFIG_ERROR, ConfigError
from das.observability import configure_logging
from das.ops.keys import key_fingerprint

API_KEY = "client-key-0123456789"
API_KEY_2 = "client-key-second-0001"
ADMIN_KEY = "admin-key-abcdef0123456789"
FP = key_fingerprint(API_KEY.encode())
ADMIN_FP = key_fingerprint(ADMIN_KEY.encode())
SECRET = "whsec_0123456789abcdef"
# Two API keys whose Key_Fingerprints are equal (found by search; sha256[:8] == "185c6095").
COLLIDING = ("collide-key-00010600", "collide-key-00036106")
HOOK = "https://hooks.example.com/das"
ENV = {"DAS__API_KEYS": API_KEY}


def gateway_cfg(**overrides: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {"role": "gateway", "agent_id": "gateway-test", "redis": {"url": "redis://127.0.0.1:1/0"}}
    cfg.update(overrides)
    return cfg


def worker_cfg(**overrides: Any) -> dict[str, Any]:
    cfg: dict[str, Any] = {
        "role": "worker",
        "agent_id": "worker-test",
        "transports": {"jobs": "rabbitmq", "control": "grpc"},
        "rabbitmq": {"url": "amqp://127.0.0.1:1/"},
        "redis": {"url": "redis://127.0.0.1:1/0"},
        "grpc": {"bind": "0.0.0.0:50051", "advertise_host": "w", "advertise_port": 50051},
        "llm": {"provider": "ollama", "model": "m", "base_url": "http://127.0.0.1:1"},
        "worker": {"capabilities": ["research"]},
    }
    cfg.update(overrides)
    return cfg


def write(tmp_path: Path, data: object, name: str = "config.yaml") -> Path:
    p = tmp_path / name
    p.write_text(yaml.safe_dump(data), encoding="utf-8")
    return p


def load(tmp_path: Path, data: dict[str, Any], env: dict[str, str] | None = None) -> AgentConfig:
    return load_config(write(tmp_path, data), ENV if env is None else env)


def errors(tmp_path: Path, data: dict[str, Any], env: dict[str, str] | None = None) -> dict[str, str]:
    with pytest.raises(ConfigError) as ei:
        load(tmp_path, data, env)
    return {e.field: e.reason for e in ei.value.errors}


def override(**fields: Any) -> dict[str, Any]:
    return {"client_limits": {"overrides": [{"fingerprint": FP, **fields}]}}


# --------------------------------------------------------------------------- defaults


def test_defaults(tmp_path: Path) -> None:
    cfg = load(tmp_path, gateway_cfg())
    d = cfg.client_limits.default
    assert (d.rate, d.burst, d.max_active_requests, d.max_streams) == (60.0, 120, 100, 10)
    assert (d.default_budget, d.webhook_url, d.webhook_secret_name) == (None, None, None)
    assert cfg.client_limits.overrides == []
    assert cfg.client_limits.idempotency_ttl == 86_400
    assert cfg.client_limits.allow_insecure_webhooks is False
    assert (cfg.events.max_len, cfg.events.max_connections, cfg.events.keepalive) == (1000, 500, 15.0)
    w = cfg.webhook
    assert (w.workers, w.timeout, w.base_backoff, w.max_backoff, w.max_attempts) == (2, 10.0, 30.0, 3600.0, 8)
    assert cfg.webhook_secrets == {}


def test_env_sets_client_limits(tmp_path: Path) -> None:
    env = {**ENV, "DAS__CLIENT_LIMITS__DEFAULT__RATE": "5", "DAS__EVENTS__MAX_LEN": "200"}
    cfg = load(tmp_path, gateway_cfg(), env)
    assert cfg.client_limits.default.rate == 5.0
    assert cfg.events.max_len == 200


# --------------------------------------------------------------------------- overrides (1.3)


def test_override_inherits_unset_fields(tmp_path: Path) -> None:
    cfg = load(
        tmp_path,
        gateway_cfg(
            client_limits={
                "default": {"rate": 10, "burst": 20, "default_budget": {"max_total_tokens": 1000}},
                "overrides": [{"fingerprint": FP, "burst": 50, "default_budget": {"max_cost": 0.5}}],
            }
        ),
    )
    mine = cfg.client_limits.settings_for(FP)
    assert (mine.rate, mine.burst, mine.max_active_requests) == (10.0, 50, 100)
    # A set default_budget replaces the default's budget as a whole.
    assert mine.default_budget is not None
    assert (mine.default_budget.max_total_tokens, mine.default_budget.max_cost) == (None, Decimal("0.5"))
    other = cfg.client_limits.settings_for("ffffffff")
    assert (other.rate, other.burst) == (10.0, 20)


def test_override_can_disable_a_limit(tmp_path: Path) -> None:
    cfg = load(tmp_path, gateway_cfg(**override(rate=0, max_active_requests=0)))
    s = cfg.client_limits.settings_for(FP)
    assert (s.rate, s.max_active_requests) == (0.0, 0)


def test_admin_key_fingerprint_is_accepted(tmp_path: Path) -> None:
    data = gateway_cfg(client_limits={"overrides": [{"fingerprint": ADMIN_FP, "rate": 1}]})
    cfg = load(tmp_path, data, {**ENV, "DAS__ADMIN_API_KEYS": ADMIN_KEY})
    assert cfg.client_limits.settings_for(ADMIN_FP).rate == 1.0


# --------------------------------------------------------------------------- field ranges (1.1, 1.2)


@pytest.mark.parametrize(
    ("section", "field"),
    [
        ({"rate": -1}, "client_limits.default.rate"),
        ({"rate": 0.0005}, "client_limits.default.rate"),
        ({"rate": 100_001}, "client_limits.default.rate"),
        ({"rate": ".nan"}, "client_limits.default.rate"),
        ({"burst": 0}, "client_limits.default.burst"),
        ({"max_active_requests": -1}, "client_limits.default.max_active_requests"),
        ({"max_streams": 10_001}, "client_limits.default.max_streams"),
        ({"default_budget": {"max_total_tokens": 0}}, "client_limits.default.default_budget.max_total_tokens"),
        ({"default_budget": {"max_cost": 0}}, "client_limits.default.default_budget.max_cost"),
        ({"default_budget": {"max_cost": "0.0000001"}}, "client_limits.default.default_budget.max_cost"),
        ({"default_budget": {"max_cost": True}}, "client_limits.default.default_budget.max_cost"),
        ({"webhook_url": "ftp://hooks.example.com/x"}, "client_limits.default.webhook_url"),
        ({"webhook_secret_name": "bad-name"}, "client_limits.default.webhook_secret_name"),
    ],
)
def test_client_settings_ranges(tmp_path: Path, section: dict[str, Any], field: str) -> None:
    text = yaml.safe_dump(gateway_cfg(client_limits={"default": section})).replace("'.nan'", ".nan")
    path = tmp_path / "config.yaml"
    path.write_text(text, encoding="utf-8")
    with pytest.raises(ConfigError) as ei:
        load_config(path, ENV)
    assert field in ei.value.fields


@pytest.mark.parametrize(
    ("data", "field"),
    [
        ({"client_limits": {"idempotency_ttl": 59}}, "client_limits.idempotency_ttl"),
        ({"client_limits": {"overrides": [{"fingerprint": "ABCDEF12"}]}}, "client_limits.overrides[0].fingerprint"),
        ({"client_limits": {"overrides": [{"fingerprint": FP, "burst": 0}]}}, "client_limits.overrides[0].burst"),
        ({"events": {"max_len": 99}}, "events.max_len"),
        ({"events": {"max_connections": 0}}, "events.max_connections"),
        ({"webhook": {"workers": 33}}, "webhook.workers"),
        ({"webhook": {"max_attempts": 0}}, "webhook.max_attempts"),
        ({"webhook": {"timeout": 61}}, "webhook.timeout"),
    ],
)
def test_other_ranges(tmp_path: Path, data: dict[str, Any], field: str) -> None:
    assert field in errors(tmp_path, gateway_cfg(**data))


def test_base_backoff_above_max_backoff(tmp_path: Path) -> None:
    found = errors(tmp_path, gateway_cfg(webhook={"base_backoff": 100, "max_backoff": 50}))
    assert found == {"webhook.base_backoff": "must be less than or equal to webhook.max_backoff"}


def test_too_many_overrides(tmp_path: Path) -> None:
    many = [{"fingerprint": f"{i:08x}"} for i in range(1001)]
    assert "client_limits.overrides" in errors(tmp_path, gateway_cfg(client_limits={"overrides": many}))


# --------------------------------------------------------------------------- fingerprints (1.4)


def test_unknown_override_fingerprint(tmp_path: Path) -> None:
    found = errors(tmp_path, gateway_cfg(client_limits={"overrides": [{"fingerprint": "00000000"}]}))
    assert found == {"client_limits.overrides[0].fingerprint": "matches no configured API key or Admin_API_Key"}


def test_duplicate_override_fingerprint(tmp_path: Path) -> None:
    data = gateway_cfg(client_limits={"overrides": [{"fingerprint": FP}, {"fingerprint": FP}]})
    found = errors(tmp_path, data)
    assert found == {
        "client_limits.overrides[1].fingerprint": "duplicate fingerprint (same as client_limits.overrides[0])"
    }


def test_colliding_key_fingerprints(tmp_path: Path) -> None:
    assert key_fingerprint(COLLIDING[0].encode()) == key_fingerprint(COLLIDING[1].encode())
    found = errors(tmp_path, gateway_cfg(), {"DAS__API_KEYS": ",".join(COLLIDING)})
    assert found == {"api_keys[1]": "has the same Key_Fingerprint as api_keys[0]; replace one of the two keys"}


def test_collision_between_api_and_admin_key(tmp_path: Path) -> None:
    env = {"DAS__API_KEYS": COLLIDING[0], "DAS__ADMIN_API_KEYS": COLLIDING[1]}
    found = errors(tmp_path, gateway_cfg(), env)
    assert found == {"admin_api_keys[0]": "has the same Key_Fingerprint as api_keys[0]; replace one of the two keys"}


def test_same_key_listed_twice_is_not_a_collision(tmp_path: Path) -> None:
    cfg = load(tmp_path, gateway_cfg(), {"DAS__API_KEYS": f"{API_KEY},{API_KEY},{API_KEY_2}"})
    assert len(cfg.api_keys) == 3


def test_fingerprint_checks_apply_to_gateway_only(tmp_path: Path) -> None:
    cfg = load(tmp_path, worker_cfg(client_limits={"overrides": [{"fingerprint": "00000000"}]}), {})
    assert cfg.client_limits.overrides[0].fingerprint == "00000000"


# --------------------------------------------------------------------------- webhooks (1.5-1.7)


def test_webhook_from_env_secret(tmp_path: Path) -> None:
    env = {**ENV, "DAS__WEBHOOK_SECRETS__PARTNER_A": SECRET}
    cfg = load(tmp_path, gateway_cfg(**override(webhook_url=HOOK, webhook_secret_name="partner_a")), env)
    assert cfg.client_limits.settings_for(FP).webhook_url == HOOK
    assert cfg.webhook_secrets["partner_a"].get_secret_value() == SECRET
    assert SECRET in cfg.secret_values()


def test_webhook_from_secrets_file(tmp_path: Path) -> None:
    secrets = tmp_path / "secrets.env"
    secrets.write_text(f"DAS__API_KEYS={API_KEY}\nDAS__WEBHOOK_SECRETS__PARTNER_A={SECRET}\n", encoding="utf-8")
    data = gateway_cfg(secrets_file=str(secrets), **override(webhook_url=HOOK, webhook_secret_name="partner_a"))
    cfg = load(tmp_path, data, {})
    assert cfg.webhook_secrets["partner_a"].get_secret_value() == SECRET


def test_secret_name_is_case_insensitive(tmp_path: Path) -> None:
    env = {**ENV, "DAS__WEBHOOK_SECRETS__PARTNER_A": SECRET}
    cfg = load(tmp_path, gateway_cfg(**override(webhook_url=HOOK, webhook_secret_name="Partner_A")), env)
    assert cfg.client_limits.settings_for(FP).webhook_secret_name == "partner_a"


def test_webhook_inherited_from_default(tmp_path: Path) -> None:
    env = {**ENV, "DAS__WEBHOOK_SECRETS__PARTNER_A": SECRET}
    data = gateway_cfg(client_limits={"default": {"webhook_url": HOOK, "webhook_secret_name": "partner_a"}})
    cfg = load(tmp_path, data, env)
    assert cfg.client_limits.settings_for(FP).webhook_url == HOOK


@pytest.mark.parametrize(
    ("fields", "missing"),
    [({"webhook_url": HOOK}, "webhook_secret_name"), ({"webhook_secret_name": "partner_a"}, "webhook_url")],
)
def test_webhook_fields_must_be_paired(tmp_path: Path, fields: dict[str, str], missing: str) -> None:
    env = {**ENV, "DAS__WEBHOOK_SECRETS__PARTNER_A": SECRET}
    found = errors(tmp_path, gateway_cfg(**override(**fields)), env)
    assert found == {
        f"client_limits.overrides[0].{missing}": "required when the other webhook field is set (set both, or neither)"
    }


def test_override_completes_webhook_of_default(tmp_path: Path) -> None:
    env = {**ENV, "DAS__WEBHOOK_SECRETS__PARTNER_A": SECRET}
    data = gateway_cfg(
        client_limits={
            "default": {"webhook_secret_name": "partner_a"},
            "overrides": [{"fingerprint": FP, "webhook_url": HOOK}],
        }
    )
    found = errors(tmp_path, data, env)
    # The default alone is half a Webhook_Target; the override's merged settings are complete.
    assert found == {
        "client_limits.default.webhook_url": "required when the other webhook field is set (set both, or neither)"
    }


def test_webhook_secret_must_exist(tmp_path: Path) -> None:
    found = errors(tmp_path, gateway_cfg(**override(webhook_url=HOOK, webhook_secret_name="partner_a")))
    assert found == {
        "client_limits.overrides[0].webhook_secret_name": (
            "names no configured webhook secret (set DAS__WEBHOOK_SECRETS__PARTNER_A "
            "in the environment or the secrets file)"
        )
    }


def test_webhook_requires_https(tmp_path: Path) -> None:
    env = {**ENV, "DAS__WEBHOOK_SECRETS__PARTNER_A": SECRET}
    data = gateway_cfg(**override(webhook_url="http://hooks.example.com/das", webhook_secret_name="partner_a"))
    found = errors(tmp_path, data, env)
    assert found == {
        "client_limits.overrides[0].webhook_url": (
            "must use https (set client_limits.allow_insecure_webhooks to allow http)"
        )
    }
    data["client_limits"]["allow_insecure_webhooks"] = True
    assert load(tmp_path, data, env).client_limits.settings_for(FP).webhook_url == "http://hooks.example.com/das"


def test_webhook_secret_in_yaml_is_rejected(tmp_path: Path) -> None:
    found = errors(tmp_path, gateway_cfg(webhook_secrets={"partner_a": SECRET}))
    assert found == {
        "webhook_secrets": (
            "credentials must not appear in the configuration file; "
            "set DAS__WEBHOOK_SECRETS__<NAME> in the environment or the secrets file"
        )
    }


def test_short_webhook_secret_is_rejected(tmp_path: Path) -> None:
    found = errors(tmp_path, gateway_cfg(), {**ENV, "DAS__WEBHOOK_SECRETS__PARTNER_A": "short-secret"})
    assert found == {"webhook_secrets.partner_a": "must be at least 16 characters"}


def test_secrets_file_rejects_unknown_names(tmp_path: Path) -> None:
    secrets = tmp_path / "secrets.env"
    secrets.write_text(f"DAS__WEBHOOK_SECRETS={SECRET}\nDAS__WEBHOOK_SECRETS__A__B={SECRET}\n", encoding="utf-8")
    with pytest.raises(ConfigError) as ei:
        load(tmp_path, gateway_cfg(secrets_file=str(secrets)))
    reasons = [e.reason for e in ei.value.errors if e.field == "secrets_file"]
    assert len(reasons) == 2
    assert all("is not a credential setting" in r for r in reasons)


def test_errors_never_contain_secret_values(tmp_path: Path) -> None:
    env = {"DAS__API_KEYS": ",".join(COLLIDING), "DAS__WEBHOOK_SECRETS__PARTNER_A": "short-secret"}
    data = gateway_cfg(webhook_secrets={"other": SECRET}, **override(webhook_url="http://x.example.com"))
    with pytest.raises(ConfigError) as ei:
        load(tmp_path, data, env)
    text = "\n".join([str(ei.value), repr(ei.value), *(str(e) for e in ei.value.errors)])
    for value in (SECRET, "short-secret", *COLLIDING, key_fingerprint(COLLIDING[0].encode())):
        assert value not in text


# --------------------------------------------------------------------------- startup (exit 2)


@pytest.fixture(autouse=True)
def _restore_logging():
    yield
    configure_logging("test", stream=io.StringIO())


def test_invalid_client_config_exits_2_before_connecting(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    called: list[str] = []

    def fail_load_transports(config: object) -> None:  # pragma: no cover - must not run
        called.append("load_transports")
        raise AssertionError("startup continued after a config error")

    monkeypatch.setattr(cli, "load_transports", fail_load_transports)
    path = write(tmp_path, gateway_cfg(client_limits={"overrides": [{"fingerprint": "00000000"}]}))
    assert cli.main(["run", "--config", str(path)], env=ENV) == EXIT_CONFIG_ERROR
    assert called == []
