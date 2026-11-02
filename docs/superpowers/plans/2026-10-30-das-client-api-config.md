# DAS Client API (Phase 4) Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the Phase 4 configuration (spec task group 2 in `.kiro/specs/das-client-api/tasks.md`): `client_limits`, `events`, `webhook` and `webhook_secrets` in Agent_Config, their startup validation, and `resolve_settings` in `das/client/settings.py`.

**Architecture:** New pydantic sections in `das/config/schema.py`. Checks that need validated values (fingerprint collisions, override fingerprints, webhook pairing, secret existence, https) run in a new `client_api_errors(config)` called from `validate_config`, so they fail with exit 2 before any connection. `webhook_secrets` is a map credential: `das/config/secrets.py` maps `DAS__WEBHOOK_SECRETS__<NAME>` to `webhook_secrets.<name>`. `ClientLimitsConfig.merged` is the single place that merges an override with the default; `resolve_settings` converts the result for the Gateway. Spec: `requirements.md` Req 1, `design.md` Components §1.

**Tech Stack:** Python 3.12, pydantic 2.14.1, pytest 9.1.1, uv.

## Global Constraints

- Python `>=3.12,<3.13`; run everything through `uv run`. No new dependencies.
- Credentials (webhook secrets) only from env or `secrets_file`, never YAML, and every one registered with the Redactor through `AgentConfig.secret_values()`.
- Config errors are `ConfigError` with one `FieldError` per field path; no error text may contain a key, secret or fingerprint value. Validation completes before any connection (exit code 2).
- Config precedence stays env (`DAS__`, `__` nesting) > file > default.
- `das/client/` stays pure (no I/O, time or random) and must not import `redis`, `aio_pika`, `aiormq`, `pamqp` or `grpc`.
- Lint: `uv run ruff check` (rules E, F; line length 120). Types: `uv run mypy` (package `das/`).
- Docstrings cite requirement numbers as `Phase 4 Req X.Y` or `Req X.Y`.
- Not a git repository. Each task ends by ticking its checkboxes in `.kiro/specs/das-client-api/tasks.md` instead of committing.
- Every code change in this plan was prototyped and run before the plan was written: the full fast suite passed (2,144 tests) with ruff and mypy clean.

---

### Task 1: Config sections, secrets and validation (spec tasks 2.1, 2.2)

**Files:**
- Modify: `das/config/schema.py` (apply the diff below)
- Modify: `das/config/secrets.py` (apply the diff below)
- Test: `tests/unit/test_config_client_api.py` (new)
- Modify: `tests/fixtures/phase3_baseline/contract.json` (regenerated, Step 6)

**Interfaces:**
- Consumes: `das.ops.keys.key_fingerprint(key: bytes) -> str`; existing `_Section`, `_is_http_url`, `FieldError`, `validate_config`.
- Produces (used by Task 2 and later task groups):
  - `BudgetConfig(max_total_tokens: int | None, max_cost: Decimal | None)`
  - `ClientSettingsConfig(rate, burst, max_active_requests, max_streams, default_budget, webhook_url, webhook_secret_name)`
  - `ClientOverride(fingerprint, <same fields, all optional>)`
  - `ClientLimitsConfig(default, overrides, idempotency_ttl, allow_insecure_webhooks)` with `merged(override) -> ClientSettingsConfig`, `override_for(fp) -> ClientOverride | None`, `settings_for(fp) -> ClientSettingsConfig`
  - `EventsConfig(max_len, max_connections, keepalive)`, `WebhookConfig(workers, timeout, base_backoff, max_backoff, max_attempts)`
  - `AgentConfig.client_limits`, `.events`, `.webhook`, `.webhook_secrets: dict[str, SecretStr]` (keys lower-case)
  - `client_api_errors(config: AgentConfig) -> list[FieldError]`; `SECRET_PREFIX_FIELDS`; `das.config.secrets.secret_path(name: str) -> str | None`

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_config_client_api.py` with exactly this content:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_config_client_api.py -q`
Expected: most tests FAIL (for example `test_defaults` with `AttributeError: 'AgentConfig' object has no attribute 'client_limits'`, and the range tests because `client_limits` is an unknown field).

- [ ] **Step 3: Apply the schema change**

Apply this diff to `das/config/schema.py` (for example save it to a file and run `patch -p1 < file` from the repo root):

```diff
--- a/das/config/schema.py
+++ b/das/config/schema.py
@@ -21,6 +21,7 @@
 from typing import Annotated, Any, Literal
 
 from pydantic import (
+    AfterValidator,
     BaseModel,
     BeforeValidator,
     ConfigDict,
@@ -35,6 +36,7 @@
 from das.errors import FieldError
 from das.ops.cost import MAX_PRICE_ENTRIES, PriceEntry, PriceTable
 from das.ops.iso4217 import ISO_4217_CODES
+from das.ops.keys import key_fingerprint
 
 Role = Literal["gateway", "manager", "worker"]
 ROLES: tuple[str, ...] = ("gateway", "manager", "worker")
@@ -56,7 +58,12 @@
     "llm.aws_session_token",
     "tracing.otlp_headers",
     "gateway.cursor_secret",
+    "webhook_secrets",
 )
+
+#: Credential fields that hold a map of named secrets. Each entry comes from
+#: ``DAS__<FIELD>__<NAME>`` in the environment or the secrets file (Phase 4 Req 1.7).
+SECRET_PREFIX_FIELDS: tuple[str, ...] = ("webhook_secrets",)
 
 #: Default histogram bucket boundaries (Req 2.11).
 DEFAULT_HTTP_BUCKETS: tuple[float, ...] = (
@@ -313,8 +320,140 @@
     dlq_record_ttl: int = Field(default=2_592_000, ge=3600, le=31_536_000)
     sweep_interval: float = Field(default=60, ge=10, le=3600)
     batch_size: int = Field(default=500, ge=1, le=10_000)
+
+
+# --------------------------------------------------------------------------- Phase 4 client API
+
+#: Webhook secret names: letters and digits in groups joined by single underscores
+#: (``partner_a``). Names are lower-cased because environment names are upper case.
+SECRET_NAME_PATTERN = r"^[A-Za-z0-9]+(?:_[A-Za-z0-9]+)*$"
+WEBHOOK_SECRET_MIN_LENGTH = 16
+MAX_CLIENT_OVERRIDES = 1000
+#: Smallest non-zero ``rate``; 0 disables the Rate_Limit (Phase 4 Req 1.2).
+MIN_RATE = 0.001
+
+
+def _rate(value: float) -> float:
+    if 0 < value < MIN_RATE:
+        raise ValueError(f"must be 0 (no limit) or at least {MIN_RATE}")
+    return value
+
+
+def _webhook_url(value: str) -> str:
+    if not _is_http_url(value):
+        raise ValueError("must be an absolute http or https URL with a host")
+    return value
+
+
+def _webhook_secret(value: SecretStr) -> SecretStr:
+    if len(value.get_secret_value()) < WEBHOOK_SECRET_MIN_LENGTH:
+        raise ValueError(f"must be at least {WEBHOOK_SECRET_MIN_LENGTH} characters")
+    return value
+
+
+def _budget_decimal(value: Any) -> Any:
+    """YAML floats become ``Decimal(repr(x))``, as for prices; booleans are rejected."""
+    if isinstance(value, bool):
+        raise ValueError("must be a decimal number")
+    if isinstance(value, float):
+        return Decimal(repr(value))
+    return value
+
+
+Rate = Annotated[float, Field(ge=0, le=100_000, allow_inf_nan=False), AfterValidator(_rate)]
+Burst = Annotated[int, Field(ge=1, le=1_000_000)]
+ActiveRequests = Annotated[int, Field(ge=0, le=1_000_000)]
+Streams = Annotated[int, Field(ge=0, le=10_000)]
+SecretName = Annotated[
+    str, Field(min_length=1, max_length=64, pattern=SECRET_NAME_PATTERN), AfterValidator(str.lower)
+]
+WebhookUrl = Annotated[str, Field(max_length=2048), AfterValidator(_webhook_url)]
+WebhookSecret = Annotated[SecretStr, AfterValidator(_webhook_secret)]
+
+
+class BudgetConfig(_Section):
+    """A key's ``default_budget`` (Phase 4 Req 5.2). ``max_cost`` is in the Price_Table currency."""
+
+    max_total_tokens: int | None = Field(default=None, ge=1)
+    max_cost: Decimal | None = Field(
+        default=None, gt=0, max_digits=18, decimal_places=6, allow_inf_nan=False
+    )
+
+    @field_validator("max_cost", mode="before")
+    @classmethod
+    def _from_yaml(cls, value: Any) -> Any:
+        return _budget_decimal(value)
+
+
+class ClientSettingsConfig(_Section):
+    """Client_Settings fields (Phase 4 Req 1.2). 0 disables ``rate``, ``max_active_requests`` or ``max_streams``."""
+
+    rate: Rate = 60.0
+    burst: Burst = 120
+    max_active_requests: ActiveRequests = 100
+    max_streams: Streams = 10
+    default_budget: BudgetConfig | None = None
+    webhook_url: WebhookUrl | None = None
+    webhook_secret_name: SecretName | None = None
+
+
+class ClientOverride(_Section):
+    """Settings for one Key_Fingerprint. A field left unset (``None``) inherits the default."""
+
+    fingerprint: str = Field(pattern=r"^[0-9a-f]{8}$")
+    rate: Rate | None = None
+    burst: Burst | None = None
+    max_active_requests: ActiveRequests | None = None
+    max_streams: Streams | None = None
+    default_budget: BudgetConfig | None = None
+    webhook_url: WebhookUrl | None = None
+    webhook_secret_name: SecretName | None = None
+
+
+class ClientLimitsConfig(_Section):
+    """``client_limits`` (Phase 4 Req 1.1). Overrides are matched by Key_Fingerprint."""
+
+    default: ClientSettingsConfig = Field(default_factory=ClientSettingsConfig)
+    overrides: list[ClientOverride] = Field(default_factory=list, max_length=MAX_CLIENT_OVERRIDES)
+    idempotency_ttl: int = Field(default=86_400, ge=60, le=604_800)
+    allow_insecure_webhooks: bool = False
+
+    def merged(self, override: ClientOverride | None) -> ClientSettingsConfig:
+        """Effective settings: each field the override sets, else the default (Phase 4 Req 1.3)."""
+        if override is None:
+            return self.default
+        update = {
+            name: getattr(override, name)
+            for name in ClientSettingsConfig.model_fields
+            if getattr(override, name) is not None
+        }
+        return self.default.model_copy(update=update)
+
+    def override_for(self, fingerprint: str) -> ClientOverride | None:
+        return next((o for o in self.overrides if o.fingerprint == fingerprint), None)
+
+    def settings_for(self, fingerprint: str) -> ClientSettingsConfig:
+        return self.merged(self.override_for(fingerprint))
+
+
+class EventsConfig(_Section):
+    """Event_Stream and SSE_Endpoint limits (Phase 4 Req 6.5, 7.5, 7.7)."""
+
+    max_len: int = Field(default=1000, ge=100, le=100_000)
+    max_connections: int = Field(default=500, ge=1, le=10_000)
+    keepalive: float = Field(default=15.0, gt=0, le=300)
+
 
+class WebhookConfig(_Section):
+    """Webhook_Sender settings (Phase 4 Req 8.6). ``workers`` 0 disables the sender on this replica."""
 
+    workers: int = Field(default=2, ge=0, le=32)
+    timeout: float = Field(default=10.0, gt=0, le=60)
+    base_backoff: float = Field(default=30.0, gt=0, le=3600)
+    max_backoff: float = Field(default=3600.0, gt=0, le=86_400)
+    max_attempts: int = Field(default=8, ge=1, le=20)
+
+
 class GatewayConfig(_Section):
     # Credential: env or secrets file only (D10).
     cursor_secret: SecretStr | None = None
@@ -358,6 +497,12 @@
     admin_api_keys: Annotated[list[SecretStr], BeforeValidator(_split_csv)] = Field(
         default_factory=list
     )
+    # Phase 4 client API (Req 1).
+    client_limits: ClientLimitsConfig = Field(default_factory=ClientLimitsConfig)
+    events: EventsConfig = Field(default_factory=EventsConfig)
+    webhook: WebhookConfig = Field(default_factory=WebhookConfig)
+    # Webhook secrets by name: DAS__WEBHOOK_SECRETS__<NAME> or the secrets file only (Req 1.7).
+    webhook_secrets: dict[SecretName, WebhookSecret] = Field(default_factory=dict)
 
     @model_validator(mode="after")
     def _defaults(self) -> AgentConfig:
@@ -381,6 +526,7 @@
         values.extend(v for _, v in self.tracing.header_pairs())
         if self.gateway.cursor_secret is not None:
             values.append(self.gateway.cursor_secret.get_secret_value())
+        values.extend(s.get_secret_value() for s in self.webhook_secrets.values())
         if self.llm is not None:
             for s in (
                 self.llm.api_key,
@@ -654,9 +800,106 @@
             )
         elif key in api_keys:
             errors.append(FieldError(path, "must not equal an API key in api_keys"))
+    return errors
+
+
+def client_api_errors(config: AgentConfig) -> list[FieldError]:
+    """Phase 4 checks that need validated values (Req 1.4-1.6, 8.6).
+
+    For role ``gateway``: Key_Fingerprint collisions, unknown or duplicate override
+    fingerprints, and webhook settings. For every role: ``webhook.base_backoff`` is
+    at most ``webhook.max_backoff``. Errors name field paths and never include a
+    key, secret or fingerprint value.
+    """
+    errors: list[FieldError] = []
+    if config.webhook.base_backoff > config.webhook.max_backoff:
+        errors.append(
+            FieldError("webhook.base_backoff", "must be less than or equal to webhook.max_backoff")
+        )
+    if config.role != "gateway":
+        return errors
+
+    owners: dict[str, str] = {}
+    values: set[str] = set()
+    for field, keys in (("api_keys", config.api_keys), ("admin_api_keys", config.admin_api_keys)):
+        for index, key in enumerate(keys):
+            value = key.get_secret_value()
+            if value in values:
+                continue  # the same key listed twice is not a collision
+            values.add(value)
+            path = f"{field}[{index}]"
+            fp = key_fingerprint(value.encode("utf-8"))
+            if fp in owners:
+                errors.append(
+                    FieldError(path, f"has the same Key_Fingerprint as {owners[fp]}; replace one of the two keys")
+                )
+            else:
+                owners[fp] = path
+
+    limits = config.client_limits
+    first: dict[str, int] = {}
+    for index, override in enumerate(limits.overrides):
+        path = f"client_limits.overrides[{index}].fingerprint"
+        if override.fingerprint in first:
+            errors.append(
+                FieldError(
+                    path,
+                    f"duplicate fingerprint (same as client_limits.overrides[{first[override.fingerprint]}])",
+                )
+            )
+            continue
+        first[override.fingerprint] = index
+        if override.fingerprint not in owners:
+            errors.append(FieldError(path, "matches no configured API key or Admin_API_Key"))
+
+    errors.extend(_webhook_errors("client_limits.default", limits.default, limits.default, config))
+    for index, override in enumerate(limits.overrides):
+        errors.extend(
+            _webhook_errors(f"client_limits.overrides[{index}]", override, limits.merged(override), config)
+        )
     return errors
 
 
+def _webhook_errors(
+    prefix: str,
+    own: ClientSettingsConfig | ClientOverride,
+    effective: ClientSettingsConfig,
+    config: AgentConfig,
+) -> list[FieldError]:
+    """Webhook_Target checks for one settings entry (Req 1.5, 1.6).
+
+    ``own`` is the entry as written at ``prefix``; ``effective`` is it merged with the
+    default. Only fields the entry sets itself are reported, so an inherited problem
+    is reported once, at ``client_limits.default``.
+    """
+    if own.webhook_url is None and own.webhook_secret_name is None:
+        return []
+    if (effective.webhook_url is None) != (effective.webhook_secret_name is None):
+        missing = "webhook_url" if effective.webhook_url is None else "webhook_secret_name"
+        reason = "required when the other webhook field is set (set both, or neither)"
+        return [FieldError(f"{prefix}.{missing}", reason)]
+    errors: list[FieldError] = []
+    name = own.webhook_secret_name
+    if name is not None and name not in config.webhook_secrets:
+        errors.append(
+            FieldError(
+                f"{prefix}.webhook_secret_name",
+                f"names no configured webhook secret (set DAS__WEBHOOK_SECRETS__{name.upper()} "
+                "in the environment or the secrets file)",
+            )
+        )
+    url = own.webhook_url
+    insecure_ok = config.client_limits.allow_insecure_webhooks
+    if url is not None and urlsplit(url).scheme.lower() != "https" and not insecure_ok:
+        errors.append(
+            FieldError(
+                f"{prefix}.webhook_url",
+                "must use https (set client_limits.allow_insecure_webhooks to allow http)",
+            )
+        )
+    return errors
+
+
 # --------------------------------------------------------------------------- error formatting
 
 
@@ -756,6 +999,9 @@
         errors.extend(pydantic_errors(exc))
     seen = {e.field for e in errors}
     errors.extend(e for e in cross_field_errors(data) if e.field not in seen)
+    if config is not None:
+        seen = {e.field for e in errors}
+        errors.extend(e for e in client_api_errors(config) if e.field not in seen)
     if errors or config is None:
         raise ConfigError(errors, reason="invalid configuration")
     return config
@@ -771,8 +1017,19 @@
     "DEFAULT_HTTP_BUCKETS",
     "MAX_ADMIN_API_KEYS",
     "MAX_BUCKETS",
+    "MAX_CLIENT_OVERRIDES",
+    "MIN_RATE",
+    "SECRET_NAME_PATTERN",
+    "SECRET_PREFIX_FIELDS",
+    "WEBHOOK_SECRET_MIN_LENGTH",
     "AgentConfig",
     "BucketsConfig",
+    "BudgetConfig",
+    "ClientLimitsConfig",
+    "ClientOverride",
+    "ClientSettingsConfig",
+    "EventsConfig",
+    "WebhookConfig",
     "GatewayConfig",
     "GrpcConfig",
     "LLMConfig",
@@ -787,6 +1044,7 @@
     "TracingConfig",
     "TransportsConfig",
     "WorkerConfig",
+    "client_api_errors",
     "cross_field_errors",
     "parse_otlp_headers",
     "pydantic_errors",
```

- [ ] **Step 4: Apply the secrets change**

Apply this diff to `das/config/secrets.py`:

```diff
--- a/das/config/secrets.py
+++ b/das/config/secrets.py
@@ -17,7 +17,7 @@
 from pathlib import Path
 from typing import Any
 
-from das.config.schema import SECRET_FIELDS
+from das.config.schema import SECRET_FIELDS, SECRET_PREFIX_FIELDS
 from das.errors import ConfigError, FieldError
 
 ENV_PREFIX = "DAS__"
@@ -29,9 +29,30 @@
     return ENV_PREFIX + ENV_SEPARATOR.join(part.upper() for part in path.split("."))
 
 
-_SECRET_BY_ENV: dict[str, str] = {env_name(p): p for p in SECRET_FIELDS}
+# Map fields (``webhook_secrets``) are only reachable through ``<NAME>`` suffixes, see secret_path.
+_SECRET_BY_ENV: dict[str, str] = {
+    env_name(p): p for p in SECRET_FIELDS if p not in SECRET_PREFIX_FIELDS
+}
 
 
+def secret_path(name: str) -> str | None:
+    """Dotted credential path for an environment or secrets-file name, or ``None``.
+
+    ``DAS__LLM__API_KEY`` maps to ``llm.api_key``. For a map field such as
+    ``webhook_secrets``, ``DAS__WEBHOOK_SECRETS__PARTNER_A`` maps to
+    ``webhook_secrets.partner_a`` (Phase 4 Req 1.7).
+    """
+    path = _SECRET_BY_ENV.get(name)
+    if path is not None:
+        return path
+    for field in SECRET_PREFIX_FIELDS:
+        prefix = env_name(field) + ENV_SEPARATOR
+        suffix = name[len(prefix) :]
+        if name.startswith(prefix) and suffix and ENV_SEPARATOR not in suffix:
+            return f"{field}.{suffix.lower()}"
+    return None
+
+
 def _get(data: Mapping[str, Any], path: str) -> Any:
     node: Any = data
     for part in path.split("."):
@@ -51,7 +72,8 @@
         FieldError(
             path,
             "credentials must not appear in the configuration file; "
-            f"set {env_name(path)} in the environment or the secrets file",
+            f"set {env_name(path)}{'__<NAME>' if path in SECRET_PREFIX_FIELDS else ''} "
+            "in the environment or the secrets file",
         )
         for path in SECRET_FIELDS
         if _has_value(_get(file_values, path))
@@ -113,7 +135,7 @@
                 FieldError("secrets_file", f"secrets file {ref!r} line {lineno}: expected NAME=VALUE")
             )
             continue
-        if name not in _SECRET_BY_ENV:
+        if secret_path(name) is None:
             errors.append(
                 FieldError(
                     "secrets_file",
@@ -131,7 +153,7 @@
     """Convert ``{DAS__NAME: value}`` from a secrets file into a nested field mapping."""
     result: dict[str, Any] = {}
     for name, value in secrets.items():
-        path = _SECRET_BY_ENV.get(name)
+        path = secret_path(name)
         if path is None:
             continue
         *parents, leaf = path.split(".")
@@ -149,6 +171,7 @@
     "env_name",
     "file_secret_errors",
     "read_secrets_file",
+    "secret_path",
     "secrets_to_values",
     "strip_secrets",
 ]
```

- [ ] **Step 5: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_config_client_api.py -q -W error`
Expected: PASS (48 tests).

- [ ] **Step 6: Regenerate the Phase 3 contract baseline**

The contract test snapshots `AgentConfig.model_json_schema()` and the `DAS__` env-var list, so it now fails on exactly those two keys. Save the current baseline, regenerate it, then compare:

```bash
uv run pytest tests/unit/test_phase3_baseline.py -q    # expect 2 failures: config_schema, env_vars
cp tests/fixtures/phase3_baseline/contract.json /tmp/das_contract_before.json
cp tests/fixtures/phase3_baseline/codec_corpus.jsonl /tmp/das_corpus_before.jsonl
uv run python -m tests.fixtures.phase3_baseline.capture
cmp tests/fixtures/phase3_baseline/codec_corpus.jsonl /tmp/das_corpus_before.jsonl && echo "corpus unchanged"
uv run python - <<'PY'
import json
a = json.load(open("/tmp/das_contract_before.json"))
b = json.load(open("tests/fixtures/phase3_baseline/contract.json"))
for k in sorted(a):
    print(k, "same" if a[k] == b[k] else "CHANGED")
print("added env vars:", sorted(set(b["env_vars"]) - set(a["env_vars"])))
print("removed env vars:", sorted(set(a["env_vars"]) - set(b["env_vars"])))
PY
rm /tmp/das_contract_before.json /tmp/das_corpus_before.jsonl
uv run pytest tests/unit/test_phase3_baseline.py -q    # expect all pass
```

Expected: `corpus unchanged`; `config_schema` and `env_vars` CHANGED; `exit_codes`, `metrics` and `routes` same; no removed env vars; exactly these 20 added: `DAS__CLIENT_LIMITS__ALLOW_INSECURE_WEBHOOKS`, `DAS__CLIENT_LIMITS__DEFAULT__BURST`, `DAS__CLIENT_LIMITS__DEFAULT__DEFAULT_BUDGET__MAX_COST`, `DAS__CLIENT_LIMITS__DEFAULT__DEFAULT_BUDGET__MAX_TOTAL_TOKENS`, `DAS__CLIENT_LIMITS__DEFAULT__MAX_ACTIVE_REQUESTS`, `DAS__CLIENT_LIMITS__DEFAULT__MAX_STREAMS`, `DAS__CLIENT_LIMITS__DEFAULT__RATE`, `DAS__CLIENT_LIMITS__DEFAULT__WEBHOOK_SECRET_NAME`, `DAS__CLIENT_LIMITS__DEFAULT__WEBHOOK_URL`, `DAS__CLIENT_LIMITS__IDEMPOTENCY_TTL`, `DAS__CLIENT_LIMITS__OVERRIDES`, `DAS__EVENTS__KEEPALIVE`, `DAS__EVENTS__MAX_CONNECTIONS`, `DAS__EVENTS__MAX_LEN`, `DAS__WEBHOOK_SECRETS`, `DAS__WEBHOOK__BASE_BACKOFF`, `DAS__WEBHOOK__MAX_ATTEMPTS`, `DAS__WEBHOOK__MAX_BACKOFF`, `DAS__WEBHOOK__TIMEOUT`, `DAS__WEBHOOK__WORKERS`. If anything else changed, stop and report it.

- [ ] **Step 7: Run the existing config and secrets suites**

Run: `uv run pytest tests/unit/test_config.py tests/unit/test_config_errors.py tests/unit/test_config_phase2.py tests/unit/test_cli.py tests/property/test_config_secrets.py tests/property/test_config_precedence.py tests/property/test_config_validation.py tests/property/test_log_redaction.py -q`
Expected: all pass. `tests/property/test_config_secrets.py` samples every name in `SECRET_FIELDS`, so it now also checks that a `webhook_secrets` value in YAML is never echoed.

- [ ] **Step 8: Lint and type-check**

Run: `uv run ruff check && uv run mypy`
Expected: `All checks passed!` and `Success: no issues found`

- [ ] **Step 9: Tick the spec tasks**

In `.kiro/specs/das-client-api/tasks.md` tick `2.2 Write unit tests for client config`. Leave 2.1 unticked: its `das/client/settings.py` deliverable is Task 2.

---

### Task 2: `resolve_settings` (spec task 2.1, `das/client/settings.py`)

**Files:**
- Create: `das/client/settings.py`
- Test: `tests/unit/test_client_settings.py`

**Interfaces:**
- Consumes: `das.client.budget.Budget`; `ClientLimitsConfig`, `BudgetConfig`, `ClientOverride`, `ClientSettingsConfig` from Task 1.
- Produces: `ClientSettings(fingerprint, rate, burst, max_active_requests, max_streams, default_budget: Budget | None, webhook_url, webhook_secret_name)` (frozen dataclass); `resolve_settings(fingerprint: str, limits: ClientLimitsConfig) -> ClientSettings`. The Gateway (task group 7) calls it once per authenticated call.

- [ ] **Step 1: Write the failing test**

Create `tests/unit/test_client_settings.py` with exactly this content:

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_client_settings.py -q`
Expected: FAIL with `ModuleNotFoundError: No module named 'das.client.settings'`

- [ ] **Step 3: Write the implementation**

Create `das/client/settings.py` with exactly this content:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_client_settings.py -q -W error`
Expected: PASS (5 tests)

- [ ] **Step 5: Lint and type-check**

Run: `uv run ruff check das/client tests/unit/test_client_settings.py && uv run mypy && uv run lint-imports`
Expected: `All checks passed!`, `Success: no issues found`, `Contracts: 1 kept, 0 broken.`

- [ ] **Step 6: Tick the spec task**

In `.kiro/specs/das-client-api/tasks.md` tick `2.1 Add config sections and validation` (its `das/client/settings.py` deliverable lands here) and the parent `2. Configuration` (2.2 was ticked in Task 1).

---

### Task 3: Checkpoint (spec task 3)

**Files:** none changed except the checkbox.

- [ ] **Step 1: Run the fast suite**

Run: `uv run pytest -m "not integration" -q -p no:cacheprovider`
Expected: `0 failed`; 2,144 passed (2,091 before this group plus 53 new tests), the same 3 pre-existing pytest-asyncio warnings.

- [ ] **Step 2: Run every static check**

Run: `uv run ruff check && uv run mypy && uv run lint-imports`
Expected: `All checks passed!`, `Success: no issues found`, `Contracts: 1 kept, 0 broken.`

- [ ] **Step 3: Tick the checkpoint**

In `.kiro/specs/das-client-api/tasks.md` tick `3. Checkpoint - Ensure all tests pass`.
