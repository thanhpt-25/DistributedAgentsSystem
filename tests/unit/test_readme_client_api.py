"""The README documents the Phase 4 client API, and what it says is true (das-client-api task 10.2; Phase 4 Req 11.3).

A test that only looks for mentions cannot tell a right statement from a wrong one, so this file checks what can
be checked mechanically: every setting, rejection reason, webhook outcome, event type, metric, header and error
code is mentioned; the defaults in the settings table equal the schema's real defaults; every YAML example loads
through the real config loader; every Python example parses and imports only names the client really exports; the
fingerprint recipe gives the right fingerprint; and internal links resolve. Whether the prose around them is right
is checked by reading the code and by running the examples against a real Gateway (see the plan), not here."""

from __future__ import annotations

import ast
import hashlib
import os
import re
from pathlib import Path
from typing import Any

import pytest
import yaml

import das_client
from das.client.events import EVENT_TYPES
from das.config import load_config
from das.config.schema import ClientLimitsConfig, ClientSettingsConfig, EventsConfig, WebhookConfig
from das.observability.metrics import CLIENT_REJECTION_REASONS, WEBHOOK_OUTCOMES
from das.ops.keys import key_fingerprint

ROOT = Path(__file__).resolve().parents[2]
#: ``DAS_README_UNDER_TEST`` points these checks at another file (a draft, or a deliberately broken copy).
README = Path(os.environ.get("DAS_README_UNDER_TEST") or ROOT / "README.md").read_text(encoding="utf-8")
FENCE = re.compile(r"^```(?P<lang>[\w-]*)[^\n]*\n(?P<body>.*?)^```", re.MULTILINE | re.DOTALL)
TABLE_ROW = re.compile(r"^\| `(?P<path>[\w.]+)` \| (?P<default>[^|]*) \|", re.MULTILINE)
SAMPLE_KEY = "change-me-api-key"
RECIPE = 'printf %s "$KEY" | shasum -a 256 | cut -c1-8'
ENV = {
    "DAS__REDIS__URL": "redis://redis:6379/0",
    "DAS__GRPC__ADVERTISE_HOST": "gateway",
    "DAS__API_KEYS": SAMPLE_KEY,
    "DAS__WEBHOOK_SECRETS__PARTNER": "a-test-webhook-secret-0123456789",
}

#: The settings sections and the model behind each (the dotted prefix is how the README names a field).
SECTIONS: dict[str, Any] = {
    "client_limits.default": ClientSettingsConfig,
    "client_limits": ClientLimitsConfig,
    "events": EventsConfig,
    "webhook": WebhookConfig,
}


def blocks(language: str) -> list[str]:
    return [m["body"] for m in FENCE.finditer(README) if m["lang"] == language]


CODE_SPAN = re.compile(r"`([^`\n]+)`")


def mentioned(token: str, text: str = README) -> bool:
    """``token`` appears inside a code span of ``text``, alone or in a longer path such as ``a.b.rate``.

    The spans are paired left to right and only their contents are searched. Matching a backtick to the next one
    with a regex would also match the prose *between* two spans, which is how a word like "retry" can pass for
    a documented value."""
    pattern = re.compile(rf"(?<![\w-]){re.escape(token)}(?![\w-])")
    return any(pattern.search(span) for span in CODE_SPAN.findall(text))


def section(heading: str) -> str:
    match = re.search(rf"^#+ {re.escape(heading)}[^\n]*\n(.*?)(?=^#{{1,3}} |\Z)", README, re.MULTILINE | re.DOTALL)
    return match.group(1) if match else ""


def shown_default(value: Any) -> str:
    """How the settings table writes a default: ``none``, ``false``, ``60`` (not ``60.0``), ``86400``."""
    if value is None or value == []:
        return "none"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, float):
        return f"{value:g}"
    return str(value)


# --------------------------------------------------------------------------- settings


@pytest.mark.parametrize("prefix", SECTIONS)
def test_every_setting_is_in_the_settings_table_with_its_real_default(prefix: str) -> None:
    table = section("Client API settings")
    assert table, "README has no `Client API settings` section"
    rows = {m["path"]: m["default"].strip() for m in TABLE_ROW.finditer(table)}
    model = SECTIONS[prefix]
    instance = model()
    for name in model.model_fields:
        if prefix == "client_limits" and name == "default":
            continue  # the section itself; its fields are listed under `client_limits.default`
        path = f"{prefix}.{name}"
        assert path in rows, f"the settings table has no row for `{path}`"
        assert rows[path] == shown_default(getattr(instance, name)), f"`{path}`: table says {rows[path]!r}"


def test_the_webhook_secrets_setting_is_documented() -> None:
    assert mentioned("webhook_secrets") and "DAS__WEBHOOK_SECRETS__" in README


# --------------------------------------------------------------------------- vocabulary


@pytest.mark.parametrize("reason", sorted(CLIENT_REJECTION_REASONS))
def test_every_rejection_reason_is_documented(reason: str) -> None:
    assert mentioned(reason), f"README never mentions the rejection `{reason}`"


@pytest.mark.parametrize("outcome", sorted(WEBHOOK_OUTCOMES))
def test_every_webhook_delivery_outcome_is_documented(outcome: str) -> None:
    """Scoped to the Webhooks section: words like ``failed`` appear all over the README."""
    assert mentioned(outcome, section("Webhooks")), f"the Webhooks section never mentions the outcome `{outcome}`"


@pytest.mark.parametrize("event_type", EVENT_TYPES)
def test_every_event_type_is_documented(event_type: str) -> None:
    assert mentioned(event_type), f"README never mentions the event type `{event_type}`"


@pytest.mark.parametrize(
    "name",
    [
        "das_client_rejections_total",
        "das_idempotent_replays_total",
        "das_sse_streams",
        "das_webhook_deliveries_total",
        "das_budget_exceeded_total",
    ],
)
def test_every_phase_4_metric_is_documented(name: str) -> None:
    assert mentioned(name), f"README never mentions the metric `{name}`"


@pytest.mark.parametrize(
    "token",
    [
        "Idempotency-Key",
        "Idempotent-Replayed",
        "Retry-After",
        "Last-Event-ID",
        "text/event-stream",
        "/v1/openapi.json",
        "DAS-Webhook-Id",
        "DAS-Webhook-Timestamp",
        "DAS-Webhook-Signature",
        "budget_exceeded",
        "budget_unenforceable",
        "budget_above_limit",
        "invalid_request",
        "secret_missing",
        "resync",
        "snapshot",
    ],
)
def test_the_protocol_vocabulary_is_documented(token: str) -> None:
    assert token in README, f"README never mentions `{token}`"


def test_the_upgrade_order_and_events_caveat_are_stated() -> None:
    """Phase 4 Req 11.3: Gateway and Manager upgrade in any order; events are complete only after every Manager."""
    upgrade = section("Upgrading to Phase 4")
    assert upgrade, "README has no `Upgrading to Phase 4` section"
    assert re.search(r"any order", upgrade), "the upgrade section does not say Gateway and Manager upgrade in any order"
    assert re.search(r"every Manager", upgrade), "the upgrade section does not say events need every Manager upgraded"


# --------------------------------------------------------------------------- examples


def test_the_fingerprint_recipe_is_stated_and_right() -> None:
    assert RECIPE in README, f"README does not give the fingerprint recipe {RECIPE!r}"
    expected = key_fingerprint(SAMPLE_KEY.encode())
    assert expected == hashlib.sha256(SAMPLE_KEY.encode()).hexdigest()[:8]
    assert expected in README, f"README does not show the fingerprint {expected} of the placeholder key"


def gateway_example_blocks() -> list[str]:
    return [b for b in blocks("yaml") if re.search(r"^(client_limits|events|webhook):", b, re.MULTILINE)]


def test_there_are_yaml_examples() -> None:
    assert gateway_example_blocks(), "README shows no yaml example of client_limits, events or webhook"


@pytest.mark.parametrize("example", gateway_example_blocks(), ids=lambda b: b.splitlines()[0])
def test_each_yaml_example_loads_through_the_config_loader(example: str, tmp_path: Path) -> None:
    sample = yaml.safe_load((ROOT / "deploy" / "config" / "gateway.yaml").read_text())
    merged = {**sample, **yaml.safe_load(example)}  # the example replaces whole top-level sections
    path = tmp_path / "gateway.yaml"
    path.write_text(yaml.safe_dump(merged))
    assert load_config(path, ENV).role == "gateway"


def test_each_python_example_parses_and_imports_only_real_names() -> None:
    examples = blocks("python")
    assert examples, "README shows no python example"
    exported = set(das_client.__all__)
    for body in examples:
        tree = ast.parse(body)  # a syntax error fails here
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "das_client":
                unknown = {alias.name for alias in node.names} - exported
                assert not unknown, f"README imports {sorted(unknown)} from das_client, which does not export them"
            if isinstance(node, ast.Import):
                assert all(alias.name.split(".")[0] != "das" for alias in node.names), "a client example imports das"


# --------------------------------------------------------------------------- links


def slug(heading: str) -> str:
    """A GitHub heading anchor: lower case, code spans and punctuation dropped, spaces to hyphens."""
    text = re.sub(r"[^\w\s-]", "", heading.replace("`", "").lower())
    return re.sub(r"\s", "-", text.strip())


def test_internal_links_resolve() -> None:
    anchors = {slug(m["title"]) for m in re.finditer(r"^#{1,6} (?P<title>.+?)\s*$", README, re.MULTILINE)}
    links = set(re.findall(r"\]\(#([\w-]+)\)", README))
    assert links, "README has no internal links to check"
    broken = sorted(links - anchors)
    assert not broken, f"README links to anchors with no heading: {broken}"
