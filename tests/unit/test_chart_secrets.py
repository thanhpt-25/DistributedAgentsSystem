"""No secret value can reach a rendered manifest (das-multi-tenant task 10.4; Phase 5 Req 10.2).

The chart never receives a secret: every one comes from an existing Secret through ``secretKeyRef``. What it can
receive is a value typed into the wrong place, and the requirement covers "all values". So a value that would put a
secret into a ConfigMap or a Deployment is refused when the chart renders: a secret field of the agent configuration
under ``config``, and a URL with credentials anywhere. A refusal names the path and never repeats the value."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from das.config.schema import SECRET_FIELDS
from tests.unit.test_chart_skeleton import ALL, CHART, MIN, helm, needs_helm

pytestmark = needs_helm

SENTINEL = "SENTINEL-pw-0123456789"


def nested(path: str, value: Any) -> dict[str, Any]:
    out: Any = value
    for part in reversed(path.split(".")):
        out = {part: out}
    return out  # type: ignore[no-any-return]


def refused(extra: dict[str, Any], tmp_path: Path, values: Path = MIN) -> str:
    """The text of a refused render; fails the test when the chart accepted the values."""
    done = helm("template", "release", values=[values], extra_values=extra, tmp=tmp_path)
    assert done.returncode != 0, "the chart rendered values that carry a secret"
    return done.stdout + done.stderr


# --------------------------------------------------------------------------- secret fields of the agent config


@pytest.mark.parametrize("field", SECRET_FIELDS)
def test_a_secret_field_of_the_agent_config_is_refused_under_config(field: str, tmp_path: Path) -> None:
    value: Any = {"partner": SENTINEL} if field == "webhook_secrets" else SENTINEL
    message = refused({"config": {"common": nested(field, value)}}, tmp_path)
    assert field in message, message
    assert SENTINEL not in message


@pytest.mark.parametrize("section", ["common", "gateway", "manager", "worker"])
def test_every_config_section_is_checked(section: str, tmp_path: Path) -> None:
    message = refused({"config": {section: {"api_keys": [SENTINEL]}}}, tmp_path)
    assert "api_keys" in message and SENTINEL not in message


def test_an_empty_secret_field_is_not_a_secret(tmp_path: Path) -> None:
    done = helm(
        "template", "release", values=[MIN], extra_values={"config": {"common": {"api_keys": []}}}, tmp=tmp_path
    )
    assert done.returncode == 0, done.stderr


# --------------------------------------------------------------------------- URLs with credentials


URL_VALUES = {
    "redis.url": {"redis": {"url": f"redis://user:{SENTINEL}@redis:6379/0"}},
    "rabbitmq.url": {"rabbitmq": {"url": f"amqp://user:{SENTINEL}@rabbitmq:5672/"}},
    "rabbitmq.managementUrl": {"rabbitmq": {"managementUrl": f"http://user:{SENTINEL}@rabbitmq:15672"}},
    "lifecycle.preStopDrain.gatewayUrl": {
        "lifecycle": {"preStopDrain": {"gatewayUrl": f"http://k:{SENTINEL}@gw:8080"}}
    },
    "a URL inside config": {"config": {"worker": {"llm": {"base_url": f"http://u:{SENTINEL}@llm:11434"}}}},
}


@pytest.mark.parametrize("extra", URL_VALUES.values(), ids=URL_VALUES.keys())
def test_a_url_with_credentials_is_refused_wherever_it_is_typed(extra: dict[str, Any], tmp_path: Path) -> None:
    assert SENTINEL not in refused(extra, tmp_path)


def test_urls_without_credentials_are_accepted(tmp_path: Path) -> None:
    extra = {
        "redis": {"url": "redis://redis:6379/0"},
        "rabbitmq": {"url": "amqp://rabbitmq:5672/", "managementUrl": "http://rabbitmq:15672"},
        "config": {"worker": {"llm": {"base_url": "http://llm:11434"}}},
    }
    assert helm("template", "release", values=[MIN], extra_values=extra, tmp=tmp_path).returncode == 0


# --------------------------------------------------------------------------- what is rendered


@pytest.mark.parametrize("values", [MIN, ALL], ids=["min", "all"])
def test_the_chart_renders_no_secret_object(values: Path) -> None:
    out = helm("template", "release", values=[values]).stdout
    assert "Secret" not in {d["kind"] for d in yaml.safe_load_all(out) if d}


# --------------------------------------------------------------------------- the chart's list is the agent's list


def test_the_charts_secret_fields_are_the_agent_configs() -> None:
    helpers = (CHART / "templates" / "_helpers.tpl").read_text()
    block = re.search(r'define "das\.secretFields" -?}}(?P<body>.*?){{-? end', helpers, re.DOTALL)
    assert block, "_helpers.tpl does not define das.secretFields"
    assert block["body"].split() == list(SECRET_FIELDS)
