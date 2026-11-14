"""The README documents the Phase 7 features, and what it says is true (das-scale-resilience task 13.2; Phase 7
Req 5.4, 5.5).

Like the Phase 4 to 6 README tests, this checks what can be checked mechanically: every new configuration field has a
row with its real default and its real range (Req 5.5); every metric, secret, command, file and profile is mentioned;
every YAML example loads through the real configuration loader; every command example parses with the real CLI parser;
the built-in profiles and the command defaults the README states are the code's; the published reference numbers are
the ones in ``docs/capacity/``; the "Not in Phase 7" list is there; and links resolve. Whether the prose is right is
checked by reading the code and by the end-to-end runs, not here."""

from __future__ import annotations

import json
import re
import shlex
from pathlib import Path
from typing import Any

import pytest
import yaml
from annotated_types import Ge, Gt, Le, Lt
from pydantic import ValidationError

from das.cli import build_parser
from das.config import load_config
from das.config.schema import (
    KAFKA_SASL_MECHANISMS, KAFKA_SECURITY_PROTOCOLS, HttpBrokerConfig, HttpConfig, KafkaConfig, RedisConfig,
)
from das.loadtest.command import DEFAULT_MAX_REGRESSION
from das.loadtest.profile import BUILTIN
from das.loadtest.report import DEFAULT_MAX_ERROR_RATE, DEFAULT_MAX_P99_MS
from das.loadtest.runner import DEFAULT_POLL_INTERVAL, DEFAULT_REQUEST_TIMEOUT
from das.observability.metrics import CATALOG
from tests.unit.test_readme_phase6 import BASE_WORKER, CODE_SPAN, ENV, FENCE, README, ROOT, TABLE_ROW, merged

BASELINE = ROOT / "tests" / "fixtures" / "phase3_baseline" / "contract.json"
CAPACITY = ROOT / "docs" / "capacity"
BASE_GATEWAY: dict[str, Any] = {
    "role": "gateway", "host": "10.0.0.8", "redis": {"url": "redis://redis.internal:6379/0"},
    "transports": {"jobs": "rabbitmq", "control": "grpc"},
}


def phase7() -> str:
    match = re.search(r"^## Scale and resilience \(Phase 7\)\n(.*?)(?=^## )", README, re.MULTILINE | re.DOTALL)
    return match.group(1) if match else ""


PHASE7 = phase7()


def section(heading: str) -> str:
    match = re.search(rf"^### {re.escape(heading)}[^\n]*\n(.*?)(?=^#{{1,3}} |\Z)", PHASE7, re.MULTILINE | re.DOTALL)
    return match.group(1) if match else ""


def blocks(language: str, text: str = PHASE7) -> list[str]:
    return [m["body"] for m in FENCE.finditer(text) if m["lang"] == language]


def mentioned(token: str, text: str = PHASE7) -> bool:
    pattern = re.compile(rf"(?<![\w-]){re.escape(token)}(?![\w-])")
    return any(pattern.search(span) for span in CODE_SPAN.findall(text))


def shown_default(field: Any) -> str:
    if field.is_required():
        return "required"
    value = field.get_default(call_default_factory=True)
    if value is None or value == [] or value == {}:
        return "none"
    if isinstance(value, bool):
        return str(value).lower()
    return f"{value:g}" if isinstance(value, float) else str(value)


def plain(value: float) -> str:
    return str(int(value)) if float(value).is_integer() else f"{value:g}"


def numeric_range(field: Any) -> str | None:
    bounds = {type(m): m for m in field.metadata if isinstance(m, Ge | Gt | Le | Lt)}
    if Ge in bounds and Le in bounds:
        return f"{plain(bounds[Ge].ge)} to {plain(bounds[Le].le)}"
    return None


NEW_FIELDS: list[tuple[Any, str, list[str] | None]] = [
    (RedisConfig, "redis", [n for n in RedisConfig.model_fields if n != "url"]),
    (HttpConfig, "http", None),
    (HttpBrokerConfig, "http_broker", None),
    (KafkaConfig, "kafka", None),
]
#: What a non-numeric range cell must name.
RANGE_WORDS = {
    "redis.mode": ["standalone", "sentinel"], "redis.sentinels": ["1 to 10", "host:port"], "redis.master_name": ["64"],
    "redis.password": ["secret"], "redis.sentinel_password": ["secret"], "redis.sentinel_tls": ["true", "false"],
    "http.url": ["http"], "http.token": ["secret", "16 to 256"], "http_broker.token": ["secret", "16 to 256"],
    "http_broker.enabled": ["true", "false", "Gateway"], "kafka.bootstrap_servers": ["1 to 10", "host:port"],
    "kafka.security_protocol": list(KAFKA_SECURITY_PROTOCOLS), "kafka.sasl_mechanism": list(KAFKA_SASL_MECHANISMS),
    "kafka.sasl_username": ["SASL"], "kafka.sasl_password": ["secret"], "kafka.ssl_cafile": ["file"],
    "kafka.ssl_certfile": ["ssl_keyfile"], "kafka.ssl_keyfile": ["ssl_certfile"], "kafka.ssl_key_password": ["secret"],
    "kafka.auto_create_topics": ["true", "false"],
}


def all_paths() -> list[tuple[str, Any]]:
    return [
        (f"{prefix}.{name}", model.model_fields[name])
        for model, prefix, names in NEW_FIELDS
        for name in (names if names is not None else list(model.model_fields))
    ]


# --------------------------------------------------------------------------- the section exists


def test_the_readme_has_a_phase_7_section_with_a_subsection_per_feature() -> None:
    assert PHASE7, "README has no `## Scale and resilience (Phase 7)` section"
    for heading in ("Redis Sentinel", "HTTP transport", "Kafka transport", "Load testing", "Phase 7 settings",
                    "Upgrading to Phase 7", "Not in Phase 7"):
        assert section(heading).strip(), f"README has no `### {heading}` subsection"


# --------------------------------------------------------------------------- settings (Req 5.5)


@pytest.mark.parametrize(("path", "field"), all_paths(), ids=lambda v: v if isinstance(v, str) else "")
def test_every_new_field_has_a_row_with_its_real_default_and_range(path: str, field: Any) -> None:
    rows = {m["path"]: m for m in TABLE_ROW.finditer(section("Phase 7 settings"))}
    assert path in rows, f"the Phase 7 settings table has no row for `{path}`"
    row = rows[path]
    assert row["default"].strip() == shown_default(field), f"`{path}`: the default"
    shown_range, expected = row["range"].strip(), numeric_range(field)
    if expected is not None:
        assert shown_range == expected, f"`{path}`: the table says {shown_range!r}, the schema says {expected!r}"
    else:
        assert shown_range, f"`{path}` has no range"
        for word in RANGE_WORDS.get(path, []):
            assert word in shown_range, f"`{path}`: the range should name {word!r}"


def test_the_table_has_no_row_for_a_field_that_does_not_exist() -> None:
    documented = {m["path"] for m in TABLE_ROW.finditer(section("Phase 7 settings"))}
    assert documented == {path for path, _ in all_paths()}


@pytest.mark.parametrize("model", [HttpConfig, HttpBrokerConfig])
def test_the_token_length_the_table_states_is_the_one_the_schema_enforces(model: Any) -> None:
    base: dict[str, Any] = {"url": "http://x"} if model is HttpConfig else {}
    model(**base, token="a" * 16)
    model(**base, token="a" * 256)
    for bad in ("a" * 15, "a" * 257):
        with pytest.raises(ValidationError):
            model(**base, token=bad)


@pytest.mark.parametrize(("model", "field", "extra"), [
    (RedisConfig, "sentinels", {"mode": "sentinel", "master_name": "m"}), (KafkaConfig, "bootstrap_servers", {}),
])
def test_the_one_to_ten_addresses_the_table_states_are_what_the_schema_accepts(
    model: Any, field: str, extra: dict[str, Any]
) -> None:
    model(**extra, **{field: [f"h{n}:1" for n in range(1, 11)]})
    with pytest.raises(ValidationError):
        model(**extra, **{field: [f"h{n}:1" for n in range(1, 12)]})
    with pytest.raises(ValidationError):
        model(**extra, **{field: []})


def test_redis_url_is_documented_as_still_required_for_standalone_only() -> None:
    text = section("Phase 7 settings")
    assert "`redis.url` stays what it was" in text and "optional in the schema only because" in text


# --------------------------------------------------------------------------- what must be mentioned


@pytest.mark.parametrize("token", [
    "das_redis_failovers_total", "das_redis_retries_total", "READONLY", "LEASE_LOST", "REQUEST_TERMINAL",
    "das/state/redisclient.py", "docker-compose.sentinel.yml", "docker-compose.http-transport.yml",
    "DAS_HTTP_BROKER_TOKEN", "DAS__REDIS__PASSWORD", "DAS__REDIS__SENTINEL_PASSWORD", "DAS__HTTP__TOKEN",
    "DAS__HTTP_BROKER__TOKEN", "DAS__KAFKA__SASL_PASSWORD", "DAS__KAFKA__SSL_KEY_PASSWORD", "<topic>.retry",
    "<topic>.high", "das.dlq", "das loadtest-compare", "--max-regression", "--transport", "--setting",
    "DAS_LOADTEST_KEY", "STUB_LATENCY_MS", "STUB_FAILURE_RATE", "--max-error-rate", "--max-p99-ms", "--poll-interval",
    "--request-timeout", "kafka.retry_delay", "http.poll_wait", "visibility_timeout",
])
def test_every_metric_secret_file_flag_and_setting_is_mentioned(token: str) -> None:
    assert mentioned(token), f"the Phase 7 section does not mention `{token}`"


def test_the_compose_commands_the_section_gives_name_the_sentinel_profile() -> None:
    assert "--profile sentinel" in PHASE7 and "deploy/docker-compose.sentinel.yml" in PHASE7
    assert "acks=all" in PHASE7


def test_the_sentinel_section_says_why_cluster_is_refused_and_that_nothing_needs_migrating() -> None:
    text = section("Redis Sentinel")
    assert "mode: cluster" in text and "different slots" in text
    assert "key layout did not change" in text and "nothing to migrate" in text
    for delay in ("0.2", "0.5", "1 second"):
        assert delay in text


def test_the_http_section_says_tls_belongs_in_front_and_rabbitmq_is_not_needed() -> None:
    text = section("HTTP transport").lower()
    assert "tls" in text and "needs no `rabbitmq` section" in text and "never in yaml" in text


def test_the_kafka_section_says_what_is_refused_and_when_an_offset_is_committed() -> None:
    text = section("Kafka transport").lower()
    assert "refused rather than ignored" in text and "only after `acknowledge`" in text
    assert "lowest message still in flight" in text and "rebalance" in text


# --------------------------------------------------------------------------- the examples run


def example_config(example: str) -> tuple[dict[str, Any], dict[str, str]]:
    """The example on top of the base configuration of its role, and the environment its secrets need."""
    data = yaml.safe_load(example)
    base = BASE_GATEWAY if "http_broker" in data else BASE_WORKER
    cfg = merged(base, data)
    env = dict(ENV) if base is BASE_WORKER else {"DAS__API_KEYS": "readme-api-key-0123"}
    if "redis" in data:
        cfg["redis"] = data["redis"]  # a Sentinel configuration has no url: the base one must not stay
    if "http_broker" in data:
        env["DAS__HTTP_BROKER__TOKEN"] = "readme-broker-token-0123456789"
    if "http" in data:
        env["DAS__HTTP__TOKEN"] = "readme-agent-token-0123456789"
    if "kafka" in data:
        env["DAS__KAFKA__SASL_PASSWORD"] = "readme-sasl-password-0123456789"
    return cfg, env


@pytest.mark.parametrize("example", blocks("yaml"), ids=lambda e: e.strip().splitlines()[0][:40])
def test_every_yaml_example_loads_through_the_real_configuration_loader(example: str, tmp_path: Path) -> None:
    cfg, env = example_config(example)
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    load_config(path, env)


def test_the_yaml_examples_between_them_show_every_new_section() -> None:
    flat = " ".join(yaml.safe_dump(yaml.safe_load(e)) for e in blocks("yaml"))
    for key in ("mode: sentinel", "sentinels:", "master_name", "poll_wait", "http_broker:", "visibility_timeout",
                "bootstrap_servers", "security_protocol", "sasl_mechanism", "replication_factor", "partitions"):
        assert key in flat, f"no YAML example shows `{key}`"


def test_the_sentinel_example_really_is_a_sentinel_configuration_and_standalone_is_still_the_default(
    tmp_path: Path,
) -> None:
    (example,) = blocks("yaml", section("Redis Sentinel"))
    cfg, env = example_config(example)
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    config = load_config(path, env)
    assert config.redis is not None and config.redis.mode == "sentinel" and config.redis.url is None
    assert RedisConfig.model_fields["mode"].default == "standalone"


# --------------------------------------------------------------------------- the commands run


def bash_commands(prefix: str) -> list[str]:
    return [ln.strip() for body in blocks("bash", section("Load testing")) for ln in body.splitlines()
            if ln.strip().startswith(prefix)]


def test_the_loadtest_command_example_parses_with_the_real_cli_parser() -> None:
    (command,) = bash_commands("das loadtest ")
    args = build_parser().parse_args(shlex.split(command)[1:])
    assert args.command == "loadtest" and args.profile == "ramp" and args.transport == "rabbitmq"
    assert args.setting == ["managers=2", "workers=2"]


def test_the_compare_command_example_parses_with_the_real_cli_parser() -> None:
    (command,) = bash_commands("das loadtest-compare")
    args = build_parser().parse_args(shlex.split(command)[1:])
    assert args.command == "loadtest-compare" and args.max_regression == 0.1


def test_the_defaults_the_load_test_section_states_are_the_codes() -> None:
    text = section("Load testing")
    args = build_parser().parse_args(["loadtest", "--gateway", "http://g", "--out", "r", "--transport", "x"])
    assert (args.poll_interval, args.request_timeout) == (DEFAULT_POLL_INTERVAL, DEFAULT_REQUEST_TIMEOUT)
    assert (DEFAULT_POLL_INTERVAL, DEFAULT_REQUEST_TIMEOUT) == (0.5, 120.0)
    assert (args.max_error_rate, args.max_p99_ms) == (DEFAULT_MAX_ERROR_RATE, DEFAULT_MAX_P99_MS)
    assert (DEFAULT_MAX_ERROR_RATE, DEFAULT_MAX_P99_MS, DEFAULT_MAX_REGRESSION) == (0.01, 10_000.0, 0.1)
    for fragment in ("`--poll-interval` 0.5 seconds", "`--request-timeout` 120", "`--max-error-rate` (0.01)",
                     "`--max-p99-ms` (10000)", "default 0.1"):
        assert fragment in text, f"the load test section does not say {fragment!r}"


def test_the_built_in_profiles_the_section_describes_are_the_codes() -> None:
    text = section("Load testing")
    smoke, steady, ramp = BUILTIN["smoke"].steps, BUILTIN["steady"].steps, BUILTIN["ramp"].steps
    assert f"`smoke` ({smoke[0].rate:g} per second for {smoke[0].seconds:g} seconds)" in text
    assert f"`steady` ({steady[0].rate:g} for {steady[0].seconds:g})" in text
    rates = [f"{s.rate:g}" for s in ramp]
    assert f"`ramp` ({', '.join(rates[:-1])} and {rates[-1]} per second, {ramp[0].seconds:g} seconds each)" in text


# --------------------------------------------------------------------------- the published numbers


def report(name: str) -> dict[str, Any]:
    return json.loads((CAPACITY / f"reference-{name}.json").read_text(encoding="utf-8"))


def test_the_reference_numbers_in_the_readme_are_the_ones_in_the_capacity_reports() -> None:
    text = section("Load testing")
    rabbit, http = report("rabbitmq"), report("http")
    assert f"all {rabbit['results']['requests']} Requests completed" in text
    assert rabbit["results"]["completed"] == rabbit["results"]["requests"]
    assert f"**{rabbit['saturation_rps']:g} Requests per second**" in text
    assert f"**{http['saturation_rps']:g}**" in text
    assert f"{rabbit['machine']['cpu_count']}-CPU" in text
    assert f"{rabbit['machine']['memory_bytes'] // 2**30} GiB" in text
    assert rabbit["settings"]["stub_latency_ms"] == http["settings"]["stub_latency_ms"] == "50"
    assert "a stub LLM answering in 50 ms" in text and "not a guarantee" in text


def test_the_capacity_document_carries_the_same_numbers_and_the_caveats() -> None:
    doc = (CAPACITY / "README.md").read_text(encoding="utf-8")
    rabbit, http = report("rabbitmq"), report("http")
    assert f"**Saturation point: {rabbit['saturation_rps']:g} Requests per second.**" in doc
    assert f"**Saturation point: {http['saturation_rps']:g} Requests per second.**" in doc
    for rate in rabbit["results"]["throughput_rps"], http["results"]["throughput_rps"]:
        assert f"{rate:.1f}" in doc
    assert "not a guarantee" in doc and "has not been investigated" in doc
    for step in rabbit["steps"] + http["steps"]:
        assert f"{step['rate']:g} rps for {step['seconds']:g} s" in doc


@pytest.mark.parametrize("name", ["rabbitmq", "http"])
def test_each_capacity_report_has_the_context_the_spec_requires(name: str) -> None:
    data = report(name)
    assert data["schema"] == 1 and data["transport"] == name and data["das_version"]
    assert {"cpu_count", "memory_bytes", "os"} <= set(data["machine"])
    assert {"managers", "workers", "stub_latency_ms"} <= set(data["settings"])


# --------------------------------------------------------------------------- not in Phase 7, upgrading


def test_the_not_in_phase_7_list_names_cluster_multi_region_and_exactly_once() -> None:
    text = section("Not in Phase 7").lower()
    for item in ("redis cluster", "multi-region", "exactly-once"):
        assert item in text


def test_the_upgrade_section_says_nothing_changes_unless_configured_and_names_the_two_metrics() -> None:
    text = section("Upgrading to Phase 7").lower()
    assert "nothing changes" in text and "two new metrics" in text and "additions" in text
    assert "aiokafka" in text and "import contract" in text


# --------------------------------------------------------------------------- the README agrees with the rest


def test_every_environment_variable_the_section_names_is_one_the_loader_maps() -> None:
    mapped = set(json.loads(BASELINE.read_text(encoding="utf-8"))["env_vars"])
    named = set(re.findall(r"DAS__[A-Z0-9_]+", PHASE7))
    assert named, "the Phase 7 section names no DAS__ variable"
    assert named <= mapped, f"not a variable the loader reads: {sorted(named - mapped)}"


def test_the_two_metrics_are_in_the_catalog() -> None:
    assert {"das_redis_failovers_total", "das_redis_retries_total"} <= {spec.name for spec in CATALOG}


def test_the_compose_files_and_the_profile_the_section_names_exist() -> None:
    for name in ("docker-compose.sentinel.yml", "docker-compose.http-transport.yml"):
        assert (ROOT / "deploy" / name).is_file(), name
    assert "profiles: [sentinel]" in (ROOT / "deploy" / "docker-compose.sentinel.yml").read_text()
    assert "DAS_HTTP_BROKER_TOKEN" in (ROOT / "deploy" / ".env.example").read_text()


def test_the_chart_takes_the_sentinel_values_the_section_names() -> None:
    values = yaml.safe_load((ROOT / "deploy" / "helm" / "das" / "values.yaml").read_text())
    assert {"mode", "sentinels", "masterName"} <= set(values["redis"])
    assert "redis.mode: sentinel" in PHASE7 and "redis.sentinels" in PHASE7 and "redis.masterName" in PHASE7


def test_every_link_in_the_phase_7_section_resolves() -> None:
    for target in re.findall(r"\]\(([^)#\s]+)(?:#[^)]*)?\)", PHASE7):
        if "://" not in target:
            assert (ROOT / target).exists(), f"broken link: {target}"
    headings = {
        re.sub(r"[^a-z0-9 -]", "", h.lower()).replace(" ", "-") for h in re.findall(r"^#+ (.*)$", README, re.MULTILINE)
    }
    for anchor in re.findall(r"\]\(#([^)\s]+)\)", PHASE7):
        assert anchor in headings, f"broken anchor: #{anchor}"
