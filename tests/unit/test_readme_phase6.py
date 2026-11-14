"""The README documents the Phase 6 features, and what it says is true (das-smarter-agents task 7.2; Phase 6 Req 7.3,
7.4).

Like the Phase 4 and 5 README tests, this checks what can be checked mechanically: every new configuration field has a
row with its real default and its real range; every metric, event, error, Capability, tool and command is mentioned;
every YAML example loads through the real configuration loader; the plan example passes the real plan validator;
the dataset examples parse with the real dataset parser; the command example parses with the real CLI parser; the
code worker's security warning and link are there; and links resolve. Whether the prose is right is checked by reading
the code and by the end-to-end run, not here."""

from __future__ import annotations

import os
import re
import shlex
from pathlib import Path
from typing import Any

import pytest
import yaml
from annotated_types import Ge, Gt, Le, Lt

from das.cli import build_parser
from das.config import load_config
from das.config.schema import HttpFetchConfig, LimitsConfig, LLMConfig, SandboxConfig, ToolsConfig
from das.eval.plans import parse_case
from das.plan.validate import validate_plan_text

ROOT = Path(__file__).resolve().parents[2]
#: ``DAS_README_UNDER_TEST`` points these checks at another file (a draft, or a deliberately broken copy).
README = Path(os.environ.get("DAS_README_UNDER_TEST") or ROOT / "README.md").read_text(encoding="utf-8")
FENCE = re.compile(r"^```(?P<lang>[\w-]*)[^\n]*\n(?P<body>.*?)^```", re.MULTILINE | re.DOTALL)
TABLE_ROW = re.compile(r"^\| `(?P<path>[\w.]+)` \| (?P<default>[^|]*) \| (?P<range>[^|]*) \|", re.MULTILINE)
CODE_SPAN = re.compile(r"`([^`\n]+)`")
ENV = {"DAS__LLM__API_KEY": "sk-test-cafebabe0123456789"}
BASE_WORKER: dict[str, Any] = {
    "role": "worker", "host": "10.0.0.7",
    "transports": {"jobs": "rabbitmq", "control": "grpc"},
    "rabbitmq": {"url": "amqp://rabbit.internal:5672/"}, "redis": {"url": "redis://redis.internal:6379/0"},
    "grpc": {"bind": "0.0.0.0:50051", "advertise_host": "10.0.0.7", "advertise_port": 50051},
    "llm": {"provider": "anthropic", "model": "big-model"}, "worker": {"capabilities": ["research"]},
}


def phase6() -> str:
    match = re.search(r"^## Smarter agents \(Phase 6\)\n(.*?)(?=^## )", README, re.MULTILINE | re.DOTALL)
    return match.group(1) if match else ""


PHASE6 = phase6()


def section(heading: str) -> str:
    match = re.search(rf"^### {re.escape(heading)}[^\n]*\n(.*?)(?=^#{{1,3}} |\Z)", PHASE6, re.MULTILINE | re.DOTALL)
    return match.group(1) if match else ""


def blocks(language: str, text: str = PHASE6) -> list[str]:
    return [m["body"] for m in FENCE.finditer(text) if m["lang"] == language]


def mentioned(token: str, text: str = PHASE6) -> bool:
    """``token`` appears inside a code span of ``text`` (see the Phase 4 test for why spans are paired first)."""
    pattern = re.compile(rf"(?<![\w-]){re.escape(token)}(?![\w-])")
    return any(pattern.search(span) for span in CODE_SPAN.findall(text))


def shown_default(value: Any) -> str:
    if value is None or value == [] or value == {}:
        return "none"
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, float):
        return f"{value:g}"
    return str(value)


def plain(value: float) -> str:
    """``10485760``, not ``1.04858e+07``; ``0.1`` stays ``0.1``."""
    return str(int(value)) if float(value).is_integer() else f"{value:g}"


def numeric_range(field: Any) -> str | None:
    bounds = {type(m): m for m in field.metadata if isinstance(m, Ge | Gt | Le | Lt)}
    if Ge in bounds and Le in bounds:
        return f"{plain(bounds[Ge].ge)} to {plain(bounds[Le].le)}"
    return None


#: The new fields: (model, README prefix, field names, or ``None`` for every field of the model).
NEW_FIELDS: list[tuple[Any, str, list[str] | None]] = [
    (LLMConfig, "llm", ["capability_models", "fallback_model"]),
    (LimitsConfig, "limits", ["max_replans"]),
    (ToolsConfig, "tools", [n for n in ToolsConfig.model_fields if n != "http_fetch"]),
    (HttpFetchConfig, "tools.http_fetch", None),
    (SandboxConfig, "sandbox", None),
]
#: What a non-numeric range cell must name.
RANGE_WORDS = {
    "llm.capability_models": ["Capability", "model"], "llm.fallback_model": ["model"],
    "tools.enabled": ["calculator", "http_fetch", "current_time"], "tools.http_fetch.allowed_hosts": ["host"],
    "tools.http_fetch.allow_http": ["true", "false"], "sandbox.enabled": ["true", "false"],
    "sandbox.container_cli": ["docker", "podman"], "sandbox.image": ["image"], "sandbox.oci_runtime": ["name"],
}


def all_paths() -> list[tuple[str, Any, str]]:
    out = []
    for model, prefix, names in NEW_FIELDS:
        for name in names if names is not None else list(model.model_fields):
            out.append((f"{prefix}.{name}", model.model_fields[name], name))
    return out


# --------------------------------------------------------------------------- the section exists


def test_the_readme_has_a_phase_6_section_with_a_subsection_per_feature() -> None:
    assert PHASE6, "README has no `## Smarter agents (Phase 6)` section"
    for heading in ("Structured output", "Models by Capability and the fallback", "Replanning", "Plan evaluation",
                    "Tools", "Code execution", "Phase 6 settings", "Upgrading to Phase 6"):
        assert section(heading).strip(), f"README has no `### {heading}` subsection"


# --------------------------------------------------------------------------- settings (Req 7.4)


@pytest.mark.parametrize(("path", "field", "name"), all_paths(), ids=lambda v: v if isinstance(v, str) else "")
def test_every_new_field_has_a_row_with_its_real_default_and_range(path: str, field: Any, name: str) -> None:
    rows = {m["path"]: m for m in TABLE_ROW.finditer(section("Phase 6 settings"))}
    assert path in rows, f"the Phase 6 settings table has no row for `{path}`"
    row = rows[path]
    assert row["default"].strip() == shown_default(field.get_default(call_default_factory=True)), path
    shown_range = row["range"].strip()
    expected = numeric_range(field)
    if expected is not None:
        assert shown_range == expected, f"`{path}`: the table says {shown_range!r}, the schema says {expected!r}"
    else:
        assert shown_range, f"`{path}` has no range"
        for word in RANGE_WORDS.get(path, []):
            assert word in shown_range, f"`{path}`: the range should name {word!r}"


def test_the_table_has_no_row_for_a_field_that_does_not_exist() -> None:
    documented = {m["path"] for m in TABLE_ROW.finditer(section("Phase 6 settings"))}
    assert documented == {path for path, _, _ in all_paths()}


# --------------------------------------------------------------------------- what must be mentioned


@pytest.mark.parametrize(
    "token",
    [
        "das_llm_fallbacks_total", "das_tool_calls_total", "das_sandbox_runs_total", "request.replanned", "replans",
        "output_schema", "output_schema_violation", "tool_rounds_exceeded", "x-llm-model", "tool-calculator",
        "tool-http-fetch", "tool-current-time", "code-execution", "calculator", "http_fetch", "current_time",
        "run_python", "das eval-plans", "--dataset", "--min-pass",
    ],
)
def test_every_metric_event_error_capability_tool_and_command_is_mentioned(token: str) -> None:
    assert mentioned(token), f"the Phase 6 section does not mention `{token}`"


def test_the_replanning_section_says_it_is_off_by_default_and_a_budget_stop_is_never_replanned() -> None:
    text = section("Replanning").lower()
    assert "max_replans" in text and "off by default" in text and "budget" in text


def test_the_fallback_section_says_an_authentication_error_does_not_trigger_it() -> None:
    text = section("Models by Capability and the fallback").lower()
    assert "401" in text and "403" in text and "not" in text


# --------------------------------------------------------------------------- the examples run


def merged(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in extra.items():
        out[key] = merged(out[key], value) if isinstance(value, dict) and isinstance(out.get(key), dict) else value
    return out


def yaml_examples() -> list[str]:
    return blocks("yaml")


@pytest.mark.parametrize("example", yaml_examples(), ids=lambda e: e.strip().splitlines()[0][:40])
def test_every_yaml_example_loads_through_the_real_configuration_loader(example: str, tmp_path: Path) -> None:
    cfg = merged(BASE_WORKER, yaml.safe_load(example))
    path = tmp_path / "worker.yaml"
    path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    load_config(path, ENV)


def test_the_yaml_examples_between_them_use_every_new_section() -> None:
    flat = " ".join(yaml.safe_dump(yaml.safe_load(e)) for e in yaml_examples())
    for key in ("capability_models", "fallback_model", "max_replans", "tools:", "max_rounds", "allowed_hosts",
                "sandbox:", "container_cli", "oci_runtime"):
        assert key in flat, f"no YAML example shows `{key}`"


def test_the_structured_plan_example_passes_the_real_plan_validator() -> None:
    examples = [e for e in blocks("json", section("Structured output")) if '"jobs"' in e]
    assert examples, "the Structured output subsection has no JSON plan example"
    for example in examples:
        result = validate_plan_text(example, {"research", "summarize"}, 10)
        assert result.ok, result.issues
        assert result.plan is not None and any(job.output_schema for job in result.plan.jobs)


def test_every_dataset_example_line_parses_with_the_real_dataset_parser() -> None:
    lines = [ln for body in blocks("jsonl", section("Plan evaluation")) for ln in body.splitlines() if ln.strip()]
    assert lines, "the Plan evaluation subsection has no dataset example"
    for number, line in enumerate(lines, start=1):
        parse_case(line, number)


def test_the_eval_plans_command_example_parses_with_the_real_cli_parser() -> None:
    commands = [ln.strip() for body in blocks("bash", section("Plan evaluation")) for ln in body.splitlines()
                if ln.strip().startswith("das eval-plans")]
    assert commands, "the Plan evaluation subsection has no `das eval-plans` example"
    for command in commands:
        args = build_parser().parse_args(shlex.split(command)[1:])
        assert args.command == "eval-plans" and args.dataset


def test_the_plan_evaluation_section_states_the_exit_codes() -> None:
    text = section("Plan evaluation")
    for code in ("0", "1", "2"):
        assert re.search(rf"\b{code}\b", text), f"exit code {code} is not stated"


# --------------------------------------------------------------------------- the code worker (Req 6.8, 7.3)


def test_the_code_execution_section_carries_the_security_warning_and_links_the_document() -> None:
    text = section("Code execution")
    lowered = text.lower()
    assert "warning" in lowered and "off by default" in lowered and "dedicated host" in lowered
    assert "docs/security/code-execution.md" in text
    assert (ROOT / "docs" / "security" / "code-execution.md").is_file()


def test_the_code_execution_section_never_suggests_mounting_the_runtime_socket() -> None:
    text = section("Code execution").lower()
    assert "never mount" in text
    assert "docker.sock:" not in text


def test_the_tools_section_lists_what_http_fetch_refuses() -> None:
    text = section("Tools").lower()
    for word in ("get", "https", "allowed_hosts", "redirect", "private", "loopback", "link-local"):
        assert word in text, word


# --------------------------------------------------------------------------- upgrading (Req 7.1)


def test_the_upgrade_section_says_nothing_changes_unless_configured_and_names_the_one_new_field() -> None:
    text = section("Upgrading to Phase 6").lower()
    assert "nothing changes" in text and "replans" in text and "additive" in text


# --------------------------------------------------------------------------- links


def test_every_link_in_the_phase_6_section_resolves() -> None:
    for target in re.findall(r"\]\(([^)#\s]+)(?:#[^)]*)?\)", PHASE6):
        if "://" not in target:
            assert (ROOT / target).exists(), f"broken link: {target}"
