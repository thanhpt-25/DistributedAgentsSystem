"""The tool registry (das-smarter-agents tasks 5.2 and 5.4; Phase 6 Req 5.1, 5.4, 5.5, 5.10): which tools a Worker
offers, the ``tool-<name>`` Capability each one advertises, ``current_time`` from the injected Clock, and the refusal
to start with a provider that cannot call tools."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from das.clock import FakeClock
from das.config.schema import ToolsConfig
from das.errors import ConfigError
from das.tools.registry import ToolRegistry, build_registry, capability_for, refuse_unsupported_provider
from das.tools.types import ToolCall, ToolError


def registry(*enabled: str, **extra: object) -> ToolRegistry:
    settings: dict[str, object] = {"enabled": list(enabled), **extra}
    if "http_fetch" in enabled and "http_fetch" not in settings:
        settings["http_fetch"] = {"allowed_hosts": ["example.com"]}
    return build_registry(ToolsConfig.model_validate(settings), FakeClock())


# --------------------------------------------------------------------------- capabilities (Req 5.4)


def test_each_tool_has_a_hyphen_case_capability() -> None:
    assert capability_for("calculator") == "tool-calculator"
    assert capability_for("http_fetch") == "tool-http-fetch"
    assert capability_for("current_time") == "tool-current-time"


def test_a_registry_advertises_a_capability_for_each_enabled_tool_only() -> None:
    assert registry("calculator", "http_fetch").capabilities == frozenset({"tool-calculator", "tool-http-fetch"})
    assert registry().capabilities == frozenset()


# --------------------------------------------------------------------------- what a Job is offered (Req 5.5)


def test_a_job_is_offered_exactly_the_tools_it_requires() -> None:
    reg = registry("calculator", "current_time", "http_fetch")
    names = [s.name for s in reg.specs_for(["research", "tool-calculator", "tool-current-time"])]
    assert names == ["calculator", "current_time"]


def test_a_job_that_requires_no_tool_is_offered_none() -> None:
    assert registry("calculator").specs_for(["research"]) == ()
    assert registry("calculator").specs_for([]) == ()


def test_a_required_tool_that_is_not_enabled_is_not_offered() -> None:
    assert registry("calculator").specs_for(["tool-http-fetch"]) == ()


def test_the_specs_are_complete_json_schema_objects() -> None:
    for spec in registry("calculator", "http_fetch", "current_time").specs_for(
        ["tool-calculator", "tool-http-fetch", "tool-current-time"]
    ):
        assert spec.description
        assert spec.parameters["type"] == "object" and spec.parameters.get("additionalProperties") is False


# --------------------------------------------------------------------------- running a call


async def test_calculator_runs_through_the_registry() -> None:
    assert await registry("calculator").run(ToolCall("1", "calculator", {"expression": "2+3"})) == "5"


async def test_a_calculator_refusal_is_a_tool_error_the_model_can_read() -> None:
    with pytest.raises(ToolError, match="zero"):
        await registry("calculator").run(ToolCall("1", "calculator", {"expression": "1/0"}))


async def test_a_missing_argument_is_a_tool_error() -> None:
    with pytest.raises(ToolError, match="expression"):
        await registry("calculator").run(ToolCall("1", "calculator", {}))


async def test_an_unknown_or_disabled_tool_is_a_tool_error() -> None:
    with pytest.raises(ToolError, match="unknown tool"):
        await registry("calculator").run(ToolCall("1", "http_fetch", {"url": "https://example.com/"}))


async def test_current_time_is_utc_from_the_injected_clock() -> None:
    clock = FakeClock(start=1_700_000_000.0)
    reg = build_registry(ToolsConfig.model_validate({"enabled": ["current_time"]}), clock)
    assert await reg.run(ToolCall("1", "current_time", {})) == "2023-11-14T22:13:20Z"
    clock.advance(3600)
    assert await reg.run(ToolCall("2", "current_time", {})) == "2023-11-14T23:13:20Z"


async def test_current_time_drops_the_fraction_of_a_second() -> None:
    clock = FakeClock(start=1_700_000_000.9)
    reg = build_registry(ToolsConfig.model_validate({"enabled": ["current_time"]}), clock)
    expected = datetime.fromtimestamp(1_700_000_000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert await reg.run(ToolCall("1", "current_time", {})) == expected


async def test_http_fetch_is_wired_with_the_configured_hosts() -> None:
    with pytest.raises(ToolError, match="not allowed"):
        await registry("http_fetch").run(ToolCall("1", "http_fetch", {"url": "https://other.example.org/"}))


# --------------------------------------------------------------------------- the provider must support tools (Req 5.1)


class Provider:
    def __init__(self, supported: bool, name: str = "ollama") -> None:
        self.supports_tools = supported
        self.provider = name


def test_a_provider_without_tool_support_is_refused_naming_it_when_a_tool_is_enabled() -> None:
    with pytest.raises(ConfigError) as info:
        refuse_unsupported_provider(registry("calculator"), Provider(False, "ollama"))
    assert "ollama" in str(info.value) and info.value.exit_code == 2


def test_a_provider_with_tool_support_is_accepted() -> None:
    refuse_unsupported_provider(registry("calculator"), Provider(True))


def test_nothing_is_refused_when_no_tool_is_enabled() -> None:
    refuse_unsupported_provider(registry(), Provider(False))
