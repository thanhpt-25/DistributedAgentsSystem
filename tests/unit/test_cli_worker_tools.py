"""A Worker's tools at startup (das-smarter-agents task 5.2; Phase 6 Req 5.1, 5.4): the registry is built from the
``tools`` section, its ``tool-<name>`` Capabilities are advertised besides the Worker's own, and a provider that
cannot call tools is refused with exit code 2 naming it."""

from __future__ import annotations

from typing import Any

import pytest

from das.clock import FakeClock
from das.cli import _worker_capabilities, _worker_tools
from das.config.schema import ToolsConfig
from das.errors import ConfigError
from das.llm.base import ModelParams
from das.llm.retry import LLMProvider
from tests.fakes.llm_adapter import FakeAdapter

PARAMS = ModelParams(model="big", temperature=0.0, max_output_tokens=64)


def tools(*enabled: str) -> ToolsConfig:
    return ToolsConfig.model_validate({"enabled": list(enabled)})


class ToolAdapter(FakeAdapter):
    """A fake adapter that, like the four real ones, can complete with tools."""

    async def complete_with_tools_once(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError


def with_tools() -> LLMProvider:
    return LLMProvider(ToolAdapter(), PARAMS)


def test_no_tools_enabled_gives_an_empty_registry_and_the_capabilities_unchanged() -> None:
    registry = _worker_tools(tools(), LLMProvider(FakeAdapter(), PARAMS), FakeClock())
    assert not registry
    assert _worker_capabilities(["research"], registry) == frozenset({"research"})


def test_enabled_tools_add_their_capabilities_to_the_workers_own() -> None:
    registry = _worker_tools(tools("calculator", "current_time"), with_tools(), FakeClock())
    assert _worker_capabilities(["research"], registry) == frozenset(
        {"research", "tool-calculator", "tool-current-time"}
    )


def test_a_provider_that_cannot_call_tools_stops_the_worker_with_exit_code_2_naming_it() -> None:
    llm = LLMProvider(FakeAdapter(), PARAMS)  # FakeAdapter has no complete_with_tools_once
    with pytest.raises(ConfigError) as info:
        _worker_tools(tools("calculator"), llm, FakeClock())
    assert info.value.exit_code == 2 and llm.provider in str(info.value)


def test_the_worker_may_not_advertise_more_than_fifty_capabilities_with_its_tools() -> None:
    registry = _worker_tools(tools("calculator", "current_time"), with_tools(), FakeClock())
    own = [f"cap-{i}" for i in range(49)]
    with pytest.raises(ConfigError, match="50"):
        _worker_capabilities(own, registry)
    assert len(_worker_capabilities(own[:48], registry)) == 50
