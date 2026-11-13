"""The tools a Worker enables, the Capability each advertises, and how a call is run (Phase 6 Req 5.1, 5.4, 5.5, 5.10).

``build_registry`` turns the ``tools`` configuration into a :class:`ToolRegistry`. The registry offers a Job exactly the
tools whose ``tool-<name>`` Capability the Job requires, and runs a call, raising :class:`ToolError` for anything the
model should read as the tool's answer.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Final, Protocol

from das.clock import Clock
from das.errors import ConfigError, FieldError
from das.tools.calculator import calculate
from das.tools.http_fetch import HttpFetch
from das.tools.types import ToolCall, ToolError, ToolSpec

if TYPE_CHECKING:
    from das.config.schema import ToolsConfig

CAPABILITY_PREFIX: Final = "tool-"

_CALCULATOR: Final = ToolSpec(
    "calculator",
    "Evaluate an arithmetic expression exactly: numbers, + - * / // % **, parentheses, and sqrt, abs, min, max, round, "
    "floor, ceil, log, log10, exp, sin, cos, tan, pi, e.",
    {
        "type": "object",
        "properties": {"expression": {"type": "string", "description": "For example (2 + 3) * 4"}},
        "required": ["expression"],
        "additionalProperties": False,
    },
)
_HTTP_FETCH: Final = ToolSpec(
    "http_fetch",
    "Fetch a web page with GET from one of the allowed hosts. Returns the HTTP status and the start of the body.",
    {
        "type": "object",
        "properties": {"url": {"type": "string", "description": "An https URL on an allowed host"}},
        "required": ["url"],
        "additionalProperties": False,
    },
)
_CURRENT_TIME: Final = ToolSpec(
    "current_time",
    "The current date and time in UTC, as YYYY-MM-DDTHH:MM:SSZ.",
    {"type": "object", "properties": {}, "additionalProperties": False},
)

Runner = Callable[[dict[str, Any]], Awaitable[str]]


def capability_for(tool: str) -> str:
    """``tool-<name>`` with the tool's name in hyphen case (Req 5.4)."""
    return CAPABILITY_PREFIX + tool.replace("_", "-")


@dataclass(frozen=True, slots=True)
class Tool:
    spec: ToolSpec
    run: Runner


def _argument(arguments: dict[str, Any], name: str) -> Any:
    if name not in arguments:
        raise ToolError(f"missing argument: {name}")
    return arguments[name]


class ToolRegistry:
    """The enabled tools, by name, in the order they were enabled."""

    def __init__(self, tools: Iterable[Tool] = ()) -> None:
        self._tools = {t.spec.name: t for t in tools}
        self._by_capability = {capability_for(name): name for name in self._tools}

    @property
    def names(self) -> tuple[str, ...]:
        return tuple(self._tools)

    @property
    def capabilities(self) -> frozenset[str]:
        """The Capabilities a Worker with these tools advertises besides its own (Req 5.4)."""
        return frozenset(self._by_capability)

    def __bool__(self) -> bool:
        return bool(self._tools)

    def specs_for(self, required_capabilities: Iterable[str]) -> tuple[ToolSpec, ...]:
        """Exactly the enabled tools the Job requires, in registry order; none when it requires none (Req 5.5)."""
        wanted = {self._by_capability[c] for c in required_capabilities if c in self._by_capability}
        return tuple(t.spec for name, t in self._tools.items() if name in wanted)

    async def run(self, call: ToolCall) -> str:
        """Run one call; a refusal or failure is a :class:`ToolError`, never anything else the model cannot read."""
        tool = self._tools.get(call.name)
        if tool is None:
            raise ToolError(f"unknown tool: {call.name[:60]!r}")
        return await tool.run(call.arguments)


def build_registry(config: ToolsConfig, clock: Clock) -> ToolRegistry:
    """The registry for the ``tools`` section; ``clock`` is what ``current_time`` reads (Req 5.10)."""
    tools: list[Tool] = []
    for name in config.enabled:
        if name == "calculator":

            async def run_calculator(arguments: dict[str, Any]) -> str:
                return calculate(_argument(arguments, "expression"))

            tools.append(Tool(_CALCULATOR, run_calculator))
        elif name == "http_fetch":
            settings = config.http_fetch
            fetch = HttpFetch(
                settings.allowed_hosts,
                allow_http=settings.allow_http,
                timeout=settings.timeout,
                max_bytes=settings.max_bytes,
            )

            async def run_fetch(arguments: dict[str, Any], _fetch: HttpFetch = fetch) -> str:
                return await _fetch.fetch(_argument(arguments, "url"))

            tools.append(Tool(_HTTP_FETCH, run_fetch))
        else:  # current_time: the config's ToolName admits no other

            async def run_time(arguments: dict[str, Any]) -> str:
                now = datetime.fromtimestamp(int(clock.now()), tz=timezone.utc)
                return now.strftime("%Y-%m-%dT%H:%M:%SZ")

            tools.append(Tool(_CURRENT_TIME, run_time))
    return ToolRegistry(tools)


class _ToolProvider(Protocol):
    @property
    def provider(self) -> str: ...

    @property
    def supports_tools(self) -> bool: ...


def refuse_unsupported_provider(registry: ToolRegistry, llm: _ToolProvider) -> None:
    """A Worker that enables a tool with a provider that cannot call tools does not start (Req 5.1, exit code 2)."""
    if registry and not llm.supports_tools:
        raise ConfigError(
            [FieldError("tools.enabled", f"provider {llm.provider} does not support tools")],
            reason="tools cannot be enabled",
        )


__all__ = [
    "CAPABILITY_PREFIX",
    "Tool",
    "ToolRegistry",
    "build_registry",
    "capability_for",
    "refuse_unsupported_provider",
]
