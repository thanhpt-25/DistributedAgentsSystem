"""Structured single-line JSON logging (Requirements 12.7, 13.1-13.3, 13.10).

Every entry is one JSON object on one line with at least ``timestamp`` (UTC
ISO 8601, millisecond precision, ``Z`` suffix), ``level``, ``agent_id`` and
``message``. ``request_id`` / ``job_id`` are added from
:mod:`das.observability.context`. Content and credential fields are dropped,
and the :class:`~das.observability.redaction.Redactor` runs on every string
value *and* on the fully rendered line, which covers exception traces.

Usage::

    configure_logging(agent_id="worker-1", secrets=[api_key])
    log = get_logger(__name__)
    log.info("job started", attempt=1)

Records emitted through the standard :mod:`logging` module (third-party
libraries) go through the same chain.
"""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Callable, Iterable, Mapping, MutableMapping
from datetime import UTC, datetime
from typing import Any, TextIO

import structlog
from opentelemetry import trace as otel_trace
from structlog.typing import EventDict, Processor, WrappedLogger

from das.observability.context import current_context
from das.observability.redaction import Redactor

#: Field names whose values are never logged (Req 13.10). Matched
#: case-insensitively at any nesting depth.
CONTENT_FIELDS: frozenset[str] = frozenset(
    {
        "prompt",
        "content",
        "instruction",
        "final_result",
        "summary",
        "dependency_results",
        "messages",
        "system",
        "api_key",
        "api_keys",
        "x-api-key",
        "x_api_key",
        "aws_access_key_id",
        "aws_secret_access_key",
        "aws_session_token",
        "password",
        "secret",
        "token",
        "authorization",
        "credentials",
    }
)

REQUIRED_FIELDS: tuple[str, ...] = ("timestamp", "level", "agent_id", "message")

# Characters that ``json.dumps(ensure_ascii=False)`` leaves raw but that
# ``str.splitlines`` treats as line breaks.
_LINE_BREAKS = {"\x85": "\\u0085", "\u2028": "\\u2028", "\u2029": "\\u2029"}

_WITHHELD = "log entry withheld by redaction"


def format_timestamp(moment: datetime) -> str:
    """UTC ISO 8601 with milliseconds, e.g. ``2025-01-01T00:00:00.123Z``."""
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    moment = moment.astimezone(UTC)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def _utcnow() -> datetime:
    return datetime.now(UTC)


# --------------------------------------------------------------------------
# Processors
# --------------------------------------------------------------------------


def add_context(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    """Add the bound ``request_id`` / ``job_id`` unless set explicitly."""
    for key, value in current_context().items():
        event_dict.setdefault(key, value)
    return event_dict


TRACE_ID_KEYS: tuple[str, str] = ("trace_id", "span_id")


def add_trace_ids(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    """Correlate the entry with the active sampled Span (Req 8.9-8.11).

    ``trace_id`` (32 lowercase hex) and ``span_id`` (16 lowercase hex) come
    only from the current OpenTelemetry Span when its context is valid and
    sampled. Otherwise both keys are removed, including caller-supplied ones,
    so a log entry never carries ids that do not match a sampled Span.
    """
    ctx = otel_trace.get_current_span().get_span_context()
    if ctx.is_valid and ctx.trace_flags.sampled:
        event_dict["trace_id"] = format(ctx.trace_id, "032x")
        event_dict["span_id"] = format(ctx.span_id, "016x")
    else:
        for key in TRACE_ID_KEYS:
            event_dict.pop(key, None)
    return event_dict


def add_level(_: WrappedLogger, method_name: str, event_dict: EventDict) -> EventDict:
    level = event_dict.pop("level", None) or method_name
    if level == "warn":
        level = "warning"
    elif level == "exception":
        level = "error"
    event_dict["level"] = str(level).lower()
    return event_dict


def make_timestamper(now: Callable[[], datetime] = _utcnow) -> Processor:
    def add_timestamp(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
        event_dict["timestamp"] = format_timestamp(now())
        return event_dict

    return add_timestamp


def make_agent_id_adder(agent_id: str) -> Processor:
    def add_agent_id(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
        event_dict["agent_id"] = agent_id
        return event_dict

    return add_agent_id


def rename_event_to_message(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    if "event" in event_dict:
        event = event_dict.pop("event")
        event_dict.setdefault("message", event)
    event_dict["message"] = "" if event_dict.get("message") is None else str(event_dict["message"])
    return event_dict


def _drop(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            k: _drop(v)
            for k, v in value.items()
            if not (isinstance(k, str) and k.lower() in CONTENT_FIELDS)
        }
    if isinstance(value, list | tuple):
        return [_drop(v) for v in value]
    return value


def drop_content_fields(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
    """Remove prompt/content/credential fields at any depth (Req 13.10)."""
    for key in [k for k in event_dict if isinstance(k, str) and k.lower() in CONTENT_FIELDS]:
        del event_dict[key]
    for key, value in list(event_dict.items()):
        if isinstance(value, Mapping | list | tuple):
            event_dict[key] = _drop(value)
    return event_dict


def _jsonable(value: Any) -> Any:
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple | set | frozenset):
        return [_jsonable(v) for v in value]
    if isinstance(value, datetime):
        return format_timestamp(value)
    return str(value)


def _redact_value(redactor: Redactor, value: Any) -> Any:
    if isinstance(value, str):
        return redactor.redact(value)
    if isinstance(value, dict):
        return {redactor.redact(k): _redact_value(redactor, v) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact_value(redactor, v) for v in value]
    return value


def make_value_redactor(redactor: Redactor) -> Processor:
    """Redact every key and string value before rendering, keeping JSON valid."""

    def redact_values(_: WrappedLogger, __: str, event_dict: EventDict) -> EventDict:
        jsonable = {str(k): _jsonable(v) for k, v in event_dict.items()}
        if not redactor:
            return jsonable
        return _redact_value(redactor, jsonable)

    return redact_values


def _dumps(event: Mapping[str, Any]) -> str:
    line = json.dumps(event, ensure_ascii=False, separators=(",", ":"), default=str)
    for raw, escaped in _LINE_BREAKS.items():
        line = line.replace(raw, escaped)
    return line


def make_renderer(redactor: Redactor) -> Processor:
    """Render to a single JSON line, then redact the full rendered line.

    Values were already redacted, so the line-level pass only matters for
    secrets that span JSON structure (escapes, key/value seams). If that pass
    would corrupt the JSON, a minimal entry with the required fields is emitted
    instead, so the output is always a single parseable JSON object.
    """

    def render(_: WrappedLogger, __: str, event_dict: EventDict) -> str:
        line = _dumps(event_dict)
        if not redactor or not redactor.contains_secret(line):
            return line
        redacted = redactor.redact(line)
        try:
            json.loads(redacted)
            return redacted
        except ValueError:
            fallback = {
                "timestamp": event_dict.get("timestamp", ""),
                "level": event_dict.get("level", ""),
                "agent_id": redactor.redact(str(event_dict.get("agent_id", ""))),
                "message": _WITHHELD,
            }
            return redactor.redact(_dumps(fallback))

    return render


# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------


def build_processors(
    agent_id: str,
    redactor: Redactor,
    now: Callable[[], datetime] = _utcnow,
) -> tuple[list[Processor], list[Processor]]:
    """Return ``(pre_chain, render_chain)`` shared by structlog and stdlib."""
    pre_chain: list[Processor] = [
        add_context,
        add_level,
        make_timestamper(now),
        make_agent_id_adder(agent_id),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        rename_event_to_message,
        add_trace_ids,
        drop_content_fields,
    ]
    render_chain: list[Processor] = [make_value_redactor(redactor), make_renderer(redactor)]
    return pre_chain, render_chain


class _LineWriter:
    """structlog logger that writes one line per entry to a text stream."""

    def __init__(self, stream: TextIO) -> None:
        self._stream = stream

    def msg(self, message: str) -> None:
        self._stream.write(message + "\n")
        self._stream.flush()

    log = debug = info = warning = warn = error = critical = exception = fatal = msg


class _LineWriterFactory:
    def __init__(self, stream: TextIO) -> None:
        self._stream = stream

    def __call__(self, *args: Any) -> _LineWriter:
        return _LineWriter(self._stream)


_redactor = Redactor()


def get_redactor() -> Redactor:
    """The Redactor used by the configured logging pipeline."""
    return _redactor


def register_secret(secret: Any) -> None:
    """Register an additional secret after :func:`configure_logging`."""
    _redactor.add(secret)


def configure_logging(
    agent_id: str,
    secrets: Iterable[Any] = (),
    level: int | str = logging.INFO,
    stream: TextIO | None = None,
    now: Callable[[], datetime] = _utcnow,
) -> Redactor:
    """Configure structlog and stdlib logging; returns the active Redactor.

    ``secrets`` may contain ``str`` or ``SecretStr``-like values; ``None`` and
    empty values are ignored.
    """
    global _redactor
    _redactor = Redactor(s for s in secrets if s is not None)
    out = stream if stream is not None else sys.stdout
    numeric_level = logging.getLevelName(level.upper()) if isinstance(level, str) else level
    if not isinstance(numeric_level, int):
        raise ValueError(f"unknown log level: {level!r}")

    pre_chain, render_chain = build_processors(agent_id, _redactor, now)

    structlog.configure(
        processors=[*pre_chain, *render_chain],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        logger_factory=_LineWriterFactory(out),
        cache_logger_on_first_use=False,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=[_stdlib_event, *pre_chain],
        processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta, *render_chain],
    )
    handler = logging.StreamHandler(out)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    for existing in list(root.handlers):
        if getattr(existing, "_das_handler", False):
            root.removeHandler(existing)
    handler._das_handler = True  # type: ignore[attr-defined]
    root.addHandler(handler)
    root.setLevel(numeric_level)
    return _redactor


def _stdlib_event(
    _: WrappedLogger, __: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """Map a stdlib LogRecord's level and logger name into the event dict."""
    record: logging.LogRecord | None = event_dict.get("_record")
    if record is not None:
        event_dict["level"] = record.levelname.lower()
        event_dict.setdefault("logger_name", record.name)
    return event_dict


def get_logger(name: str | None = None, **initial: Any) -> Any:
    """A lazy structlog logger; ``name`` is logged as ``logger_name``.

    The returned proxy resolves the configuration on each use, so module-level
    loggers created before :func:`configure_logging` still use it.
    """
    if name is not None:
        initial.setdefault("logger_name", name)
    return structlog.get_logger(**initial)
