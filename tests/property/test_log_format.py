# Feature: distributed-agent-system, Property 39: Structured log format and context
"""Property 39: Structured log format and context.

*For any* log message text (including newlines and Unicode) and any bound
context:

- The rendered entry is a single line that parses as JSON containing
  ``timestamp`` (UTC ISO 8601 with ms), ``level``, ``agent_id``, and ``message``.
- It contains ``request_id`` when a Request is bound, and both ``job_id`` and
  ``request_id`` when a Job is bound.
- It contains none of the values of content or credential fields.

**Validates: Requirements 13.1, 13.2, 13.3, 13.10**
"""

from __future__ import annotations

import io
import json
import logging
import re
from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime, timedelta, timezone

import structlog
from hypothesis import given
from hypothesis import strategies as st

from das.observability.context import bind_job, bind_request
from das.observability.logging import configure_logging, get_logger

_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")

_AGENT_ID = "agent-property-39"

# Content / credential field names, in mixed case, that must never be logged.
_CONTENT_KEYS = st.sampled_from(
    [
        "prompt",
        "content",
        "instruction",
        "final_result",
        "summary",
        "dependency_results",
        "messages",
        "api_key",
        "Authorization",
        "PASSWORD",
        "aws_secret_access_key",
        "token",
    ]
)
_SAFE_KEYS = st.sampled_from(["attempt", "worker", "detail", "note", "meta"])

# Messages: arbitrary Unicode including control chars, newlines and the
# characters ``str.splitlines`` treats as line breaks.
_messages = st.text(
    st.one_of(
        st.characters(codec="utf-8"),
        st.sampled_from(["\n", "\r", "\x85", "\u2028", "\u2029", '"', "\\"]),
    ),
    max_size=60,
)
_ids = st.text(st.characters(codec="utf-8"), min_size=1, max_size=40)

# Sentinel values for content fields: unique, unlikely to occur by chance.
_sentinels = st.uuids().map(lambda u: f"SENTINEL-{u.hex}")

_moments = st.builds(
    lambda naive, offset: naive.replace(tzinfo=timezone(timedelta(minutes=offset))),
    st.datetimes(min_value=datetime(1971, 1, 2), max_value=datetime(9998, 12, 30)),
    st.integers(-14 * 60, 14 * 60),
)


@st.composite
def _content_fields(draw: st.DrawFn) -> tuple[dict[str, object], list[str]]:
    """Fields mixing safe values and content values (top-level and nested)."""
    fields: dict[str, object] = {}
    sentinels: list[str] = []
    for key in draw(st.lists(_CONTENT_KEYS, max_size=3, unique_by=str.lower)):
        value = draw(_sentinels)
        sentinels.append(value)
        fields[key] = value
    for key in draw(st.lists(_SAFE_KEYS, max_size=3, unique=True)):
        shape = draw(st.sampled_from(["plain", "nested", "list"]))
        if shape == "plain":
            fields[key] = draw(st.text(max_size=10))
        else:
            hidden_key = draw(_CONTENT_KEYS)
            hidden = draw(_sentinels)
            sentinels.append(hidden)
            inner = {"ok": draw(st.integers()), hidden_key: hidden}
            fields[key] = inner if shape == "nested" else [inner, draw(st.text(max_size=5))]
    return fields, sentinels


@contextmanager
def _configured(moment: datetime) -> Iterator[io.StringIO]:
    stream = io.StringIO()
    root = logging.getLogger()
    old_level = root.level
    configure_logging(agent_id=_AGENT_ID, level="DEBUG", stream=stream, now=lambda: moment)
    try:
        yield stream
    finally:
        for handler in list(root.handlers):
            if getattr(handler, "_das_handler", False):
                root.removeHandler(handler)
        root.setLevel(old_level)
        structlog.reset_defaults()


def _expected_timestamp(moment: datetime) -> str:
    utc = moment.astimezone(UTC)
    return utc.strftime("%Y-%m-%dT%H:%M:%S.") + f"{utc.microsecond // 1000:03d}Z"


@given(
    message=_messages,
    level=st.sampled_from(["debug", "info", "warning", "error"]),
    scope=st.sampled_from(["none", "request", "job"]),
    request_id=_ids,
    job_id=_ids,
    content=_content_fields(),
    moment=_moments,
    use_stdlib=st.booleans(),
)
def test_log_entry_format_and_context(
    message: str,
    level: str,
    scope: str,
    request_id: str,
    job_id: str,
    content: tuple[dict[str, object], list[str]],
    moment: datetime,
    use_stdlib: bool,
) -> None:
    """Property 39: single-line JSON with required fields and bound context."""
    fields, sentinels = content
    if scope == "request":
        binding = bind_request(request_id)
    elif scope == "job":
        binding = bind_job(request_id, job_id)
    else:
        binding = nullcontext()

    with _configured(moment) as stream, binding:
        if use_stdlib:
            # Third-party libraries log through stdlib; no structured fields.
            logging.getLogger("das.test.format").log(
                logging.getLevelName(level.upper()), message
            )
            sentinels = []
        else:
            getattr(get_logger("das.test.format"), level)(message, **fields)

    output = stream.getvalue()
    # 13.1: exactly one entry, on exactly one line.
    assert output.endswith("\n")
    lines = output.splitlines()
    assert len(lines) == 1, f"expected one line, got {lines!r}"
    entry = json.loads(lines[0])
    assert isinstance(entry, dict)

    assert _TIMESTAMP_RE.match(entry["timestamp"]), entry["timestamp"]
    assert entry["timestamp"] == _expected_timestamp(moment)
    assert entry["level"] == level
    assert entry["agent_id"] == _AGENT_ID
    assert entry["message"] == message

    # 13.2 / 13.3: bound context is present; unbound context is absent.
    if scope == "none":
        assert "request_id" not in entry
        assert "job_id" not in entry
    elif scope == "request":
        assert entry["request_id"] == request_id
        assert "job_id" not in entry
    else:
        assert entry["request_id"] == request_id
        assert entry["job_id"] == job_id

    # 13.10: no content or credential values anywhere in the line.
    for value in sentinels:
        assert value not in lines[0], f"content value {value!r} leaked in {lines[0]!r}"
