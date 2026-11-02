"""Unit tests for das.observability (Requirements 12.7, 13.1-13.3, 13.10)."""

from __future__ import annotations

import asyncio
import io
import json
import logging
import re
from datetime import UTC, datetime

import pytest
from pydantic import SecretStr

from das.observability import (
    REDACTION_MARKER,
    Redactor,
    bind_correlation_id,
    bind_job,
    bind_request,
    configure_logging,
    current_context,
    get_logger,
    get_request_id,
)
from das.observability.logging import format_timestamp

TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


def _windows(secret: str) -> list[str]:
    if len(secret) < 8:
        return [secret]
    return [secret[i : i + 8] for i in range(len(secret) - 7)]


def _assert_clean(text: str, *secrets: str) -> None:
    for secret in secrets:
        for window in _windows(secret):
            assert window not in text, (window, text)


# ------------------------------------------------------------------ Redactor


class TestRedactor:
    def test_replaces_full_secret(self) -> None:
        r = Redactor(["sk-abcdefghijklmnop"])
        assert r.redact("key=sk-abcdefghijklmnop end") == f"key={REDACTION_MARKER} end"

    def test_replaces_long_substring(self) -> None:
        secret = "sk-abcdefghijklmnop"
        r = Redactor([secret])
        out = r.redact("partial: cdefghijk!")
        assert REDACTION_MARKER in out
        _assert_clean(out, secret)

    def test_seven_char_substring_of_long_secret_kept(self) -> None:
        r = Redactor(["sk-abcdefghijklmnop"])
        assert r.redact("abcdefg") == "abcdefg"

    def test_short_secret_whole_occurrences(self) -> None:
        r = Redactor(["hunter2"])
        assert r.redact("pw hunter2 and hunter2x") == (
            f"pw {REDACTION_MARKER} and {REDACTION_MARKER}x"
        )
        assert r.redact("hunter") == "hunter"

    def test_secretstr_and_empty_ignored(self) -> None:
        r = Redactor([SecretStr("supersecretvalue"), ""])
        r.add(None)
        assert r.redact("x supersecretvalue y") == f"x {REDACTION_MARKER} y"
        assert not Redactor([""])

    def test_secret_colliding_with_marker_terminates(self) -> None:
        secret = "[REDACTED]"
        r = Redactor([secret])
        out = r.redact(f"a {secret} b")
        _assert_clean(out, secret)

    def test_seam_created_by_marker_is_redacted(self) -> None:
        # Secret that contains marker characters; replacement may create seams.
        secret = "ED]abcdefgh"
        r = Redactor([secret, "xxRED"])
        out = r.redact("xxRED abcdefgh ED]abcdefgh")
        _assert_clean(out, secret, "xxRED")

    def test_no_secrets_is_identity(self) -> None:
        assert Redactor().redact("anything") == "anything"


# ------------------------------------------------------------------ context


class TestContext:
    def test_bind_request_and_restore(self) -> None:
        assert get_request_id() is None
        with bind_request("r1"):
            assert current_context() == {"request_id": "r1"}
            with bind_job("r1", "j1"):
                assert current_context() == {"request_id": "r1", "job_id": "j1"}
            assert current_context() == {"request_id": "r1"}
        assert current_context() == {}

    def test_bind_correlation_id(self) -> None:
        with bind_correlation_id("rid-9") as bound:
            assert bound is True
            assert get_request_id() == "rid-9"
        with bind_request("outer"), bind_correlation_id("") as bound:
            assert bound is False
            assert get_request_id() is None

    async def test_isolated_across_tasks(self) -> None:
        async def worker(rid: str) -> str | None:
            with bind_request(rid):
                await asyncio.sleep(0)
                return get_request_id()

        assert await asyncio.gather(worker("a"), worker("b")) == ["a", "b"]


# ------------------------------------------------------------------ logging


FIXED = datetime(2025, 1, 2, 3, 4, 5, 678901, tzinfo=UTC)


@pytest.fixture
def stream() -> io.StringIO:
    return io.StringIO()


def _lines(stream: io.StringIO) -> list[dict]:
    raw = stream.getvalue().splitlines()
    return [json.loads(line) for line in raw]


def test_format_timestamp() -> None:
    assert format_timestamp(FIXED) == "2025-01-02T03:04:05.678Z"


class TestLogging:
    def test_required_fields_and_single_line(self, stream: io.StringIO) -> None:
        configure_logging("agent-1", stream=stream, now=lambda: FIXED)
        get_logger("t").info("multi\nline \u2028 message ✓", attempt=2)
        assert stream.getvalue().count("\n") == 1
        [entry] = _lines(stream)
        assert entry["timestamp"] == "2025-01-02T03:04:05.678Z"
        assert entry["level"] == "info"
        assert entry["agent_id"] == "agent-1"
        assert entry["message"] == "multi\nline \u2028 message ✓"
        assert entry["attempt"] == 2
        assert entry["logger_name"] == "t"
        assert "request_id" not in entry and "job_id" not in entry

    def test_real_clock_timestamp_format(self, stream: io.StringIO) -> None:
        configure_logging("a", stream=stream)
        get_logger().warning("w")
        [entry] = _lines(stream)
        assert TS_RE.match(entry["timestamp"])
        assert entry["level"] == "warning"

    def test_request_and_job_context(self, stream: io.StringIO) -> None:
        configure_logging("a", stream=stream)
        log = get_logger()
        with bind_request("r1"):
            log.info("req")
            with bind_job("r1", "j1"):
                log.info("job")
        a, b = _lines(stream)
        assert a["request_id"] == "r1" and "job_id" not in a
        assert b["request_id"] == "r1" and b["job_id"] == "j1"

    def test_content_fields_dropped(self, stream: io.StringIO) -> None:
        configure_logging("a", stream=stream)
        get_logger().info(
            "x",
            prompt="PROMPT-TEXT",
            content="RESULT-TEXT",
            instruction="INSTR",
            nested={"api_key": "KEYVAL", "ok": 1},
        )
        [entry] = _lines(stream)
        text = stream.getvalue()
        for value in ("PROMPT-TEXT", "RESULT-TEXT", "INSTR", "KEYVAL"):
            assert value not in text
        assert entry["nested"] == {"ok": 1}

    def test_secrets_redacted_in_fields_and_message(self, stream: io.StringIO) -> None:
        secret = "sk-live-0123456789abcdef"
        configure_logging("a", secrets=[SecretStr(secret)], stream=stream)
        get_logger().error(f"auth failed for {secret[2:14]}", detail=f"key={secret}")
        text = stream.getvalue()
        _assert_clean(text, secret)
        [entry] = _lines(stream)
        assert REDACTION_MARKER in entry["message"]
        assert entry["detail"] == f"key={REDACTION_MARKER}"

    def test_exception_trace_redacted(self, stream: io.StringIO) -> None:
        secret = "very-secret-token-value"
        configure_logging("a", secrets=[secret], stream=stream)
        try:
            raise RuntimeError(f"bad credential {secret}")
        except RuntimeError:
            get_logger().exception("boom")
        text = stream.getvalue()
        _assert_clean(text, secret)
        [entry] = _lines(stream)
        assert "RuntimeError" in entry["exception"]
        assert entry["level"] == "error"

    def test_secret_spanning_json_structure_keeps_valid_json(
        self, stream: io.StringIO
    ) -> None:
        # A secret containing JSON structural characters that appears only once
        # the line is rendered (key/value seam).
        secret = 'aaaa":"bbbb'
        configure_logging("a", secrets=[secret], stream=stream)
        get_logger().info("m", aaaa="bbbb")
        text = stream.getvalue()
        _assert_clean(text, secret)
        [entry] = _lines(stream)
        assert {"timestamp", "level", "agent_id", "message"} <= entry.keys()

    def test_stdlib_records_use_same_pipeline(self, stream: io.StringIO) -> None:
        secret = "stdlib-secret-123456"
        configure_logging("a", secrets=[secret], stream=stream)
        with bind_job("r2", "j2"):
            logging.getLogger("third.party").warning("leak %s", secret)
        [entry] = _lines(stream)
        _assert_clean(stream.getvalue(), secret)
        assert entry["level"] == "warning"
        assert entry["request_id"] == "r2" and entry["job_id"] == "j2"
        assert entry["agent_id"] == "a"

    def test_logger_created_before_configure(self, stream: io.StringIO) -> None:
        early = get_logger("early")
        configure_logging("late-agent", stream=stream)
        early.info("hi")
        [entry] = _lines(stream)
        assert entry["agent_id"] == "late-agent"

    def test_level_filtering(self, stream: io.StringIO) -> None:
        configure_logging("a", level="WARNING", stream=stream)
        get_logger().info("hidden")
        get_logger().warning("shown")
        assert [e["message"] for e in _lines(stream)] == ["shown"]
