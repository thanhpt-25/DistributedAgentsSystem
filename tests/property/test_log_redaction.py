# Feature: distributed-agent-system, Property 38: Log redaction
"""Property 38: Log redaction.

*For any* set of registered secrets and any log event (message text, fields,
exception text), the rendered log line contains no substring of length >= 8 of
any secret. It also doesn't contain the full value of any secret shorter than 8
characters, wherever that value appeared in the input.

**Validates: Requirements 12.7**
"""

from __future__ import annotations

import io
import logging
from collections.abc import Iterator
from contextlib import contextmanager

import structlog
from hypothesis import given
from hypothesis import strategies as st

from das.cli import secrets_for_redaction
from das.config.schema import ADMIN_KEY_MAX_LENGTH, ADMIN_KEY_MIN_LENGTH, AgentConfig
from das.observability.logging import configure_logging, get_logger
from das.observability.redaction import WINDOW, Redactor

# Mix of plain characters, JSON-significant characters (quotes, backslash,
# braces), control characters and non-ASCII so secrets interact with escaping.
_ALPHABET = st.sampled_from(
    list("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
    + list("-_./+=:,{}[]\"'\\ ")
    + ["\n", "\t", "\x00", "é", "ß", "漢", "😀", "\u2028"]
)

_secrets = st.one_of(
    st.text(_ALPHABET, min_size=WINDOW, max_size=48),  # long: window redaction
    st.text(_ALPHABET, min_size=2, max_size=WINDOW - 1),  # short: whole value
)
_noise = st.text(_ALPHABET, max_size=20)


def _forbidden(secrets: list[str]) -> set[str]:
    """Independent oracle: every forbidden substring per the property."""
    out: set[str] = set()
    for s in secrets:
        if len(s) >= WINDOW:
            out.update(s[i : i + WINDOW] for i in range(len(s) - WINDOW + 1))
        else:
            out.add(s)
    return out


@st.composite
def _fragments(draw: st.DrawFn, secrets: list[str]) -> str:
    """Text built from noise, whole secrets and long substrings of secrets."""
    parts: list[str] = []
    for _ in range(draw(st.integers(1, 4))):
        kind = draw(st.sampled_from(["noise", "secret", "substring"]))
        secret = draw(st.sampled_from(secrets))
        if kind == "noise":
            parts.append(draw(_noise))
        elif kind == "secret" or len(secret) < WINDOW:
            parts.append(secret)
        else:
            start = draw(st.integers(0, len(secret) - WINDOW))
            end = draw(st.integers(start + WINDOW, len(secret)))
            parts.append(secret[start:end])
    return "".join(parts)


@st.composite
def _scenarios(draw: st.DrawFn) -> tuple[list[str], str, dict[str, object], str, bool]:
    secrets = draw(st.lists(_secrets, min_size=1, max_size=4, unique=True))
    message = draw(_fragments(secrets))
    field_names = st.sampled_from(["detail", "url", "headers", "note", "extra"])
    fields = draw(
        st.dictionaries(
            field_names,
            st.one_of(
                _fragments(secrets),
                st.lists(_fragments(secrets), max_size=3),
                st.dictionaries(st.sampled_from(["a", "b"]), _fragments(secrets), max_size=2),
            ),
            max_size=3,
        )
    )
    exc_text = draw(_fragments(secrets))
    use_stdlib = draw(st.booleans())
    return secrets, message, fields, exc_text, use_stdlib


@contextmanager
def _configured(secrets: list[str]) -> Iterator[io.StringIO]:
    stream = io.StringIO()
    root = logging.getLogger()
    old_level = root.level
    configure_logging(agent_id="agent-1", secrets=secrets, stream=stream)
    try:
        yield stream
    finally:
        for handler in list(root.handlers):
            if getattr(handler, "_das_handler", False):
                root.removeHandler(handler)
        root.setLevel(old_level)
        structlog.reset_defaults()


def _assert_clean(output: str, secrets: list[str]) -> None:
    lines = output.splitlines()
    assert lines, "expected at least one rendered log line"
    for line in lines:
        for bad in _forbidden(secrets):
            assert bad not in line, f"secret fragment {bad!r} leaked in {line!r}"
        # Cross-check with the Redactor's own matcher.
        assert not Redactor(secrets).contains_secret(line)


@given(_scenarios())
def test_rendered_log_lines_contain_no_secret(scenario) -> None:
    """Property 38: message text, fields and exception traces are redacted."""
    secrets, message, fields, exc_text, use_stdlib = scenario
    with _configured(secrets) as stream:
        if use_stdlib:
            logger = logging.getLogger("das.test.redaction")
            logger.info(message)
            try:
                raise RuntimeError(exc_text)
            except RuntimeError:
                logger.exception(message)
        else:
            log = get_logger("das.test.redaction")
            log.info(message, **fields)
            try:
                raise ValueError(exc_text)
            except ValueError:
                log.exception(message, **fields)
    _assert_clean(stream.getvalue(), secrets)


# --------------------------------------------------------------------------
# Feature: das-observability-operations, Property 30: Redaction cho secret mới
#
# *For any* randomly generated Admin_API_Keys, OTLP header values and
# ``cursor_secret`` (length 16-256), and any log entry, Audit_Entry, startup
# error message or exception trace containing those values or substrings of
# them, the rendered output contains no substring of >= 8 consecutive
# characters of any secret. This is the v1 redaction property with an
# extended generator; the secrets reach the Redactor through the real wiring
# (``AgentConfig.secret_values()`` via ``das.cli.secrets_for_redaction``).
#
# **Validates: Requirements 11.9, 19.3**
# --------------------------------------------------------------------------

_PRINTABLE = [chr(c) for c in range(0x21, 0x7F)]
# Admin_API_Key: printable ASCII 0x21-0x7E (Req 11.10).
_admin_keys = st.text(
    st.sampled_from(_PRINTABLE), min_size=ADMIN_KEY_MIN_LENGTH, max_size=ADMIN_KEY_MAX_LENGTH
)
# OTLP header value: the "k=v,k2=v2" syntax reserves ',' and values are stripped.
_header_values = st.text(
    st.sampled_from([c for c in _PRINTABLE if c != ","]), min_size=16, max_size=256
)
_header_names = st.text(
    st.sampled_from(list("abcdefghijklmnopqrstuvwxyz-")), min_size=1, max_size=16
)
# cursor_secret is an opaque string: allow JSON-significant and non-ASCII chars too.
_cursor_secrets = st.text(_ALPHABET, min_size=16, max_size=256)


@st.composite
def _phase2_configs(draw: st.DrawFn) -> AgentConfig:
    admin = draw(st.lists(_admin_keys, min_size=1, max_size=3, unique=True))
    headers = draw(
        st.lists(st.tuples(_header_names, _header_values), min_size=1, max_size=3)
    )
    cursor = draw(st.one_of(st.none(), _cursor_secrets))
    data: dict[str, object] = {
        "role": "gateway",
        "agent_id": "agent-1",
        "admin_api_keys": admin,
        "tracing": {"otlp_headers": ",".join(f"{k}={v}" for k, v in headers)},
    }
    if cursor is not None:
        data["gateway"] = {"cursor_secret": cursor}
    return AgentConfig.model_validate(data)


def _new_secrets(config: AgentConfig) -> list[str]:
    """The Phase 2 secrets taken straight from the config (independent of the wiring)."""
    out = [k.get_secret_value() for k in config.admin_api_keys]
    out += [v for _, v in config.tracing.header_pairs()]
    if config.gateway.cursor_secret is not None:
        out.append(config.gateway.cursor_secret.get_secret_value())
    return out


@st.composite
def _phase2_scenarios(
    draw: st.DrawFn,
) -> tuple[AgentConfig, list[str], str, str, dict[str, object], str]:
    config = draw(_phase2_configs())
    secrets = _new_secrets(config)
    kind = draw(st.sampled_from(["log", "audit", "startup_error", "exception"]))
    message = draw(_fragments(secrets))
    fields = draw(
        st.dictionaries(
            st.sampled_from(["detail", "headers", "cursor", "reason", "extra"]),
            st.one_of(
                _fragments(secrets),
                st.lists(_fragments(secrets), max_size=3),
                st.dictionaries(st.sampled_from(["a", "b"]), _fragments(secrets), max_size=2),
            ),
            max_size=3,
        )
    )
    exc_text = draw(_fragments(secrets))
    return config, secrets, kind, message, fields, exc_text


@given(_phase2_scenarios())
def test_new_secrets_are_redacted_in_every_output(scenario) -> None:
    """Property 30: Admin_API_Key, OTLP header and cursor_secret never leak."""
    config, secrets, kind, message, fields, exc_text = scenario
    registered = secrets_for_redaction(config)
    # Wiring check: every new secret is handed to the Redactor.
    assert set(secrets) <= set(registered)
    with _configured(registered) as stream:
        log = get_logger("das.test.redaction.phase2")
        if kind == "log":
            log.info(message, **fields)
        elif kind == "audit":
            # Audit_Entry shape: actor/action/target plus free-form detail.
            log.info(
                "audit",
                actor=message,
                action="agent_drain",
                target=exc_text,
                outcome="ok",
                **fields,
            )
        elif kind == "startup_error":
            logging.getLogger("das.cli").error(
                "invalid configuration: %s (%s)", message, exc_text
            )
        else:
            try:
                raise RuntimeError(exc_text)
            except RuntimeError:
                log.exception(message, **fields)
    _assert_clean(stream.getvalue(), secrets)
