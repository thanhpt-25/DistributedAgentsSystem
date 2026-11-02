"""Credential handling for Agent_Config (Requirements 12.4, 12.5, 12.6).

Credentials (Gateway API keys, Admin_API_Keys, LLM credentials, OTLP exporter
headers and the Page_Cursor secret, see ``SECRET_FIELDS``) are accepted only from
environment variables or from a secrets file referenced by ``secrets_file``.
The secrets file uses ``NAME=VALUE`` lines with the same ``DAS__`` names as the
environment, e.g. ``DAS__LLM__API_KEY=...``. Blank lines, ``#`` comments, an
optional ``export`` prefix, and matching surrounding quotes are allowed.

No error produced here ever includes a credential value.
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from das.config.schema import SECRET_FIELDS, SECRET_PREFIX_FIELDS
from das.errors import ConfigError, FieldError

ENV_PREFIX = "DAS__"
ENV_SEPARATOR = "__"


def env_name(path: str) -> str:
    """Environment variable name for a dotted field path (``llm.api_key`` -> ``DAS__LLM__API_KEY``)."""
    return ENV_PREFIX + ENV_SEPARATOR.join(part.upper() for part in path.split("."))


# Map fields (``webhook_secrets``) are only reachable through ``<NAME>`` suffixes, see secret_path.
_SECRET_BY_ENV: dict[str, str] = {
    env_name(p): p for p in SECRET_FIELDS if p not in SECRET_PREFIX_FIELDS
}


def secret_path(name: str) -> str | None:
    """Dotted credential path for an environment or secrets-file name, or ``None``.

    ``DAS__LLM__API_KEY`` maps to ``llm.api_key``. For a map field such as
    ``webhook_secrets``, ``DAS__WEBHOOK_SECRETS__PARTNER_A`` maps to
    ``webhook_secrets.partner_a`` (Phase 4 Req 1.7).
    """
    path = _SECRET_BY_ENV.get(name)
    if path is not None:
        return path
    for field in SECRET_PREFIX_FIELDS:
        prefix = env_name(field) + ENV_SEPARATOR
        suffix = name[len(prefix) :]
        if name.startswith(prefix) and suffix and ENV_SEPARATOR not in suffix:
            return f"{field}.{suffix.lower()}"
    return None


def _get(data: Mapping[str, Any], path: str) -> Any:
    node: Any = data
    for part in path.split("."):
        if not isinstance(node, Mapping) or part not in node:
            return None
        node = node[part]
    return node


def _has_value(value: Any) -> bool:
    return value not in (None, "", [], ())


def file_secret_errors(file_values: Mapping[str, Any]) -> list[FieldError]:
    """One error per credential field set in the configuration file (Req 12.5)."""
    return [
        FieldError(
            path,
            "credentials must not appear in the configuration file; "
            f"set {env_name(path)}{'__<NAME>' if path in SECRET_PREFIX_FIELDS else ''} "
            "in the environment or the secrets file",
        )
        for path in SECRET_FIELDS
        if _has_value(_get(file_values, path))
    ]


def strip_secrets(file_values: Mapping[str, Any]) -> dict[str, Any]:
    """Return a deep copy of ``file_values`` with every credential field removed."""
    result = copy.deepcopy(dict(file_values))
    for path in SECRET_FIELDS:
        *parents, leaf = path.split(".")
        node: Any = result
        for part in parents:
            node = node.get(part) if isinstance(node, dict) else None
        if isinstance(node, dict):
            node.pop(leaf, None)
    return result


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
        return value[1:-1]
    return value


def read_secrets_file(reference: str | Path) -> dict[str, str]:
    """Parse a secrets file into ``{DAS__NAME: value}`` (credential names only).

    Raises :class:`ConfigError` whose errors name the ``secrets_file`` reference
    and, for bad lines, the line number and variable name, never a value (Req 12.6).
    """
    ref = str(reference)
    try:
        text = Path(reference).read_text(encoding="utf-8")
    except FileNotFoundError:
        raise ConfigError([FieldError("secrets_file", f"secrets file {ref!r} does not exist")])
    except UnicodeDecodeError:
        raise ConfigError(
            [FieldError("secrets_file", f"secrets file {ref!r} cannot be read: not valid UTF-8")]
        )
    except OSError as exc:
        reason = exc.strerror or type(exc).__name__
        raise ConfigError(
            [FieldError("secrets_file", f"secrets file {ref!r} cannot be read: {reason}")]
        )

    values: dict[str, str] = {}
    errors: list[FieldError] = []
    for lineno, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        name, sep, value = line.partition("=")
        name = name.strip()
        if not sep or not name:
            errors.append(
                FieldError("secrets_file", f"secrets file {ref!r} line {lineno}: expected NAME=VALUE")
            )
            continue
        if secret_path(name) is None:
            errors.append(
                FieldError(
                    "secrets_file",
                    f"secrets file {ref!r} line {lineno}: {name} is not a credential setting",
                )
            )
            continue
        values[name] = _unquote(value.strip())
    if errors:
        raise ConfigError(errors)
    return values


def secrets_to_values(secrets: Mapping[str, str]) -> dict[str, Any]:
    """Convert ``{DAS__NAME: value}`` from a secrets file into a nested field mapping."""
    result: dict[str, Any] = {}
    for name, value in secrets.items():
        path = secret_path(name)
        if path is None:
            continue
        *parents, leaf = path.split(".")
        node = result
        for part in parents:
            node = node.setdefault(part, {})
        node[leaf] = value
    return result


__all__ = [
    "ENV_PREFIX",
    "ENV_SEPARATOR",
    "SECRET_FIELDS",
    "env_name",
    "file_secret_errors",
    "read_secrets_file",
    "secret_path",
    "secrets_to_values",
    "strip_secrets",
]
