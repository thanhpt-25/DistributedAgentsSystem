"""Agent_Config loading (Requirements 12.1-12.6).

Resolution order for every field: environment variable, then configuration
file value, then the schema default (Req 12.1). Environment variables use the
``DAS__`` prefix and ``__`` as the nesting separator, for example
``DAS__LIMITS__MAX_ATTEMPTS=5`` sets ``limits.max_attempts``. List fields
accept comma-separated values from the environment (``DAS__WORKER__CAPABILITIES=a,b``).

``load_config`` performs no network I/O, so validation always completes before
any connection is attempted (Req 12.2).
"""

from __future__ import annotations

import copy
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from das.config.schema import AgentConfig, validate_config
from das.config.secrets import (
    ENV_PREFIX,
    ENV_SEPARATOR,
    file_secret_errors,
    read_secrets_file,
    secrets_to_values,
    strip_secrets,
)
from das.errors import ConfigError, FieldError


def env_path(name: str) -> tuple[str, ...] | None:
    """Map ``DAS__A__B`` to ``("a", "b")``; return ``None`` for non-DAS or malformed names."""
    if not name.startswith(ENV_PREFIX):
        return None
    parts = name[len(ENV_PREFIX) :].split(ENV_SEPARATOR)
    if not parts or any(not p for p in parts):
        return None
    return tuple(p.lower() for p in parts)


def _set(target: dict[str, Any], path: tuple[str, ...], value: Any) -> None:
    node = target
    for part in path[:-1]:
        child = node.get(part)
        if not isinstance(child, dict):
            child = {}
            node[part] = child
        node = child
    node[path[-1]] = value


def deep_merge(base: Mapping[str, Any], overlay: Mapping[str, Any]) -> dict[str, Any]:
    """Return ``base`` with ``overlay`` applied recursively (overlay wins). Inputs are not mutated."""
    result = copy.deepcopy(dict(base))
    for key, value in overlay.items():
        if isinstance(value, Mapping) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def resolve(file_values: Mapping[str, Any], env: Mapping[str, str]) -> dict[str, Any]:
    """Pure precedence merge: environment values override file values.

    Fields absent from both are left out so the schema default applies.
    Neither input is mutated.
    """
    result = copy.deepcopy(dict(file_values))
    # Shallower names first, so DAS__LLM__MODEL still applies after DAS__LLM.
    for name in sorted(env, key=lambda n: (n.count(ENV_SEPARATOR), n)):
        path = env_path(name)
        if path is not None:
            _set(result, path, env[name])
    return result


def read_config_file(path: Path) -> dict[str, Any]:
    """Read and parse the YAML file; errors name the path and reason only (Req 12.3)."""
    source = str(path)
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        raise ConfigError(source=source, reason="file does not exist")
    except OSError as exc:
        raise ConfigError(
            source=source, reason=f"file cannot be read: {exc.strerror or type(exc).__name__}"
        )
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        raise ConfigError(source=source, reason="file cannot be parsed: not valid UTF-8")
    try:
        data = yaml.safe_load(text)
    except yaml.MarkedYAMLError as exc:
        # Never include str(exc): it quotes file content, which may hold a credential.
        where = ""
        if exc.problem_mark is not None:
            where = f" at line {exc.problem_mark.line + 1}, column {exc.problem_mark.column + 1}"
        raise ConfigError(
            source=source, reason=f"file cannot be parsed: invalid YAML{where}"
        )
    except yaml.YAMLError:
        raise ConfigError(source=source, reason="file cannot be parsed: invalid YAML")
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ConfigError(source=source, reason="file cannot be parsed: top level must be a mapping")
    return data


def load_config(path: str | Path, env: Mapping[str, str]) -> AgentConfig:
    """Load, resolve, and validate the Agent_Config.

    Raises :class:`ConfigError` with ``source`` set to the file path and one
    :class:`FieldError` per invalid, missing, or forbidden field.
    """
    path = Path(path)
    file_values = read_config_file(path)

    errors: list[FieldError] = list(file_secret_errors(file_values))
    file_values = strip_secrets(file_values)

    secrets_ref = env.get(ENV_PREFIX + "SECRETS_FILE") or file_values.get("secrets_file")
    if secrets_ref:
        try:
            secret_values = secrets_to_values(read_secrets_file(str(secrets_ref)))
        except ConfigError as exc:
            errors.extend(exc.errors)
        else:
            file_values = deep_merge(file_values, secret_values)

    resolved = resolve(file_values, env)
    config: AgentConfig | None = None
    try:
        config = validate_config(resolved)
    except ConfigError as exc:
        seen = {e.field for e in errors}
        errors.extend(e for e in exc.errors if e.field not in seen)

    if errors or config is None:
        raise ConfigError(errors, source=str(path), reason="invalid configuration")
    return config


__all__ = ["deep_merge", "env_path", "load_config", "read_config_file", "resolve"]
