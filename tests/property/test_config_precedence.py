# Feature: distributed-agent-system, Property 35: Configuration precedence
"""Property 35: Configuration precedence.

For any field and any combination of presence or absence of an environment
value, a file value, and a default, the resolved value is the environment
value if present, else the file value if present, else the default.

**Validates: Requirements 12.1**
"""

from __future__ import annotations

import copy
from typing import Any

from hypothesis import given
from hypothesis import strategies as st

from das.config.loader import resolve
from das.config.schema import LimitsConfig, validate_config

_MISSING = object()

# Lowercase identifier segments without "__", so env names map back losslessly.
_segments = st.from_regex(r"\A[a-z][a-z0-9]{0,7}(_[a-z0-9]{1,4})?\Z")
_paths = st.lists(_segments, min_size=1, max_size=3).map(tuple)


def _prefix_free(paths: list[tuple[str, ...]]) -> bool:
    return not any(a != b and b[: len(a)] == a for a in paths for b in paths)


def _env_name(path: tuple[str, ...]) -> str:
    return "DAS__" + "__".join(p.upper() for p in path)


def _set(target: dict[str, Any], path: tuple[str, ...], value: Any) -> None:
    for part in path[:-1]:
        target = target.setdefault(part, {})
    target[path[-1]] = value


def _get(data: Any, path: tuple[str, ...]) -> Any:
    for part in path:
        if not isinstance(data, dict) or part not in data:
            return _MISSING
        data = data[part]
    return data


@st.composite
def _precedence_cases(draw: st.DrawFn) -> tuple[list, dict, dict]:
    """Prefix-free field paths, each with an independent env/file presence."""
    paths = draw(st.lists(_paths, min_size=1, max_size=6, unique=True).filter(_prefix_free))
    file_values: dict[str, Any] = {}
    env: dict[str, str] = {}
    fields = []
    for path in paths:
        has_file = draw(st.booleans())
        file_val = draw(st.none() | st.integers() | st.text(max_size=10)) if has_file else _MISSING
        env_val = draw(st.text(max_size=10)) if draw(st.booleans()) else _MISSING
        if file_val is not _MISSING:
            _set(file_values, path, file_val)
        if env_val is not _MISSING:
            env[_env_name(path)] = env_val
        fields.append((path, file_val, env_val))
    # Unrelated environment variables must be ignored.
    unrelated = st.from_regex(r"\A[A-Z][A-Z0-9_]{0,10}\Z").filter(
        lambda n: not n.startswith("DAS__")
    )
    env.update(draw(st.dictionaries(unrelated, st.text(max_size=5), max_size=3)))
    return fields, file_values, env


@given(_precedence_cases())
def test_resolve_prefers_env_then_file_then_absent(case: tuple[list, dict, dict]) -> None:
    fields, file_values, env = case
    file_before, env_before = copy.deepcopy(file_values), dict(env)

    resolved = resolve(file_values, env)

    for path, file_val, env_val in fields:
        expected = env_val if env_val is not _MISSING else file_val
        # _MISSING means "left out so the schema default applies".
        assert _get(resolved, path) == expected
    assert file_values == file_before and env == env_before


# End-to-end through the schema: integer LimitsConfig fields have real defaults.
# base_backoff/max_backoff are excluded because of their cross-field constraint.
_INT_LIMITS = {
    name: info
    for name, info in LimitsConfig.model_fields.items()
    if info.annotation is int
}


def _in_range(name: str) -> st.SearchStrategy[int]:
    lo = hi = None
    for m in _INT_LIMITS[name].metadata:
        lo = getattr(m, "ge", lo)
        hi = getattr(m, "le", hi)
    return st.integers(min_value=lo, max_value=hi)


@st.composite
def _limit_cases(draw: st.DrawFn) -> dict[str, tuple[Any, Any]]:
    names = draw(st.lists(st.sampled_from(sorted(_INT_LIMITS)), min_size=1, unique=True))
    return {
        n: (
            draw(_in_range(n)) if draw(st.booleans()) else _MISSING,
            draw(_in_range(n)) if draw(st.booleans()) else _MISSING,
        )
        for n in names
    }


@given(_limit_cases())
def test_validated_config_uses_env_then_file_then_default(
    case: dict[str, tuple[Any, Any]],
) -> None:
    file_values: dict[str, Any] = {"role": "gateway", "redis": {"url": "redis://r:6379"}}
    env = {"DAS__API_KEYS": "k1"}
    for name, (file_val, env_val) in case.items():
        if file_val is not _MISSING:
            _set(file_values, ("limits", name), file_val)
        if env_val is not _MISSING:
            env[_env_name(("limits", name))] = str(env_val)

    config = validate_config(resolve(file_values, env))

    for name, (file_val, env_val) in case.items():
        if env_val is not _MISSING:
            expected = env_val
        elif file_val is not _MISSING:
            expected = file_val
        else:
            expected = _INT_LIMITS[name].default
        assert getattr(config.limits, name) == expected
