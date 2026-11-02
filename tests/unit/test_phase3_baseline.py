"""Phase 3 contract matches the baseline (das-hardening-ci Requirement 11.1, 11.3).

Each top-level key of ``tests/fixtures/phase3_baseline/contract.json`` is compared
with the value computed from the current code. On mismatch the test fails with a
unified diff of the sorted-key JSON (baseline vs current).

If a change is intentional, regenerate the baseline::

    uv run python -m tests.fixtures.phase3_baseline.capture
"""

from __future__ import annotations

import difflib
import json
from collections.abc import Callable
from typing import Any

import pytest

from tests.fixtures.phase3_baseline import CONTRACT
from tests.fixtures.phase3_baseline import capture


def _dumps(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, ensure_ascii=True, indent=2)


@pytest.fixture(scope="module")
def baseline() -> dict[str, Any]:
    return json.loads(CONTRACT.read_text(encoding="utf-8"))


def _assert_matches(key: str, expected: Any, compute: Callable[[], Any]) -> None:
    # Round-trip through JSON so tuples, ints-as-keys etc. compare as stored.
    current = json.loads(_dumps(compute()))
    if current == expected:
        return
    diff = "\n".join(
        difflib.unified_diff(
            _dumps(expected).splitlines(),
            _dumps(current).splitlines(),
            fromfile=f"contract.json[{key}] (baseline)",
            tofile=f"{key} (current)",
            lineterm="",
        )
    )
    pytest.fail(f"{key} differs from the Phase 3 baseline:\n{diff}", pytrace=False)


_CAPTURES: dict[str, Callable[[], Any]] = {
    "metrics": capture.capture_metrics,
    "config_schema": capture.capture_config_schema,
    "env_vars": capture.capture_env_vars,
    "routes": capture.capture_routes,
    "exit_codes": capture.capture_exit_codes,
}


def test_contract_has_expected_keys(baseline: dict[str, Any]) -> None:
    assert sorted(baseline) == sorted(_CAPTURES)


@pytest.mark.parametrize("key", sorted(_CAPTURES))
def test_contract_matches_baseline(baseline: dict[str, Any], key: str) -> None:
    _assert_matches(key, baseline[key], _CAPTURES[key])
