"""Tests for the ``DAS_REQUIRE_DOCKER`` hook in ``tests/conftest.py`` (Req 8.6).

Each test runs an inner pytest session (``pytester``) whose conftest re-exports
the real ``pytest_runtest_makereport`` hook, with a fake test module that skips
from a module-scoped fixture, like the integration ``broker`` fixtures do.
"""

from __future__ import annotations

import pytest

from tests.conftest import REQUIRE_DOCKER_ENV

pytest_plugins = ["pytester"]

_HOOK_CONFTEST = "from tests.conftest import pytest_runtest_makereport  # noqa: F401\n"

_FAKE_TESTS = """
import pytest


@pytest.fixture(scope="module")
def broker():
    pytest.skip({reason!r})


def test_one(broker):
    pass


def test_two(broker):
    pass
"""

_DOCKER_REASON = "Docker is not available; integration tests need a running daemon"
_PORT_REASON = "port 5672 is already in use on this host"


def _run(pytester: pytest.Pytester, reason: str) -> pytest.RunResult:
    pytester.makeconftest(_HOOK_CONFTEST)
    pytester.makepyfile(test_fake=_FAKE_TESTS.format(reason=reason))
    return pytester.runpytest("-p", "no:cacheprovider", "-rs")


def test_docker_skip_becomes_error_when_required(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(REQUIRE_DOCKER_ENV, "1")
    result = _run(pytester, _DOCKER_REASON)
    result.assert_outcomes(errors=2)
    assert result.ret == pytest.ExitCode.TESTS_FAILED
    result.stdout.fnmatch_lines([f"*{REQUIRE_DOCKER_ENV}=1 but the test was skipped*Docker is not available*"])


def test_docker_skip_stays_skipped_without_variable(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(REQUIRE_DOCKER_ENV, raising=False)
    result = _run(pytester, _DOCKER_REASON)
    result.assert_outcomes(skipped=2)
    assert result.ret == pytest.ExitCode.OK


def test_busy_port_skip_stays_skipped_when_required(pytester: pytest.Pytester, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(REQUIRE_DOCKER_ENV, "1")
    result = _run(pytester, _PORT_REASON)
    result.assert_outcomes(skipped=2)
    assert result.ret == pytest.ExitCode.OK
