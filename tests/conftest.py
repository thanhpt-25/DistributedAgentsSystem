"""Shared pytest configuration and fixtures.

Hypothesis profiles:

- ``default``: at least 100 examples per property (design: Testing Strategy).
- ``ci``: 200 examples, derandomized for reproducible CI runs.

Select a profile with ``HYPOTHESIS_PROFILE=ci`` (defaults to ``default``).
"""

from __future__ import annotations

import os
import re
from collections.abc import Generator

import pytest
from hypothesis import HealthCheck, settings

from das.clock import FakeClock
from tests.fixtures.redis_time import (  # noqa: F401  (re-exported pytest fixtures)
    fake_redis_time,
    fake_redis_time_factory,
)

settings.register_profile(
    "default",
    max_examples=100,
    deadline=None,
)
settings.register_profile(
    "ci",
    max_examples=200,
    derandomize=True,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
    print_blob=True,
)
settings.load_profile(os.environ.get("HYPOTHESIS_PROFILE", "default"))


REQUIRE_DOCKER_ENV = "DAS_REQUIRE_DOCKER"
_DOCKER_SKIP = re.compile(r"\bdocker\b", re.IGNORECASE)


def _skip_reason(report: pytest.TestReport) -> str | None:
    """Return the message of a skip report (``longrepr`` is ``(path, lineno, msg)``)."""
    longrepr = report.longrepr
    if isinstance(longrepr, tuple) and len(longrepr) == 3:
        return str(longrepr[2])
    if isinstance(longrepr, str):
        return longrepr
    return None


@pytest.hookimpl(wrapper=True)
def pytest_runtest_makereport(
    item: pytest.Item, call: pytest.CallInfo[None]
) -> Generator[None, pytest.TestReport, pytest.TestReport]:
    """With ``DAS_REQUIRE_DOCKER=1``, turn Docker-related skips into failures.

    Applies to every phase (setup/call/teardown), so a skip raised inside a
    module-scoped fixture fails each test that depends on it. Skips whose
    reason does not mention Docker (e.g. busy ports) and xfails are untouched.
    """
    report = yield
    if os.environ.get(REQUIRE_DOCKER_ENV) != "1" or not report.skipped:
        return report
    if hasattr(report, "wasxfail"):
        return report
    reason = _skip_reason(report)
    if reason is not None and _DOCKER_SKIP.search(reason):
        report.outcome = "failed"
        report.longrepr = f"{REQUIRE_DOCKER_ENV}=1 but the test was skipped: {reason}"
    return report


@pytest.fixture
def fake_clock() -> FakeClock:
    """A deterministic clock whose ``sleep`` advances time instantly."""
    return FakeClock()


@pytest.fixture
async def fake_redis():
    """An in-process fakeredis client with Lua support (fresh server per test)."""
    from fakeredis import FakeServer
    from fakeredis.aioredis import FakeRedis

    client = FakeRedis(server=FakeServer(), decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
async def state_store(fake_redis):
    """A State_Store backed by ``fake_redis`` running the real Lua scripts."""
    from das.state import StateStore

    return StateStore(fake_redis)
