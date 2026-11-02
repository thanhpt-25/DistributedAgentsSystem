from __future__ import annotations

import pytest

from das.clock import Clock, FakeClock, SystemClock


def test_both_clocks_satisfy_protocol() -> None:
    assert isinstance(SystemClock(), Clock)
    assert isinstance(FakeClock(), Clock)


async def test_fake_sleep_advances_time_and_records(fake_clock: FakeClock) -> None:
    start_now, start_mono = fake_clock.now(), fake_clock.monotonic()
    await fake_clock.sleep(2.5)
    await fake_clock.sleep(-1)  # negative counts as zero
    assert fake_clock.sleeps == [2.5, 0.0]
    assert fake_clock.now() == start_now + 2.5
    assert fake_clock.monotonic() == start_mono + 2.5


def test_fake_clock_cannot_go_backwards(fake_clock: FakeClock) -> None:
    fake_clock.advance(1)
    with pytest.raises(ValueError):
        fake_clock.advance(-0.1)


async def test_system_clock_sleep_zero_returns() -> None:
    clock = SystemClock()
    before = clock.monotonic()
    await clock.sleep(0)
    assert clock.monotonic() >= before
