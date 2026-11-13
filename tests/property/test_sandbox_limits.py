"""For any limits a configuration could ask for, the command never exceeds a ceiling, never goes below a floor, and
never contains a flag the requirement forbids (das-smarter-agents task 6.1; Phase 6 Req 6.4, 6.5)."""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from das.sandbox.command import build_command
from das.sandbox.limits import CEILINGS, FLOORS, Limits

finite = st.floats(allow_nan=False, allow_infinity=False, min_value=-1e12, max_value=1e12)
whole = st.integers(min_value=-(10**12), max_value=10**12)
FORBIDDEN = frozenset({
    "--privileged", "--cap-add", "--device", "--volume", "--mount", "--pid", "--ipc", "--userns", "--add-host",
    "--publish", "--volumes-from", "--env-file", "--cgroupns", "--group-add",
})


def value_after(argv: list[str], flag: str) -> str:
    return argv[argv.index(flag) + 1]


@settings(max_examples=300)
@given(timeout=finite, memory_mb=whole, cpus=finite, pids=whole, output_bytes=whole)
def test_any_limits_are_clamped_into_their_range(
    timeout: float, memory_mb: int, cpus: float, pids: int, output_bytes: int
) -> None:
    limits = Limits(timeout=timeout, memory_mb=memory_mb, cpus=cpus, pids=pids, output_bytes=output_bytes)
    for name in CEILINGS:
        assert FLOORS[name] <= getattr(limits, name) <= CEILINGS[name]


@settings(max_examples=300)
@given(timeout=finite, memory_mb=whole, cpus=finite, pids=whole, output_bytes=whole)
def test_the_command_for_any_limits_stays_within_the_ceilings_and_has_no_forbidden_flag(
    timeout: float, memory_mb: int, cpus: float, pids: int, output_bytes: int
) -> None:
    limits = Limits(timeout=timeout, memory_mb=memory_mb, cpus=cpus, pids=pids, output_bytes=output_bytes)
    argv = build_command("docker", "das-sbx-0123456789abcdef", "python:3.12-slim", limits)
    assert 64 <= int(value_after(argv, "--memory").removesuffix("m")) <= 1024
    assert 0.1 <= float(value_after(argv, "--cpus")) <= 2.0
    assert 16 <= int(value_after(argv, "--pids-limit")) <= 256
    assert value_after(argv, "--network") == "none" and value_after(argv, "--cap-drop") == "ALL"
    assert not any(a in FORBIDDEN or a.startswith(tuple(f"{f}=" for f in FORBIDDEN)) for a in argv)


@settings(max_examples=100)
@given(st.integers(min_value=0, max_value=10**6))
def test_limits_inside_their_range_are_kept_exactly(memory_mb: int) -> None:
    if FLOORS["memory_mb"] <= memory_mb <= CEILINGS["memory_mb"]:
        assert Limits(memory_mb=memory_mb).memory_mb == memory_mb
