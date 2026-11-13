"""The SandboxRunner (das-smarter-agents task 6.2; Phase 6 Req 6.2, 6.6, 6.7, 6.4): one container per run, killed on
a timeout, removed on every path, output capped, and neither the code nor its output ever logged. No Docker: a fake
``spawn`` stands in for the container CLI."""

from __future__ import annotations

import asyncio
import re
from typing import Any

import pytest

import das.sandbox.runner as runner_module
from das.clock import FakeClock
from das.errors import DependencyUnreachable
from das.sandbox.command import build_command
from das.sandbox.limits import Limits
from das.sandbox.runner import MAX_CODE_BYTES, SandboxRunner

NAME = re.compile(r"^das-sbx-[0-9a-f]{16}$")
IMAGE = "python:3.12-slim"


class FakeStdin:
    def __init__(self, broken: bool = False) -> None:
        self.data = b""
        self.closed = False
        self._broken = broken

    def write(self, data: bytes) -> None:
        if self._broken:
            raise BrokenPipeError
        self.data += data

    async def drain(self) -> None:
        if self._broken:
            raise ConnectionResetError

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        return None


class FakeStdout:
    def __init__(self, data: bytes, hang: bool, chunk: int = 4096) -> None:
        self._data, self._hang, self._chunk = data, hang, chunk
        self.bytes_read = 0

    async def read(self, n: int = -1) -> bytes:
        if self._hang:
            await asyncio.Event().wait()
        size = min(n, self._chunk) if n > 0 else self._chunk
        piece, self._data = self._data[:size], self._data[size:]
        self.bytes_read += len(piece)
        return piece


class FakeProcess:
    def __init__(self, output: bytes = b"", code: int = 0, hang: bool = False, broken_stdin: bool = False) -> None:
        self.stdin = FakeStdin(broken_stdin)
        self.stdout = FakeStdout(output, hang)
        self.returncode: int | None = None
        self._code, self._hang = code, hang
        self.killed = False

    async def wait(self) -> int:
        if self._hang and not self.killed:
            await asyncio.Event().wait()
        self.returncode = self._code
        return self._code

    def kill(self) -> None:
        self.killed = True
        self._hang = False


class Spawner:
    """Records every spawn; the ``run`` is scripted, ``kill``/``rm``/``info`` answer from simple settings."""

    def __init__(self, run: FakeProcess | BaseException | None = None) -> None:
        self.run_process = run if run is not None else FakeProcess()
        self.calls: list[tuple[tuple[str, ...], dict[str, Any]]] = []
        self.rm_result: tuple[int, bytes] | BaseException = (0, b"")
        self.kill_result: tuple[int, bytes] | BaseException = (0, b"")
        self.info_result: tuple[int, bytes] | BaseException = (0, b"")

    async def __call__(self, *argv: str, **kwargs: Any) -> FakeProcess:
        self.calls.append((argv, kwargs))
        verb = argv[1]
        if verb == "run":
            if isinstance(self.run_process, BaseException):
                raise self.run_process
            return self.run_process
        outcome = {"rm": self.rm_result, "kill": self.kill_result, "info": self.info_result}[verb]
        if isinstance(outcome, BaseException):
            raise outcome
        return FakeProcess(outcome[1], outcome[0])

    def verbs(self) -> list[str]:
        return [argv[1] for argv, _ in self.calls]

    def argv_of(self, verb: str) -> tuple[str, ...]:
        return next(argv for argv, _ in self.calls if argv[1] == verb)

    def name_of_run(self) -> str:
        argv = self.argv_of("run")
        return argv[argv.index("--name") + 1]


class LogSpy:
    def __init__(self) -> None:
        self.entries: list[tuple[str, str, dict[str, Any]]] = []

    def __getattr__(self, level: str) -> Any:
        return lambda event, **fields: self.entries.append((level, event, fields))

    def warnings(self) -> list[dict[str, Any]]:
        return [fields for level, _, fields in self.entries if level == "warning"]


def make(
    spawner: Spawner, *, limits: Limits | None = None, runtime: str | None = None, observer: Any = None
) -> SandboxRunner:
    return SandboxRunner(
        "docker", IMAGE, limits or Limits(), oci_runtime=runtime, spawn=spawner, clock=FakeClock(), observer=observer
    )


# --------------------------------------------------------------------------- a run


async def test_a_run_sends_the_code_on_stdin_and_returns_its_output_and_exit_code() -> None:
    spawner = Spawner(FakeProcess(b"hello\n", 0))
    result = await make(spawner).run("print('hello')")
    assert (result.outcome, result.exit_code, result.output, result.truncated) == ("ok", 0, "hello\n", False)
    assert spawner.run_process.stdin.data == b"print('hello')" and spawner.run_process.stdin.closed


async def test_the_container_is_started_with_exactly_the_command_the_builder_makes() -> None:
    spawner = Spawner()
    await make(spawner, runtime="runsc").run("1")
    name = spawner.name_of_run()
    assert NAME.fullmatch(name)
    assert list(spawner.argv_of("run")) == build_command("docker", name, IMAGE, Limits(), oci_runtime="runsc")


async def test_the_process_is_started_without_a_shell_and_with_stderr_merged_into_stdout() -> None:
    spawner = Spawner()
    await make(spawner).run("1")
    [(_, kwargs)] = [c for c in spawner.calls if c[0][1] == "run"]
    assert kwargs["stdin"] == asyncio.subprocess.PIPE and kwargs["stdout"] == asyncio.subprocess.PIPE
    assert kwargs["stderr"] == asyncio.subprocess.STDOUT and "shell" not in kwargs and "env" not in kwargs


async def test_a_failing_program_is_an_error_with_its_exit_code_and_output() -> None:
    result = await make(Spawner(FakeProcess(b"Traceback...\n", 1))).run("1/0")
    assert (result.outcome, result.exit_code, result.output) == ("error", 1, "Traceback...\n")


async def test_exit_code_137_means_the_container_was_killed() -> None:
    result = await make(Spawner(FakeProcess(b"", 137))).run("x = bytearray(10**10)")
    assert (result.outcome, result.exit_code) == ("killed", 137)


async def test_every_run_has_its_own_container_name() -> None:
    spawner = Spawner()
    runner = make(spawner)
    await asyncio.gather(runner.run("1"), runner.run("2"), runner.run("3"))
    names = [argv[argv.index("--name") + 1] for argv, _ in spawner.calls if argv[1] == "run"]
    assert len(set(names)) == 3


async def test_the_duration_is_measured_with_the_injected_clock() -> None:
    clock = FakeClock()

    class Slow(FakeProcess):
        async def wait(self) -> int:
            clock.advance(2.5)
            return await super().wait()

    runner = SandboxRunner("docker", IMAGE, Limits(), spawn=Spawner(Slow()), clock=clock)
    assert (await runner.run("1")).duration == 2.5


async def test_the_observer_is_told_the_outcome_once_per_run() -> None:
    seen: list[str] = []
    await make(Spawner(FakeProcess(b"", 0)), observer=seen.append).run("1")
    await make(Spawner(FakeProcess(b"", 1)), observer=seen.append).run("1")
    await make(Spawner(FakeProcess(b"", 137)), observer=seen.append).run("1")
    assert seen == ["ok", "error", "killed"]


# --------------------------------------------------------------------------- bad input


@pytest.mark.parametrize("bad", [None, 5, b"print(1)", ["print(1)"]])
async def test_code_that_is_not_text_is_refused_before_anything_starts(bad: object) -> None:
    spawner = Spawner()
    with pytest.raises(TypeError):
        await make(spawner).run(bad)  # type: ignore[arg-type]
    assert spawner.calls == []


async def test_code_over_the_size_limit_is_refused_before_anything_starts() -> None:
    spawner = Spawner()
    with pytest.raises(ValueError, match="too large"):
        await make(spawner).run("x" * (MAX_CODE_BYTES + 1))
    assert spawner.calls == []
    await make(spawner).run("x" * MAX_CODE_BYTES)  # exactly the limit is accepted


async def test_the_size_limit_counts_bytes_not_characters() -> None:
    with pytest.raises(ValueError, match="too large"):
        await make(Spawner()).run("é" * (MAX_CODE_BYTES // 2 + 1))


async def test_a_container_that_exits_before_reading_its_input_is_not_an_exception() -> None:
    result = await make(Spawner(FakeProcess(b"boom", 1, broken_stdin=True))).run("1")
    assert (result.outcome, result.output) == ("error", "boom")


# --------------------------------------------------------------------------- the output cap (Req 6.4)


async def test_output_over_the_cap_is_cut_and_marked_and_still_drained() -> None:
    process = FakeProcess(b"a" * 100_000, 0)
    result = await make(Spawner(process), limits=Limits(output_bytes=1000)).run("print('a' * 10**5)")
    assert result.truncated and len(result.output.encode()) == 1000 and result.output == "a" * 1000
    assert process.stdout.bytes_read == 100_000  # read to the end, so the program is not blocked on a full pipe


async def test_output_exactly_at_the_cap_is_not_marked_truncated() -> None:
    result = await make(Spawner(FakeProcess(b"b" * 1000, 0)), limits=Limits(output_bytes=1000)).run("1")
    assert not result.truncated and len(result.output) == 1000


async def test_output_one_byte_over_the_cap_is_marked_truncated() -> None:
    result = await make(Spawner(FakeProcess(b"b" * 1001, 0)), limits=Limits(output_bytes=1000)).run("1")
    assert result.truncated and len(result.output) == 1000


async def test_the_cap_never_splits_a_character_and_invalid_bytes_are_replaced() -> None:
    result = await make(Spawner(FakeProcess("é".encode() * 600, 0)), limits=Limits(output_bytes=999)).run("1")
    assert result.truncated and result.output == "é" * 499  # 998 bytes: the odd byte is dropped, not half a character
    odd = await make(Spawner(FakeProcess(b"ok \xff\xfe end", 0))).run("1")
    assert odd.output.startswith("ok ") and odd.output.endswith(" end") and "�" in odd.output


# --------------------------------------------------------------------------- the timeout (Req 6.4, 6.6)


async def test_a_run_that_takes_too_long_is_killed_by_name_and_reported_as_a_timeout() -> None:
    spawner = Spawner(FakeProcess(hang=True))
    result = await make(spawner, limits=Limits(timeout=1.0)).run("while True: pass")
    assert (result.outcome, result.exit_code) == ("timeout", None)
    name = spawner.name_of_run()
    assert spawner.argv_of("kill") == ("docker", "kill", name)
    assert spawner.run_process.killed  # the CLI process itself is killed too
    assert spawner.argv_of("rm") == ("docker", "rm", "-f", name)
    assert spawner.verbs().index("kill") < spawner.verbs().index("rm")


async def test_output_produced_before_the_timeout_is_kept() -> None:
    class Chatty(FakeStdout):
        async def read(self, n: int = -1) -> bytes:
            if not self._data:
                await asyncio.Event().wait()  # then nothing more, and the program never ends
            return await super().read(n)

    process = FakeProcess()
    process.stdout = Chatty(b"partial output", False)

    async def never_ends() -> int:
        await asyncio.Event().wait()
        return 0

    process.wait = never_ends  # type: ignore[method-assign]
    result = await make(Spawner(process), limits=Limits(timeout=1.0)).run("1")
    assert result.outcome == "timeout" and result.output == "partial output"


# --------------------------------------------------------------------------- always removed (Req 6.6)


@pytest.mark.parametrize("code", [0, 1, 137])
async def test_the_container_is_force_removed_after_every_finished_run(code: int) -> None:
    spawner = Spawner(FakeProcess(b"", code))
    await make(spawner).run("1")
    assert spawner.argv_of("rm") == ("docker", "rm", "-f", spawner.name_of_run())


async def test_the_container_is_removed_when_the_container_cli_cannot_be_started() -> None:
    spawner = Spawner(FileNotFoundError("docker"))
    result = await make(spawner).run("1")
    assert (result.outcome, result.exit_code, result.output) == ("error", None, "")
    assert "rm" in spawner.verbs()


async def test_the_container_is_removed_when_the_caller_is_cancelled() -> None:
    spawner = Spawner(FakeProcess(hang=True))
    task = asyncio.create_task(make(spawner, limits=Limits(timeout=60.0)).run("while True: pass"))
    while "run" not in spawner.verbs():
        await asyncio.sleep(0)
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert "rm" in spawner.verbs() and "kill" in spawner.verbs()


async def test_a_container_that_cannot_be_removed_is_logged_by_name_and_does_not_fail_the_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spy = LogSpy()
    monkeypatch.setattr(runner_module, "log", spy)
    spawner = Spawner(FakeProcess(b"fine", 0))
    spawner.rm_result = (1, b"Error response from daemon: something is wrong")
    result = await make(spawner).run("1")
    assert result.outcome == "ok" and result.output == "fine"
    assert [w["container"] for w in spy.warnings()] == [spawner.name_of_run()]


@pytest.mark.parametrize("gone", [b"Error: No such container: x", b"Error: no container with name or ID x found"])
async def test_a_container_that_is_already_gone_is_not_a_warning(monkeypatch: pytest.MonkeyPatch, gone: bytes) -> None:
    spy = LogSpy()
    monkeypatch.setattr(runner_module, "log", spy)
    spawner = Spawner(FakeProcess(b"", 0))
    spawner.rm_result = (1, gone)  # --rm already removed it
    await make(spawner).run("1")
    assert spy.warnings() == []


async def test_a_removal_that_cannot_even_start_is_logged_by_name(monkeypatch: pytest.MonkeyPatch) -> None:
    spy = LogSpy()
    monkeypatch.setattr(runner_module, "log", spy)
    spawner = Spawner(FakeProcess(b"", 0))
    spawner.rm_result = OSError("no docker")
    result = await make(spawner).run("1")
    assert result.outcome == "ok"
    assert [w["container"] for w in spy.warnings()] == [spawner.name_of_run()]


# --------------------------------------------------------------------------- what is logged (Req 6.7)


async def test_the_code_and_the_output_are_never_logged_but_the_exit_code_duration_and_sizes_are(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spy = LogSpy()
    monkeypatch.setattr(runner_module, "log", spy)
    await make(Spawner(FakeProcess(b"SECRET-OUTPUT-77c1\n", 3))).run("print('SECRET-CODE-9f3a')")
    text = repr(spy.entries)
    assert "SECRET-CODE-9f3a" not in text and "SECRET-OUTPUT-77c1" not in text
    [finished] = [f for _, event, f in spy.entries if event == "sandbox run finished"]
    assert finished["exit_code"] == 3 and finished["output_bytes"] == 19 and "duration_ms" in finished
    assert finished["outcome"] == "error" and finished["truncated"] is False


async def test_a_timeout_logs_no_code_either(monkeypatch: pytest.MonkeyPatch) -> None:
    spy = LogSpy()
    monkeypatch.setattr(runner_module, "log", spy)
    await make(Spawner(FakeProcess(hang=True)), limits=Limits(timeout=1.0)).run("SECRET_LOOP = 1")
    assert "SECRET_LOOP" not in repr(spy.entries)


# --------------------------------------------------------------------------- the startup check (Req 6.2)


async def test_a_runtime_that_answers_passes_the_check() -> None:
    spawner = Spawner()
    await make(spawner).check()
    assert spawner.argv_of("info")[:2] == ("docker", "info")


async def test_a_runtime_that_fails_the_check_stops_the_worker_with_exit_code_4_naming_it() -> None:
    spawner = Spawner()
    spawner.info_result = (1, b"Cannot connect to the Docker daemon")
    with pytest.raises(DependencyUnreachable) as info:
        await make(spawner).check()
    assert info.value.exit_code == 4 and "docker" in str(info.value)


async def test_a_container_cli_that_is_not_installed_fails_the_check_the_same_way() -> None:
    spawner = Spawner()
    spawner.info_result = FileNotFoundError("docker")
    with pytest.raises(DependencyUnreachable) as info:
        await make(spawner).check()
    assert info.value.exit_code == 4 and "docker" in str(info.value)


async def test_a_runtime_that_never_answers_fails_the_check(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(runner_module, "CHECK_TIMEOUT", 0.05)

    class Silent(Spawner):
        async def __call__(self, *argv: str, **kwargs: Any) -> FakeProcess:
            return FakeProcess(hang=True)

    with pytest.raises(DependencyUnreachable, match="in time"):
        await make(Silent()).check()


async def test_an_observer_that_raises_never_breaks_a_run() -> None:
    def broken(outcome: str) -> None:
        raise RuntimeError("boom")

    result = await make(Spawner(FakeProcess(b"fine", 0)), observer=broken).run("1")
    assert (result.outcome, result.output) == ("ok", "fine")
