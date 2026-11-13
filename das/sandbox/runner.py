"""Runs one snippet of Python in a throwaway container (Phase 6 Req 6.2, 6.4, 6.6, 6.7).

A run: start the container (:func:`das.sandbox.command.build_command`), send the code on standard input, read the output
(stdout and stderr merged) keeping the first ``output_bytes`` and discarding the rest, and wait for the exit, all inside
the wall-clock timeout. On a timeout, or if the caller is cancelled, the container is killed by name. The container is
force-removed in every path (``finally``); a removal that fails is logged with the container name. The code, its output
and its errors are never logged; the outcome, exit code, duration and sizes are.

The container CLI is started without a shell, with arguments only, and ``spawn`` is injectable so every path is tested
without Docker. Outcomes: ``ok`` (exit 0), ``error`` (any other exit, or the CLI could not start), ``timeout`` and
``killed`` (exit 137: the kernel's OOM killer or an outside ``kill``).
"""

from __future__ import annotations

import asyncio
import codecs
import contextlib
import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Final

from das.clock import Clock, SystemClock
from das.errors import DependencyUnreachable
from das.observability import get_logger
from das.sandbox.command import build_command
from das.sandbox.limits import Limits

log = get_logger("das.sandbox.runner")

#: The most code one call may send (Req 6.3: the model's code arrives as a tool argument).
MAX_CODE_BYTES: Final = 65_536
#: How long the startup check, a kill and a removal may each take.
CHECK_TIMEOUT: Final = 15.0
CLEANUP_TIMEOUT: Final = 15.0
KILLED_EXIT: Final = 137
_READ_CHUNK: Final = 65_536
_GONE: Final = re.compile(r"no such (container|object)|no container with name")

Spawn = Callable[..., Awaitable[Any]]
RunObserver = Callable[[str], None]


@dataclass(frozen=True, slots=True)
class RunResult:
    """What one run produced: ``outcome``, the exit code (``None`` when there was none), the kept ``output``, whether
    more was produced than kept, and the seconds it took."""

    outcome: str
    exit_code: int | None
    output: str
    truncated: bool
    duration: float


class _Output:
    """Keeps the first ``limit`` bytes of what a run prints and counts the rest."""

    def __init__(self, limit: int) -> None:
        self._limit = limit
        self.kept = bytearray()
        self.total = 0

    def add(self, chunk: bytes) -> None:
        self.total += len(chunk)
        room = self._limit - len(self.kept)
        if room > 0:
            self.kept += chunk[:room]

    @property
    def truncated(self) -> bool:
        return self.total > self._limit

    def text(self) -> str:
        """The kept bytes as text. Invalid bytes become a replacement mark; when the output was cut, a character the cut
        split is dropped rather than shown as one."""
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        return decoder.decode(bytes(self.kept), final=not self.truncated)


def _outcome(exit_code: int) -> str:
    if exit_code == 0:
        return "ok"
    return "killed" if exit_code == KILLED_EXIT else "error"


class SandboxRunner:
    """Runs snippets with fixed protections and the given :class:`Limits`."""

    def __init__(
        self,
        cli: str,
        image: str,
        limits: Limits,
        *,
        oci_runtime: str | None = None,
        spawn: Spawn | None = None,
        clock: Clock | None = None,
        observer: RunObserver | None = None,
    ) -> None:
        build_command(cli, "das-sbx-" + "0" * 16, image, limits, oci_runtime=oci_runtime)  # refuse a bad setting now
        self._cli, self._image, self._limits, self._oci_runtime = cli, image, limits, oci_runtime
        self._spawn: Spawn = spawn if spawn is not None else asyncio.create_subprocess_exec
        self._clock: Clock = clock or SystemClock()
        self._observer = observer

    # ------------------------------------------------------------------ the container CLI

    async def _exec(self, timeout: float, *args: str) -> tuple[int, str]:
        """Run ``<cli> <args>`` to its end; its exit code and (at most 4 KiB of) output."""
        process = await self._spawn(
            self._cli, *args, stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            async with asyncio.timeout(timeout):
                raw = bytearray()
                while chunk := await process.stdout.read(4096):
                    raw += chunk[: max(0, 4096 - len(raw))]
                return await process.wait(), bytes(raw).decode(errors="replace")
        except BaseException:
            with contextlib.suppress(ProcessLookupError):
                process.kill()
            raise

    async def check(self) -> None:
        """The container runtime answers, or :class:`DependencyUnreachable` names it (exit code 4, Req 6.2)."""
        try:
            code, _ = await self._exec(CHECK_TIMEOUT, "info")
        except TimeoutError:
            raise DependencyUnreachable(self._cli, "it did not answer in time") from None
        except OSError:
            raise DependencyUnreachable(self._cli, "it cannot be run") from None
        if code != 0:
            raise DependencyUnreachable(self._cli, f"it answered with exit code {code}")

    async def _kill(self, name: str, process: Any) -> None:
        with contextlib.suppress(OSError, TimeoutError):
            await self._exec(CLEANUP_TIMEOUT, "kill", name)
        with contextlib.suppress(ProcessLookupError):
            process.kill()

    async def _remove(self, name: str) -> None:
        """Force-remove the container; a failure is logged with its name and never raised (Req 6.6)."""
        try:
            code, text = await self._exec(CLEANUP_TIMEOUT, "rm", "-f", name)
        except (OSError, TimeoutError):
            log.warning("sandbox container not removed", container=name)
            return
        if code != 0 and _GONE.search(text.lower()) is None:  # --rm may already have removed it
            log.warning("sandbox container not removed", container=name, exit_code=code)

    # ------------------------------------------------------------------ a run

    async def _feed(self, process: Any, data: bytes) -> None:
        try:
            process.stdin.write(data)
            await process.stdin.drain()
        except (BrokenPipeError, ConnectionResetError):
            pass  # the container ended before reading its input; its exit code says why
        finally:
            with contextlib.suppress(Exception):
                process.stdin.close()

    async def _collect(self, process: Any, output: _Output) -> None:
        while chunk := await process.stdout.read(_READ_CHUNK):
            output.add(chunk)

    async def run(self, code: str) -> RunResult:
        """Run ``code``; raises ``TypeError`` or ``ValueError`` for code that is not acceptable, nothing else."""
        if not isinstance(code, str):
            raise TypeError("the code must be text")
        data = code.encode()
        if len(data) > MAX_CODE_BYTES:
            raise ValueError(f"the code is too large (at most {MAX_CODE_BYTES} bytes)")
        name = f"das-sbx-{uuid.uuid4().hex[:16]}"
        argv = build_command(self._cli, name, self._image, self._limits, oci_runtime=self._oci_runtime)
        output = _Output(self._limits.output_bytes)
        start = self._clock.monotonic()
        outcome, exit_code, process, running = "error", None, None, False
        try:
            try:
                process = await self._spawn(
                    *argv, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                )
            except OSError:
                process = None
            if process is not None:
                running = True
                try:
                    async with asyncio.timeout(self._limits.timeout):
                        await asyncio.gather(self._feed(process, data), self._collect(process, output))
                        exit_code = await process.wait()
                    running = False
                    outcome = _outcome(exit_code)
                except TimeoutError:
                    outcome = "timeout"
        finally:
            if running:  # a timeout, or the caller was cancelled
                await self._kill(name, process)
            await self._remove(name)
        duration = self._clock.monotonic() - start
        self._finished(outcome, exit_code, output, duration)
        return RunResult(outcome, exit_code, output.text(), output.truncated, duration)

    def _finished(self, outcome: str, exit_code: int | None, output: _Output, duration: float) -> None:
        log.info(
            "sandbox run finished", outcome=outcome, exit_code=exit_code, duration_ms=int(duration * 1000),
            output_bytes=len(output.kept), truncated=output.truncated,
        )
        if self._observer is not None:
            try:
                self._observer(outcome)
            except Exception:  # noqa: BLE001 - an observer must never break a run
                log.warning("sandbox observer failed", exc_info=True)


__all__ = ["CHECK_TIMEOUT", "MAX_CODE_BYTES", "RunResult", "SandboxRunner"]
