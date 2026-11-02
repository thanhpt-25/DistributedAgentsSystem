"""Typed startup exceptions and their CLI exit codes.

The CLI maps these exceptions to process exit codes so every role reports
startup failures consistently:

=====  ===========================================
Code   Cause
=====  ===========================================
2      configuration error (``ConfigError``)
3      transport loader error (``TransportLoadError`` and subclasses)
4      dependency unreachable (``DependencyUnreachable``)
5      registration failure (``RegistrationFailed``)
=====  ===========================================

Any other exception maps to the generic failure code 1.

Error messages built here must never include credential values. Callers pass
field names and reasons only (Requirements 12.5, 12.6).
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Final

EXIT_OK: Final = 0
EXIT_GENERIC_FAILURE: Final = 1
EXIT_CONFIG_ERROR: Final = 2
EXIT_TRANSPORT_ERROR: Final = 3
EXIT_DEPENDENCY_UNREACHABLE: Final = 4
EXIT_REGISTRATION_FAILED: Final = 5


class DASError(Exception):
    """Base class for all DAS errors that carry a process exit code."""

    exit_code: int = EXIT_GENERIC_FAILURE


# --------------------------------------------------------------------------- config


@dataclass(frozen=True, slots=True)
class FieldError:
    """One invalid or missing configuration field.

    ``field`` is a dotted path such as ``limits.max_attempts``. ``reason``
    describes the failure and, for range errors, the permitted range. Neither
    may contain a secret value.
    """

    field: str
    reason: str

    def __str__(self) -> str:
        return f"{self.field}: {self.reason}"


class ConfigError(DASError):
    """Agent_Config is missing, unreadable, unparseable, or invalid (Req 12.2-12.6)."""

    exit_code = EXIT_CONFIG_ERROR

    def __init__(
        self,
        errors: Iterable[FieldError] = (),
        *,
        source: str | None = None,
        reason: str | None = None,
    ) -> None:
        self.errors: tuple[FieldError, ...] = tuple(errors)
        self.source = source
        self.reason = reason
        super().__init__(self._render())

    @property
    def fields(self) -> tuple[str, ...]:
        """Dotted paths of every invalid field, in reported order."""
        return tuple(e.field for e in self.errors)

    def _render(self) -> str:
        parts: list[str] = []
        if self.source is not None:
            parts.append(f"configuration source {self.source!r}")
        if self.reason is not None:
            parts.append(self.reason)
        head = ": ".join(parts) if parts else "invalid configuration"
        if not self.errors:
            return head
        lines = "\n".join(f"  - {e}" for e in self.errors)
        return f"{head}\n{lines}"


# --------------------------------------------------------------------------- transport


class TransportLoadError(DASError):
    """Base class for transport loader failures (Req 8.5, 15.5)."""

    exit_code = EXIT_TRANSPORT_ERROR


class TransportNotInstalled(TransportLoadError):
    """No adapter is registered under the requested transport name (Req 8.5, 15.3)."""

    def __init__(self, name: str) -> None:
        self.name = name
        super().__init__(f"transport implementation {name!r} is not installed")


class TransportIncomplete(TransportLoadError):
    """An installed adapter lacks one or more Transport operations (Req 15.5)."""

    def __init__(self, name: str, missing: Sequence[str]) -> None:
        self.name = name
        self.missing: tuple[str, ...] = tuple(missing)
        super().__init__(
            f"transport implementation {name!r} is missing operations: "
            + ", ".join(self.missing)
        )


# --------------------------------------------------------------------------- startup


class DependencyUnreachable(DASError):
    """A required dependency (RabbitMQ, Redis, ...) stayed unreachable (Req 14.7)."""

    exit_code = EXIT_DEPENDENCY_UNREACHABLE

    def __init__(self, dependency: str, reason: str | None = None) -> None:
        self.dependency = dependency
        self.reason = reason
        msg = f"dependency {dependency!r} is unreachable"
        if reason:
            msg = f"{msg}: {reason}"
        super().__init__(msg)


class RegistrationFailed(DASError):
    """The agent could not register in the Agent_Registry in time (Req 5.10)."""

    exit_code = EXIT_REGISTRATION_FAILED

    def __init__(self, agent_id: str, reason: str | None = None) -> None:
        self.agent_id = agent_id
        self.reason = reason
        msg = f"agent {agent_id!r} failed to register in the Agent_Registry"
        if reason:
            msg = f"{msg}: {reason}"
        super().__init__(msg)


class OpsPortUnavailable(DASError):
    """The Ops_Port (``health_port``) could not be bound at startup (Req 16.11)."""

    exit_code = EXIT_CONFIG_ERROR

    def __init__(self, port: int, reason: str | None = None) -> None:
        self.port = port
        self.reason = reason
        msg = f"cannot bind Ops_Port {port}"
        if reason:
            msg = f"{msg}: {reason}"
        super().__init__(msg)


def exit_code_for(exc: BaseException) -> int:
    """Return the process exit code for an exception raised during startup."""
    if isinstance(exc, DASError):
        return exc.exit_code
    return EXIT_GENERIC_FAILURE


__all__ = [
    "EXIT_CONFIG_ERROR",
    "EXIT_DEPENDENCY_UNREACHABLE",
    "EXIT_GENERIC_FAILURE",
    "EXIT_OK",
    "EXIT_REGISTRATION_FAILED",
    "EXIT_TRANSPORT_ERROR",
    "ConfigError",
    "DASError",
    "DependencyUnreachable",
    "FieldError",
    "OpsPortUnavailable",
    "RegistrationFailed",
    "TransportIncomplete",
    "TransportLoadError",
    "TransportNotInstalled",
    "exit_code_for",
]
