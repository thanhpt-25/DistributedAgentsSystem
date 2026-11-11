"""Typed errors (Phase 4 Req 9.5).

Messages carry the HTTP status and the server's error code, never a request body, prompt or key.
"""

from __future__ import annotations

from typing import Any

_MAX_CODE_IN_MESSAGE = 80


def _describe(status_code: int | None, code: str | None) -> str:
    parts = []
    if status_code is not None:
        parts.append(f"HTTP {status_code}")
    if code:
        parts.append(code[:_MAX_CODE_IN_MESSAGE])
    return " ".join(parts) or "DAS error"


class DasError(Exception):
    """Base of every error the client raises on purpose.

    ``status_code`` is ``None`` for failures with no HTTP response (transport, malformed reply);
    ``code`` is the server's ``error`` string and ``body`` its parsed JSON object (``{}`` if unusable).
    """

    def __init__(
        self,
        message: str | None = None,
        *,
        status_code: int | None = None,
        code: str | None = None,
        body: dict[str, Any] | None = None,
    ) -> None:
        self.status_code = status_code
        self.code = code
        self.body: dict[str, Any] = body if body is not None else {}
        super().__init__(message or _describe(status_code, code))


class AuthError(DasError):
    """401 or 403: the API key is missing, wrong, or not allowed to do this."""


class NotFound(DasError):
    """404: no such Request."""


class Conflict(DasError):
    """409: the Request is in a state that does not allow this (for example cancelling a finished one)."""


class IdempotencyKeyReused(DasError):
    """422 ``idempotency_key_reused``: the key was used before with a different prompt or budget."""


class BudgetAboveLimit(DasError):
    """400 ``budget_above_limit``: the budget asks for more than this key's default allows."""

    def __init__(self, message: str | None = None, *, parameter: str | None = None, limit: Any = None, **kw: Any):
        super().__init__(message, **kw)
        self.parameter = parameter
        self.limit = limit


class InvalidRequest(DasError):
    """Any other 400: the request is malformed. ``parameter`` names the field when the server says which."""

    def __init__(self, message: str | None = None, *, parameter: str | None = None, **kw: Any):
        super().__init__(message, **kw)
        self.parameter = parameter


class RateLimited(DasError):
    """429 after the retries: the rate limit or a stream cap. ``retry_after`` is the server's wait in seconds."""

    def __init__(self, message: str | None = None, *, retry_after: float | None = None, **kw: Any):
        super().__init__(message, **kw)
        self.retry_after = retry_after


class QuotaExceeded(DasError):
    """429 ``quota_exceeded`` after the retries: too many active Requests for the key. ``limit`` is its maximum."""

    def __init__(self, message: str | None = None, *, limit: int | None = None, **kw: Any):
        super().__init__(message, **kw)
        self.limit = limit


class ServiceUnavailable(DasError):
    """503 after the retries."""


class TransportFailure(DasError):
    """The server could not be reached, or the connection broke, on every attempt. ``__cause__`` is the error."""


class WaitTimeout(DasError, TimeoutError):
    """:meth:`wait` ran out of time before the Request finished. Also a built-in ``TimeoutError``."""


class InvalidSignature(DasError):
    """A webhook delivery that cannot be trusted: bad signature, timestamp outside the tolerance, or not an event.

    The message never contains the secret, the signature or the body.
    """
