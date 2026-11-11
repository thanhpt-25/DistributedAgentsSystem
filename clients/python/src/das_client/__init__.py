"""Python client for the DAS Gateway API (Phase 4 Req 9).

Runs on Python 3.10 to 3.13, depends only on ``httpx`` and never imports the ``das`` server package.
"""

from __future__ import annotations

from .aclient import AsyncDasClient
from .client import DasClient
from .errors import (
    AuthError,
    BudgetAboveLimit,
    Conflict,
    DasError,
    IdempotencyKeyReused,
    InvalidRequest,
    InvalidSignature,
    NotFound,
    QuotaExceeded,
    RateLimited,
    ServiceUnavailable,
    TransportFailure,
    WaitTimeout,
)
from .models import (
    Budget,
    BudgetView,
    CancelResult,
    Event,
    JobView,
    Page,
    RequestSummary,
    RequestView,
    Submitted,
    WebhookState,
)
from .webhook import verify_webhook

__all__ = [
    "AsyncDasClient",
    "AuthError",
    "Budget",
    "BudgetAboveLimit",
    "BudgetView",
    "CancelResult",
    "Conflict",
    "DasClient",
    "DasError",
    "Event",
    "IdempotencyKeyReused",
    "InvalidRequest",
    "InvalidSignature",
    "JobView",
    "NotFound",
    "Page",
    "QuotaExceeded",
    "RateLimited",
    "RequestSummary",
    "RequestView",
    "ServiceUnavailable",
    "Submitted",
    "TransportFailure",
    "WaitTimeout",
    "WebhookState",
    "verify_webhook",
]
