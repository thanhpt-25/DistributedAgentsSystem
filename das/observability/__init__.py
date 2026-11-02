"""Structured logging, redaction, and context binding (Requirements 12.7, 13)."""

from das.observability.context import (
    bind_correlation_id,
    bind_job,
    bind_request,
    current_context,
    get_job_id,
    get_request_id,
)
from das.observability.logging import (
    CONTENT_FIELDS,
    configure_logging,
    drop_content_fields,
    get_logger,
    get_redactor,
    register_secret,
)
from das.observability.redaction import REDACTION_MARKER, Redactor

__all__ = [
    "CONTENT_FIELDS",
    "REDACTION_MARKER",
    "Redactor",
    "bind_correlation_id",
    "bind_job",
    "bind_request",
    "configure_logging",
    "current_context",
    "drop_content_fields",
    "get_job_id",
    "get_logger",
    "get_redactor",
    "get_request_id",
    "register_secret",
]
