"""The package imports, ships its type marker and never imports the server (Phase 4 Req 9.1)."""

from __future__ import annotations

import ast
from pathlib import Path

import das_client

SRC = Path(das_client.__file__).resolve().parent


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            names.add(node.module)
    return names


def test_no_module_imports_the_das_server_package() -> None:
    files = sorted(SRC.rglob("*.py"))
    assert files  # the scan must have something to look at
    for path in files:
        offending = {m for m in _imported_modules(path) if m == "das" or m.startswith("das.")}
        assert not offending, f"{path.name} imports the server package: {sorted(offending)}"


def test_the_package_marks_itself_typed() -> None:
    assert (SRC / "py.typed").is_file()


#: The public API of the plan (``docs/superpowers/plans/2026-11-11-das-client-api-client-package.md``).
EXPECTED_NAMES = {
    "AsyncDasClient", "DasClient", "verify_webhook",
    "DasError", "AuthError", "NotFound", "Conflict", "IdempotencyKeyReused", "BudgetAboveLimit",
    "InvalidRequest", "RateLimited", "QuotaExceeded", "ServiceUnavailable", "InvalidSignature",
    "TransportFailure", "WaitTimeout",
    "Budget", "Submitted", "CancelResult", "RequestView", "JobView", "BudgetView", "WebhookState",
    "RequestSummary", "Page", "Event",
}  # fmt: skip


def test_the_public_names_are_exactly_the_documented_ones() -> None:
    assert set(das_client.__all__) == EXPECTED_NAMES
    assert len(das_client.__all__) == len(set(das_client.__all__))  # no duplicates
    for name in das_client.__all__:
        assert hasattr(das_client, name), name


def test_every_typed_error_derives_from_das_error() -> None:
    typed = [
        das_client.AuthError,
        das_client.NotFound,
        das_client.Conflict,
        das_client.IdempotencyKeyReused,
        das_client.BudgetAboveLimit,
        das_client.InvalidRequest,
        das_client.RateLimited,
        das_client.QuotaExceeded,
        das_client.ServiceUnavailable,
        das_client.TransportFailure,
        das_client.InvalidSignature,
        das_client.WaitTimeout,
    ]
    assert all(issubclass(e, das_client.DasError) for e in typed)
    assert issubclass(das_client.WaitTimeout, TimeoutError)  # so callers can catch the built-in
