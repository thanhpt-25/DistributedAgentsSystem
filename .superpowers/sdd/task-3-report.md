# Task 3 report: Idempotency helpers and Event payloads (spec task 1.4)

## Implemented
- `das/client/idempotency.py`: `MAX_IDEMPOTENCY_KEY_LENGTH`, `validate_idempotency_key`, `body_hash`, `idem_key_hash`, verbatim from the brief.
- `das/client/events.py`: `EventType`, `EVENT_TYPES`, `TERMINAL_EVENT_TYPES`, `terminal_event_type`, `created/planned/transition/reopened/terminal_event_data`, verbatim from the brief.
- Tests `tests/unit/test_client_idempotency.py` and `tests/unit/test_client_events.py`, verbatim from the brief.

## TDD evidence
### idempotency
- RED: `uv run pytest tests/unit/test_client_idempotency.py -v` -> collection error `ModuleNotFoundError: No module named 'das.client.idempotency'` (expected: module not yet written).
- GREEN: same command -> `17 passed in 0.05s`.
### events
- RED: `uv run pytest tests/unit/test_client_events.py -v` -> `ModuleNotFoundError: No module named 'das.client.events'` (expected).
- GREEN: `uv run pytest tests/unit/test_client_idempotency.py tests/unit/test_client_events.py -v` -> `31 passed in 0.06s`. Also re-run with `-W error`: 31 passed, no warnings.

## Lint / types
- `uv run ruff check das/client tests/unit/test_client_idempotency.py tests/unit/test_client_events.py` -> `All checks passed!`
- `uv run mypy` -> `Success: no issues found in 105 source files`

## Files changed
- Created: das/client/idempotency.py, das/client/events.py, tests/unit/test_client_idempotency.py, tests/unit/test_client_events.py
- Edited: .kiro/specs/das-client-api/tasks.md (line 19, ticked 1.4 only)

## Self-review
- Code and tests match the brief exactly; no extras. Existing `das/client` modules and models untouched.
- No concerns.
