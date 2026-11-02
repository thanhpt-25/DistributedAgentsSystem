# Task 4 report: Webhook signing and retry schedule (spec task 1.5)

## Implemented
- `das/client/webhook.py`: `SIGNATURE_PREFIX`, `JITTER_LOW`/`JITTER_SPAN`, `webhook_body`, `sign`, `verify`, `next_due`, verbatim from the brief. Reuses `das.manager.backoff.backoff` (unchanged).
- `tests/fixtures/webhook_vectors.json`: 3 vectors, verbatim.
- `tests/unit/test_client_webhook.py`: verbatim from the brief.
- Ticked `1.5 Implement webhook signing` and parent `1. Pure core das/client` in `.kiro/specs/das-client-api/tasks.md` (only those two lines).

## TDD evidence
RED: `uv run pytest tests/unit/test_client_webhook.py -v`
```
tests/unit/test_client_webhook.py:10: in <module>
    from das.client.webhook import next_due, sign, verify, webhook_body
E   ModuleNotFoundError: No module named 'das.client.webhook'
1 error in 0.07s
```
Expected: module did not exist yet.

GREEN: `uv run pytest tests/unit/test_client_webhook.py -v` -> `23 passed in 0.02s`.
Also re-ran with `-W error`: 23 passed, no warnings.

## Lint / types
`uv run ruff check das/client tests/unit/test_client_webhook.py` -> `All checks passed!`
`uv run mypy` -> `Success: no issues found in 106 source files`

## Files changed
- /Users/phamtienthanh/Projects/DisrtributedAgentsSystem/das/client/webhook.py (new)
- /Users/phamtienthanh/Projects/DisrtributedAgentsSystem/tests/fixtures/webhook_vectors.json (new)
- /Users/phamtienthanh/Projects/DisrtributedAgentsSystem/tests/unit/test_client_webhook.py (new)
- /Users/phamtienthanh/Projects/DisrtributedAgentsSystem/.kiro/specs/das-client-api/tasks.md (two checkboxes)

## Self-review
- All three precomputed vectors match `sign()`; no vector edits.
- Nothing beyond the brief added; existing das/client modules and backoff untouched.
- Concerns: none.

## Fix (review round 1)

### Changes
- `das/client/webhook.py`: added `MAX_TIMESTAMP_DIGITS: Final = 19` (with the brief's `#:` comment) after the jitter constants. `verify` now returns False when `len(timestamp) > MAX_TIMESTAMP_DIGITS`, checked before `int()`. Docstring now says the header text "must be 1 to 19 ASCII digits". Still pure.
- `tests/fixtures/webhook_vectors.json`: `vectors` left as is; description updated ("accept vectors and reject cases"); added a top-level `reject` list with the six entries from the brief (tampered body, wrong secret, missing prefix, malformed timestamp, over-long timestamp = 400 x "9", outside tolerance at now=1793000301), all with tolerance 300. Built by a script from vector 0, after asserting its secret and signature match the brief.
- `tests/unit/test_client_webhook.py`: loads the JSON once into `FIXTURE` and derives `VECTORS` and `REJECT` from it; added `"9" * 20`, `"9" * 400`, `"1" * 5000` to `test_verify_rejects_malformed_timestamps`; added `test_verify_rejects_vectors` exactly as in the brief.

### RED (before the fix to verify)
`uv run pytest tests/unit/test_client_webhook.py -W error -q`:
```
>       ts = int(timestamp)
E       ValueError: Exceeds the limit (4300 digits) for integer string conversion: value has 5000 digits; ...
das/client/webhook.py:42: ValueError
FAILED tests/unit/test_client_webhook.py::test_verify_rejects_malformed_timestamps[111...1]  (5000 digits)
1 failed, 31 passed in 0.06s
```
Only `"1"*5000` raised in the suite. The `"9"*400` cases (the parametrized one and the fixture entry) passed even before the fix, because the tests pass an int `now`, so `abs(now - ts)` stays in int arithmetic and the window check rejects them. The OverflowError appears only with a float `now`, which is what production passes. Scratch check before the fix:
`verify(b's', '9'*400, b'', 'x', now=1793000000.0, tolerance=300)` -> `OverflowError int too large to convert to float`. After the fix the same call returns `False`.

### GREEN
`uv run pytest tests/unit/test_client_webhook.py -v -W error` -> `32 passed in 0.03s` (all 10 malformed-timestamp cases and all 6 `test_verify_rejects_vectors[...]` cases pass).

### Tolerance sanity check (scratch, not a test)
The "outside tolerance" reject entry called with `now=1793000000` -> `True`, so the time window is the only reason it gets rejected.

### Lint / types
- `uv run ruff check das/client tests/unit/test_client_webhook.py` -> `All checks passed!`
- `uv run mypy` -> `Success: no issues found in 106 source files`

## Controller follow-up (review round 1)
The fixer noted the "9"*400 cases passed even on old code because tests used an int `now`. The controller changed
`test_verify_rejects_malformed_timestamps` to pass `now=float(TS)` and made every `reject[].now` in the fixture a float.
RED: pre-fix `verify` on the "over-long timestamp" reject vector raised OverflowError (other five returned False).
GREEN: `uv run pytest tests/unit/test_client_webhook.py -q -W error` -> 32 passed; ruff clean.
