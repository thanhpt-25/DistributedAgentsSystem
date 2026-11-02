# Final-review fix report

Status: DONE. Files changed: `das/client/webhook.py`, `das/client/budget.py`, `tests/unit/test_client_webhook.py`, `tests/unit/test_client_budget.py`, `tests/fixtures/webhook_vectors.json`.

## Changes

- Fix 1 (`webhook.py` `verify`): `abs(now - ts) > tolerance` is now `not abs(now - ts) <= tolerance`, with the comment from the brief. Added `test_verify_rejects_nan_now`.
- Fix 2 (fixture): in `reject`, the `malformed timestamp` entry is replaced by `timestamp with leading space` (" 1793000000"), `timestamp with plus sign` ("+1793000000") and `zero-padded timestamp over 19 digits` ("00000000001793000000", 20 chars). The other five entries are unchanged, so `reject` has 8 entries. Added a top-level `accept` list (placed between `vectors` and `reject`) with `leading zero` ("01793000000") and `19-digit zero-padded timestamp` ("0000000001793000000", 19 chars). Every entry uses the body of the first vector, the given SIG, now 1793000000.0 and tolerance 300. `description` now reads "Webhook signing vectors, header accept cases and reject cases (Phase 4 Req 8.4, 9.7). Shared by das.client.webhook and das_client tests." The test file loads `ACCEPT = FIXTURE["accept"]` and adds `test_verify_accepts_header_vectors`.
- Fix 3 (`budget.py` `Budget.from_json_obj`): `max_cost` must be a `str`, and `InvalidOperation` becomes `ValueError`, using the brief's code verbatim. `InvalidOperation` is imported from `decimal`. The docstring now says "Invalid stored values raise ``ValueError``." Added `test_budget_from_json_rejects_invalid_values` (7 cases).

## RED (new tests on old code)

```
FAILED test_client_webhook.py::test_verify_rejects_nan_now
FAILED test_client_budget.py::test_budget_from_json_rejects_invalid_values[obj0]   # "abc"
FAILED test_client_budget.py::test_budget_from_json_rejects_invalid_values[obj1]   # 1.5
PASSED all 8 test_verify_rejects_vectors (including the 3 new ones)
PASSED both test_verify_accepts_header_vectors
PASSED budget cases obj2-obj6
3 failed, 15 passed
```
This matches the brief's expectation.

## Format-guard sanity check

I ran a scratch copy of `verify` with the length/isdigit guard removed, through exec in memory, without editing the file. All three new reject entries returned True. `cmp` against a backup confirmed `das/client/webhook.py` was unchanged, and the backups were deleted afterwards.

## GREEN

`uv run pytest tests/unit/test_client_webhook.py tests/unit/test_client_budget.py tests/property/test_budget_resolution.py -v -W error`
-> `68 passed in 0.20s`

## Lint / types

- `uv run ruff check das/client tests/unit` -> `All checks passed!`
- `uv run mypy` -> `Success: no issues found in 106 source files`

## Notes

- The `das_client` package copy named in the fixture description does not load this fixture anywhere in the repo yet. Only `das/client/webhook.py` and `tests/unit/test_client_webhook.py` reference it, plus a snapshot under `.superpowers/sdd/snap-ffix/`, which I left alone. If the client-package tests are added later, they should also cover the new `accept` list.
