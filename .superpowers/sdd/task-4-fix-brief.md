# Task 4 fix brief (approved by the user)

Two review findings on `das/client/webhook.py` and `tests/fixtures/webhook_vectors.json`. Fix both; change nothing else.

## Fix 1: `verify` must return False, never raise, on an over-long digit timestamp

Today `verify(b"s", "9"*400, ...)` raises OverflowError and `"1"*5000` raises ValueError, because the
all-digits check has no length limit. A sender can set that header without knowing the secret.

In `das/client/webhook.py`:
- Add a module constant next to the jitter constants:
  `#: Longest accepted DAS-Webhook-Timestamp; Unix seconds need 10 digits.`
  `MAX_TIMESTAMP_DIGITS: Final = 19`
- In `verify`, change the format check to also reject `len(timestamp) > MAX_TIMESTAMP_DIGITS`
  (return False before calling `int()`).
- Update the `verify` docstring sentence about the header text to say it must be 1 to 19 ASCII digits.

In `tests/unit/test_client_webhook.py`, add `"9" * 20`, `"9" * 400` and `"1" * 5000` to the
`test_verify_rejects_malformed_timestamps` parametrize list.

## Fix 2: shared fixture must include reject cases

Task 1.5 asks for valid and invalid cases; the client package (task 9.5) reuses this file, so reject
behaviour must be shared too. Keep the existing `"vectors"` list unchanged (valid cases). Add a top-level
`"reject"` list. Each entry: `secret` (str), `timestamp` (str: the header text), `body` (str),
`signature` (str), `now` (number), `tolerance` (int), `reason` (str). Use these six entries
(`BODY` = the body string of the first valid vector, `SIG` = its signature
`v1=8847d8f3b0002096373ae63db4f4d3fc79ffa69566fb6d1d53baf77ed40095e7`, secret `whsec_test_0123456789`,
tolerance 300):

| reason | secret | timestamp | body | signature | now |
|---|---|---|---|---|---|
| tampered body | whsec_test_0123456789 | "1793000000" | BODY with `"Completed"` replaced by `"Failed"` | SIG | 1793000000 |
| wrong secret | wrong-secret | "1793000000" | BODY | SIG | 1793000000 |
| missing prefix | whsec_test_0123456789 | "1793000000" | BODY | SIG without the leading `v1=` | 1793000000 |
| malformed timestamp | whsec_test_0123456789 | "17930000x0" | BODY | SIG | 1793000000 |
| over-long timestamp | whsec_test_0123456789 | 400 `9` characters | BODY | SIG | 1793000000 |
| outside tolerance | whsec_test_0123456789 | "1793000000" | BODY | SIG | 1793000301 |

Update the fixture's `description` to say it holds accept vectors and reject cases.

In `tests/unit/test_client_webhook.py`, load the list as `REJECT = ...["reject"]` and add:

```python
@pytest.mark.parametrize("v", REJECT, ids=lambda v: v["reason"])
def test_verify_rejects_vectors(v: dict[str, Any]) -> None:
    assert not verify(
        v["secret"].encode(), v["timestamp"], v["body"].encode(), v["signature"],
        now=v["now"], tolerance=v["tolerance"],
    )
```

Load the JSON once into a module variable and derive both `VECTORS` and `REJECT` from it.
Sanity check: the "outside tolerance" entry must verify True if you call it with `now=1793000000`
(so it is rejected only by the time window). Check that in a scratch run; do not add it as a test.

## Verify

- `uv run pytest tests/unit/test_client_webhook.py -v -W error` — all pass.
- `uv run ruff check das/client tests/unit/test_client_webhook.py && uv run mypy` — clean.
- No other file changes; do not touch `tasks.md` (already ticked).
