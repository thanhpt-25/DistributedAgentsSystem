# Final-review fix brief (approved by the user)

Three fixes in `das/client`. Change only the files named here. Keep every module pure.

## Fix 1: `verify` must not accept a NaN `now`

`das/client/webhook.py`, in `verify`: `abs(now - ts) > tolerance` is False when `now` is NaN, so the
window check is skipped. Replace it with:

```python
    # Written as "not <=" so a NaN ``now`` fails the window check instead of skipping it.
    if not abs(now - ts) <= tolerance:
        return False
```

`tests/unit/test_client_webhook.py`: add

```python
def test_verify_rejects_nan_now() -> None:
    assert not verify(SECRET, str(TS), BODY, sign(SECRET, TS, BODY), now=float("nan"), tolerance=300)
```

Do not put NaN in the JSON fixture (not portable JSON).

## Fix 2: shared fixture cases that exercise the format guard

`tests/fixtures/webhook_vectors.json`. Values below use: secret `whsec_test_0123456789`,
BODY = the `body` of the first entry in `vectors`, SIG =
`v1=8847d8f3b0002096373ae63db4f4d3fc79ffa69566fb6d1d53baf77ed40095e7`, tolerance 300, now 1793000000.0.
Every timestamp below parses with `int()` to 1793000000, so SIG is a valid HMAC for all of them;
only the format rules decide the outcome.

(a) In `reject`, replace the entry whose reason is `malformed timestamp` with these three entries
(same field layout as the other reject entries):

| reason | timestamp |
|---|---|
| timestamp with leading space | `" 1793000000"` |
| timestamp with plus sign | `"+1793000000"` |
| zero-padded timestamp over 19 digits | `"00000000001793000000"` (20 characters) |

Keep the other five reject entries unchanged. `reject` then has 8 entries.

(b) Add a top-level `accept` list with the same field layout as `reject`
(`secret`, `timestamp` (str), `body`, `signature`, `now`, `tolerance`, `reason`):

| reason | timestamp |
|---|---|
| leading zero | `"01793000000"` |
| 19-digit zero-padded timestamp | `"0000000001793000000"` (19 characters) |

Update `description` to say the file holds signing vectors, header accept cases and reject cases.

`tests/unit/test_client_webhook.py`: load `ACCEPT = FIXTURE["accept"]` next to `REJECT`, and add

```python
@pytest.mark.parametrize("v", ACCEPT, ids=lambda v: v["reason"])
def test_verify_accepts_header_vectors(v: dict[str, Any]) -> None:
    assert verify(
        v["secret"].encode(), v["timestamp"], v["body"].encode(), v["signature"],
        now=v["now"], tolerance=v["tolerance"],
    )
```

Sanity check in a scratch run (do not add as tests): with the format check in `verify` removed,
the three new reject entries return True. Restore the code afterwards and confirm the file is unchanged.

## Fix 3: `Budget.from_json_obj` raises ValueError on bad stored data

`das/client/budget.py`, `Budget.from_json_obj`: today `{"max_cost": "abc"}` raises
`decimal.InvalidOperation` and `{"max_cost": 1.5}` (a float) is accepted. Change it so `max_cost`
must be a `str` and any conversion failure becomes `ValueError`:

```python
        cost = obj.get("max_cost")
        if cost is not None and not isinstance(cost, str):
            raise ValueError("max_cost must be a decimal string")
        try:
            max_cost = Decimal(cost) if cost is not None else None
        except InvalidOperation as exc:
            raise ValueError("max_cost must be a decimal string") from exc
        return cls(max_total_tokens=obj.get("max_total_tokens"), max_cost=max_cost)
```

Import `InvalidOperation` from `decimal` alongside `Decimal`. Extend the method docstring with one
sentence: invalid stored values raise `ValueError`.

`tests/unit/test_client_budget.py`: add

```python
@pytest.mark.parametrize(
    "obj",
    [
        {"max_cost": "abc"},
        {"max_cost": 1.5},
        {"max_cost": "NaN"},
        {"max_cost": "0"},
        {"max_total_tokens": "5"},
        {"max_total_tokens": 1.5},
        {"max_total_tokens": True},
    ],
)
def test_budget_from_json_rejects_invalid_values(obj: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        Budget.from_json_obj(obj)
```

## Verify

- Test-first: add the new tests, run them on the current code, and record the results. Expected:
  `test_verify_rejects_nan_now` FAILS; the `abc` and `1.5` cases of
  `test_budget_from_json_rejects_invalid_values` FAIL; the new reject and accept vectors already PASS
  (the format guard exists; they protect the client copy from drifting).
- `uv run pytest tests/unit/test_client_webhook.py tests/unit/test_client_budget.py tests/property/test_budget_resolution.py -v -W error`
- `uv run ruff check das/client tests/unit && uv run mypy`
- Do not touch `tasks.md`.
