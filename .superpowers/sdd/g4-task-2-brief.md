### Task 2: Idempotent, quota-checked create (spec tasks 4.3, 4.4)

**Files:**
- Modify: `das/state/lua/create_request.lua`, `das/state/store.py`, `das/state/__init__.py`, `das/state/lua/_common.lua` (only if a helper is needed)
- Test: `tests/unit/test_state_phase4_create.py`, `tests/property/test_idempotent_create.py`

**Interfaces:**
- Consumes: Task 1 keys and helpers; `das.client.budget.Budget.to_json_obj`; `das.client.events.created_event_data`.
- Produces (used by the Gateway in group 7):

```python
@dataclass(frozen=True, slots=True)
class ClientRequestSpec:
    """Phase 4 fields of a new Request (Req 3.1, 4.2, 5.4, 6.5, 8.1)."""
    fingerprint: str
    budget: Budget | None = None
    webhook_url: str | None = None
    webhook_secret_name: str | None = None
    idempotency_key_hash: str | None = None   # das.client.idempotency.idem_key_hash
    body_hash: str | None = None              # required when idempotency_key_hash is set
    idempotency_ttl: int = 86_400
    max_active_requests: int = 0              # 0 = no quota
    events_max_len: int = DEFAULT_EVENTS_MAX_LEN

CreateOutcome = Literal["created", "replayed", "idempotency_conflict", "quota_exceeded"]

@dataclass(frozen=True, slots=True)
class CreateResult:
    outcome: CreateOutcome
    request_id: str | None = None   # created / replayed
    limit: int | None = None        # quota_exceeded

async def submit_request(self, prompt: str, client: ClientRequestSpec,
                         trace_context: TraceContext | None = None) -> CreateResult
```

  - `create_request(prompt, trace_context=None) -> str` keeps its signature and behaviour, and writes `request.created` too (Req 6.1).
  - Both methods share one private helper that loops over Request_ID allocation as today.
  - `submit_request` raises `ValueError` when `idempotency_key_hash` is set without `body_hash`.
  - Export `ClientRequestSpec`, `CreateResult` and `DEFAULT_EVENTS_MAX_LEN` from `das.state`.

**`create_request.lua` contract:**
- KEYS are unchanged (4).
- ARGV[1..3] are unchanged. Optional ARGV[4..12] are `client_fp`, `budget_json`, `webhook_url`, `webhook_secret_name`, `idem_hash`, `body_hash`, `idem_ttl` (seconds), `max_active`, `events_max_len`. Each defaults to `''` (or `'0'`), meaning "not set", so a 3-argument call behaves exactly as before plus the event.
- Order:
  1. When `client_fp` and `idem_hash` are set and `idem_key(KEYS[1], fp, h)` exists: reply `{'REPLAYED', rid}` if its `body_hash` matches, else `{'IDEM_CONFLICT'}`. Nothing is written in either case.
  2. Existing `EXISTS` check on KEYS[1].
  3. When `client_fp` is set and `max_active > 0` and `SCARD active_key >= max_active`: reply `{'QUOTA_EXCEEDED', max_active}`.
  4. Existing writes, plus these fields when set: `client_fp`, `budget`, `webhook_url` with `webhook_secret_name`, and `events_max_len`. Then `SADD active_key rid` (when `client_fp` is set), the idem hash `rid`/`body_hash` with `EXPIRE idem_ttl` (when `idem_hash` is set), and `append_event(KEYS[1], 'request.created', '{"status":"Pending"}')`.
- Reply `{'OK', created}`.
- `append_event` reads `events_max_len` from the request hash (1000 when absent) and runs `XADD events_key MAXLEN <n> * type <type> data <json>`. Task 1 doesn't need it, so add it here.

- [ ] **Step 1: Write the failing unit tests** (`tests/unit/test_state_phase4_create.py`):
  - `create_request("p")` still returns a rid. Its hash has no `client_fp`, and its Event_Stream holds one entry, `type == "request.created"`, with `json.loads(data) == created_event_data()`.
  - `submit_request` with a fingerprint returns `created`. Afterwards `client_fp` and `budget` (equal to `Budget.to_json_obj()` JSON) are stored, the rid is in `client_active(fp)`, and `events_max_len` is stored.
  - Same idempotency key and body hash: the second call returns `replayed` with the same rid. Exactly one request hash exists, and the Active_Set still has 1 member.
  - Same key with a different body hash: `idempotency_conflict`, and nothing new written (compare `client.keys("*")` before and after).
  - The same key value under two fingerprints creates two Requests (Req 4.7).
  - The idem key's TTL is between 1 and `idempotency_ttl`.
  - Quota: `max_active_requests=2`. Two creates succeed; the third returns `quota_exceeded` with `limit == 2`. A replay of the first (same key and body) still returns `replayed` (Req 4.5).
  - `max_active_requests=0` never limits.
  - A webhook target is stored only when both fields are given.
  - `ValueError` when `idempotency_key_hash` is set without `body_hash`.

- [ ] **Step 2: Run, confirm the tests fail.**

- [ ] **Step 3: Implement** the script, `append_event`, `ClientRequestSpec`, `CreateResult`, `submit_request` and the shared helper. Update the script's header comment.

- [ ] **Step 4: Write the property test** `tests/property/test_idempotent_create.py`, **Property 2: Idempotent create** (**Validates: Requirements 4.2, 4.3, 4.4, 4.6, 4.7**):
  - Draw 2..8 submits. Each has a fingerprint from a pool of 2, the idempotency key `"k"`, and a body hash from a pool of 2.
  - Run them concurrently with `asyncio.gather` on one store.
  - Group by fingerprint and assert:
    - exactly one Request is created per fingerprint that appears;
    - every submit with the creator's body hash returns the creator's rid (`created` or `replayed`);
    - every other submit returns `idempotency_conflict`;
    - the Active_Set of each fingerprint has exactly 1 member.

- [ ] **Step 5: Run** the new tests with `-W error`, then `uv run pytest tests/unit -k "state or gateway" -q` and `tests/property/test_submission_uniqueness.py`. Existing callers of `create_request` must not change.

- [ ] **Step 6: Lint, type-check, contracts.**

- [ ] **Step 7: Tick** 4.3 and 4.4.

---


## Shared context for this group (from the plan header)

- Global Constraints and "How to read this plan": see the top of `docs/superpowers/plans/2026-10-30-das-client-api-state-store.md` (read only the header up to the first `### Task`).
- Reference: `.kiro/specs/das-client-api/design.md` Components §3 (key table, `_common.lua` helpers, script changes) and the Correctness Properties section.
