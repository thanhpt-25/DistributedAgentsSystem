### Task 5: Webhook claim and completion (spec tasks 4.8, 4.9)

**Files:**
- Create: `das/state/lua/claim_webhook.lua`, `das/state/lua/complete_webhook.lua`
- Modify: `das/state/scripts.py`, `das/state/store.py`, `das/state/__init__.py`
- Test: `tests/unit/test_state_phase4_webhook.py`, `tests/property/test_webhook_outbox.py`

**Interfaces:**
- Consumes: Task 4 (deliveries enqueued by `finalize` and `cancel_request`).
- Produces:

```python
@dataclass(frozen=True, slots=True)
class WebhookClaim:
    request_id: str
    claim: int          # pass back to complete_webhook
    attempts: int       # attempts made before this one
    url: str
    secret_name: str
    event_id: str
    event_type: str
    data: dict[str, Any]

WebhookOutcome = Literal["delivered", "retry", "failed"]

async def claim_webhook(self, lease_seconds: float) -> WebhookClaim | None
async def complete_webhook(self, claim: WebhookClaim, outcome: WebhookOutcome,
                           last_status: str, next_due: float | None = None) -> bool
    # False when stale; next_due (Redis seconds) is required for "retry"
```

  Export `WebhookClaim` and `WebhookOutcome` from `das.state`.

**`claim_webhook.lua` contract** (Req 8.3):
- KEYS[1] is the outbox. ARGV[1] is `lease_seconds` and ARGV[2] is the request key prefix `das:req:`.
- `ZRANGEBYSCORE outbox -inf now LIMIT 0 10`. For each rid, the hash is `ARGV[2] .. rid .. ':webhook'`. If it is missing or its `state ~= 'pending'`, `ZREM` the rid and continue. Otherwise:
  - `ZADD outbox now + lease rid`
  - `HINCRBY claim 1`
  - reply `{rid, claim, attempts, url, secret_name, event_id, type, data}`
- Reply `{}` when nothing is due.

**`complete_webhook.lua` contract** (Req 8.5–8.7):
- KEYS[1] is the outbox and KEYS[2] the webhook hash. ARGV[1] is `rid`, ARGV[2] `claim`, ARGV[3] `outcome`, ARGV[4] `last_status`, ARGV[5] `next_due` (`''` unless retry).
- An unknown outcome, or retry without a numeric `next_due`, is an error reply, checked before any write.
- If the hash is missing, `state ~= 'pending'`, or `claim ~= ARGV[2]`: reply `{'STALE'}`.
- Otherwise `HINCRBY attempts 1`, `HSET last_status`, then:
  - `delivered`: `HSET state delivered`, `ZREM`.
  - `failed`: `HSET state failed`, `ZREM`.
  - `retry`: `ZADD outbox next_due rid`.
- Reply `{'OK', attempts}`.

- [ ] **Step 1: Write the failing unit tests** (`tests/unit/test_state_phase4_webhook.py`), using `fake_redis_time` and enqueuing through `submit_request` with a webhook target and then `cancel_request`:
  - Nothing due → `None`.
  - A due delivery is claimed once. A second claim at the same time returns `None` (re-scored), and `claim == 1`.
  - After `advance(lease + 1)`, the delivery is claimable again with `claim == 2`, and `complete_webhook` with the first claim returns `False` and changes nothing.
  - `delivered` sets state and attempts and removes it from the outbox, and `get_webhook` agrees.
  - `retry` keeps it pending and due at `next_due`.
  - `failed` removes it.
  - An entry whose hash was deleted is dropped from the outbox on the next claim.
  - `retry` without `next_due` raises `ValueError` in the Python method before calling Redis.

- [ ] **Step 2: Run, confirm the tests fail.** **Step 3: Implement.**

- [ ] **Step 4: Write the property test** `tests/property/test_webhook_outbox.py`, **Property 7: Webhook outbox delivery** (**Validates: Requirements 8.2, 8.3, 8.5, 8.6, 8.7**):
  - Draw 1..5 Requests, each with or without a webhook target, and terminate each with finalize or cancel.
  - Assert each terminal write enqueued exactly one delivery for Requests with a target and none for the others.
  - Then simulate 2..3 senders with a step list. Each step picks a sender and an action:
    - claim
    - crash (forget the claim)
    - advance Redis time by 0..`2 * lease`
    - complete the held claim with a random outcome
  - The sender chooses `failed` once `attempts + 1 == max_attempts` (3), otherwise `retry` at `now + 1`.
  - After the steps, drain: advance time and let one well-behaved sender claim and complete until the outbox is empty.
  - Assert:
    - every delivery ends `delivered` or `failed` with `1 <= attempts <= max_attempts`;
    - while a claim's lease is live, no other claim of the same rid succeeds (track lease expiry in the model);
    - every `complete_webhook` with a superseded claim returned `False` and changed nothing.

- [ ] **Step 5: Run** the new tests with `-W error`. **Step 6: Lint, type-check, contracts.** **Step 7: Tick** 4.8, 4.9 and the parent `4. State_Store: Lua scripts`.

---


## Shared context for this group (from the plan header)

- Global Constraints and "How to read this plan": see the top of `docs/superpowers/plans/2026-10-30-das-client-api-state-store.md` (read only the header up to the first `### Task`).
- Reference: `.kiro/specs/das-client-api/design.md` Components §3 (key table, `_common.lua` helpers, script changes) and the Correctness Properties section.
