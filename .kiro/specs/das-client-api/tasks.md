# Implementation Plan: DAS Client API (Phase 4)

## Overview

Implement Phase 4 in Python 3.12 (asyncio) on top of Phases 1–3, keeping the pure core + thin I/O shell model. Order: pure core (`das/client`); config schema; Lua scripts and State_Store; reducer budget enforcement and Manager wiring; Gateway limits, submit, query, SSE, Webhook_Sender, admin and OpenAPI; metrics; Client_Package and contract test; CI, Compose and README; integration/e2e. Property tests use Hypothesis with `fakeredis[lua]`, running the real `das/state/lua/*.lua` files. Each correctness property in the design maps to exactly one property-test sub-task.

## Tasks

- [x] 1. Pure core `das/client`
  - [x] 1.1 Implement the token bucket
    - `das/client/__init__.py`, `das/client/bucket.py`: `BucketState`, `take(state, now, rate, burst) -> (allowed, state, retry_after)`; `rate == 0` always allows; `now < updated` treated as no elapsed time
    - _Requirements: 2.1, 2.2, 2.3_
  - [x] 1.2 Implement budget resolution and status
    - `das/client/budget.py`: `Budget`, `BudgetError`, `resolve_budget`, `BudgetStatus`, `budget_status` (reuse `das.ops.cost.unrounded_costs`, so the cost check compares unrounded totals; unpriced model with `max_cost` → `unenforceable`)
    - _Requirements: 5.2, 5.3, 5.6_
  - [x] 1.3 Write property test for budget resolution
    - **Property 4: Budget resolution never raises a limit**
    - **Validates: Requirements 5.2, 5.3**
  - [x] 1.4 Implement idempotency helpers and event payloads
    - `das/client/idempotency.py`: `validate_idempotency_key`, `body_hash` (canonical JSON of `{prompt, budget}`), `idem_key_hash`
    - `das/client/events.py`: `EventType`, payload builders for every event type, `terminal_event_data`; no prompt, instruction or result text
    - _Requirements: 4.1, 6.1, 6.2_
  - [x] 1.5 Implement webhook signing
    - `das/client/webhook.py`: `sign`, `verify` (constant-time, tolerance), `next_due` (reuse `das.manager.backoff.backoff`, jitter 0.8–1.2)
    - `tests/fixtures/webhook_vectors.json`: fixed vectors (secret, timestamp, body, signature, valid/invalid cases)
    - _Requirements: 8.4, 8.6_

- [x] 2. Configuration
  - [x] 2.1 Add config sections and validation
    - `das/config/schema.py`: `BudgetConfig`, `ClientSettingsConfig`, `ClientOverride` (fields default `None`), `ClientLimitsConfig`, `EventsConfig`, `WebhookConfig`, `webhook_secrets`; `SECRET_FIELDS` gains the `webhook_secrets` prefix
    - `das/client/settings.py`: `ClientSettings`, `resolve_settings(fp, cfg)`
    - Gateway-role validation in `load_config`: unknown or duplicate override fingerprint, duplicate key fingerprint, webhook URL/secret pairing, missing secret, non-https without `allow_insecure_webhooks`; all raise `ConfigError` with field paths before any connection
    - Register webhook secrets with the Redactor
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7_
  - [x] 2.2 Write unit tests for client config
    - Each rejection in 2.1 with exact field path; override inheritance; secrets rejected in YAML; secret value absent from every error message
    - _Requirements: 1.3, 1.4, 1.5, 1.6, 1.7_

- [x] 3. Checkpoint - Ensure all tests pass

- [x] 4. State_Store: Lua scripts
  - [x] 4.1 Add Lua helpers and the rate-limit script
    - `_common.lua`: key-name helpers derived from the request key (`key_prefix`, `events_key`, `webhook_key`, `outbox_key`, `active_key`, `idem_key`) and `fmt_double`
    - `rate_check.lua` matching `bucket.take`; bucket key TTL `ceil(burst/rate)+60`
    - `das/state/keys.py`: new key builders; `das/state/scripts.py`: register new scripts
    - _Requirements: 2.1, 2.4, 6.5_
  - [x] 4.2 Write property test for the token bucket
    - **Property 1: Token bucket bound**
    - **Validates: Requirements 2.3, 2.4**
  - [x] 4.3 Extend `create_request.lua` and `StateStore.create_request`
    - Idempotency check → quota check → writes (`client_fp`, `budget`, webhook target, `SADD` active, idem record with TTL, `request.created` event); returns `OK` / `REPLAYED` / `IDEM_CONFLICT` / `QUOTA_EXCEEDED`
    - `_common.lua`: `append_event`
    - `ClientRequestSpec`, `CreateResult` and `StateStore.submit_request` in `das/state/store.py`; `create_request(prompt)` callers keep working
    - _Requirements: 3.1, 3.2, 4.2, 4.3, 4.4, 4.5, 5.4, 6.1, 8.1_
  - [x] 4.4 Write property test for idempotent create
    - **Property 2: Idempotent create**
    - **Validates: Requirements 4.2, 4.3, 4.4, 4.6, 4.7**
  - [x] 4.5 Add events, quota release and webhook enqueue to state-changing scripts
    - `commit_plan.lua` (`request.planned`), `apply_transition.lua` (one `job.transition` per entry, same order), `reopen_request.lua` (`request.reopened`), `replay_dead_letter.lua` (`client_active_add` when it reopens a Failed Request), `finalize.lua` and `cancel_request.lua` (`client_active_remove`, `terminal_event` with budget view from ARGV, which also enqueues the webhook), `purge_request.lua` (`ZREM` outbox, `client_active_remove`)
    - `_common.lua`: `client_active_add`, `client_active_remove`, `terminal_event`; `put_job` appends `job.transition`
    - Update the matching `StateStore` methods (`commit_plan`, `finalize`, `cancel_request` budget view, `delete_request`, `get_webhook`)
    - _Requirements: 3.3, 3.4, 3.5, 6.1, 6.3, 6.4, 6.6, 8.2_
  - [x] 4.6 Write property test for Active_Set accounting
    - **Property 3: Active_Set equals non-terminal Requests**
    - **Validates: Requirements 3.1, 3.3, 3.4, 3.5, 3.6, 3.7**
  - [x] 4.7 Write property test for the Event_Stream
    - **Property 6: Event_Stream mirrors persisted changes**
    - **Validates: Requirements 6.1, 6.2, 6.3, 6.4**
  - [x] 4.8 Add webhook claim and completion scripts
    - `claim_webhook.lua` (lease re-score, drop entries without hash), `complete_webhook.lua` (fenced by the `claim` counter and the delivery `event_id`); `StateStore.claim_webhook`, `StateStore.complete_webhook`
    - _Requirements: 8.3, 8.5, 8.6_
  - [x] 4.9 Write property test for webhook outbox delivery
    - **Property 7: Webhook outbox delivery**
    - **Validates: Requirements 8.2, 8.3, 8.5, 8.6, 8.7**

- [x] 5. Manager budget enforcement
  - [x] 5.1 Enforce budgets in the reducer
    - `RequestSnapshot.budget`, `StepContext.price_table`; budget-stop mode per design §4 (no dispatch, skip non-in-flight Jobs with reason `budget_exceeded`, no retries, `FinalizeFailed` with `budget_exceeded` / `budget_unenforceable` when nothing is in flight, never `RunSummarizer`)
    - `load_snapshot` reads `budget` (and `client_fp`), so the Manager and Gateway share one consistent read
    - _Requirements: 5.6, 5.7, 5.8_
  - [x] 5.2 Write property test for budget stop
    - **Property 5: Budget stop**
    - **Validates: Requirements 5.6, 5.7, 5.8**
  - [x] 5.3 Wire the driver and planning check
    - Driver passes `price_table` into `StepContext`; before planning, `max_cost` without Price_Table → `FinalizeFailed(budget_unenforceable)`; for a budgeted Request the driver passes `budget_view(budget, budget_status(...))` to `StateStore.finalize` / `fail_request`, so the Terminal_Event carries Budget_Used (Req 6.1); `das_budget_exceeded_total{kind}` once per successful finalize
    - Unit tests: planning check, takeover reaches the same budget decision, metric counted once, Terminal_Event of a budgeted Request has `budget`
    - _Requirements: 5.5, 10.2_

- [ ] 6. Checkpoint - Ensure all tests pass

- [ ] 7. Gateway_API
  - [ ] 7.1 Add client settings and the rate-limit dependency
    - `auth.py` stores `ClientSettings` on `request.state`; `limits.py` `rate_limit` dependency on `/v1` and admin routers; 429 with `Retry-After`; 503 on store failure; `/healthz` exempt
    - _Requirements: 2.1, 2.2, 2.5, 2.6_
  - [ ] 7.2 Extend submit
    - `SubmitBody.budget` (an empty budget is stored as no budget), `Idempotency-Key` validation, `resolve_budget`, `CreateResult` mapping to 202 / 200 (`Idempotent-Replayed: true`) / 422 / 429 / 400; replays not counted as submitted
    - _Requirements: 3.2, 4.1, 4.3, 4.4, 4.5, 5.1, 5.2_
  - [ ] 7.3 Extend the query view and cancel
    - `budget` and `webhook` fields, read in the same MULTI as the snapshot
    - `POST /v1/requests/{id}/cancel` passes the Gateway's Price_Table to `StateStore.cancel_request(rid, prices=...)`, which recomputes Budget_Used on every retry, so the `request.cancelled` event of a budgeted Request carries it (Req 6.1)
    - Read the Request, its Webhook_Delivery, its budget and its last Event_ID with one combined store read (one MULTI), not separate calls
    - Harden `StateStore.cancel_request`: corrupt stored budget or usage data must not block cancellation (Phase 1 Req 1.6); log a warning and emit the Terminal_Event without `budget`. Add a docstring note that Budget_Used is fenced by the Job-record compare-and-set (every usage append comes with a Job change). Test `cancel_request(prices=...)` with a `max_cost` Budget
    - _Requirements: 5.9, 8.9, 11.1_
  - [ ] 7.4 Implement the SSE endpoint
    - `das/gateway/events.py` per design §5: snapshot first, `Last-Event-ID` and resync, `XREAD BLOCK` on a dedicated pool, keepalive via `Clock`, close after terminal, per-key Redis counter with TTL and per-replica cap, release in `finally`
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 7.9_
  - [ ] 7.5 Write unit tests for the SSE endpoint
    - Snapshot first; resume after `Last-Event-ID`; resync on invalid or trimmed ID; keepalive; close after terminal; already-terminal Request; 404; both caps and their release; no prompt text in any frame
    - _Requirements: 7.1–7.8, 6.2_
  - [ ] 7.6 Implement the Webhook_Sender
    - `das/gateway/webhooks.py`: `webhook.workers` tasks in the lifespan; claim → sign → POST (no redirects, 64 KiB cap, timeout) → complete; secret looked up at send time (`secret_missing`); graceful stop
    - _Requirements: 8.3, 8.4, 8.5, 8.6, 8.8, 8.10_
  - [ ] 7.7 Write unit tests for the Webhook_Sender
    - respx: 2xx delivered; 5xx/timeout/connect error retried at `next_due(claim.claimed_at, ...)` (Redis time from the claim, no host clock); `failed` exactly when `attempts + 1 == max_attempts`; redirect not followed; oversized body; missing secret; secret and signature absent from logs
    - _Requirements: 8.5, 8.6, 8.8, 8.10_
  - [ ] 7.8 Admin replay guard and OpenAPI route
    - `decide_replay` returns `budget_exceeded` → 409; `/v1/openapi.json` behind API_Key auth
    - _Requirements: 5.10, 11.4_
  - [ ] 7.9 Add metrics
    - Catalog entries and fixed labels for `das_client_rejections_total`, `das_idempotent_replays_total`, `das_sse_streams`, `das_webhook_deliveries_total`, `das_budget_exceeded_total`; unit tests for names and labels
    - _Requirements: 10.1, 10.2, 10.3_
  - [ ] 7.10 Write unit tests for the Gateway error responses
    - Every row of the design Error Handling table, including 503 when Redis is unreachable for rate check and create
    - _Requirements: 2.2, 2.5, 3.2, 4.1, 4.4, 5.1, 5.2, 7.7_

- [ ] 8. Checkpoint - Ensure all tests pass

- [ ] 9. Client_Package
  - [ ] 9.1 Scaffold the package
    - `clients/python/pyproject.toml` (`das-client`, Python >=3.10,<3.14, `httpx==0.28.1`), `src/das_client/` modules per design §7; uv workspace entry in the root `pyproject.toml`; client installed in the root dev group for the contract test
    - _Requirements: 9.1_
  - [ ] 9.2 Implement clients, retries and errors
    - `DasClient`, `AsyncDasClient`; generated UUID4 idempotency key reused across retries; retry policy with `Retry-After` floor; typed errors; models ignoring unknown fields
    - _Requirements: 9.2, 9.3, 9.4, 9.5, 9.9_
  - [ ] 9.3 Implement events, wait and webhook verification
    - SSE parser, `events()` with reconnect and `Last-Event-ID`, stop after Terminal_Event; `wait()` with polling fallback; `verify_webhook` (copy of `das.client.webhook`)
    - _Requirements: 9.6, 9.7_
  - [ ] 9.4 Write property test for the webhook signature round trip
    - **Property 8: Webhook signature round trip**
    - **Validates: Requirements 8.4, 9.7, 9.8**
  - [ ] 9.5 Write client unit tests
    - respx: retries, `Retry-After`, each typed error, SSE reconnect, `wait()` fallback, unknown fields, shared webhook vectors (every entry of `vectors`, `accept` and `reject` in `tests/fixtures/webhook_vectors.json`)
    - _Requirements: 9.3, 9.4, 9.5, 9.6, 9.9_
  - [ ] 9.6 Write the contract test
    - `tests/contract/test_client_contract.py`: client against `create_app(...)` with fakeredis via `httpx.ASGITransport`, covering every client method and error mapping
    - _Requirements: 9.10_

- [ ] 10. CI, deployment and docs
  - [ ] 10.1 Update CI
    - `client` job (Python 3.10, 3.13); `tests/contract` in the fast job
    - _Requirements: 9.10, 11.5_
  - [ ] 10.2 Update Compose, sample config and README
    - `deploy/config/gateway.yaml` `client_limits`, `events`, `webhook` samples; `deploy/.env.example` webhook secret example; README sections: limits and quotas, idempotency, budgets, events, webhooks (signature verification), client package, upgrade order
    - _Requirements: 11.3_

- [ ] 11. Integration and end-to-end tests
  - [ ] 11.1 Write the Compose e2e tests
    - `tests/integration/e2e/test_client_api_e2e.py`, `webhook_sink.py`, e2e override changes (`gateway=2`, insecure webhooks, `webhook.base_backoff=1`): shared bucket across two Gateways; SSE through a Manager kill; webhook retries and verified signature; budget stop with partial results; `das-client` `wait()`
    - _Requirements: 2.4, 7.9, 8.6, 5.7, 9.6_
  - [ ] 11.2 Run the full regression
    - All Phase 1–4 suites, `lint-imports`, ruff and mypy
    - _Requirements: 11.5_

- [ ] 12. Final checkpoint - Ensure all tests pass, ask the user if questions arise.

## Notes

- Each task references specific requirement criteria. Each of the 8 design properties maps to exactly one property-test sub-task.
- Property tests live in `tests/property/`, carry `# Feature: das-client-api, Property {n}: {title}`, and run at least 100 examples through the Hypothesis profiles.
- No new runtime dependency for the `das` package; `das-client` depends only on `httpx==0.28.1` (already pinned in the server).
- Timing bounds (Req 7.9) and multi-Gateway behavior are covered by e2e tests, not property tests.
- If implementation finds a missing or contradictory requirement, update `requirements.md` before changing the design.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2", "1.4", "1.5"] },
    { "id": 1, "tasks": ["1.3", "2.1", "4.1"] },
    { "id": 2, "tasks": ["2.2", "4.2", "4.3", "4.8"] },
    { "id": 3, "tasks": ["4.4", "4.5", "4.9"] },
    { "id": 4, "tasks": ["4.6", "4.7", "5.1"] },
    { "id": 5, "tasks": ["5.2", "5.3", "7.1"] },
    { "id": 6, "tasks": ["7.2", "7.3", "7.4", "7.6", "7.8", "7.9"] },
    { "id": 7, "tasks": ["7.5", "7.7", "7.10", "9.1"] },
    { "id": 8, "tasks": ["9.2", "9.3"] },
    { "id": 9, "tasks": ["9.4", "9.5", "9.6", "10.1", "10.2"] },
    { "id": 10, "tasks": ["11.1"] },
    { "id": 11, "tasks": ["11.2"] }
  ]
}
```
