# DAS Client API (Phase 4) Gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Spec task group 7 in `.kiro/specs/das-client-api/tasks.md`: the Gateway enforces per-key rate limits and quotas, accepts `Idempotency-Key` and `budget` on submit, exposes budget/webhook in the query view, streams events over SSE, delivers signed webhooks, guards admin replay of budget-failed Requests, serves OpenAPI, and emits the Phase 4 metrics.

**Architecture:** All state changes already exist in `StateStore` (groups 4 and 5 are done): `rate_check`, `submit_request`/`CreateResult`, `cancel_request(prices=)`, `get_webhook`, `claim_webhook`, `complete_webhook`, events stream. The Gateway is a thin I/O shell over those and over the pure `das/client` core (`resolve_settings`, `resolve_budget`, `body_hash`, `idem_key_hash`, `validate_idempotency_key`, event builders, `sign`, `next_due`). No new Lua. No new reducer logic.

Spec references:
- `requirements.md` Req 2 (limits), 3.2, 4 (idempotency), 5.1/5.2/5.9/5.10 (budget), 6.2, 7 (SSE), 8 (webhooks), 10 (metrics), 11.1/11.4.
- `design.md` Architecture "Submit path" / "Read paths", Components §5 (Gateway_API), §6 (Metrics), Error Handling table, Property 7.

**Tech Stack:** Python 3.12, FastAPI 0.161.1, httpx 0.28.1, respx 0.23.1, fakeredis[lua] 2.40.0, pytest 9.1.1, Hypothesis, uv.

## Global Constraints

- Run everything through `uv run`. No new dependencies. `das.gateway` may import `das.client` and `das.ops`; the `das.manager`/`das.worker` import-linter contract is untouched.
- Requests that send no new headers or fields behave exactly as today. Every existing `tests/unit/test_gateway_*` test must pass unchanged, except tests that pin the exact set of routes or metrics, which are updated in the task that adds them.
- Redis failure anywhere in a new path is `503` through `_bounded` (fail closed, design D7). Never skip the rate check on error.
- Never log or return prompt text, instruction text, results, webhook secrets or signatures.
- Time comes from the injected `Clock` (keepalive, sender idle wait) or from Redis (`WebhookClaim.claimed_at`). No `time`/`asyncio.sleep` in `das/gateway`.
- Docstrings cite requirements as `Phase 4 Req X.Y`. Tick the boxes in `tasks.md` at the end of each task. Before the checkpoint run `uv run ruff check`, `uv run mypy`, `uv run lint-imports`, `uv run pytest tests/unit tests/property`.

## Findings that shape the plan (read from the code)

- `create_app` has no lifespan and `openapi_url=None`; it takes no client-limits, events or webhook config yet. Its signature grows: `client_limits`, `events`, `webhook`, `webhook_secrets`. `das/cli.py:519` is the only call site and must pass them from `AgentConfig`.
- `api_key_auth` returns `None` and only classifies the key; the fingerprint is computed in `audit_context`. Task 2 must expose the fingerprint and `ClientSettings` on `request.state` from one place, so audit and limits cannot disagree.
- `submit` calls `store.create_request(prompt, trace_context=...)`. Phase 4 needs the `ClientRequestSpec`/`CreateResult` path (`CreateResult`'s docstring names `submit_request`); check the `create_request` vs `submit_request` split in `das/state/store.py` before editing.
- `get_request` reads `load_snapshot(...)` alone. Spec 7.3 requires budget, webhook and last event id in the same MULTI, so `load_snapshot` (or a sibling) gains optional reads instead of a second call.
- `cancel` calls `store.cancel_request(request_id)` with no prices; it must pass `prices=price_table`.
- Carry-overs from `.superpowers/sdd/progress.md`: the Gateway passes `None`, never an empty `Budget()`, to the store (empty is stored as `"{}"`); `claim_webhook` scans at most 10 entries and does not enforce `max_attempts`, so the sender must (`failed` exactly when `attempts + 1 == max_attempts`).

## Progress

Tasks 1–8 (spec 7.1–7.10) and checkpoint 8 are done: 2509 unit and property tests pass, with ruff, mypy and lint-imports clean. Group 7 is complete. Task 8 (`tests/unit/test_gateway_client_errors.py`, 13 cases) maps each row of the design Error Handling table to one case through the real app and also checks side effects; it adds no production code. Deviations from the plan as first written:

- **Metrics (T1):** labels follow Req 10.1 (`budget_above_limit`, not `invalid_request`; `outcome`, not `result`). `das_sse_streams` is fed by a callable (`GatewayMetrics.sse_streams(fn)`), not open/close calls.
- **Limits (T2):** the default `client_limits` (60/s, burst 120) is on for every deployment that does not configure it. Every authenticated call writes its caller's `das:client:<fp>:bucket`, so "no side effects" tests (`everything()`, `_keyspace`, `_dump`) skip bucket keys, and the pagination properties set `rate=0`. `test_phase3_baseline` was regenerated for the 4 new metrics only.
- **Submit (T3):** `create_app` gained `events` (for `events_max_len`) ahead of Task 5. `max_total_tokens` is a strict JSON integer. `budget_above_limit` returns a `max_cost` limit as decimal text.
- **View/cancel (T4):** `load_snapshot(include_client=True)` also returns `last_event_id`, for Task 5's snapshot frame. `test_gateway_contract` now expects `budget` and `webhook` as the Phase 4 additions (Req 11.1). A corrupt stored budget still makes the `GET` view a 503; only cancel degrades.
- **Prices:** a `PriceEntry` price per million tokens is capped at 1,000,000, so tests price at 1 per token.
- **SSE (T5):** the per-key stream cap is a sorted set (one expiring member per connection, atomic `stream_acquire.lua`), not a counter with a TTL; the design text was updated. Blocking `XREAD` runs on a dedicated client (`events_client`, built in `cli.py` with a 10 s socket timeout and a pool sized to `events.max_connections`). `Last-Event-ID: 0-0` is older than the first event, so it resyncs rather than replaying history.
- **Webhooks (T6):** `HealthServer` ran uvicorn with `lifespan="off"`, so lifespan-started workers would never have run in production; it now takes an opt-in `lifespan` argument and only the Gateway's server sets it to `"on"`. `create_app` gained `webhook` and `webhook_secrets`. Delivery is at-least-once: receivers dedupe on `DAS-Webhook-Id`.
- **Replay guard (T7):** `budget_exceeded` is a new replay outcome, checked after `request_state` and before `job_state` in the pure decision (`RequestView.error`), in `replay_dead_letter.lua`, in the metrics outcome set and in Property 20. The 409 body is exactly `{"error": "budget_exceeded"}` (no `current_state`).
- **OpenAPI (T7):** `/v1/openapi.json` serves a corrected copy of FastAPI's schema: the admin routes are excluded, error responses are documented through one `ErrorResponse` model, and the auto-generated 422 is replaced by the 400 `invalid_body` this app returns. The Phase 3 baseline was regenerated again for the new routes and the new outcome.

## Order and dependencies

```
T1 metrics (7.9) ─┬─▶ T2 limits (7.1) ─▶ T3 submit (7.2) ─▶ T4 query + cancel (7.3) ─┐
                  ├─▶ T5 SSE (7.4, 7.5) ──────────────────────────────────────────────┤
                  ├─▶ T6 Webhook_Sender (7.6, 7.7) ───────────────────────────────────┼─▶ T8 error tests (7.10) ─▶ checkpoint 8
                  └─▶ T7 admin guard + OpenAPI (7.8) ─────────────────────────────────┘
```

T5, T6 and T7 need only T1 and the `create_app` signature from T2, so they can run in parallel worktrees after T2. T3 and T4 edit the same handlers; keep them sequential.

---

### Task 1: Metrics catalog (spec 7.9)

**Files:** Modify `das/observability/metrics.py`, `das/observability/labels.py`, `das/gateway/http_metrics.py`. Test `tests/unit/test_observability_labels.py` (extend), new `tests/unit/test_client_metrics.py`.

- [x] Add specs and fixed label values per Req 10.1: `das_client_rejections_total{reason}` (`rate_limited`, `quota_exceeded`, `too_many_streams`, `budget_above_limit`, `idempotency_key_reused`), `das_idempotent_replays_total`, `das_sse_streams` (gauge), `das_webhook_deliveries_total{outcome}` (`delivered`, `retry`, `failed`). `das_budget_exceeded_total{kind}` already existed (group 5). The first draft of this plan listed `invalid_request` and a `result` label; Req 10.1 is the source of truth.
- [x] `GatewayMetrics` gets `client_rejected(reason)`, `idempotent_replay()`, `sse_streams(fn)` (a callable read at scrape time, like `leases_held`, so no open/close pairing can drift), `webhook_delivery(outcome)`. `NoopRecorder` needed no change.
- [x] Unit tests (`tests/unit/test_client_metrics.py`) for names, labels, bounding and no fingerprint/Request_ID/URL labels. `test_metrics_recorder.py` pinned the catalog size (27 → 31).

### Task 2: Client settings and rate limit (spec 7.1)

**Files:** New `das/gateway/limits.py`. Modify `das/gateway/auth.py`, `das/gateway/app.py` (`create_app` signature, router dependencies), `das/cli.py:519`, `das/gateway/admin.py` (router dependency). Test `tests/unit/test_gateway_limits.py`.

- [ ] `api_key_auth` stores `fingerprint` and `ClientSettings` (`resolve_settings(fp, client_limits)`) on `request.state`; admin keys take the same path.
- [ ] `rate_limit` dependency on the `/v1` router and the admin router: `await _bounded(store.rate_check(fp, settings.rate, settings.burst))`. A returned `retry_after` becomes `429 rate_limited` with `Retry-After` (whole seconds, rounded up, at least 1). `rate == 0` skips the Redis call. `/healthz` stays outside the router.
- [ ] Order: auth first (a 401 never consumes a token), then the rate check, then handler validation.
- [ ] Tests: allowed; empty bucket → 429 + `Retry-After`; `rate == 0` never touches Redis; Redis down → 503; `/healthz` exempt; per-key override isolation; 401 consumes no token; a 429 is still audited (`AuditMiddleware` runs before dependencies).

### Task 3: Submit (spec 7.2)

**Files:** Modify `das/gateway/app.py` (`SubmitBody`, `submit`); move the handler to a new `das/gateway/submit.py` if `app.py` would approach 800 lines. Test `tests/unit/test_gateway_submit_client.py`.

- [ ] `SubmitBody.budget: BudgetBody | None` with `extra="forbid"`; an all-`None` budget becomes `None` before it reaches the store.
- [ ] `Idempotency-Key` header: `validate_idempotency_key` failure → `400 invalid_request {parameter: "Idempotency-Key"}`. Budget errors → `400 invalid_request {parameter: "budget.<field>"}`; above the key default → `400 budget_above_limit`.
- [ ] Build `ClientRequestSpec` from `ClientSettings`: fingerprint, resolved Effective_Budget, webhook target if configured, `idempotency_key_hash`, `body_hash` of `{prompt, budget as submitted}`, `max_active_requests`, `events_max_len`.
- [ ] Map `CreateResult.outcome`: `created` → 202 `{"request_id"}`; `replayed` → 200 with the same body and `Idempotent-Replayed: true`, not counted in `das_requests_submitted_total`, increments `das_idempotent_replays_total`; `idempotency_conflict` → 422 `idempotency_key_reused`; `quota_exceeded` → 429 `quota_exceeded` with `limit`.
- [ ] Tests: each outcome; a replay creates no second Request; same key with a different body; no `Idempotency-Key` still works; quota checked after idempotency; no budget field behaves as before; `budget: {}` is stored as no budget.

### Task 4: Query view and cancel (spec 7.3)

**Files:** Modify `das/state/store.py` (`load_snapshot` optional webhook/budget/last-event reads), `das/gateway/app.py` (`request_view`, `get_request`, `cancel`). Test `tests/unit/test_gateway_v1_extensions.py` (extend) and the store read tests.

- [ ] One MULTI returns the snapshot, webhook state, stored budget and last event id. A partial read is 503, never a partial view.
- [ ] `request_view` adds `budget` (`budget_view(budget, budget_status(...))` with the Gateway `price_table`) and `webhook` (`state`, `attempts`, `last_status`) only when present; existing fields unchanged.
- [ ] `cancel` passes `prices=price_table`.
- [ ] Harden `StateStore.cancel_request` (spec 7.3): corrupt stored budget or usage data must not block cancel; log a warning and emit the Terminal_Event without `budget`; docstring note that Budget_Used is fenced by the Job-record compare-and-set; test with a `max_cost` Budget.
- [ ] Tests: budgeted vs unbudgeted view; webhook block; corrupt budget cancel still returns 202; the cancel event of a budgeted Request carries `budget`.

### Task 5: SSE endpoint (spec 7.4, 7.5)

**Files:** New `das/gateway/events.py`. Modify `das/gateway/app.py` (route, dedicated Redis pool, lifespan close). Test `tests/unit/test_gateway_events.py`.

- [ ] `GET /v1/requests/{id}/events`: 404 for an unknown Request; stream headers per design §5 (`Cache-Control: no-cache`, `X-Accel-Buffering: no`); first frame is `snapshot` from the Task 4 MULTI.
- [ ] `Last-Event-ID`: valid → resume after it; older than `XINFO STREAM first-entry` or malformed → resync (Req 7.4), then snapshot.
- [ ] Loop: `XREAD BLOCK` on the dedicated pool (`events.max_connections`); keepalive through `Clock`; `request.is_disconnected()` between reads; close after the Terminal_Event, or right after the snapshot when the Request is already terminal.
- [ ] Caps: Redis counter `das:client:{fp}:streams` (TTL 1 h, refreshed on keepalive) against `ClientSettings.max_streams` → 429; in-process per-replica cap → 503. Both are released in `finally`, including on disconnect and exceptions.
- [ ] No prompt, instruction or result text in any frame; assert it over every frame in a test.
- [ ] Decision for the implementer: test the blocking `XREAD` with a scripted fake pool, not fakeredis blocking reads, and leave real-Redis coverage to group 11.1.
- [ ] Read `EventsConfig` in `das/config/schema.py` first; do not invent field names.

### Task 6: Webhook_Sender (spec 7.6, 7.7)

**Files:** New `das/gateway/webhooks.py`. Modify `das/gateway/app.py` (lifespan starts `webhook.workers` tasks and stops them gracefully). Test `tests/unit/test_gateway_webhooks.py`; extend `tests/property/test_webhook_outbox.py` if the sender joins Property 7.

- [ ] Per task loop: `claim_webhook(lease_seconds)` → look up the secret by `secret_name` at send time (missing → `complete_webhook` as `failed` with `last_status="secret_missing"`, logged once) → `sign` → POST with `httpx.AsyncClient(follow_redirects=False, timeout=webhook.timeout)`, response body read through `aiter_bytes` and capped at 64 KiB → `complete_webhook`: 2xx delivered; otherwise retry at `next_due(claim.claimed_at, ...)`; `failed` exactly when `attempts + 1 == max_attempts`.
- [ ] Idle wait `min(1 s, time to next due)` through `Clock`; the stop flag is checked between deliveries, never mid-send.
- [ ] Read `WebhookConfig` in `das/config/schema.py` first (insecure-target flag, workers, timeout, backoff names); group 11.1 needs the insecure option.
- [ ] Tests (respx): 2xx; 5xx; timeout; connect error; redirect not followed; oversized response; missing secret; final attempt → failed; no secret or signature in captured logs; shutdown leaves an unfinished claim to lapse.

### Task 7: Admin replay guard and OpenAPI (spec 7.8)

**Files:** Modify `das/gateway/admin.py` (`decide_replay`), `das/gateway/app.py` (`openapi_url`). Test `tests/unit/test_gateway_admin_dlq.py`, `tests/unit/test_ops_replay.py`, `tests/unit/test_gateway_contract.py` (extend).

- [ ] A Request whose stored error is `budget_exceeded` or `budget_unenforceable` → `decide_replay` returns `budget_exceeded` → 409.
- [ ] `/v1/openapi.json` served from a route inside the authenticated router; `docs_url` and `redoc_url` stay `None`; unauthenticated → 401. The schema must describe the new 429, 422 and 503 responses, since the group 9 contract test relies on it.

### Task 8: Error-response tests and checkpoint (spec 7.10, then 8)

**Files:** New `tests/unit/test_gateway_client_errors.py`.

- [ ] One parametrized case per row of the design Error Handling table: bucket empty, quota, bad key, key reuse, bad budget, above limit, stream caps, Redis down for the rate check and for create, budget-failed replay, secret removed after enqueue.
- [ ] Checkpoint: `uv run ruff check && uv run mypy && uv run lint-imports && uv run pytest tests/unit tests/property`. Baseline before this group: 2144 passed, 3 known pytest-asyncio warnings.

## Risks and open decisions

1. **Where the fingerprint lives.** If `audit_context` and `api_key_auth` compute it separately they can diverge. Compute once, read twice (Task 2).
2. **`app.py` size.** It is 392 lines now. Put new handlers in `limits.py`, `submit.py`, `events.py`, `webhooks.py` and register them like `register_list_route`, keeping every file under 800 lines.
3. **SSE tests with blocking Redis.** See the Task 5 decision. If a scripted pool proves too far from real behaviour, group 11.1 carries the real-Redis coverage.
4. **Not read while planning.** `EventsConfig` and `WebhookConfig` field names (`das/config/schema.py`), the Phase 4 requirements text, and the `create_request` vs `submit_request` split in the store. Each task that depends on them says to read them first.
