# Design Document: DAS Client API (Phase 4)

## Overview

Phase 4 makes the Gateway_API safe and convenient for application clients: per-key rate limits and quotas, idempotent submit, per-Request budgets, a live Event_Stream over Server-Sent Events, signed webhooks, and a Python client package. Requirement numbers below refer to `requirements.md` of this spec unless prefixed with v1 or v2.

### Design rules kept from earlier phases

- Pure core, thin I/O shell. New pure logic lives in `das/client/` (token bucket, budget resolution and checks, body hash, event payloads, webhook signing) and is property-tested.
- Redis is the source of truth. Every new piece of state (Active_Set, Idempotency_Record, Event_Stream, Webhook_Delivery, Webhook_Outbox, Rate_Limit bucket) is written by a Lua script, in the same script as the state change it belongs to.
- Time comes from Redis `TIME` inside scripts. The Gateway's Webhook_Sender and SSE loops inject `Clock` for their own waits.
- Credentials (webhook secrets) only from env or `secrets_file`, registered with the Redactor.
- Manager_Agent and Worker_Agent keep the import-linter contract. Worker_Agent is unchanged in Phase 4.

### Key decisions

| ID | Decision | Reason |
|---|---|---|
| D1 | Events are appended by the same Lua scripts that change state (approach A) | One atomic write: no lost or reordered events, any Gateway replica can serve a resumed stream |
| D2 | Webhooks use a Redis outbox written by `finalize` / `cancel_request` | A crash between finalize and send cannot lose a delivery |
| D3 | Client identity is the existing Key_Fingerprint; settings are config default + overrides | No key-management API; Phase 5 tenancy replaces it |
| D4 | Budget is a soft cap enforced in the reducer | Fits the pure reducer and fencing; in-flight work is not wasted. Overshoot is bounded by in-flight Jobs (Req 5.8) |
| D5 | Webhook URLs only from operator config | No user-supplied URLs, so no SSRF surface |
| D6 | Webhook failures are not Dead_Letter_Records | DLQ replay semantics are about Job messages; webhook state is visible on the Request |
| D7 | Rate limit fails closed (503) when Redis is unreachable | Same as every other `/v1` call today (Req 2.5) |
| D8 | Client package is a separate distribution with only `httpx` | Applications must not install Redis, RabbitMQ, gRPC and LLM SDKs |
| D9 | Events carry no prompt, instruction or result content | Keeps entries small and avoids copying user data into another key (Req 6.2) |

## Architecture

### Submit path

```
client ──POST /v1/requests──▶ Gateway
  1. api_key_auth → fingerprint fp, Client_Settings
  2. rate_check.lua (bucket das:client:{fp}:bucket)          → 429 rate_limited
  3. validate prompt, Idempotency-Key, budget; resolve Effective_Budget → 400
  4. create_request.lua:
       idem record exists? same hash → REPLAYED rid  (200)
                           other hash → IDEM_CONFLICT     (422)
       active set full?                → QUOTA_EXCEEDED   (429)
       else: request hash (+ fp, budget, webhook), active set SADD,
             idem record SET EX, events XADD request.created  → OK (202)
```

### State change path (Manager)

```
reducer step() ──▶ Decision(effects, state changes)
driver ──▶ apply_transition.lua / commit_plan.lua / reopen_request.lua / finalize.lua
            each also XADDs events in the same script
finalize.lua / cancel_request.lua additionally:
   SREM das:client:{fp}:active rid
   if webhook stored: HSET das:req:{rid}:webhook …, ZADD das:webhooks:due now rid
```

### Read paths (Gateway)

```
GET /v1/requests/{id}/events
   load snapshot + XREVRANGE last id (one MULTI) → send `snapshot`
   loop: XREAD BLOCK 2000 events after cursor → send; keepalive every 15 s
         stop after Terminal_Event when request is terminal

Webhook_Sender (N tasks per Gateway)
   loop: claim_webhook.lua (scan ≤ 10 due entries, re-score the first pending one to now+timeout+5, reply carries Redis now)
         POST signed body → complete_webhook.lua (delivered | retry at backoff | failed)
```

### Package layout (new and changed)

```
das/client/                  # pure core, no I/O
  __init__.py
  settings.py                # ClientSettings, resolve_settings(fp, cfg)
  bucket.py                  # token bucket: take(state, now, rate, burst) -> TakeResult
  budget.py                  # Budget, resolve_budget, budget_status(usage, budget, price_table)
  idempotency.py             # validate_idempotency_key, body_hash
  events.py                  # Event types and payload builders
  webhook.py                 # sign(secret, ts, body), verify(...), WebhookBackoff
das/state/lua/
  rate_check.lua             # new
  claim_webhook.lua          # new
  complete_webhook.lua       # new
  _common.lua                # + append_event(), active_remove(), webhook_enqueue()
  create_request.lua, commit_plan.lua, apply_transition.lua, reopen_request.lua,
  finalize.lua, cancel_request.lua, purge_request.lua   # changed
das/gateway/
  limits.py                  # rate-limit dependency, per-key stream counter
  events.py                  # SSE endpoint
  webhooks.py                # Webhook_Sender loop
  app.py, admin.py, auth.py  # changed
das/manager/machine.py       # budget enforcement
clients/python/              # das-client distribution
```

## Components and Interfaces

### 1. Configuration (Req 1)

```python
class BudgetConfig(_Section):
    max_total_tokens: int | None = Field(default=None, ge=1)
    max_cost: Decimal | None = Field(default=None, gt=0, max_digits=18, decimal_places=6)  # YAML float -> Decimal(repr)

Rate = Annotated[float, Field(ge=0, le=100_000, allow_inf_nan=False)]   # plus: 0 or >= 0.001
SecretName = Annotated[str, Field(pattern=r"^[A-Za-z0-9]+(?:_[A-Za-z0-9]+)*$", max_length=64)]  # lower-cased

class ClientSettingsConfig(_Section):
    rate: Rate = 60.0
    burst: int = Field(default=120, ge=1, le=1_000_000)
    max_active_requests: int = Field(default=100, ge=0, le=1_000_000)
    max_streams: int = Field(default=10, ge=0, le=10_000)
    default_budget: BudgetConfig | None = None
    webhook_url: str | None = None
    webhook_secret_name: SecretName | None = None

class ClientOverride(_Section):            # same fields, every one optional (None = inherit)
    fingerprint: str = Field(pattern=r"^[0-9a-f]{8}$")

class ClientLimitsConfig(_Section):
    default: ClientSettingsConfig = Field(default_factory=ClientSettingsConfig)
    overrides: list[ClientOverride] = Field(default_factory=list, max_length=1000)
    idempotency_ttl: int = Field(default=86_400, ge=60, le=604_800)
    allow_insecure_webhooks: bool = False
    def merged(self, override) -> ClientSettingsConfig: ...      # default + fields the override sets
    def settings_for(self, fingerprint) -> ClientSettingsConfig: ...

class EventsConfig(_Section):
    max_len: int = Field(default=1000, ge=100, le=100_000)
    max_connections: int = Field(default=500, ge=1, le=10_000)
    keepalive: float = Field(default=15.0, gt=0, le=300)

class WebhookConfig(_Section):
    workers: int = Field(default=2, ge=0, le=32)      # 0 disables the sender on this replica
    timeout: float = Field(default=10.0, gt=0, le=60)
    base_backoff: float = Field(default=30.0, gt=0, le=3600)     # <= max_backoff
    max_backoff: float = Field(default=3600.0, gt=0, le=86_400)
    max_attempts: int = Field(default=8, ge=1, le=20)

# AgentConfig gains: client_limits, events, webhook, and
# webhook_secrets: dict[SecretName, SecretStr (>= 16 chars)] — only DAS__WEBHOOK_SECRETS__<NAME> or secrets_file
```

`ClientOverride` declares each field with default `None` so "not set" is distinguishable from the default. `ClientLimitsConfig.merged` is the single place that applies Req 1.3; config validation and `resolve_settings(fp, limits) -> ClientSettings` (pure, `das/client/settings.py`, which also converts `BudgetConfig` to `Budget`) both use it. Secret names are lower-cased because environment names are (`DAS__WEBHOOK_SECRETS__PARTNER_A` → `partner_a`).

Validation (Req 1.4–1.6) runs in `load_config` for role `gateway`, after secrets are merged and before any connection, raising `ConfigError` with field paths such as `client_limits.overrides[2].fingerprint`. `SECRET_FIELDS` gains `webhook_secrets`, and a new `SECRET_PREFIX_FIELDS` marks it as a map whose keys come from `DAS__WEBHOOK_SECRETS__<NAME>`, so the existing loader rejects it in YAML, accepts it from env and the secrets file, and `secret_values()` registers each secret with the Redactor.

The Manager_Agent needs only `events.max_len` (it runs the event-writing scripts). Effective_Budget and Webhook_Target are read from the Request record, so the Manager needs no client settings.

### 2. Pure core `das/client`

```python
# bucket.py  (Req 2.3)
@dataclass(frozen=True)
class BucketState:
    tokens: float
    updated: float            # Redis seconds

@dataclass(frozen=True)
class TakeResult:
    allowed: bool
    state: BucketState
    retry_after: float        # seconds until one token is available; 0.0 when allowed

def take(state: BucketState | None, now: float, rate: float, burst: int) -> TakeResult:
    """Refill min(burst, tokens + rate*(now-updated)), then take 1 if tokens >= 1.
    rate == 0 disables the limit. now < updated is treated as now == updated."""

def retry_after_header(seconds: float) -> int    # ceil, at least 1 (Req 2.2)

# budget.py  (Req 5)
@dataclass(frozen=True)
class Budget:
    max_total_tokens: int | None = None
    max_cost: Decimal | None = None

class BudgetError(Exception): parameter: str; limit: int | Decimal

def resolve_budget(client: Budget | None, default: Budget | None) -> Budget | None: ...
    # raises BudgetError("budget.max_cost", limit) when client field > default field (Req 5.2)

@dataclass(frozen=True)
class BudgetStatus:
    used_tokens: int
    used_cost: Decimal | None         # None when no max_cost or cost unknown
    exceeded: Literal[None, "tokens", "cost", "unenforceable"]

def budget_status(usage: Sequence[UsageRecord], budget: Budget, prices: PriceTable | None) -> BudgetStatus: ...
    # reuses das.ops.cost.unrounded_costs (compares unrounded totals); unpriced model with max_cost → "unenforceable" (Req 5.6)

# idempotency.py  (Req 4)
def validate_idempotency_key(value: str) -> bool
def body_hash(prompt: str, budget: Budget | None) -> str          # sha256 of canonical JSON
def idem_key_hash(value: str) -> str                               # sha256 hex

# events.py  (Req 6)
EventType = Literal["request.created", "request.planned", "job.transition",
                    "request.reopened", "request.completed", "request.failed", "request.cancelled"]
def terminal_event_data(status, error, usage, budget, prices) -> dict   # error is the stored error string

# webhook.py  (Req 8, 9.7)
def sign(secret: bytes, timestamp: int, body: bytes) -> str        # "v1=" + hex hmac-sha256
def verify(secret: bytes, timestamp: str, body: bytes, signature: str, now: float, tolerance: int) -> bool
def next_due(now: float, attempts: int, base: float, max_backoff: float, rand: float) -> float
    # now + backoff(attempts, base, max) * (0.8 + 0.4*rand) — reuses das.manager.backoff.backoff
```

`das/client/webhook.py` `sign`/`verify` is the reference the Client_Package copies. A shared test vector file (`tests/fixtures/webhook_vectors.json`) is checked by both test suites so the copies cannot drift.

### 3. State_Store keys and scripts

New keys (all prefixed `das:`):

| Key | Type | Written by | Deleted by |
|---|---|---|---|
| `client:{fp}:bucket` | hash `tokens`, `updated` | `rate_check` | TTL `ceil(burst/rate*1000)+60000` ms |
| `client:{fp}:active` | set of rid | `create_request`, `replay_dead_letter` | `finalize`, `cancel_request`, `purge_request`, `delete_request` |
| `client:{fp}:idem:{h}` | hash `rid`, `body_hash` | `create_request` | TTL `idempotency_ttl` |
| `req:{rid}:events` | stream | 6 scripts (Req 6.1) | `purge_request` |
| `req:{rid}:webhook` | hash (Req glossary) | `finalize`, `cancel_request`, `claim_webhook`, `complete_webhook` | `purge_request` |
| `webhooks:due` | zset rid → due | `finalize`, `cancel_request`, `claim_webhook`, `complete_webhook` | `claim_webhook` (dead entries), `complete_webhook`, `purge_request`, `delete_request` |

New fields on `das:req:{rid}`: `client_fp`, `budget` (JSON), `webhook_url`, `webhook_secret_name`, `events_max_len`.

`_common.lua` helpers derive every Phase 4 key name from the request key `das:req:{rid}` passed as KEYS[1] (prefix = text before `:req:`), so they always match `das/state/keys.py`:

```lua
key_prefix(req_key)                      -- 'das'
events_key(req_key), webhook_key(req_key) -- req_key .. ':events' / ':webhook'
outbox_key(req_key)                      -- 'das:webhooks:due'
active_key(req_key, fp)                  -- 'das:client:{fp}:active'
append_event(req_key, type, data_json)   -- XADD MAXLEN <stored events_max_len or 1000> * type .. data ..
client_active_add / client_active_remove(req_key, rid)   -- uses the stored client_fp; no-op without one
terminal_event(req_key, rid, status, error_or_nil, budget_json, ms)
    -- builds {status, error, usage} from the request hash, splices "budget":<budget_json> when given,
    -- appends the Terminal_Event and, when webhook_url is stored, writes the Webhook_Delivery and ZADDs the outbox
put_job(..., tag, req_key)               -- also appends one job.transition per transition entry it writes
```

Script changes:

- **`rate_check.lua`** (new). KEYS[1] bucket; ARGV rate, burst. Implements `bucket.take` with `TIME`, storing `tokens` and `updated` as `%.17g` text so Python and Lua compute the same doubles. `rate == 0` returns `{'OK'}` without touching the key. TTL `ceil(burst/rate) s + 60 s`. Returns `{'OK'}` or `{'LIMITED', retry_after_seconds_text}`.
- **`create_request.lua`**. KEYS unchanged. New optional ARGV[4..12]: `client_fp`, `budget_json`, `webhook_url`, `webhook_secret_name`, `idem_hash`, `body_hash`, `idem_ttl`, `max_active` (0 = off), `events_max_len`. Order: (1) idempotency: an existing record whose Request still exists returns `{'REPLAYED', rid}` or `{'IDEM_CONFLICT'}`; a record naming a Request that no longer exists is treated as absent and replaced (Req 4.8); (2) Request_ID collision `{'EXISTS'}`; (3) quota `{'QUOTA_EXCEEDED', max_active}`; (4) writes, `SADD` active set, idem record with TTL, `request.created`.
- **`commit_plan.lua`**: new ARGV[6] = `planned_event_data` JSON ('' for none), job pairs move to ARGV[7..]; `request.planned` is appended before the Jobs' `job.transition` events.
- **`apply_transition.lua`**: `put_job` appends `job.transition` events; no interface change.
- **`reopen_request.lua`**: unchanged (no Event; the Replay already recorded the reopen).
- **`replay_dead_letter.lua`**: when it reopens a Failed Request, `client_active_add` (Req 3.4) and `append_event(request.reopened, {"status":"Pending"})` (Req 6.4).
- **`finalize.lua`**: new ARGV[8] = budget view JSON ('' for none); `client_active_remove` and `terminal_event` after the existing writes.
- **`cancel_request.lua`**: new ARGV[3] = budget view JSON, job triples move to ARGV[4..]; `put_job` events, then `client_active_remove` and `terminal_event`.
- **`purge_request.lua`**: `request_keys(rid)` gains `events` and `webhook` (KEYS[10], KEYS[11]; index keys shift to KEYS[12..21]); also `client_active_remove` and `ZREM` of the outbox.
- **`claim_webhook.lua`** (new). KEYS[1] `webhooks:due`; ARGV[1] lease seconds, ARGV[2] request key prefix `das:req:`. Takes the first rid with score ≤ now whose hash exists and is `pending` (others are removed, at most 10 per call), re-scores it to `now + lease`, `HINCRBY claim 1`, and returns `{rid, claim, attempts, url, secret_name, event_id, type, data, now}` (`now` is the Redis time of the claim, `fmt_double`, so the sender computes `next_due` without a host clock) or `{}`. `StateStore.next_webhook_due()` returns the lowest outbox score, for the sender's idle wait.
- **`complete_webhook.lua`** (new). KEYS[1] outbox, KEYS[2] webhook hash; ARGV rid, claim, outcome (`delivered` | `retry` | `failed`), `last_status`, `next_due`, `event_id`. Applies only when the hash is `pending`, its `claim` equals ARGV claim (a sender whose lease ran out and was re-claimed is fenced) and its `event_id` equals ARGV event_id (a sender whose delivery was reset by a later terminal write, which restarts `claim` at 0, is fenced); otherwise returns `{'STALE'}`. Every outcome increments `attempts`.

### 4. Reducer (`das/manager/machine.py`) (Req 5.5–5.8)

- `RequestSnapshot` gains `budget: Budget | None`.
- `StepContext` gains `price_table: PriceTable | None`.
- After usage is merged in `step`, the reducer computes `budget_status(snapshot.usage + new_usage, budget, ctx.price_table)`. A Request is in budget-stop mode when `exceeded` is not None.
- In budget-stop mode:
  1. The dispatch phase is skipped, and no `PublishDispatch` is emitted.
  2. Each Waiting Job, and each Ready Job that isn't `_in_flight`, goes to Skipped with reason `budget_exceeded`.
  3. A result that would trigger a retry instead marks the Job Failed without scheduling a retry timer.
  4. Once no Job is `_in_flight`, the reducer emits `FinalizeFailed` with error `budget_exceeded` (or `budget_unenforceable`) instead of `RunSummarizer`.
- Budget-stop details (decided while planning group 5):
  - "Not in flight" includes a Ready Job with a pending `publish` or `capwait` timer (never published); it is Skipped and its timers removed.
  - A Failed or TimedOut Job awaiting a retry timer becomes Failed with reason `budget_exceeded`; its retry timer is removed and no `PublishDeadLetter` is emitted. A result that would schedule a retry does the same.
  - A firing retry or publish timer does not re-dispatch.
  - A `ReplayRequested` is reverted with violation `budget exceeded` (the Admin_API also refuses it, Req 5.10).
  - `FinalizeFailed.final_result` is `build_final_result(plan, jobs, results, None, kind_error)`, where `kind_error` is `budget_exceeded` or `budget_unenforceable`; the driver stores that string as the Request error.
  - `RequestSnapshot.usage` holds every persisted UsageRecord (the driver loads snapshots with `include_usage=True`), so a Manager that takes over the Lease computes the same Budget_Used.
- The planning check (Req 5.5) runs in the driver before the Planner is called: `max_cost` set with no price table emits `FinalizeFailed(budget_unenforceable)`. Planning usage is recorded first, so a plan that alone exceeds the budget fails before any dispatch.
- Budget-stop is recomputed from persisted usage on every step. A Manager that takes over the Lease reaches the same decision without needing extra state. This keeps the takeover property of v1 Req 2.5.
- The driver passes `price_table` into `StepContext`. Metrics: `das_budget_exceeded_total{kind}` increments once, when `FinalizeFailed` is written successfully.

### 5. Gateway_API

**Auth and limits (`auth.py`, `limits.py`).** `api_key_auth` already computes the fingerprint for audit. It now also stores `ClientSettings` on `request.state`. A `rate_limit` dependency on the `/v1` router (and on the admin router) runs `rate_check.lua` via `_bounded`. The 429 response sets `Retry-After`. `/healthz` sits outside the router (Req 2.6).

**Submit (`app.py`).** `SubmitBody` gains `budget: BudgetBody | None` (`extra="forbid"`). Steps follow the Architecture diagram. A replayed submit returns `200 {"request_id"}` with `Idempotent-Replayed: true`, does not count in `das_requests_submitted_total`, and increments `das_idempotent_replays_total`.

**Query.** `request_view` adds `budget` (from the stored budget, the usage records and the Gateway's price table) and `webhook` (from `das:req:{rid}:webhook`, read in the same MULTI as the snapshot).

**SSE (`events.py`).**
- It uses `StreamingResponse(media_type="text/event-stream")` with `Cache-Control: no-cache` and `X-Accel-Buffering: no`.
- The per-key open streams are a Redis sorted set `das:client:{fp}:streams`: one member per connection, scored by its expiry in Redis time. `stream_acquire.lua` prunes expired members, refuses at `max_streams` and adds the member in one atomic step. A live stream renews its member about every third of its TTL (`max(30 s, 3 x events.keepalive)`) and removes it in `finally`. This replaces the first draft's counter with a TTL: a counter cannot tell a crashed Gateway's stale +1 from a live stream, so while any stream of the key kept refreshing the TTL the stale count would never expire; a member per connection expires on its own.
- The per-replica count is an in-process counter.
- Each stream uses one connection from a dedicated `redis.asyncio` pool of size `events.max_connections`. A blocking `XREAD` would otherwise hold a connection in the shared pool.
- The snapshot is read with `load_snapshot` and `XREVRANGE + - COUNT 1` in one MULTI.
- `Last-Event-ID` older than `XINFO STREAM first-entry` causes a resync (Req 7.4).
- Client disconnects are detected through `request.is_disconnected()` between reads.

**Webhook_Sender (`webhooks.py`).**
- `webhook.workers` asyncio tasks start in the Gateway lifespan.
- Each task loops: claim, send, complete. When nothing is due it waits `min(1 s, time to next due)` using `Clock`.
- HTTP goes through a shared `httpx.AsyncClient(follow_redirects=False, timeout=webhook.timeout)`. The response body is read through `aiter_bytes` and stopped at 64 KiB.
- Results are written with `complete_webhook.lua`. On shutdown, tasks stop after their current send, and claimed but unfinished deliveries become due again when their lease ends.

**Admin replay (`admin.py`).** `decide_replay` gains a check: a Request whose stored `error` string is `budget_exceeded` or `budget_unenforceable` returns `budget_exceeded`, mapped to 409 (Req 5.10).

**OpenAPI.** `openapi_url="/v1/openapi.json"` is served from a route inside the authenticated router. Docs UIs stay disabled.

### 6. Metrics (Req 10)

The new specs are added to the Phase 2 catalog in `das/observability/metrics.py`, with fixed label values from `das/observability/labels.py`. The existing cardinality property test (v2 Property 3) covers them once they're in the catalog.

### 7. Client Package (`clients/python/`) (Req 9)

```
clients/python/
  pyproject.toml           # name das-client, requires-python >=3.10,<3.14, dependencies ["httpx==0.28.1"]
  src/das_client/
    __init__.py            # DasClient, AsyncDasClient, errors, verify_webhook, models
    _transport.py          # shared request/retry logic (sync + async variants)
    _sse.py                # minimal text/event-stream parser
    models.py              # frozen dataclasses: Submitted, RequestView, JobView, Budget, Event, …
    errors.py
    webhook.py             # copy of das.client.webhook sign/verify
  tests/                   # respx unit tests, webhook vectors
```

- Retries use exponential backoff (base 0.5 s, max 8 s, full jitter) with a 429 floor of `Retry-After`.
- `submit` creates its UUID4 idempotency key before the first attempt.
- `events()` reconnects up to `max_retries` times in a row, resetting after any received event.
- `wait()` falls back to polling `get` every 1 s when `events()` raises a transport error.
- Models are built with `from_json(cls, data)`, which picks known fields and ignores the rest (Req 9.9).
- The contract test lives in the main repo, in `tests/contract/test_client_contract.py`. It installs the client as a path dependency in the dev group and runs it against `create_app(...)` with fakeredis through `httpx.ASGITransport`. The root dev group installs the client as an editable **path source** (`[tool.uv.sources] das-client = { path = "clients/python", editable = true }`), so `uv sync` installs both packages. It is deliberately not a uv workspace member: a member inherits the server's `requires-python = "==3.12.*"`, the lock keeps that range, and uv then refuses `--python 3.10` and `--python 3.13` for the client (checked with uv 0.9.28). The client's own tests therefore run in an isolated environment (see §8).

### 8. CI

`.github/workflows/ci.yml` gains a `client` job with a matrix of every Python the package claims, 3.10, 3.11, 3.12 and 3.13 (not only the two ends: the fast job never runs the client's own suite, so without 3.12 nothing would test it on the server's interpreter). Each matrix entry runs `uv run --isolated --no-project --with-editable clients/python --with pytest==9.1.1 --with pytest-asyncio==1.4.0 --with respx==0.23.1 pytest clients/python/tests -c clients/python/pyproject.toml --rootdir clients/python` (the root project is skipped because its lock is Python 3.12 only; see §7). The Python comes from `UV_PYTHON`, which `astral-sh/setup-uv` sets from its `python-version` input, so the command is the same for every entry and equals the README block. The job runs on every trigger and does not wait for the fast job. The fast job also runs `tests/contract`, in its existing pytest step. `test_ci_workflow_static.py` checks the matrix against the `requires-python` range and the tool pins against the root dev group.

## Data Models

```python
class BudgetBody(BaseModel, extra="forbid"):
    max_total_tokens: int | None = Field(default=None, ge=1)
    max_cost: Decimal | None = Field(default=None, gt=0, max_digits=18, decimal_places=6)

class EventEnvelope(DasModel):            # what the Gateway serves; also webhook body
    request_id: str
    event_id: str                          # stream id "<ms>-<seq>"
    type: EventType
    data: dict[str, Any]

class WebhookState(DasModel):
    state: Literal["pending", "delivered", "failed"]
    attempts: int
    last_status: str | None
```

The terminal event's `data`:

```json
{"status": "Failed", "error": "budget_exceeded",
 "usage": {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
 "budget": {"limits": {"max_total_tokens": 5000}, "used": {"total_tokens": 5120, "cost": null}, "exceeded": true}}
```

## Correctness Properties

Each property maps to exactly one property-test sub-task. Tests carry the tag `# Feature: das-client-api, Property {n}: {title}` and run at least 100 examples.

### Property 1: Token bucket bound
*For any* rate, burst and non-decreasing sequence of call times, the calls admitted by `bucket.take` in any interval of length T number at most `floor(burst + rate × T)`. `rate_check.lua` returns the same decisions as `bucket.take` for the same sequence on fakeredis with patched `TIME`.
**Validates: Requirements 2.3, 2.4**

### Property 2: Idempotent create
*For any* set of concurrent submits sharing one fingerprint and Idempotency_Key, with bodies drawn from a small pool, exactly one Request is created. Every submit whose Body_Hash equals the creator's gets its Request_ID, and every other submit gets `IDEM_CONFLICT`. A second fingerprint with the same key value is independent.
**Validates: Requirements 4.2, 4.3, 4.4, 4.6, 4.7**

### Property 3: Active_Set equals non-terminal Requests
*For any* sequence of create, finalize, cancel, reopen and purge operations across several fingerprints, after every operation each `das:client:{fp}:active` equals that fingerprint's set of non-terminal Requests. Requests without `client_fp` are in no set.
**Validates: Requirements 3.1, 3.3, 3.4, 3.5, 3.6, 3.7**

### Property 4: Budget resolution never raises a limit
*For any* client Budget and default Budget, `resolve_budget` either raises `BudgetError` for a field where the client value exceeds the default, or returns a Budget whose every field is ≤ the default's when the default's is set, and equals the client value when given.
**Validates: Requirements 5.2, 5.3**

### Property 5: Budget stop
*For any* valid plan, Budget, and sequence of reducer events (results with usage, failures, timers, ticks), once `budget_status` first reports exceeded:
- No later Decision contains `PublishDispatch`, and no retry timer is scheduled.
- Final Budget_Used minus Budget_Used at that moment is at most the usage of results for Jobs that were `_in_flight` at that moment.
- The Request is finalized Failed with `budget_exceeded` or `budget_unenforceable` exactly when no Job remains in flight, and `RunSummarizer` is never emitted.

**Validates: Requirements 5.6, 5.7, 5.8**

### Property 6: Event_Stream mirrors persisted changes
*For any* sequence of store operations on one Request (create, commit plan, transition batches, finalize or cancel, a Replay that reopens it, then either a cancel before resumption or a resume and another finalize):
- The Event_Stream holds one `request.created`, one `request.planned` per commit, one `job.transition` per transition-log entry in the same order, and one `request.reopened` per Replay that reopens a Failed Request, including when the reopened Request is cancelled before a Manager resumes it.
- It holds exactly one Terminal_Event after each terminal write, and that event is the last entry while the Request is terminal.
- Operations rejected with `LEASE_LOST` or `REQUEST_TERMINAL` add no Event.
- No event field contains the prompt or any instruction or result text.

**Validates: Requirements 6.1, 6.2, 6.3, 6.4**

### Property 7: Webhook outbox delivery
*For any* mix of Requests with and without a Webhook_Target, each terminal write enqueues exactly one delivery for Requests with a target and none for those without. Then *for any* interleaving of several simulated senders that claim, crash (stop without completing), time out, or complete with random responses:
- Every delivery ends `delivered` or `failed`, with `attempts ≤ max_attempts`.
- No claim succeeds while another sender's lease on the same delivery is live.
- A stale sender's `complete_webhook` call has no effect.

**Validates: Requirements 8.2, 8.3, 8.5, 8.6, 8.7**

### Property 8: Webhook signature round trip
*For any* secret, timestamp and body, `verify(secret, ts, body, sign(secret, ts, body), now, tol)` holds when `|now − ts| ≤ tol`. It fails when `|now − ts| > tol`, or when the body, timestamp, signature or secret is changed. The Client_Package's `verify_webhook` agrees with `das.client.webhook.verify` on every generated case.
**Validates: Requirements 8.4, 9.7, 9.8**

## Error Handling

| Condition | Response | Notes |
|---|---|---|
| Bucket empty | 429 `rate_limited`, `Retry-After` | before any other handling |
| Active_Set full | 429 `quota_exceeded`, `limit` | after the idempotency check |
| Bad `Idempotency-Key` | 400 `invalid_request` `parameter: Idempotency-Key` | |
| Key reused with another body | 422 `idempotency_key_reused` | |
| Budget field invalid | 400 `invalid_request` `parameter: budget.<f>` | |
| Budget above key default | 400 `budget_above_limit` | |
| Stream cap per key / per replica | 429 / 503 `too_many_streams` | |
| Redis unreachable during any of these | 503 as today | fail closed (D7) |
| Replay of a budget-failed Request | 409 `budget_exceeded` | |
| Webhook secret removed after enqueue | delivery `failed`, `last_status: secret_missing` | logged once per delivery |

## Testing Strategy

- **Property tests** (`tests/property/`): Properties 1–8. Properties 1, 2, 3, 6 and 7 run the real Lua scripts on `fakeredis[lua]` with the existing `FakeRedisTime` fixture. Property 5 drives the pure reducer.
- **Unit tests**:
  - Config validation (Req 1.4–1.6).
  - Every error row above.
  - SSE: snapshot first, resync, keepalive with `FakeClock`, close after terminal, stream caps and release, a terminal Request served then closed.
  - Webhook sender: no redirect, 64 KiB cap, timeout path, secret missing.
  - Admin replay 409.
  - OpenAPI behind auth.
  - Metrics names and labels.
- **Contract test** (`tests/contract/`): the client against the in-process Gateway, covering submit (with and without a key), replay, conflict, budget errors, get, cancel, events to terminal (the test drives store writes directly), and `list_requests` paging.
- **Client package tests**: respx-based retries, `Retry-After`, typed errors, the SSE parser and reconnect, `wait()` fallback, unknown-field tolerance, and the shared webhook vectors.
- **Integration / e2e** (`tests/integration/e2e/test_client_api_e2e.py`, Compose stack):
  - Two Gateway replicas share one bucket. The override file scales `gateway=2` behind distinct ports.
  - An SSE stream runs from submit to Completed while the lease-owning Manager is killed. The stream must show the takeover's transitions and one Terminal_Event.
  - A webhook receiver container (`webhook_sink.py`, stdlib only) verifies signatures with the shared vectors logic and fails the first two attempts, with `base_backoff` set to 1 s in the e2e override.
  - A budget of `max_total_tokens` small enough to stop after the first Job ends Failed with `budget_exceeded` and partial results visible.
  - `das-client` `wait()` against the stack.
- **Regression**: the full Phase 1–3 suites and `lint-imports`, unchanged.
