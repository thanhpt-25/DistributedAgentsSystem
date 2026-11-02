# Requirements Document

## Introduction

This is Phase 4 "Production-ready client API" of the Distributed Agent System (DAS). Phase 1 (`.kiro/specs/distributed-agent-system/`), Phase 2 (`.kiro/specs/das-observability-operations/`) and Phase 3 (`.kiro/specs/das-hardening-ci/`) are implemented. Phase 4 lets an application integrate with DAS without polling loops or duplicate work, and keeps one client from starving the others. It adds:

1. Per-key client settings keyed by the existing Key_Fingerprint.
2. Rate limits and concurrent-Request quotas per API key, shared across Gateway_API replicas.
3. An `Idempotency-Key` header on submit.
4. Per-Request token and cost budgets enforced by the Manager_Agent.
5. A per-Request Event_Stream written atomically with every state change, served as Server-Sent Events.
6. Signed webhooks on terminal status, delivered from a durable outbox.
7. A Python client package, `das-client`.

Reference convention: "v1 Req X.Y" is Requirement X, criterion Y in the Phase 1 spec; "v2 Req X.Y" in the Phase 2 spec. Every Phase 1 and Phase 2 term keeps its glossary meaning.

Design rules kept from earlier phases: pure core + thin I/O shell; Manager_Agent and Worker_Agent reach the network only through Transport and Message_Codec; inject Clock; credentials never in YAML or logs; config precedence env (`DAS__`) > file > default; Redis is the source of truth and every state change goes through a Lua script with fencing-epoch checks; time comes from Redis `TIME`.

Intended semantic changes from earlier phases:

- Rate limits and quotas apply to existing clients after upgrade. Defaults are generous and each limit can be set to 0 to disable it (Requirement 2, 3).
- A Request may end Failed with error `budget_exceeded` or `budget_unenforceable`; such a Request cannot be replayed (Requirement 5).

Places where property-based testing applies are marked by criteria of the form "FOR ALL".

## Glossary

- **Key_Fingerprint**: First 8 hex characters of `sha256(api_key)`, as defined in Phase 2 (`das.ops.keys.key_fingerprint`).
- **Client_Settings**: The effective settings for one Key_Fingerprint: `client_limits.default` merged with the matching entry of `client_limits.overrides`.
- **Rate_Limit**: A token bucket per Key_Fingerprint with `rate` (tokens per second) and `burst` (bucket size).
- **Concurrent_Quota**: Maximum number of non-terminal Requests per Key_Fingerprint (`max_active_requests`).
- **Active_Set**: Redis set `das:client:{fp}:active` of non-terminal Request_IDs created with that Key_Fingerprint.
- **Idempotency_Key**: Client-supplied header value on `POST /v1/requests`, 1 to 128 printable ASCII characters (0x21–0x7E).
- **Idempotency_Record**: Redis key `das:client:{fp}:idem:{sha256(Idempotency_Key)}` holding the Request_ID and a Body_Hash, with TTL `client_limits.idempotency_ttl`.
- **Body_Hash**: `sha256` of the canonical JSON of `{prompt, budget}` as submitted (before default merging).
- **Budget**: Optional per-Request limits `max_total_tokens` (int ≥ 1) and `max_cost` (decimal string > 0, in the Price_Table currency).
- **In_Flight_Job**: A Job the reducer treats as in flight (`das.manager.machine._in_flight`): state Dispatched, or Ready with attempt ≥ 1, and no Job_Result recorded for its current attempt. A Ready Job with a pending `publish` or `capwait` timer was never published for its attempt and is not an In_Flight_Job (the same rule `resume` uses).
- **Effective_Budget**: The Budget stored on the Request after merging the client Budget with the key's `default_budget`.
- **Budget_Used**: `total_tokens` and Estimated_Cost summed over every UsageRecord of the Request (planning, Job, summarize).
- **Event_Stream**: Redis stream `das:req:{rid}:events`; each entry is one Event.
- **Event**: An Event_Stream entry with fields `type` and `data` (JSON). Its Event_ID is the stream entry ID.
- **Terminal_Event**: An Event of type `request.completed`, `request.failed` or `request.cancelled`.
- **SSE_Endpoint**: `GET /v1/requests/{id}/events`, serving `text/event-stream`.
- **Webhook_Target**: The `webhook_url` and `webhook_secret_name` of a key's Client_Settings.
- **Webhook_Outbox**: Redis sorted set `das:webhooks:due` of Request_IDs scored by next due time (Redis seconds).
- **Webhook_Delivery**: Redis hash `das:req:{rid}:webhook` with `url`, `secret_name`, `state` (`pending`, `delivered`, `failed`), `attempts`, `last_status`, `claim` (claim counter), `event_id`, `type`, `data`.
- **Webhook_Sender**: The Gateway_API loop that claims and sends Webhook_Deliveries.
- **Client_Package**: Python distribution `das-client` in `clients/python/`.

## Requirements

### Requirement 1: Client Settings

**User Story:** As an operator, I want to give individual API keys their own limits and webhook, so that heavy or special clients can be configured without code changes.

#### Acceptance Criteria

1. THE Agent_Config SHALL accept a `client_limits` section with `default` (Client_Settings fields) and `overrides` (list of entries, each with a required `fingerprint` and any Client_Settings field), plus `idempotency_ttl` (seconds, default 86400, range 60–604800). `overrides` holds at most 1000 entries and is set in the configuration file only.
2. THE Client_Settings fields SHALL be `rate` (finite float, 0 or 0.001 to 100000, default 60), `burst` (int 1 to 1000000, default 120), `max_active_requests` (int 0 to 1000000, default 100), `max_streams` (int 0 to 10000, default 10), `default_budget` (Budget or null, default null), `webhook_url` (string or null) and `webhook_secret_name` (string or null). A value of 0 for `rate`, `max_active_requests` or `max_streams` SHALL disable that limit.
3. WHEN resolving Client_Settings for a Key_Fingerprint, THE Gateway_API SHALL use each field of the matching override when set and the `default` value otherwise.
4. IF an override `fingerprint` matches no configured API_Key or Admin_API_Key, or two configured keys share a Key_Fingerprint, or two overrides name the same fingerprint, THEN THE Gateway_API SHALL abort startup with exit code 2 before connecting to any dependency, naming the field path.
5. IF exactly one of `webhook_url` and `webhook_secret_name` is set in effective Client_Settings, or `webhook_secret_name` names no secret available from env (`DAS__WEBHOOK_SECRETS__<NAME>`) or `secrets_file`, THEN THE Gateway_API SHALL abort startup with exit code 2 naming the field path. Webhook secret names are letters and digits in groups joined by single underscores, and are compared case-insensitively.
6. IF `webhook_url` does not use scheme `https` and `client_limits.allow_insecure_webhooks` is not true, THEN THE Gateway_API SHALL abort startup with exit code 2 naming the field path.
7. THE System SHALL accept webhook secrets only from env or `secrets_file`, never from YAML, SHALL require each to be at least 16 characters, and SHALL register every webhook secret with the Redactor.

### Requirement 2: Rate Limit

**User Story:** As an operator, I want each key's call rate limited across all Gateways, so that one client cannot overload the system.

#### Acceptance Criteria

1. WHEN an authenticated call to any `/v1` route arrives and the key's `rate` is greater than 0, THE Gateway_API SHALL take one token from the key's Rate_Limit bucket in Redis before handling the call, using Redis `TIME`.
2. IF the bucket has fewer than one token, THEN THE Gateway_API SHALL return HTTP 429 with body `{"error":"rate_limited"}` and a `Retry-After` header holding the whole seconds (rounded up, minimum 1) until one token is available, and SHALL NOT handle the call.
3. FOR ALL sequences of call times for one key, the number of admitted calls in any interval of length T SHALL be at most `burst + rate × T` (rounded down).
4. THE Rate_Limit state SHALL be shared by every Gateway_API replica connected to the same Redis.
5. IF the Rate_Limit check cannot reach Redis within the existing State_Store timeout, THEN THE Gateway_API SHALL return HTTP 503 as for other State_Store failures and SHALL NOT handle the call.
6. THE Gateway_API SHALL NOT apply the Rate_Limit to `/healthz` or to Ops_Port endpoints.

### Requirement 3: Concurrent Quota

**User Story:** As an operator, I want a cap on how many Requests a key has in progress, so that one client cannot fill the worker pool.

#### Acceptance Criteria

1. WHEN `POST /v1/requests` creates a Request, THE create script SHALL store the Key_Fingerprint on the Request record and add the Request_ID to the key's Active_Set in the same atomic write.
2. IF `max_active_requests` is greater than 0 and the Active_Set already holds that many members, THEN THE Gateway_API SHALL return HTTP 429 with body `{"error":"quota_exceeded","limit":<n>}` and SHALL NOT create the Request.
3. WHEN a Request reaches a terminal status through `finalize` or `cancel_request`, THE script SHALL remove the Request_ID from its key's Active_Set in the same atomic write.
4. WHEN a Replay reopens a Failed Request (Failed → Pending), THE `replay_dead_letter` script SHALL add the Request_ID to its key's Active_Set in the same atomic write, regardless of the Concurrent_Quota.
5. WHEN retention purges a Request, THE `purge_request` script SHALL remove the Request_ID from its key's Active_Set.
6. FOR ALL sequences of create, finalize, cancel, reopen and purge operations, each key's Active_Set SHALL equal the set of that key's Requests whose status is non-terminal.
7. THE System SHALL NOT count Requests without a stored Key_Fingerprint (created before the upgrade) in any Active_Set.

### Requirement 4: Idempotent Submit

**User Story:** As a client developer, I want to retry a submit safely, so that a timeout never creates duplicate Requests.

#### Acceptance Criteria

1. THE `POST /v1/requests` route SHALL accept an optional `Idempotency-Key` header. IF the value is empty, longer than 128 characters, or contains a character outside 0x21–0x7E, THEN THE Gateway_API SHALL return HTTP 400 with `{"error":"invalid_request","parameter":"Idempotency-Key"}`.
2. WHEN a submit carries an Idempotency_Key and no Idempotency_Record exists for it, THE create script SHALL create the Request and the Idempotency_Record in the same atomic write, and THE Gateway_API SHALL return 202 as in v1 Req 1.1.
3. WHEN a submit carries an Idempotency_Key whose Idempotency_Record exists with the same Body_Hash, THE Gateway_API SHALL return HTTP 200 with the original `request_id` and header `Idempotent-Replayed: true`, and SHALL NOT create a Request.
4. IF a submit carries an Idempotency_Key whose Idempotency_Record exists with a different Body_Hash, THEN THE Gateway_API SHALL return HTTP 422 with `{"error":"idempotency_key_reused"}` and SHALL NOT create a Request.
5. THE Gateway_API SHALL evaluate the Idempotency_Record before the Concurrent_Quota, so that a replayed submit is never rejected with `quota_exceeded`. The Rate_Limit (Requirement 2) still applies to replayed submits.
6. FOR ALL interleavings of concurrent submits sharing one Key_Fingerprint and Idempotency_Key, exactly one Request SHALL be created and every response with the same Body_Hash SHALL return its Request_ID.
7. Idempotency_Keys SHALL be scoped per Key_Fingerprint: the same value from two keys SHALL refer to two independent Idempotency_Records.
8. IF an Idempotency_Record exists but the Request it names no longer exists (removed by retention or deletion), THEN THE create script SHALL treat the record as absent: it SHALL create a new Request and replace the record, as in criterion 2.

### Requirement 5: Request Budget

**User Story:** As a client developer, I want to cap tokens and cost per Request, so that a runaway plan cannot spend without limit.

#### Acceptance Criteria

1. THE `POST /v1/requests` body SHALL accept an optional `budget` object with optional `max_total_tokens` and `max_cost`. IF a field has the wrong type or range, THEN THE Gateway_API SHALL return HTTP 400 with `{"error":"invalid_request","parameter":"budget.<field>"}`.
2. THE Gateway_API SHALL compute the Effective_Budget per field: the client value when given, otherwise the key's `default_budget` value. IF a client value is greater than the key's `default_budget` value for the same field, THEN THE Gateway_API SHALL return HTTP 400 with `{"error":"budget_above_limit","parameter":"budget.<field>","limit":<value>}`.
3. FOR ALL client Budgets and `default_budget` values, every field of an accepted Effective_Budget SHALL be less than or equal to the corresponding `default_budget` field when that field is set.
4. THE create script SHALL store the Effective_Budget on the Request record in the same atomic write that creates it.
5. IF the Effective_Budget sets `max_cost` and the Manager_Agent has no Price_Table, THEN THE Manager_Agent SHALL finalize the Request as Failed with error `budget_unenforceable` before planning.
6. IF a UsageRecord of a Request with `max_cost` has a (provider, model) absent from the Price_Table, THEN THE Manager_Agent SHALL treat the budget as exceeded and record error `budget_unenforceable`.
7. WHEN the reducer accepts usage that makes Budget_Used reach or exceed any Effective_Budget field, THE Manager_Agent SHALL dispatch no further Jobs, SHALL move every Waiting Job and every Ready Job that is not an In_Flight_Job to Skipped with reason `budget_exceeded`, SHALL let In_Flight_Jobs finish without retrying or dead-lettering any that fail (a Job awaiting a retry becomes Failed with reason `budget_exceeded`), SHALL revert any accepted Replay of one of its Jobs, and WHEN no In_Flight_Job remains SHALL finalize the Request as Failed with error `budget_exceeded` without running the Summarizer.
8. FOR ALL plans, usage sequences and Budgets, after Budget_Used first reaches a limit the Manager_Agent SHALL make no dispatch, and the final Budget_Used SHALL exceed the value at that moment only by usage of Jobs that were In_Flight_Jobs at that moment.
9. THE `GET /v1/requests/{id}` response SHALL include `budget: {"limits": {...}, "used": {"total_tokens": n, "cost": "<decimal>"|null}, "exceeded": bool}` when the Request has an Effective_Budget, and `budget: null` otherwise. Job results of Succeeded Jobs SHALL stay queryable after `budget_exceeded`.
10. IF an Admin_API Replay targets a Dead_Letter_Record whose Request failed with error `budget_exceeded` or `budget_unenforceable`, THEN THE Gateway_API SHALL return HTTP 409 with `{"error":"budget_exceeded"}` and SHALL NOT change any state.

### Requirement 6: Event Stream

**User Story:** As a client developer, I want every change to my Request recorded in order, so that I can follow progress without polling.

#### Acceptance Criteria

1. THE scripts SHALL append Events to the Event_Stream in the same atomic write as the state change they describe: `create_request` → `request.created`; `commit_plan` → `request.planned` (Job_IDs, Capabilities, dependencies); `apply_transition` → one `job.transition` per Job state change (job_id, from, to, attempt, worker_id); `replay_dead_letter` → `request.reopened` (status Pending) when it reopens a Failed Request; `finalize` and `cancel_request` → one Terminal_Event (status, error, token usage total, Budget_Used when budgeted).
2. Events SHALL NOT contain the prompt, Job instructions, Job_Result content or the Final_Result.
3. FOR ALL sequences of state changes of one Request, the Event_Stream SHALL hold exactly one Event per persisted change, in the order of the transition log, and at most one Terminal_Event, which is the last Event while the Request is terminal.
4. WHEN a Replay reopens a Failed Request, THE `replay_dead_letter` script SHALL append `request.reopened` in the same atomic write, so the Event_Stream continues after the earlier Terminal_Event with `request.reopened` before any later Event, including a Terminal_Event from a cancel that happens before a Manager resumes the Request. THE Request SHALL then produce a new Terminal_Event when it finishes. `reopen_request` (the Manager resuming the Request, Pending → Running) SHALL append no Event.
5. THE scripts SHALL trim the Event_Stream to at most `events.max_len` entries (default 1000, range 100–100000). THE `create_request` script SHALL store the Gateway's `events.max_len` on the Request record, and later scripts SHALL use the stored value (1000 when absent).
6. WHEN retention purges a Request, THE `purge_request` script SHALL delete its Event_Stream.

### Requirement 7: Server-Sent Events Endpoint

**User Story:** As a client developer, I want a live stream of my Request's events that survives reconnects, so that my UI updates immediately.

#### Acceptance Criteria

1. THE SSE_Endpoint SHALL require the same authentication and Rate_Limit as other `/v1` routes, and SHALL return 404 `not_found` before streaming when the Request does not exist.
2. WHEN a stream opens, THE Gateway_API SHALL first send an event of type `snapshot` holding the Request status and Job states (the `GET` view without `final_result`), with the Event_ID of the last Event read at snapshot time.
3. AFTER the snapshot, THE Gateway_API SHALL send every Event with an Event_ID greater than the client's `Last-Event-ID` header when given, otherwise greater than the snapshot's Event_ID, in Event_Stream order, each with `id:` set to its Event_ID.
4. IF `Last-Event-ID` is not a valid stream ID, or is older than the first retained Event, THEN THE Gateway_API SHALL send the snapshot with `"resync": true` and continue after the snapshot's Event_ID.
5. THE Gateway_API SHALL send a comment line `: keepalive` when no Event was sent for 15 seconds.
6. WHEN the Gateway_API has sent a Terminal_Event and the Request is terminal, THE Gateway_API SHALL close the stream. WHEN a stream opens for a Request already terminal, THE Gateway_API SHALL send the snapshot, any Events after the client's position, and close.
7. IF the key already holds `max_streams` open streams (when greater than 0), THEN THE Gateway_API SHALL return HTTP 429 with `{"error":"too_many_streams"}`. IF the Gateway_API replica already holds `events.max_connections` open streams (default 500), THEN it SHALL return HTTP 503 with `{"error":"too_many_streams"}`.
8. WHEN a stream ends for any reason, THE Gateway_API SHALL release its per-key and per-replica stream counts.
9. A new Event SHALL reach an open stream within 3 seconds of the write that created it.

### Requirement 8: Webhooks

**User Story:** As a client developer, I want DAS to call my service when a Request finishes, so that I do not need an open connection.

#### Acceptance Criteria

1. WHEN `create_request` runs for a key with a Webhook_Target, THE script SHALL store the Webhook_Target on the Request record.
2. WHEN `finalize` or `cancel_request` writes a terminal status for a Request with a stored Webhook_Target, THE script SHALL create or reset the Webhook_Delivery (state `pending`, attempts 0, event = the Terminal_Event) and add the Request_ID to the Webhook_Outbox due now, in the same atomic write. A Request without a Webhook_Target SHALL add nothing. A Replay does not cancel a delivery that is still pending; the next terminal write resets it with the new Terminal_Event, and receivers tell the two apart by `DAS-Webhook-Id`.
3. THE Webhook_Sender SHALL claim a due delivery with an atomic script that re-scores it to `now + webhook.timeout + 5` and increments its `claim` counter, so that at most one sender holds it, a delivery held by a crashed sender becomes due again, and a completion is applied only when it carries the current `claim` value and the `event_id` of the delivery it claimed (a later terminal write resets the delivery with a new `event_id`, so a claim from before the reset never matches).
4. THE Webhook_Sender SHALL POST the JSON body `{"request_id", "event_id", "type", "data"}` with headers `Content-Type: application/json`, `DAS-Webhook-Id` (`{request_id}:{event_id}`), `DAS-Webhook-Timestamp` (Unix seconds) and `DAS-Webhook-Signature: v1=<hex HMAC-SHA256(secret, "{timestamp}.{body}")>`.
5. WHEN the response status is 2xx, THE Webhook_Sender SHALL mark the delivery `delivered` and remove it from the Webhook_Outbox.
6. IF the response is not 2xx, times out after `webhook.timeout` (default 10 s), or fails to connect, THEN THE Webhook_Sender SHALL increment `attempts`, record `last_status` (HTTP status or `timeout` or `connect_error`), and re-score the delivery to `now + backoff(attempts, webhook.base_backoff, webhook.max_backoff)` with jitter; WHEN `attempts` reaches `webhook.max_attempts` (default 8), it SHALL mark the delivery `failed` and remove it from the Webhook_Outbox.
7. FOR ALL interleavings of sender claims, crashes and responses, every delivery SHALL be attempted until `delivered` or `failed`, and no delivery SHALL be held by two senders at once.
8. THE Webhook_Sender SHALL NOT follow redirects, SHALL read at most 64 KiB of a response body, and SHALL look up the secret by `secret_name` at send time; IF the secret is no longer configured, THEN it SHALL mark the delivery `failed` with `last_status` `secret_missing`.
9. THE `GET /v1/requests/{id}` response SHALL include `webhook: {"state", "attempts", "last_status"}` when a Webhook_Delivery exists, and `webhook: null` otherwise.
10. Webhook secrets and signatures SHALL NOT appear in any log line, metric or error message.

### Requirement 9: Python Client Package

**User Story:** As a client developer, I want a small Python library for DAS, so that I can integrate without writing HTTP, retry and signature code.

#### Acceptance Criteria

1. THE Client_Package SHALL live in `clients/python/` with its own `pyproject.toml`, support Python 3.10 to 3.13, depend at runtime only on an exactly pinned `httpx`, and SHALL NOT import the `das` package.
2. THE Client_Package SHALL provide `DasClient` and `AsyncDasClient` with methods `submit(prompt, *, budget=None, idempotency_key=None)`, `get(request_id)`, `cancel(request_id)`, `events(request_id, *, last_event_id=None)`, `wait(request_id, *, timeout)` and `list_requests(**filters)`.
3. WHEN `submit` is called without `idempotency_key`, THE client SHALL generate a random one (UUID4) and reuse it across its own retries of that call.
4. THE client SHALL retry connection errors, HTTP 503 and HTTP 429 up to `max_retries` (default 3) with exponential backoff, waiting at least the `Retry-After` value on 429.
5. THE client SHALL raise typed errors derived from `DasError`: `AuthError` (401/403), `NotFound` (404), `Conflict` (409), `IdempotencyKeyReused` (422), `BudgetAboveLimit`, `InvalidRequest` (400), `RateLimited` and `QuotaExceeded` (429 after retries), `ServiceUnavailable` (503 after retries).
6. `events` SHALL yield typed events, reconnect on stream errors with `Last-Event-ID`, and stop after a Terminal_Event. `wait` SHALL return the final `RequestView`, using `events` and falling back to polling `get` when streaming fails.
7. THE Client_Package SHALL provide `verify_webhook(body, headers, secret, *, tolerance=300, now=None)` that compares signatures in constant time, rejects timestamps more than `tolerance` seconds from `now`, and returns the parsed event or raises `InvalidSignature`.
8. FOR ALL bodies, timestamps and secrets, `verify_webhook` SHALL accept exactly the signatures produced by the server's signing function within tolerance, and SHALL reject any change to the body, timestamp, signature or secret.
9. Response models SHALL ignore unknown fields, so that a newer server does not break an older client.
10. A contract test SHALL run the Client_Package against the real Gateway_API app (in-process, fakeredis) and fail on any mismatch between client and server.

### Requirement 10: Observability

**User Story:** As an operator, I want metrics for the new behavior, so that I can see throttling, budgets, streams and webhooks.

#### Acceptance Criteria

1. THE Gateway_API SHALL expose `das_client_rejections_total{reason}` with reason in {`rate_limited`, `quota_exceeded`, `too_many_streams`, `budget_above_limit`, `idempotency_key_reused`}, `das_idempotent_replays_total`, `das_sse_streams` (gauge) and `das_webhook_deliveries_total{outcome}` with outcome in {`delivered`, `retry`, `failed`}.
2. THE Manager_Agent SHALL expose `das_budget_exceeded_total{kind}` with kind in {`tokens`, `cost`, `unenforceable`}.
3. No new metric SHALL carry a Key_Fingerprint, Request_ID or URL as a label value.

### Requirement 11: Compatibility

**User Story:** As an operator, I want to upgrade a running Phase 3 fleet, so that existing clients keep working.

#### Acceptance Criteria

1. A client that sends no `budget`, no `Idempotency-Key` and stays within default limits SHALL observe the same responses as in Phase 3, apart from the added `budget` and `webhook` fields in `GET /v1/requests/{id}`.
2. Requests created before the upgrade SHALL have no budget, no webhook and no Active_Set membership, and the SSE_Endpoint SHALL serve them a snapshot followed by any Events that exist.
3. THE System SHALL let Gateway_API and Manager_Agent instances upgrade in any order; the README SHALL state that Events are complete only after every Manager_Agent runs Phase 4.
4. THE Gateway_API SHALL publish its OpenAPI schema at `GET /v1/openapi.json` behind API_Key authentication.
5. THE existing Phase 1–3 test suites SHALL keep passing, and the import-linter contract SHALL stay kept.
