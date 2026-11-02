# Implementation Plan: Distributed Agent System

## Overview

Build DAS in Python 3.12 (asyncio) as a pure core with a thin I/O shell. The order is: scaffolding, then shared models, config, and logging. Next come the pure modules (plan codec and validation, DAG, message codec, backoff, final result, RequestMachine), then the I/O adapters (LLM, transports, Redis State_Store, registry). The Manager, Worker, and Gateway runtimes are wired on top of those, and Docker deployment comes last. Property tests use Hypothesis with `fakeredis[lua]`. Integration tests use testcontainers. Multi-capability routing follows the design: routing-capability selection on the Manager plus the worker-side guard.

## Tasks

- [x] 1. Set up project structure and tooling
  - [x] 1.1 Create the project skeleton and test harness
    - Create `pyproject.toml` (Python 3.12, exact-pinned deps: pydantic v2, fastapi, uvicorn, starlette, structlog, redis, aio-pika, grpcio, grpcio-tools, httpx, openai, anthropic, aioboto3, pyyaml; dev: pytest, pytest-asyncio, hypothesis, fakeredis[lua], testcontainers, respx, import-linter), plus `uv.lock`
    - Create the `das/` package layout from the design (config, models, plan, codec, transport, llm, state, registry, manager, worker, gateway, observability, health) with `__init__.py` files
    - Create `das/errors.py` with typed exceptions (ConfigError, TransportNotInstalled, TransportIncomplete, DependencyUnreachable, RegistrationFailed, …) and the exit-code mapping (2, 3, 4, 5)
    - Create `das/clock.py` with a `Clock` protocol, a system clock, and a fake clock/sleep for tests
    - Create `tests/unit/`, `tests/property/`, `tests/integration/`, `tests/conftest.py`, an empty `tests/strategies.py`, and Hypothesis profiles (`default` with max_examples=100, `ci` with 200 and derandomize)
    - _Requirements: 15.4_

- [x] 2. Implement shared data models and configuration
  - [x] 2.1 Implement the shared pydantic models
    - `das/models/request.py`, `job.py`, `plan.py`, `results.py`: RequestStatus, JobState, terminal sets, PlannedJob, JobPlan, JobRecord, JobResultPayload, JobDispatchPayload, ControlPayload, HeartbeatPayload, TransitionEntry, Lease, AgentRecord, JobReport, FailureEntry, FinalResult, TokenUsage (all frozen, `extra="forbid"`)
    - Add a JobPlan equivalence helper (same Job_ID set; per id same instruction, Capability set, dependency set)
    - _Requirements: 3.2, 3.8, 5.5, 7.5, 13.7_

  - [x] 2.2 Implement Agent_Config schema, loader, and secrets handling
    - `das/config/schema.py`: pydantic models for role, agent_id, host, transports, rabbitmq, redis, grpc, llm, limits, worker, health_port, secrets_file, with every documented range and default; cross-field checks (base_backoff ≤ max_backoff, worker 1-50 Capabilities, concurrency 1-100, provider in the four names, model required, provider credentials required); required network addresses have no defaults
    - `das/config/loader.py`: a pure `resolve(file_values, env)` using the `DAS__` prefix and `__` nesting (env, then file, then default); `load_config(path, env)` that collects every error into `ConfigError` and reports file path and reason for missing, unreadable, or unparseable files
    - `das/config/secrets.py`: secret fields as `SecretStr` accepted only from env or `secrets_file`; reject secrets present in YAML by field name without the value; missing or unreadable secrets file errors name the reference only
    - _Requirements: 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 6.8, 10.7, 10.8, 14.4, 14.8_

  - [x] 2.3 Write property test for configuration precedence
    - **Property 35: Configuration precedence**
    - **Validates: Requirements 12.1**

  - [x] 2.4 Write property test for configuration validation
    - **Property 36: Configuration validation reports every bad field**
    - **Validates: Requirements 12.2, 6.8, 10.7, 10.8**

  - [x] 2.5 Write property test for credentials in config files
    - **Property 37: Credentials in config files are never echoed**
    - **Validates: Requirements 12.5**

  - [x] 2.6 Write unit tests for config file and secrets file errors
    - Missing, unreadable, and unparseable config file; missing secrets file; worker with zero Capabilities; unknown role
    - _Requirements: 12.3, 12.6, 14.4_

- [x] 3. Implement observability
  - [x] 3.1 Implement structured logging, redaction, and context binding
    - `das/observability/redaction.py`: `Redactor` that replaces every substring of length ≥ 8 of any registered secret (whole value for shorter secrets) with `[REDACTED]`, repeating until no match remains
    - `das/observability/context.py`: contextvars for `request_id` and `job_id`, plus helpers to bind an inbound correlation ID
    - `das/observability/logging.py`: structlog single-line JSON with `timestamp` (UTC ISO 8601, ms), `level`, `agent_id`, `message`; a `drop_content_fields` processor; the Redactor applied to the fully rendered line, including exception traces
    - _Requirements: 12.7, 13.1, 13.2, 13.3, 13.10_

  - [x] 3.2 Write property test for log redaction
    - **Property 38: Log redaction**
    - **Validates: Requirements 12.7**

  - [x] 3.3 Write property test for log format and context
    - **Property 39: Structured log format and context**
    - **Validates: Requirements 13.1, 13.2, 13.3, 13.10**

- [x] 4. Implement Job_Plan codec, validation, and DAG utilities
  - [x] 4.1 Implement the Job_Plan printer, parser, and JSON schema
    - `das/plan/codec.py`: `print_plan` (canonical JSON conforming to the schema), `parse_plan` (raises `PlanSchemaError`), code-fence stripping helper, exported Job_Plan JSON schema
    - Add a `job_plans(max_jobs)` strategy to `tests/strategies.py` (random DAG via topological order)
    - _Requirements: 3.4, 3.7, 3.8_

  - [x] 4.2 Implement the pure plan validator
    - `das/plan/validate.py`: `validate_plan(plan, capabilities, max_jobs) -> set[CheckFailure]` covering SCHEMA, DUPLICATE_JOB_ID, EMPTY_INSTRUCTION, INSTRUCTION_TOO_LONG, NO_CAPABILITIES, SELF_DEPENDENCY, UNKNOWN_DEPENDENCY, CYCLE, JOB_COUNT_OUT_OF_RANGE, UNADVERTISED_CAPABILITY
    - _Requirements: 3.1, 3.2, 3.4_

  - [x] 4.3 Implement DAG scheduling helpers
    - `das/plan/dag.py`: `topo_order` (raises `CycleError`), `ready_jobs`, `transitive_dependents`, `all_terminal`
    - _Requirements: 4.3, 4.4, 7.1_

  - [x] 4.4 Write property test for the Job_Plan round trip
    - **Property 1: Job_Plan round trip**
    - **Validates: Requirements 3.7, 3.8**

  - [x] 4.5 Write property test for the plan validator
    - **Property 2: Plan validator matches reference oracle**
    - Add the `invalid_plan_mutations` strategy and a simple reference oracle in the test
    - **Validates: Requirements 3.1, 3.2, 3.4**

- [x] 5. Implement the Message_Codec
  - [x] 5.1 Implement envelope and codec
    - `das/codec/envelope.py`: Envelope model, typed Message classes per type, ms-truncated UTC timestamps, `new_message(...)` that copies the bound `request_id` into `correlation_id`
    - `das/codec/message_codec.py`: canonical JSON encode (`sort_keys`, compact separators, `ensure_ascii=False`) with `MessageTooLarge`; decode ordered as size, JSON, envelope, type, schema_version, payload, raising `ParseError(reason, field)` and never returning partial objects
    - Add a `messages()` strategy for all four types to `tests/strategies.py`
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.6, 13.4_

  - [x] 5.2 Write property test for codec round trip and canonical encoding
    - **Property 4: Message codec round trip and canonical encoding** (adapter byte-equality is asserted against the fake transport here and the real adapters in 8.7)
    - **Validates: Requirements 9.1, 9.2, 9.3, 9.7**

  - [x] 5.3 Write property test for malformed input rejection
    - **Property 5: Codec rejects malformed input atomically**
    - **Validates: Requirements 9.4**

  - [x] 5.4 Write property test for the size limit
    - **Property 6: Message size limit**
    - **Validates: Requirements 9.6**

- [x] 6. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Implement the LLM_Provider layer
  - [x] 7.1 Implement the provider interface and retry wrapper
    - `das/llm/base.py`: ChatMessage, ModelParams, TokenUsage, Completion, `ProviderAdapter` protocol
    - `das/llm/errors.py`: TransientLLMError, PermanentLLMError, LLMCallFailed(provider, model, reason, attempts)
    - `das/llm/retry.py`: `LLMProvider.complete` with input validation (1-100 messages, temperature 0.0-2.0, max tokens ≥ 1), `asyncio.timeout`, waits of `min(2^(k-1), 30)` s, injectable sleep, no retry on permanent errors, no partial content on failure
    - Add a `FakeAdapter` under `tests/fakes/` (code-injected, not selectable by config)
    - _Requirements: 10.1, 10.4, 10.5, 10.6_

  - [x] 7.2 Write property test for the LLM retry policy
    - **Property 30: LLM retry policy**
    - **Validates: Requirements 10.4, 10.5, 10.6**

  - [x] 7.3 Implement the four Provider_Adapters and the factory
    - `das/llm/adapters/openai.py` (Chat Completions), `anthropic.py` (Messages API), `bedrock.py` (Converse via aioboto3), `ollama.py` (`/api/chat` via httpx, `stream=false`), each mapping SDK errors to transient or permanent and implementing `ping()`
    - `das/llm/factory.py`: build exactly one `LLMProvider` from Agent_Config
    - _Requirements: 10.1, 10.2, 10.3, 10.7_

  - [x] 7.4 Write unit tests for Provider_Adapters
    - Request mapping, usage extraction, and error classification against recorded responses (respx and stubbed SDK clients)
    - _Requirements: 10.1, 10.2, 10.3_

- [x] 8. Implement the Transport layer
  - [x] 8.1 Implement the Transport protocol and an in-memory fake
    - `das/transport/base.py`: Outcome, Delivery, Handler, SubscribeOptions, Subscription, `Transport` protocol, TransportUnavailable, PublishNotConfirmed, RequestTimeout(destination), a pure `ReconnectPolicy`, and a subscription registry that re-establishes subscriptions before delivery resumes
    - `tests/fakes/memory_transport.py`: in-memory Transport supporting ack/requeue/reject, simulated drops, and reconnects
    - _Requirements: 8.1, 8.6, 8.7, 8.8_

  - [x] 8.2 Implement the transport loader
    - `das/transport/loader.py`: entry-point group `das.transports`; `TransportNotInstalled(name)`; `TransportIncomplete(name, missing)`; register `rabbitmq` and `grpc` entry points only in `pyproject.toml`
    - _Requirements: 8.4, 8.5, 15.1, 15.2, 15.3, 15.5_

  - [x] 8.3 Write property test for the reconnect schedule
    - **Property 31: Transport reconnect schedule**
    - **Validates: Requirements 8.6**

  - [x] 8.4 Write property test for loader errors
    - **Property 32: Transport loader errors**
    - **Validates: Requirements 8.5, 15.5**

  - [x] 8.5 Implement the RabbitMQ adapter
    - `das/transport/rabbitmq.py`: aio-pika with publisher confirms and persistent delivery; declare `das.jobs`, `das.jobs.<cap>` quorum queues with DLX, `das.results`, `das.dlx`, `das.dlq`, `das.dlq.events`; apply the `das-jobs` policy (`delivery-limit = max_attempts − 1`) through the management API; destination name mapping; pull-mode (`basic.get`) consumption sharing an in-flight limiter with 200 ms polling; expose `x-delivery-count`; reconnect with ReconnectPolicy
    - _Requirements: 4.1, 4.5, 4.7, 6.3, 6.4, 6.6, 8.2, 8.6, 8.8, 8.9_

  - [x] 8.6 Implement the gRPC adapter
    - `das/transport/proto/das.proto` plus generated stubs; `das/transport/grpc_rpc.py` with an `AgentControl` service (`Send`, `Call`), destination resolution from agent ID to endpoint through an injected resolver, `acknowledge` completing pending `Send`, and request-reply timeout
    - _Requirements: 8.3, 8.7, 8.8_

  - [x] 8.7 Write unit tests for transport contracts
    - Signature equality across adapters via `inspect.signature`; request-reply timeout; publish while down; wire bytes equal `MessageCodec.encode`; dummy `http` and `kafka` entry-point adapters load without code changes
    - _Requirements: 8.1, 8.7, 8.8, 9.3, 15.1, 15.2_

  - [x] 8.8 Write RabbitMQ integration tests (testcontainers)
    - Capability-queue routing; unacked redelivery on connection drop with unchanged attempt; delivery-limit dead-lettering with the `max_attempts − 1` mapping; reconnect and subscription restore; gRPC request-reply between two processes
    - _Requirements: 4.5, 4.7, 6.4, 6.6, 8.2, 8.3, 8.6, 8.9_

- [x] 9. Implement the Redis State_Store
  - [x] 9.1 Implement keys, Lua scripts, and the store API
    - `das/state/keys.py` with the design's key layout
    - `das/state/lua/`: `create_request`, `acquire_lease` (Redis `TIME`, epoch++, non-terminal only), `renew_lease`, `release_lease`, `commit_plan`, `apply_transition` (lease and terminal checks, Job updates, results, transitions, timers, inbox cursor), `finalize` (Final_Result before status), `cancel_request`
    - `das/state/store.py`: typed async methods returning `LEASE_LOST` / `REQUEST_TERMINAL` / `TERMINAL` results, `record_result_inbox` via XADD, inbox reads, snapshot loading
    - _Requirements: 1.1, 1.6, 1.7, 1.8, 2.2, 2.3, 2.4, 2.7, 3.6, 3.9, 3.12, 7.3, 7.4, 13.7, 13.8_

  - [x] 9.2 Write property test for lease mutual exclusion and fencing
    - **Property 13: Lease mutual exclusion and fencing** (RuleBasedStateMachine on fakeredis)
    - **Validates: Requirements 2.2, 2.4, 2.7, 3.12**

  - [x] 9.3 Write property test for cancellation semantics
    - **Property 11: Cancellation semantics**
    - **Validates: Requirements 1.6, 1.8**

  - [x] 9.4 Write property test for the transition log
    - **Property 25: Transition log matches state changes** (stateful, with lease handover)
    - **Validates: Requirements 13.7, 13.8**

  - [x] 9.5 Write unit tests for atomic writes
    - `commit_plan` writes plan and Running together or not at all, with and without a lease; `finalize` writes Final_Result before Completed
    - _Requirements: 3.9, 7.4_

- [x] 10. Implement the Agent_Registry and routing selection
  - [x] 10.1 Implement registry storage and pure availability logic
    - `das/registry/availability.py`: `compute_status`, `advertised_capabilities` (union over Available workers), `capable_routing_capabilities(required, agents)`
    - `das/registry/registry.py`: `register` (Available), `heartbeat` (flips Unavailable to Available), `deregister`, `snapshot` (with server time), `sweep`
    - Add a `registry_snapshots` strategy to `tests/strategies.py`
    - _Requirements: 3.3, 4.5, 5.1, 11.1, 11.2, 11.6, 11.7_

  - [x] 10.2 Write property test for the registry availability model
    - **Property 33: Registry availability model** (stateful on fakeredis)
    - **Validates: Requirements 11.2, 11.7**

  - [x] 10.3 Write property test for routing-capability selection
    - **Property 17: Routing-capability selection is sound**
    - **Validates: Requirements 4.5**

- [x] 11. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 12. Implement the Manager pure core
  - [x] 12.1 Implement backoff and Final_Result assembly
    - `das/manager/backoff.py`: `backoff(N) = min(base × 2^(N−1), max_backoff)`
    - `das/manager/final_result.py`: pure `build_final_result(plan, jobs, results, summary, error)` producing JobReports, FailureEntries with reasons (Job_Result error, `skipped: dependency <id> failed`, `cancelled: request cancelled`), and summarizer usage
    - _Requirements: 6.2, 7.2, 7.3, 7.5, 7.6_

  - [x] 12.2 Write property test for the backoff schedule
    - **Property 21: Backoff schedule**
    - **Validates: Requirements 6.2**

  - [x] 12.3 Write property test for Final_Result completeness
    - **Property 24: Final_Result completeness**
    - **Validates: Requirements 7.2, 7.5**

  - [x] 12.4 Implement the RequestMachine reducer and resume
    - `das/manager/routing.py`: choose `jobs/min(C)` from `capable_routing_capabilities`
    - `das/manager/machine.py`: RequestSnapshot, StepContext, Event, Effect, Decision types; `step` covering every rule in the design table (cancelled inert, Waiting→Ready with `ready_since`, PublishConfirmed→Dispatched, publish retry ×3 then Failed, capability wait timeout, idempotent results, Succeeded, Failed/TimedOut retry with backoff timer, max attempts → Failed + PublishDeadLetter, Cancelled, UnackedDeliveryExhausted, transitive Skipped, RunSummarizer / FinalizeFailed); dependency inputs keyed by dep Job_ID; transition entries for every state change
    - `resume(snapshot)`: re-dispatch Ready/Dispatched Jobs without a current-attempt result and re-arm persisted timers
    - _Requirements: 1.7, 2.6, 2.8, 4.1, 4.2, 4.3, 4.4, 4.6, 4.8, 6.1, 6.2, 6.3, 6.5, 6.6, 6.7, 7.1, 7.3, 13.7_

  - [x] 12.5 Write property test for cancelled Requests
    - **Property 12: Cancelled Requests are inert**
    - **Validates: Requirements 1.7**

  - [x] 12.6 Write property test for resume
    - **Property 14: Resume re-dispatches exactly the unfinished in-flight Jobs**
    - **Validates: Requirements 2.6**

  - [x] 12.7 Write property test for dispatch readiness
    - **Property 15: Dispatch readiness and dependency inputs**
    - **Validates: Requirements 4.1, 4.2, 4.3**

  - [x] 12.8 Write property test for failure propagation
    - **Property 16: Failure propagation skips exactly the transitive dependents**
    - **Validates: Requirements 4.4**

  - [x] 12.9 Write property test for capability wait
    - **Property 18: Capability wait timeout**
    - **Validates: Requirements 4.6**

  - [x] 12.10 Write property test for the publish retry bound
    - **Property 19: Publish retry bound**
    - **Validates: Requirements 4.8**

  - [x] 12.11 Write property test for attempt progression
    - **Property 20: Attempt progression, retry, and dead-lettering**
    - **Validates: Requirements 6.1, 6.3, 6.7**

  - [x] 12.12 Write property test for result idempotency
    - **Property 22: Result handling is idempotent**
    - **Validates: Requirements 2.8, 6.5**

  - [x] 12.13 Write property test for the summarization trigger
    - **Property 23: Summarization trigger and inputs**
    - **Validates: Requirements 7.1, 7.3**

- [x] 13. Implement the Planner and Summarizer
  - [x] 13.1 Implement the Planner
    - `das/manager/planner.py`: NoCapabilities short-circuit with no LLM call; planning prompt with the advertised Capabilities, schema, and `max_jobs`; parse and validate; corrected-plan requests listing each failed check; at most `planning_retries + 1` attempts; return PlanAccepted / PlanRejected(failed_checks); propagate `LLMCallFailed`
    - _Requirements: 3.1, 3.3, 3.4, 3.5, 3.6, 3.10, 3.11_

  - [x] 13.2 Write property test for bounded planning attempts
    - **Property 3: Bounded planning attempts with corrections**
    - **Validates: Requirements 3.3, 3.5, 3.6**

  - [x] 13.3 Write unit tests for Planner edge paths
    - Zero advertised Capabilities makes no LLM call; provider error surfaces provider, model, and reason
    - _Requirements: 3.10, 3.11_

  - [x] 13.4 Implement the Summarizer
    - `das/manager/summarizer.py`: input is the original prompt plus Succeeded contents only; 120 s per-call timeout; up to 3 attempts; returns Completion or raises a summarization failure
    - _Requirements: 7.1, 7.6_

  - [x] 13.5 Write unit tests for Summarizer retries
    - Error and timeout on each attempt; success on the 3rd attempt; failure after 3 attempts
    - _Requirements: 7.6_

- [x] 14. Implement the Manager runtime
  - [x] 14.1 Implement lease keeping and claiming
    - `das/manager/lease.py`: `LeaseKeeper` renewing every `lease_duration / 4`, marking the lease lost on rejection or on Redis unreachable past expiry, cancelling the Request task, and supporting "stop renewing" for 7.7
    - `das/manager/claim.py`: `ClaimLoop` (1 s) scanning `das:requests:pending` and `das:leases:expiry` and calling `acquire_lease`
    - _Requirements: 2.1, 2.3, 2.4, 2.5, 2.7, 7.7_

  - [x] 14.2 Implement the RequestDriver and ingest loops
    - `das/manager/driver.py`: per-owned-Request task (plan → `commit_plan`, or Failed with checks / no capabilities / provider error; then event loop over inbox, timers, and 250 ms ticks calling `step`; executing effects through Transport and `apply_transition`; summarize and `finalize` with 3 extra 1 s retries, otherwise stop renewing); discard Planner or Summarizer output on lease loss; send `cancel_job` on `REQUEST_TERMINAL`
    - `ResultIngest` (consume `results`, XADD to inbox, then ack; REJECT on ParseError) and `DeadLetterIngest` (translate delivery-limit dead-letters into `UnackedDeliveryExhausted`); handle `cancel_request` control commands
    - Bind correlation IDs for logging; warn on missing correlation IDs
    - _Requirements: 1.6, 1.7, 2.6, 2.7, 3.1, 3.6, 3.9, 3.10, 3.11, 3.12, 4.1, 4.2, 6.3, 6.6, 7.1, 7.3, 7.4, 7.6, 7.7, 9.5, 13.4, 13.5, 13.6_

  - [x] 14.3 Wire the Manager agent process
    - `das/manager/agent.py`: startup order (config, transports, dependency connections, registry registration, subscriptions), then ClaimLoop, ResultIngest, DeadLetterIngest, RegistrySweeper, Heartbeat, gRPC control server; graceful shutdown; only Transport and MessageCodec used for messaging
    - _Requirements: 2.1, 8.4, 11.1, 11.2, 11.3, 11.6, 15.4_

  - [x] 14.4 Write unit tests for the Manager runtime
    - Startup ordering; lease loss stops writes and discards output; Final_Result persist retry and stop-renewing; plan failure paths release the lease; driver with fake transport and fakeredis runs a two-Job DAG to Completed
    - _Requirements: 2.1, 2.7, 3.6, 7.4, 7.7, 8.4, 11.6_

- [x] 15. Implement the Worker_Agent
  - [x] 15.1 Implement the limiter and executor
    - `das/worker/limiter.py`: `InFlightLimiter` semaphore of `concurrency_limit`
    - `das/worker/executor.py`: `JobExecutor.execute` making one completion with the instruction and labelled dependency results; `asyncio.timeout(job_timeout)` → TimedOut with duration reason; cancel event → Cancelled; `LLMCallFailed` → Failed with empty content and provider/model/reason; usage and duration_ms
    - _Requirements: 5.4, 5.5, 5.6, 5.7, 5.8_

  - [x] 15.2 Implement the Worker process
    - `das/worker/worker.py`: startup order with 30 s registration deadline (exit non-zero, no subscriptions on failure); one pull subscription per Capability sharing the limiter; decode and REJECT malformed or incomplete dispatches; worker-side capability guard (nack requeue, pause queue 1 s); publish Job_Result, then ACK only after confirm; `cancel_job` gRPC handler; heartbeat; graceful shutdown (stop pulling, drain, REQUEUE unfinished, deregister, close)
    - _Requirements: 4.5, 5.1, 5.2, 5.3, 5.5, 5.7, 5.9, 5.10, 6.4, 9.5, 11.1, 11.3, 11.6, 13.5_

  - [x] 15.3 Write property test for Worker subscriptions
    - **Property 26: Worker subscriptions match Capabilities**
    - **Validates: Requirements 5.2**

  - [x] 15.4 Write property test for the Worker concurrency limit
    - **Property 27: Worker concurrency limit**
    - **Validates: Requirements 5.3**

  - [x] 15.5 Write property test for the Worker execution contract
    - **Property 28: Worker execution contract**
    - **Validates: Requirements 5.4, 5.5**

  - [x] 15.6 Write property test for malformed message rejection
    - **Property 29: Malformed messages are rejected without side effects** (Worker dispatch handler and Manager ResultIngest)
    - **Validates: Requirements 5.9, 9.5**

  - [x] 15.7 Write unit tests for Worker edge paths
    - Timeout, cancellation, and LLM error results; registration failure exit; capability guard requeue; graceful shutdown sequence
    - _Requirements: 4.5, 5.6, 5.7, 5.8, 5.10, 11.3_

- [x] 16. Implement the Gateway_API
  - [x] 16.1 Implement auth and prompt validation
    - `das/gateway/auth.py`: FastAPI dependency comparing `X-API-Key` with `hmac.compare_digest`, rejecting with 401 before any store access
    - `das/gateway/validation.py`: pure `validate_prompt(prompt, max_len)` returning `empty_prompt` or `prompt_too_long` with `max_length`
    - _Requirements: 1.3, 1.4, 11.8_

  - [x] 16.2 Write property test for prompt validation
    - **Property 7: Prompt validation**
    - **Validates: Requirements 1.4**

  - [x] 16.3 Implement Gateway endpoints
    - `das/gateway/app.py`: `POST /v1/requests`, `GET /v1/requests/{id}` (status, jobs, ordered transitions, final_result only when Completed, error), `POST /v1/requests/{id}/cancel` (202 / 404 / 409), `GET /v1/agents` (second-precision UTC heartbeat), `GET /healthz`; 2 s Redis socket timeout and 4.5 s overall timeout → 503; best-effort `cancel_request` gRPC to the lease owner
    - _Requirements: 1.1, 1.2, 1.5, 1.6, 1.8, 1.9, 11.4, 13.9_

  - [x] 16.4 Write property test for non-mutating rejections
    - **Property 8: Rejected calls are non-mutating**
    - **Validates: Requirements 1.3, 1.5, 11.8**

  - [x] 16.5 Write property test for submission uniqueness
    - **Property 9: Submission creates unique Pending Requests**
    - **Validates: Requirements 1.1**

  - [x] 16.6 Write property test for the query response shape
    - **Property 10: Query response shape**
    - **Validates: Requirements 1.2, 13.9**

  - [x] 16.7 Write property test for the agent listing
    - **Property 34: Agent listing completeness**
    - **Validates: Requirements 11.4**

  - [x] 16.8 Write unit tests for Gateway unavailability
    - Redis unreachable or write failure returns 503 within 5 s with no `request_id`; cancel on terminal returns 409
    - _Requirements: 1.8, 1.9_

- [x] 17. Implement health checks and the CLI
  - [x] 17.1 Implement the health server
    - `das/health/server.py`: Starlette `GET /healthz` running Transport, Redis, and LLM checks concurrently with 2 s timeouts each (LLM ping cached 30 s), overall healthy only if all connected, response bounded at 3 s
    - _Requirements: 11.5_

  - [x] 17.2 Implement the CLI entry point
    - `das/cli.py`: `das run --config path`; logging setup with Redactor secrets; config load before any connection; transport loading; startup dependency retry (`startup_retry_interval` up to `startup_timeout`, then exit naming the dependency); role dispatch to gateway, manager, or worker; exit codes from `das.errors`
    - _Requirements: 8.4, 8.5, 12.2, 12.3, 14.4, 14.7, 15.3, 15.5_

  - [x] 17.3 Write unit tests for health and CLI
    - Health truth table and dependency timeout; CLI exit codes for config error, missing or incomplete transport, unreachable dependency
    - _Requirements: 8.5, 11.5, 14.4, 14.7, 15.5_

  - [x] 17.4 Write property test for correlation ID propagation
    - **Property 40: Correlation ID propagation** (Manager driver and Worker handlers over the fake transport)
    - **Validates: Requirements 13.4, 13.5**

  - [x] 17.5 Write unit test for missing correlation IDs
    - Request-scoped Message with empty correlation ID logs a warning with type and sender and is still processed
    - _Requirements: 13.6_

- [x] 18. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 19. Implement local multi-node deployment
  - [x] 19.1 Create role Dockerfiles
    - `deploy/docker/gateway.Dockerfile`, `manager.Dockerfile`, `worker.Dockerfile` on `python:3.12-slim`, running `das run --config /etc/das/config.yaml`
    - _Requirements: 14.3_

  - [x] 19.2 Create the Compose stack and configs
    - `deploy/docker-compose.yml`: rabbitmq:4.1-management (definitions file), redis:7.4, gateway, manager (2 replicas), worker-research (`research, summarize`), worker-review (`code-review`), optional ollama profile; healthchecks with `depends_on: service_healthy`; `stop_grace_period` above `shutdown_timeout`
    - `deploy/rabbitmq/definitions.json` (exchanges, queues, `das-jobs` policy) and per-role sample configs under `deploy/config/` with addresses supplied by env
    - _Requirements: 14.1, 14.2, 14.3, 14.5, 14.6, 14.8_

  - [x] 19.3 Write smoke and static checks
    - Compose service counts and distinct Capability sets; no `localhost` or Compose service names in `das/`; no `http` or `kafka` entry points; import-linter contracts forbidding `aio_pika`, `grpc`, `redis` in `das.manager` and `das.worker` logic modules
    - _Requirements: 14.1, 14.3, 14.8, 15.3, 15.4_

  - [x] 19.4 Write end-to-end integration tests
    - Prompt → Final_Result with a fake LLM adapter injected at image build; submit/query/cancel latency bounds; Manager kill and takeover within 10 s; stack ready within 120 s; scale up and down; dependency-unreachable exit
    - _Requirements: 1.1, 1.2, 1.6, 2.5, 14.2, 14.5, 14.6, 14.7_

- [x] 20. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for a faster MVP.
- Each task references specific requirements for traceability. Each of the 40 design properties maps to exactly one property-test sub-task.
- Property tests use Hypothesis with the `# Feature: distributed-agent-system, Property {n}: {title}` tag comment and live in `tests/property/`.
- Multi-capability routing uses routing-capability selection (12.4, 10.1) plus the worker-side guard (15.2).
- Timing bounds and broker behavior are covered by integration tests (8.8, 19.4), not property tests.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["2.1"] },
    { "id": 2, "tasks": ["2.2", "3.1", "4.1", "5.1"] },
    { "id": 3, "tasks": ["2.3", "2.4", "2.5", "2.6", "3.2", "3.3", "4.2", "4.3", "5.2", "5.3", "5.4", "7.1", "8.1", "9.1", "10.1"] },
    { "id": 4, "tasks": ["4.4", "4.5", "7.2", "7.3", "8.2", "9.2", "9.3", "9.4", "9.5", "10.2", "10.3", "12.1"] },
    { "id": 5, "tasks": ["7.4", "8.3", "8.4", "8.5", "8.6", "12.2", "12.3", "12.4", "13.1", "13.4"] },
    { "id": 6, "tasks": ["8.7", "8.8", "12.5", "12.6", "12.7", "12.8", "12.9", "12.10", "12.11", "12.12", "12.13", "13.2", "13.3", "13.5", "14.1", "15.1", "16.1"] },
    { "id": 7, "tasks": ["14.2", "15.2", "16.2", "16.3", "17.1"] },
    { "id": 8, "tasks": ["14.3", "15.3", "15.4", "15.5", "15.6", "15.7", "16.4", "16.5", "16.6", "16.7", "16.8"] },
    { "id": 9, "tasks": ["14.4", "17.2"] },
    { "id": 10, "tasks": ["17.3", "17.4", "17.5", "19.1"] },
    { "id": 11, "tasks": ["19.2"] },
    { "id": 12, "tasks": ["19.3", "19.4"] }
  ]
}
```
