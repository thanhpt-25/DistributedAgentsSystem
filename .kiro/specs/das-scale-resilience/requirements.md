# Requirements Document: DAS Phase 7, Scale and Resilience

## Introduction

Phase 7 removes the two single points the platform still has and proves its limits: Redis (one node holds all durable
state) and the transport choice (RabbitMQ or gRPC only), and it publishes a capacity number. It adds Redis Sentinel
support, an HTTP transport, a Kafka transport and a load test suite. The plan is
`docs/superpowers/plans/2026-11-13-das-phase6-7-smarter-and-resilient.md`; its decisions D2, D3 and D7 are assumed here.

Everything is additive. A deployment that sets none of it behaves as in Phase 6, and the wire format baselines stay
byte-identical: transports carry the same canonical JSON bytes.

Phase 7 implements v1 Req 15.1 and 15.2, which said the HTTP and Kafka adapters could be added later without changing
Manager or Worker code. It supersedes the sentence of the loader docstring "v1 ships rabbitmq and grpc only".

## Glossary

- **Sentinel_Mode**: `redis.mode: sentinel`, where the client asks Redis Sentinel for the current master.
- **Master_Name**: the Sentinel service name of the master.
- **Failover**: Sentinel promotes a replica; for a short time writes fail or the connection resets.
- **HTTP_Broker**: the durable queue service the HTTP transport talks to, served by the Gateway on its Ops_Port.
- **Broker_Token**: the shared secret an HTTP transport client sends in `Authorization: Bearer`.
- **Kafka_Topic**: one topic per Transport destination, named as the RabbitMQ queue (`das.jobs.<cap>`, `das.results`, ...).
- **Load_Profile**: a named load test configuration: arrival rate, duration, prompt mix.
- **Capacity_Report**: the JSON a load run writes: throughput, latency percentiles, saturation point, machine description.

## Requirements

### Requirement 1: Redis Sentinel

**User Story:** As an operator, I want a Redis failure to cost a few seconds, not the platform.

#### Acceptance Criteria

1. THE `redis` section SHALL accept `mode` (`standalone`, the default, or `sentinel`). In `standalone` mode only `url` is allowed and everything behaves as before.
2. IN `sentinel` mode THE section SHALL require `sentinels` (a list of 1 to 10 `host:port`) and `master_name`, and MAY set `db` (0 to 15), `sentinel_password` and `sentinel_tls` (secrets only from the environment or the secrets file), and SHALL reject `url` together with `sentinels`.
3. A `mode` of `cluster` SHALL be refused with a configuration error whose message says that the State_Store scripts update keys in different slots at once, so Redis Cluster is not supported.
4. EVERY role (Gateway, Manager, Worker) SHALL build its Redis client through one function that honours the mode; there SHALL be no other place that creates a Redis client.
5. WHEN a Failover happens, THE State_Store SHALL retry an operation that failed with a connection error or a `READONLY` error up to 3 times with a short backoff. A retried script is safe because every script is keyed by Lease epoch and fencing, so a repeat answers `LEASE_LOST`, `REQUEST_TERMINAL` or the same result.
6. THE Lease logic SHALL keep working across a Failover: a Lease that expired during the Failover SHALL be lost, a live one kept; no Job_Result SHALL be applied twice.
7. THE health check SHALL report the Redis dependency as unhealthy while no master is reachable and healthy again once Sentinel names one.
8. THE metrics SHALL gain `das_redis_failovers_total` (increments when the client switches master) and `das_redis_retries_total{reason}` (`connection`, `readonly`).
9. THE Helm chart SHALL accept `redis.mode: sentinel` with `redis.sentinels` and `redis.masterName`, and the Compose files SHALL offer a Sentinel profile (`--profile sentinel`) with one master, one replica and three Sentinels.
10. THE documentation SHALL say that the key layout did not change (so there is nothing to migrate), that tenant, index and Request keys stay in one Redis, and why Cluster is not offered.

### Requirement 2: HTTP Transport

**User Story:** As an operator, I want agents to talk over plain HTTPS where AMQP is not allowed.

#### Acceptance Criteria

1. THE adapter SHALL register as `http` in the `das.transports` entry-point group and implement the Transport protocol: `publish`, `subscribe`, `acknowledge`, `request_reply`, `start`, `close`.
2. THE HTTP_Broker SHALL be served by the Gateway only when `http_broker.enabled` is true, on the Ops_Port, never on the public port, and every call SHALL require the Broker_Token (compared in constant time); a missing or wrong token SHALL answer 401 with no detail in the body.
3. THE HTTP_Broker SHALL keep each destination as a Redis stream with a consumer group, so Messages survive a Gateway restart and each Message goes to one consumer of the group.
4. `publish` SHALL return only after the broker has stored the Message (the confirm of v1 Req 8.2); a broker error SHALL raise `PublishNotConfirmed`, an unreachable broker `TransportUnavailable`.
5. `subscribe` SHALL long-poll the broker; a delivery not acknowledged within the visibility timeout (default 30 s) SHALL become deliverable again, and after the configured delivery limit SHALL move to the dead-letter destination (v1 Req 5.9).
6. `acknowledge` SHALL map ACK, REQUEUE and REJECT as for RabbitMQ; REJECT SHALL dead-letter.
7. THE adapter SHALL carry the same codec bytes and the same headers (`das-priority` included) as the RabbitMQ adapter, and a high-priority Message SHALL be delivered before normal ones queued earlier on the same destination.
8. `request_reply` SHALL be implemented with a per-call reply destination and the configured timeout, raising `RequestTimeout` after it.
9. Capability routing SHALL be unchanged: one destination per Capability, the same min-capability rule, the same worker-side requeue guard.
10. THE HTTP_Broker SHALL bound Message size (1 MiB), batch size (100) and long-poll time (30 s), and answer 413 or 400 beyond them.
11. THE Compose files SHALL offer a profile that runs the stack on the HTTP transport, and an end-to-end test SHALL run a Request through it.

### Requirement 3: Kafka Transport

**User Story:** As an operator with a Kafka platform, I want agents to use it as the Transport.

#### Acceptance Criteria

1. THE adapter SHALL register as `kafka` and implement the Transport protocol with `aiokafka`.
2. EACH Transport destination SHALL be one Kafka_Topic named as the RabbitMQ queue, created at startup when `kafka.auto_create_topics` is true (default true) with `kafka.partitions` (default 3) and `kafka.replication_factor` (default 1).
3. `publish` SHALL use an idempotent producer with `acks=all` and return after the broker acknowledges.
4. A Worker SHALL consume with its Capability's consumer group and commit an offset only after `acknowledge(ACK)`; the in-flight limit SHALL keep at most `concurrency_limit` uncommitted Messages.
5. Kafka has no per-message requeue or dead letter, so the adapter SHALL implement them: REQUEUE republishes the Message to `<topic>.retry` with a delay stamp and commits the original; REJECT, and a Message whose delivery count passes the limit, SHALL be published to `das.dlq` with the same headers the other transports write, then the original is committed.
6. A rebalance SHALL NOT lose a Message and SHALL NOT commit one that was not acknowledged.
7. `request_reply` SHALL use a per-process reply topic and correlation ids, raising `RequestTimeout` after the timeout.
8. THE adapter SHALL carry the same codec bytes and headers as the other transports. Priority SHALL be honoured with a separate `<topic>.high` topic consumed first.
9. Credentials (SASL password, TLS key) SHALL be secrets accepted only from the environment or the secrets file.
10. A Kafka-backed run of the contract tests every transport must pass (publish/subscribe round trip, ack, requeue, reject, request-reply, reconnect) SHALL be part of the integration suite and skip without Docker.

### Requirement 4: Load Test Suite

**User Story:** As an operator, I want to know how much the platform can take, and to see it when a change makes it worse.

#### Acceptance Criteria

1. THE repository SHALL contain `das loadtest --gateway <url> --key <key> --profile <name|file> --out <json>` which drives a running stack through the client API.
2. A Load_Profile SHALL define: arrival rate (Requests per second, constant or stepped), duration and the prompt mix. THE profiles `smoke`, `steady` and `ramp` SHALL be built in.
3. THE command SHALL measure, per Request, submission latency and the time to a terminal state, and over the run: throughput, error rate, the percentiles 50, 90, 99 and the highest rate at which the error rate and p99 stayed inside limits.
4. THE Capacity_Report SHALL record the machine (CPU count, memory, OS), the DAS version, the transport, and the settings the run was given (Managers, Workers, stub LLM latency), so a number is never published without its context.
5. THE stub LLM used by the load tests SHALL be configurable in latency and failure rate, so a load test measures DAS and not a provider.
6. A pure module SHALL compute percentiles and the saturation point from raw samples, with unit tests and a property test (percentiles are monotone and bounded by the samples).
7. A compare command `das loadtest-compare <base.json> <new.json> --max-regression <ratio>` SHALL exit 1 when throughput fell or p99 rose by more than the ratio.
8. THE README SHALL publish the measured numbers of one reference run, with the Capacity_Report in `docs/capacity/`, and SHALL say they are a measurement on one machine, not a guarantee.

### Requirement 5: Compatibility and Documentation

#### Acceptance Criteria

1. WITH none of the new configuration set, every response, message, metric and key of Phase 6 SHALL be unchanged apart from the new metrics and the OpenAPI additions.
2. THE codec corpus SHALL stay byte-identical; the contract baseline SHALL change only by additions.
3. THE import-linter contract SHALL hold: `das.manager` and `das.worker` import no SDK, including `aiokafka` and the Sentinel client.
4. THE README SHALL document Sentinel, the HTTP transport, the Kafka transport and the load test, each with its configuration, defaults and ranges, and a "Not in Phase 7" list (Redis Cluster, multi-region, exactly-once).
5. EVERY new configuration field SHALL have a documented default and range, checked by a test against the schema.

### Requirement 6: Correctness Properties

#### Acceptance Criteria

1. FOR ALL Sentinel configurations, validation SHALL accept exactly the configurations of Req 1.2 and refuse the others with a message naming the field (Property 1).
2. FOR ALL sequences of publishes, acknowledgements, requeues and rejects, every Transport adapter SHALL deliver each Message at least once, never deliver an acknowledged Message again, and dead-letter exactly the rejected ones (Property 2, against the in-memory fake of each adapter's broker).
3. FOR ALL interleavings of a Failover with Request processing, no Request SHALL end in two different terminal states and no Job_Result SHALL be applied twice (Property 3, with a fake Redis that fails at a chosen call).
4. FOR ALL sample sets, load test percentiles SHALL be monotone in the percentile and lie within the samples (Property 4).
