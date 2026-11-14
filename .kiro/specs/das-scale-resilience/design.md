# Design Document: DAS Phase 7, Scale and Resilience

Requirement numbers refer to `requirements.md` in this folder. Everything is additive; with no new configuration a Phase 6
deployment behaves as before (Req 5.1).

## Overview

```text
agents ──▶ build_redis_client(config.redis) ──▶ standalone: Redis.from_url
                                              └▶ sentinel:   Sentinel(...).master_for(name)      (Req 1)
agents ──▶ Transport protocol ──▶ rabbitmq | grpc | http | kafka                                 (Req 2, 3)
                                    http:  HttpTransport ⇄ HTTP_Broker (Gateway Ops_Port, Redis streams)
                                    kafka: KafkaTransport ⇄ topics das.jobs.<cap>, .retry, .high, das.results, das.dlq
das loadtest / loadtest-compare ──▶ client API of a running stack ──▶ Capacity_Report             (Req 4)
```

New modules: `das/state/redisclient.py` (the one place that builds a client), `das/transport/http.py`,
`das/transport/httpbroker.py` (Gateway side), `das/transport/kafka.py`, `das/loadtest/` (`profile.py`, `stats.py`,
`runner.py`, `report.py`, `compare.py`). Changed: `das/config/schema.py`, `das/cli.py`, `das/gateway/app.py`,
`das/state/store.py` (retry), `das/observability/metrics.py`, `pyproject.toml` (entry points, `aiokafka`), `deploy/`.

## Redis Sentinel (Req 1)

`RedisConfig` gains `mode` plus either `url` or `sentinels` and `master_name`. A model validator enforces Req 1.2 and
refuses `cluster` with the reason of Req 1.3. `build_redis_client(cfg, *, role)` is the only function that calls
`Redis.from_url` or `Sentinel`; `cli._redis_client` and `gateway.app.build_redis_client` delegate to it, and a static test
searches the package for any other constructor call (Req 1.4).

Sentinel mode uses `redis.asyncio.sentinel.Sentinel(sentinels, sentinel_kwargs=..., socket_timeout=...)` and
`master_for(master_name, db=db)`, which re-asks Sentinel for the master on every new connection, so a Failover needs no
code of ours to find the new master. `das_redis_failovers_total` increments from a connection-pool subclass when the
resolved master address changes.

### Retrying through a Failover (Req 1.5, 1.6)

A retry wrapper in the store's single call seam: on `ConnectionError`, `TimeoutError` or a `READONLY` response (a demoted
master) it waits 0.2 s, 0.5 s, 1 s and tries again, at most 3 times, counting `das_redis_retries_total{reason}`. This is
correct for every script because each answers a repeat deterministically: the Lease epoch fences a stale owner
(`LEASE_LOST`), a finished Request answers `REQUEST_TERMINAL`, and job and stream writes are keyed by `(job_id, attempt)`.
A write that reached the old master but not the replica is lost by Redis's asynchronous replication; the design accepts
that, as at-least-once delivery plus idempotent handling already cover it: the queue redelivers the Job_Result and the
reducer discards a duplicate.

What the fault-injection test (Property 3, `tests/property/test_failover.py`) found, and the design accepts: the retry sits in
the redis-py client (`FailoverRetry`), not in the store, because the store makes 43 direct calls and a pipeline or a script
goes through the same connection. A failure of any of the three kinds (reset before the command, reply lost after it, or
`READONLY`) at any command of a real Request, up to three at a time, leaves exactly the state of a run with none. Two
effects are not data errors and are kept: (1) a lost reply to `acquire_lease` makes the retry answer `HELD`, because the
first try did take the Lease; the Request waits one lease duration and any Manager then takes the expired Lease (a
re-entrant acquire could start a second driver for one Request, which is worse than the delay); (2) a lost reply to the
final write makes the retry answer that the Lease is gone, so the driver reports `LEASE_LOST` for a Request stored as
Completed exactly once. A submit whose reply is lost can leave an orphan Request the client never learns of; an
Idempotency-Key (Phase 4) makes a submit exactly-once.

Health: the Redis check pings through the same client; while Sentinel has no master the ping raises and the check is
unhealthy (Req 1.7).

## HTTP transport (Req 2)

### Broker

`create_broker_router(redis, settings)` is mounted on the Gateway's Ops_Port app only when `http_broker.enabled`. Routes,
all `POST`, all behind `hmac.compare_digest` of the bearer token:

| Route | Body | Answer |
|---|---|---|
| `/broker/v1/publish` | `{destination, body (base64), headers}` | `{id}` after `XADD` |
| `/broker/v1/pull` | `{destination, consumer, max, wait}` | `{deliveries: [{id, body, headers, delivery_count}]}` |
| `/broker/v1/settle` | `{destination, id, outcome}` | `{ok}` |

A destination is the stream `das:broker:{destination}` with the consumer group `das`. `pull` first reclaims entries pending
longer than the visibility timeout (`XAUTOCLAIM`, which also raises the delivery count), then reads new ones (`XREADGROUP`,
blocking up to `wait`). Priority: a high-priority publish goes to `das:broker:{destination}:high`, which `pull` reads
first. `settle`: ACK is `XACK` and `XDEL`; REQUEUE re-publishes the entry with its delivery count in a field and acks the
old one; REJECT, and a delivery count above `delivery_limit`, copy the entry to `das:broker:dlq` and ack. Limits per
Req 2.10.

### Adapter

`HttpTransport` mirrors `RabbitMQTransport`'s shape: one `httpx.AsyncClient`, one poll task per subscription honouring
`SubscribeOptions.max_in_flight` and `limiter`, `Delivery.token` holding `(destination, id)`. `publish` returns after the
HTTP answer. `request_reply` publishes with a `reply-to` header naming `reply.<uuid>` and long-polls that destination.
Connection errors raise `TransportUnavailable` and the reconnect loop uses `ReconnectPolicy`. Routing reuses
`das.transport.base.queue_name_for` and the RabbitMQ capability checks.

## Kafka transport (Req 3)

`KafkaTransport` with `AIOKafkaProducer(enable_idempotence=True, acks="all")` and one `AIOKafkaConsumer` per subscription
(`enable_auto_commit=False`, group id `das-<destination>`). A delivery's token is `(TopicPartition, offset)`. Kafka commits
a high-water mark, so the adapter keeps a per-partition set of acknowledged offsets and commits only the highest
contiguous one (`OffsetTracker`, pure, with a property test): an out-of-order ack never commits past an unacknowledged
message (Req 3.6). The in-flight limit is the count of delivered-not-acknowledged offsets.

REQUEUE: publish to `<topic>.retry` with headers `das-not-before` and an increased delivery count, then ack the original; a
small retry consumer per topic holds a message until `das-not-before` and republishes it to the main topic. REJECT, and a
count above `delivery_limit`: publish to `das.dlq` with the standard headers, then ack. High priority: publish to
`<topic>.high`; the poll loop drains `.high` before reading the main topic. `request_reply`: a per-process topic
`das.reply.<process-uuid>` plus correlation ids. Topics are created through `AIOKafkaAdminClient` at `start` when
`auto_create_topics` is true.

## Load tests (Req 4)

- `stats.py` (pure): `percentile(samples, p)` by the nearest-rank method, `summarize(samples)`, `saturation(steps, limits)`.
- `profile.py`: `Profile(name, steps: [(rate, seconds)], prompts)`; built-ins `smoke` (2 rps for 10 s), `steady` (10 rps for
  60 s), `ramp` (2, 5, 10, 20, 40 rps, 30 s each); `load_profile(name_or_path)` validates a YAML file.
- `runner.py`: an `httpx.AsyncClient` submits at the profile's rate with an open-loop schedule (arrival times fixed in
  advance, so a slow server does not slow the offered load), polls each Request to a terminal state with a deadline, and
  records `(submit_ms, total_ms, status)`.
- `report.py` builds the Capacity_Report with the machine description from `platform` and `os.cpu_count`; `compare.py`
  implements `loadtest-compare`.
- The stub LLM (`tests/integration/e2e/llm_stub.py`) gains `STUB_LATENCY_MS` and `STUB_FAILURE_RATE`.

## Compatibility

`RedisConfig` keeps `url` working unchanged. One thing is more than an addition, and it is deliberate: `url` is no longer a
*required* property in the config JSON Schema (it is `string | null`), because a `sentinel` configuration must not have one
(Req 1.2). `standalone` still requires it, in `cross_field_errors`, with the same error naming `redis.url`. Req 5.2 is read
as "no configuration valid in Phase 6 becomes invalid", which holds; the compatibility tests name the two baseline paths
that changed and fail on any other non-addition. Entry points `http` and `kafka` are added beside `rabbitmq` and `grpc`; the
loader and its refusal of unknown names are unchanged. `aiokafka` is imported lazily inside `das/transport/kafka.py`; the
import-linter contract restricts only `das.manager` and `das.worker`, and gains `aiokafka` and
`redis.asyncio.sentinel` in its forbidden list.

## Test strategy

Unit tests with in-memory fakes for the broker, the Kafka client and Sentinel; one shared transport contract suite
(`tests/transport_contract.py`) run against every adapter's fake and, under Docker, against the real RabbitMQ, the broker
over a real Redis and a real single-node Kafka (KRaft); Properties 1 to 4 under `tests/property/`; a Sentinel e2e that
stops the master container during a Request and expects it to finish; a load test smoke run in CI; fault injection on
`OffsetTracker`, the broker's reclaim and the Failover retry.
