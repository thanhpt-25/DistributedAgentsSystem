# Implementation Plan: DAS Phase 7, Scale and Resilience

Each task cites the requirements it implements. A test is written first and seen failing. Logic that matters gets a
fault-injection pass. One commit per wave. Groups are numbered from 8 so they continue the Phase 6 waves.

## Tasks

- [x] 8. Redis Sentinel client and configuration (Wave 8)
  - [x] 8.1 `RedisConfig` modes, validation, the refusal of `cluster`; Property 1 (Req 1.1 to 1.3, 6.1)
  - [x] 8.2 `das/state/redisclient.py`, the single builder used by every role; a static test that nothing else builds a client (Req 1.4)
  - [x] 8.3 The store's retry through a Failover and `READONLY`; `das_redis_retries_total`, `das_redis_failovers_total` (Req 1.5, 1.8)
  - [x] 8.4 Property 3: a fake Redis that fails at a chosen call, no double terminal state, no double result (Req 1.6, 6.3)
  - [x] 8.5 Health through Sentinel (Req 1.7)
- [x] 9. HTTP transport (Wave 9)
  - [x] 9.1 The shared transport contract suite and the in-memory fakes, run against the RabbitMQ adapter's existing fake (Req 6.2)
  - [x] 9.2 The HTTP_Broker router: auth, limits, publish, pull with reclaim, settle, dead letter, priority (Req 2.2, 2.3, 2.5, 2.6, 2.7, 2.10)
  - [x] 9.3 `HttpTransport`: start/close, publish confirm, subscribe long-poll, acknowledge, request_reply, reconnect (Req 2.1, 2.4, 2.8, 2.9)
  - [x] 9.4 Entry point `http`; the Gateway mounts the broker on the Ops_Port only when enabled (Req 2.1, 2.2)
- [x] 10. Kafka transport (Wave 10)
  - [x] 10.1 Add `aiokafka` (pinned); `OffsetTracker` with a property test (Req 3.4, 3.6)
  - [x] 10.2 `KafkaTransport`: topics, idempotent publish, consume and commit after ack, retry topic, dead letter, priority topic, request_reply (Req 3.1 to 3.8)
  - [x] 10.3 Configuration and secrets; entry point `kafka`; the contract suite against the Kafka fake (Req 3.9, 6.2)
  - [x] 10.4 Integration: the contract suite against a real Kafka under Docker (Req 3.10)
- [ ] 11. Failover end-to-end and deployment (Wave 11)
  - [ ] 11.1 Compose profiles `sentinel` and `http-transport`; the Helm values for Sentinel (Req 1.9, 2.11)
  - [ ] 11.2 E2E: stop the Redis master during a Request and expect it to finish (Req 1.6)
  - [ ] 11.3 E2E: a Request over the HTTP transport (Req 2.11)
- [ ] 12. Load test suite (Wave 12)
  - [ ] 12.1 `stats.py` and Property 4; `profile.py`; `report.py`; `compare.py` (Req 4.2, 4.3, 4.4, 4.6, 4.7, 6.4)
  - [ ] 12.2 `runner.py` with an open-loop schedule, tested against the in-process Gateway (Req 4.1, 4.3)
  - [ ] 12.3 The stub LLM's latency and failure rate (Req 4.5)
  - [ ] 12.4 A reference run on the Compose stack; the report in `docs/capacity/` (Req 4.4, 4.8)
- [ ] 13. Documentation, compatibility and final checkpoint (Wave 13)
  - [ ] 13.1 Contract baseline regenerated (additive only, corpus byte-identical); a compatibility test with no new configuration (Req 5.1, 5.2)
  - [ ] 13.2 Import-linter contract extended; README sections and drift tests; schema test for defaults and ranges (Req 5.3 to 5.5)
  - [ ] 13.3 Final checkpoint: the whole suite, ruff, mypy, lint-imports, CI on a branch
