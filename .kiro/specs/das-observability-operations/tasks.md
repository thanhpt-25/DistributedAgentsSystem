# Implementation Plan: DAS Observability & Operations (Phase 2)

## Overview

Hiện thực Phase 2 bằng Python 3.12 (asyncio) trên nền DAS v1, giữ nguyên mô hình pure core + thin I/O shell. Thứ tự: dependency và fixture test; pure core mới (`das/ops`, `LabelBounder`, `TraceContext`, codec Envelope_V2); config schema; observability shell (`MetricsRecorder`, `Tracer`, log correlation) và instrumentation LLM; State_Store key và Lua script; thay đổi RequestMachine và driver; wiring Manager, Worker, Gateway; Ops server và probe; drain/undrain và CLI; cập nhật import-linter; Compose profile `observability` và README; cuối cùng là integration/e2e test. Property test dùng Hypothesis 6.170.8 với `fakeredis[lua]` 2.40.0, chạy chính các file `das/state/lua/*.lua`. Mỗi correctness property của design ứng với đúng một sub-task property test.

## Tasks

- [x] 1. Cập nhật dependency và hạ tầng test
  - [x] 1.1 Ghim dependency mới và thêm fixture dùng chung
    - `pyproject.toml`: thêm đúng các phiên bản ghim `prometheus-client==0.26.0`, `opentelemetry-api==1.45.1`, `opentelemetry-sdk==1.45.1`, `opentelemetry-exporter-otlp-proto-grpc==1.45.1`, `opentelemetry-exporter-otlp-proto-http==1.45.1`; cập nhật `uv.lock` để ghim mọi dependency bắc cầu (`opentelemetry-proto`, `opentelemetry-semantic-conventions`, `googleapis-common-protos`, …)
    - Tạo package `das/ops/` (`__init__.py`) và module rỗng `usage.py`, `cost.py`, `cursor.py`, `retention.py`, `replay.py`, `dlq.py`, `keys.py`, `iso4217.py`
    - `tests/fixtures/v1_codec.py`: `V1ReferenceCodec`, bản sao đóng băng của `MessageCodec` v1 (encode/decode) dùng cho kiểm tra byte-identity
    - `tests/fixtures/redis_time.py`: fixture `FakeRedisTime` patch lệnh `TIME` của fakeredis theo đồng hồ của test
    - Bổ sung strategy dùng chung vào `tests/strategies.py`: `trace_contexts()`, `usage_records()`, `price_tables()`, `dead_letter_records()`
    - _Requirements: 19.4, 19.5_

- [x] 2. Pure core: token usage và chi phí
  - [x] 2.1 Hiện thực model và hàm usage
    - `das/models/usage.py`: `LLMOperation`, `UsageRecord`, `ModelUsage`, `TokenUsageSummary` (frozen, `extra="forbid"`)
    - `das/ops/usage.py`: `normalize_usage(i, o, t) -> NormalizedUsage(usage, reported, problems)` (giá trị thiếu, âm, không phải int hoặc là bool → 0; total thiếu → input + output) và `summarize_usage(records, job_ids)` (đủ ba key `by_operation`, `by_job` gồm mọi Job_ID của Job_Plan, `by_model` sắp theo (provider, model))
    - _Requirements: 9.6, 9.7, 9.8, 9.9, 9.10_
  - [x] 2.2 Viết property test cho chuẩn hóa token usage
    - **Property 11: Chuẩn hóa token usage**
    - **Validates: Requirements 9.6, 9.7**
  - [x] 2.3 Viết property test cho bất biến tổng hợp Token_Usage_Summary
    - **Property 12: Bất biến tổng hợp Token_Usage_Summary**
    - **Validates: Requirements 9.9, 9.10**
  - [x] 2.4 Hiện thực Price_Table và tính Estimated_Cost
    - `das/ops/iso4217.py`: hằng `ISO_4217_CODES`
    - `das/ops/cost.py`: `PriceEntry`, `PriceTable`, `ProviderModel`, `ModelCost`, `CostReport`; `record_cost` (chưa làm tròn), `estimate_cost` trong `decimal.localcontext(prec=50)`, khớp provider/model chính xác phân biệt hoa thường, `unpriced_models` không trùng và sắp theo (provider, model), `complete`; `format_amount` (`quantize(Decimal("0.000001"), ROUND_HALF_EVEN)`, định dạng `"f"`)
    - _Requirements: 10.1, 10.3, 10.4, 10.5, 10.6, 10.7_
  - [x] 2.5 Viết property test cho tính cộng tính và định dạng của Estimated_Cost
    - **Property 14: Tính cộng tính và định dạng của Estimated_Cost**
    - **Validates: Requirements 10.4, 10.5, 10.6, 10.8**
  - [x] 2.6 Viết property test cho tính đơn điệu của Estimated_Cost
    - **Property 15: Tính đơn điệu của Estimated_Cost**
    - **Validates: Requirements 10.9**

- [x] 3. Pure core: cursor, retention, dead letter, replay và phân loại khóa
  - [x] 3.1 Hiện thực Page_Cursor
    - `das/ops/cursor.py`: `PageCursor` (`v`, `e`, `f`, `s`, `m`), `encode_cursor(c, key)` (JSON chuẩn tắc → base64url không padding + `.` + base64url(HMAC-SHA256 cắt 16 byte)), `decode_cursor(s, key, *, endpoint, filters)` kiểm tra lần lượt độ dài ≤ 512, ký tự `[A-Za-z0-9_.-]`, MAC (`hmac.compare_digest`), `v`, `e`, `f`; mọi lỗi ném `InvalidCursor`
    - _Requirements: 19.8, 19.9_
  - [x] 3.2 Viết property test cho round-trip của Page_Cursor
    - **Property 28: Round-trip của Page_Cursor**
    - **Validates: Requirements 19.8**
  - [x] 3.3 Hiện thực quyết định retention
    - `das/ops/retention.py`: `retention_decision(status, finished_at_ms, now_ms, ttl_ms) -> "delete" | "keep" | "stamp"` (non-terminal hoặc vắng mặt → keep; terminal thiếu `finished_at` → stamp; `finished_at + ttl ≤ now` → delete)
    - _Requirements: 14.3, 14.4, 14.9_
  - [x] 3.4 Hiện thực helper dead letter
    - `das/models/dlq.py`: `DeadLetterReason`, `RecordStatus`, `DeadLetterRecord`, `ReplayHistoryEntry`, `RequestTransitionEntry`
    - `das/ops/dlq.py`: `dead_letter_identity` (`msg:<message_id>` hoặc `raw:<sha256(body)>`), `dead_letter_id` (32 hex đầu của sha256), `classify_reason(headers)` (`x-death-reason`), `build_record(...)` (trường null khi không parse được, last error cắt 4096 ký tự, malformed: `parse_reason`, `parse_field`, `original_size`, `raw_b64` cắt theo `max_message_bytes`, `truncated`)
    - _Requirements: 12.1, 12.2, 12.3_
  - [x] 3.5 Hiện thực quyết định Replay và Discard
    - Thêm `attempt_limit: int | None = None` (`ge=1`, serialize `exclude_none`) vào `JobRecord` và `dead_letter_id: str | None` vào `TransitionEntry` trong `das/models/`, giữ JSON v1 không đổi khi không có giá trị
    - `das/ops/replay.py`: `decide_replay(rec, req, job, lease_active)` theo thứ tự `not_found`, `record_not_pending`, `not_replayable`, `request_gone`, `request_state`, `job_state`, `stale_attempt` (gồm cả `attempt > job.attempt`), `lease_held` (chỉ với Failed); `decide_discard(rec)`; `skipped_to_reset(plan, jobs, replayed)` theo tập nguyên nhân gốc bắc cầu
    - _Requirements: 13.1, 13.4, 13.6, 13.7, 13.8, 13.11, 13.12, 13.13, 19.5_
  - [x] 3.6 Hiện thực phân loại khóa
    - `das/ops/keys.py`: `KeyClass`, `classify_key(candidate, api, admin)` gọi `hmac.compare_digest` với mọi khóa của cả hai danh sách, không thoát sớm, gộp bằng OR; `key_fingerprint(key)` = `sha256(key).hexdigest()[:8]`
    - _Requirements: 11.2, 11.3, 11.4, 11.5, 11.6, 11.7_
  - [x] 3.7 Viết unit test cho pure core
    - `retention_decision` ở biên TTL; `classify_reason` cho ba reason; `build_record` cắt last error và raw bytes; `skipped_to_reset` với DAG kim cương và nhiều nguyên nhân gốc; `classify_key` với khóa trùng prefix và danh sách admin rỗng
    - _Requirements: 11.7, 12.1, 12.3, 13.4, 14.9_

- [x] 4. Pure core: LabelBounder, TraceContext và codec Envelope_V2
  - [x] 4.1 Hiện thực `LabelBounder`
    - `das/observability/labels.py`: `LabelPolicy(enumerated, dynamic_limits)`, `LabelBounder.bound(metric, labels) -> BoundResult(labels, overflowed)`; label enumerated ngoài tập → `_other`; label động giữ giá trị đã thấy, nhận giá trị mới khi còn dưới limit, ngược lại `_other` và không thêm vào `seen`; `seen` không bao giờ reset
    - _Requirements: 5.2, 5.3, 5.7_
  - [x] 4.2 Hiện thực `TraceContext` thuần
    - `das/observability/trace_context.py`: `TraceContext(traceparent, tracestate)` với `parse(traceparent, tracestate)` theo W3C (version hex thường khác `ff`, trace-id/parent-id không toàn số 0, flags, độ dài 55 với version `00`; `tracestate` ≤ 512 ký tự, ≤ 32 list-member, đúng cú pháp) và thuộc tính `sampled`; module không import `opentelemetry`
    - _Requirements: 7.1_
  - [x] 4.3 Mở rộng MessageCodec với Envelope_V2 và message `agent_control`
    - `das/codec/envelope.py`: `TraceContextModel`, trường `trace_context` tùy chọn, chuẩn hóa `schema_version` (2 khi có context, ngược lại 1); thêm `MessageType` `agent_control`, `agent_control_reply` cùng `AgentControlPayload` (`command`, `agent_id`, `release_leases`) và `AgentControlReplyPayload` (`agent_id`, `status`, `in_flight ≥ 0`)
    - `das/codec/message_codec.py`: `SUPPORTED_VERSIONS = {1, 2}`; encode loại `trace_context` khỏi dump, chỉ thêm khi có, `tracestate` chỉ khi có, giữ canonical JSON v1; decode kiểm version trước field set, v1 từ chối `trace_context`, v2 parse khoan dung (`parse_trace_context` → `None` cộng log debug, bỏ `tracestate` sai), version khác → `ParseError(field="schema_version")`
    - `new_message(payload, *, trace_context=None, …)`
    - _Requirements: 6.11, 7.1, 7.2, 7.3, 7.4, 7.7, 7.8, 7.9, 7.10, 19.7_
  - [x] 4.4 Viết property test cho round-trip envelope có Trace_Context
    - **Property 5: Round-trip envelope có Trace_Context**
    - **Validates: Requirements 7.3, 7.4, 7.5, 7.9**
  - [x] 4.5 Viết property test cho bytes độc lập Transport và trùng v1
    - **Property 6: Bytes độc lập Transport và trùng v1 khi không có Trace_Context**
    - Dùng `V1ReferenceCodec`, fake channel RabbitMQ và fake stub gRPC
    - **Validates: Requirements 6.13, 7.2, 7.6, 19.7**
  - [x] 4.6 Viết property test cho decode khoan dung với `trace_context`
    - **Property 7: Decode khoan dung với `trace_context`**
    - **Validates: Requirements 6.11, 7.1, 7.7, 7.8, 7.10**

- [x] 5. Mở rộng config schema
  - [x] 5.1 Thêm các section cấu hình mới và kiểm tra chéo
    - `das/config/schema.py`: section `metrics` (`enabled`, `queue_sample_interval` 5..300, `label_limits` 1..500, `buckets` với mặc định của design), `tracing` (`enabled`, `otlp_endpoint`, `otlp_protocol`, `sample_ratio`, `service_name` mặc định `das-<role>`, `max_queue_size`), `pricing` (tùy chọn, giá `Decimal`, YAML float qua `Decimal(repr(x))`), `retention` (khoảng của Req 14.1), `gateway`, `health_port` cho Gateway
    - `cross_field_errors`: bucket (không rỗng, ≤ 50, > 0, tăng ngặt), `tracing.otlp_endpoint` là URL `http`/`https` tuyệt đối có host khi enabled, lỗi Price_Table theo chỉ số entry (`pricing.models[i].<field>`), Admin_API_Key (≤ 32 khóa, 16-256 ký tự ASCII 0x21-0x7E, không trùng API_Key, lỗi nêu `admin_api_keys[i]` không kèm giá trị), `health_port != http_port`
    - `das/config/secrets.py` và loader: secret mới `admin_api_keys` (`DAS__ADMIN_API_KEYS`, tách dấu phẩy, bỏ khoảng trắng, bỏ entry rỗng), `tracing.otlp_headers`, `gateway.cursor_secret` chỉ từ env hoặc `secrets_file`; có trong YAML → lỗi nêu tên trường; secrets file thiếu → lỗi nêu tham chiếu
    - `AgentConfig.secret_values()` trả thêm Admin_API_Key, giá trị OTLP header và `cursor_secret`
    - _Requirements: 1.8, 2.11, 2.12, 3.11, 5.8, 8.1, 8.3, 8.5, 8.6, 10.1, 10.2, 11.1, 11.10, 11.11, 14.1, 19.1, 19.2_
  - [x] 5.2 Viết property test cho validator cấu hình mới
    - **Property 16: Validator cấu hình mới báo đúng các trường lỗi**
    - **Validates: Requirements 2.12, 5.8, 10.2, 19.2**
  - [x] 5.3 Viết unit test cho cấu hình mới
    - Bucket mặc định; secret mới trong YAML → exit 2 không in giá trị; secrets file thiếu; `health_port == http_port`; cắt khoảng trắng của `DAS__ADMIN_API_KEYS`; precedence `DAS__TRACING__ENABLED`
    - _Requirements: 1.8, 8.6, 11.1, 11.10, 11.11, 19.1_

- [x] 6. Observability shell: metrics, tracing và tương quan log
  - [x] 6.1 Hiện thực catalog metric và `MetricsRecorder`
    - `das/observability/metrics.py`: bảng `MetricSpec` tĩnh cho mọi metric ở design §2 cùng tập Bounded_Label_Value; `MetricsRecorder` dùng `CollectorRegistry(auto_describe=False)` riêng, `disable_created_metrics()`, `inc`/`observe`/`set`/`set_function`/`render` (content type `text/plain; version=0.0.4; charset=utf-8`); mỗi lần ghi bọc `try/except` với `RateLimitedLog` 60 s; warning overflow một lần cho mỗi cặp (metric, label); `NoopRecorder`
    - Facade có kiểu `GatewayMetrics`, `ManagerMetrics`, `WorkerMetrics` chỉ nhận enum, Capability, queue, provider, model
    - _Requirements: 1.1, 1.4, 1.5, 1.6, 1.9, 2.11, 5.1, 5.2, 5.3, 5.4, 5.6, 5.7_
  - [x] 6.2 Viết property test cho giới hạn cardinality và tuân thủ catalog
    - **Property 3: Giới hạn cardinality và tuân thủ catalog**
    - Dùng `ReferenceBounder` và parse output bằng `text_string_to_metric_families`
    - **Validates: Requirements 1.4, 1.5, 2.10, 5.1, 5.2, 5.3, 5.4, 5.5, 5.7**
  - [x] 6.3 Viết property test cho không mất observation khi overflow
    - **Property 4: Không mất observation khi overflow**
    - **Validates: Requirements 5.6**
  - [x] 6.4 Hiện thực `Tracer` và `BoundedExportProcessor`
    - `das/observability/tracing.py`: `ALLOWED_ATTRIBUTES`, protocol `Tracer`/`SpanHandle`, `NoopTracer` (không Span, không kết nối, `current_context()` trả `None`), `build_tracer(config, role, agent_id, clock)` với `Resource(service.name)`, `ParentBased(TraceIdRatioBased(sample_ratio))`, exporter OTLP gRPC hoặc HTTP khởi tạo lười kèm `otlp_headers`
    - `span(...)` đặt status error và `error.type` khi có exception; `fail(code)` cho kết quả thất bại; `set_attributes`/`add_event` loại key ngoài allowlist
    - `BoundedExportProcessor`: `deque` có khóa, `on_end` O(1), bỏ Span và đếm `dropped` khi đầy, thread nền export lô ≤ 512 mỗi 5 s, đưa lô lỗi lại đầu hàng đợi với backoff 1→30 s, warning "spans dropped" tối đa mỗi 60 s, warning `otlp_endpoint_unreachable` ở lần lỗi đầu; `shutdown(timeout)` flush trong `min(5, remaining)` và trả số Span bị bỏ
    - _Requirements: 6.9, 6.10, 6.13, 8.1, 8.2, 8.4, 8.7, 8.8, 8.12_
  - [x] 6.5 Viết property test cho allowlist attribute của Span
    - **Property 8: Allowlist attribute của Span**
    - **Validates: Requirements 6.9**
  - [x] 6.6 Viết property test cho hạch toán hàng đợi export Span
    - **Property 9: Hạch toán hàng đợi export Span**
    - **Validates: Requirements 8.7**
  - [x] 6.7 Thêm processor `add_trace_ids` và đăng ký secret mới vào Redactor
    - `das/observability/logging.py`: processor chạy trước `drop_content_fields`, chỉ thêm `trace_id` (`032x`) và `span_id` (`016x`) khi Span hiện tại hợp lệ và có cờ sampled, ngược lại xóa hai key nếu caller tự truyền
    - Nạp `AgentConfig.secret_values()` mở rộng vào Redactor trước dòng log đầu tiên
    - _Requirements: 8.9, 8.10, 8.11, 11.9, 19.3_
  - [x] 6.8 Viết property test cho tương quan log-trace
    - **Property 10: Tương quan log-trace**
    - **Validates: Requirements 8.9, 8.10, 8.11**
  - [x] 6.9 Viết property test cho redaction của secret mới
    - **Property 30: Redaction cho secret mới**
    - Mở rộng `tests/property/test_log_redaction.py` của v1 với generator Admin_API_Key, OTLP header và `cursor_secret`
    - **Validates: Requirements 11.9, 19.3**

- [x] 7. Instrumentation cho LLM_Provider
  - [x] 7.1 Thêm operation, observer, Span và chuẩn hóa usage vào lớp LLM
    - `das/llm/errors.py`: `RateLimitedLLMError` (lớp con của `TransientLLMError`); adapter ném lỗi này khi gặp HTTP 429 hoặc throttling
    - `das/llm/retry.py`: `LLMOperation`, protocol `LLMCallObserver` (`on_retry_scheduled`, `on_finished`); `complete(..., operation=...)` đo duration bằng `clock.monotonic()` gồm cả backoff, gọi `on_retry_scheduled` trước mỗi `sleep`, phân loại outcome theo lỗi của attempt cuối, re-raise `CancelledError` không gọi `on_finished`; Span `llm.completion` với một event `retry` mỗi lần retry
    - Adapter gọi `normalize_usage` trước khi tạo `Completion`; `Completion.usage_reported: bool = True`; log warning khi `problems` không rỗng
    - Cập nhật mọi caller (Planner, Summarizer, executor) truyền `operation`
    - _Requirements: 4.1, 4.2, 4.3, 4.7, 6.6, 9.6, 9.7_
  - [x] 7.2 Hiện thực `LLMMetricsObserver`
    - Trong `das/observability/metrics.py`: ghi `das_llm_calls_total`, `das_llm_call_duration_seconds`, `das_llm_retries_total`; cộng `das_llm_tokens_total` chỉ khi `success` và `usage_reported`; cộng `das_llm_estimated_cost_total` bằng `record_cost` chưa làm tròn khi Price_Table có cặp provider/model
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_
  - [x] 7.3 Viết property test cho nhất quán metrics LLM
    - **Property 2: Nhất quán metrics LLM**
    - Dùng `FakeAdapter` lập trình được và `FakeClock`, hủy task ở điểm tùy ý
    - **Validates: Requirements 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8**

- [x] 8. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. State_Store: key mới và Lua script
  - [x] 9.1 Thêm key mới và cập nhật script tạo Request và acquire Lease
    - `das/state/keys.py`: mọi key ở design §6 (`usage`, `pending_replays`, `requests:index`, `requests:by_status:{s}`, `requests:index:ready`, `requests:index:backfill_lock`, `requests:finished`, `requests:inbox_purge`, `dlq:{dlid}`, `dlq:index`, `dlq:by_status:{s}`, `dlq:by_reason:{r}`, `dlq:terminal`, `gateway:cursor_key`); `request_keys(rid)` gồm thêm `usage`, `pending_replays`
    - `_common.lua`: helper `index_move`, `mark_finished`, `append_usage`; `put_job` nhận `tag`
    - `create_request.lua`: ARGV `trace_context`, ZADD `index` và `by_status:Pending`
    - `acquire_lease.lua`: HSET `last_owner`, trả `{'OK', epoch, expires, kind}`
    - `das/state/store.py`: `create_request(..., trace_context)`, `acquire_lease` trả `kind`; `StoredRequest` thêm `trace_context`, `finished_at`, `job_count`, `usage_totals`, `replay_history`, `request_transitions`
    - _Requirements: 3.3, 6.1, 14.2, 15.11, 20.7_
  - [x] 9.2 Cập nhật các script ghi transition, finalize, cancel và inbox
    - `commit_plan.lua`: ARGV UsageRecord planning, `index_move(Pending→Running)`, `job_count`, `append_usage`
    - `apply_transition.lua`: batch thêm `usage` (RPUSH chỉ khi HSETNX result trả 1), `tag`, `replays_applied` (SREM), `dead_letter_reverts` (`Replayed→Pending` có compare-and-set và chuyển index), `job_count`; decode và kiểm tra hết input trước lệnh ghi đầu tiên
    - `finalize.lua`: `REPLAY_PENDING` khi `pending_replays` không rỗng, UsageRecord summarize/planning, `mark_finished`, `index_move`, trả `{'OK', finished_at_ms, created_at_ms}`
    - `cancel_request.lua`: `mark_finished`, `index_move(→Cancelled)`, đưa record trong `pending_replays` về `Pending` (khác nội dung set → `CONFLICT`), DEL set, trả thêm `finished_at_ms`, `created_at_ms`
    - `record_inbox.lua`: ARGV `meta` tùy chọn; `InboxEntry.meta`
    - `das/state/store.py`: method tương ứng và `load_usage(rid)` qua `_bounded()`
    - _Requirements: 2.2, 2.3, 9.1, 9.2, 9.3, 9.4, 9.5, 13.4, 13.15, 14.2, 15.11_
  - [x] 9.3 Thêm script và method cho dead letter, Replay và reopen
    - `create_dead_letter.lua`, `replay_dead_letter.lua` (cùng thứ tự kiểm tra với `decide_replay`, hai nhánh Running và Failed theo design §6), `discard_dead_letter.lua`, `reopen_request.lua`, `list_dead_letters.lua`
    - `das/state/store.py`: `create_dead_letter`, `get_dead_letter`, `list_dead_letters`, `replay_dead_letter`, `discard_dead_letter`, `reopen_request`; method đọc của Gateway qua `_bounded()` (timeout 2 s, tổng 4,5 s)
    - _Requirements: 12.1, 12.2, 12.4, 12.5, 12.12, 13.1, 13.2, 13.3, 13.6, 13.7, 13.8, 13.11, 13.12, 13.13, 13.16_
  - [x] 9.4 Thêm script và method cho retention và Request_Index
    - `purge_request.lua`, `purge_inbox.lua`, `purge_dead_letter.lua` (dùng `TIME` của chính script, Pending không bao giờ bị xóa), `index_request.lua`, `list_index.lua` (gộp k-way theo (score giảm dần, member giảm dần))
    - `das/state/store.py`: `purge_request`, `purge_inbox`, `purge_dead_letter`, `index_request`, `list_requests`, `finished_candidates`, `inbox_candidates`, `dlq_terminal_candidates`, `index_ready`, `server_time`
    - _Requirements: 14.3, 14.4, 14.7, 14.8, 14.9, 14.11, 15.1, 15.2, 15.7, 15.10, 15.11, 15.12_
  - [x] 9.5 Mở rộng Agent_Registry với Draining và in_flight
    - `das/registry/registry.py`: `_HEARTBEAT` nhận `desired_status` và `in_flight`; `_SWEEP` đánh dấu Unavailable cho cả Available lẫn Draining; script mới `_SET_STATUS`; `set_status(...)`; Capability chỉ được quảng bá bởi Worker Available
    - `AgentRecord.status` có thêm `Draining`, `in_flight: int = 0`
    - _Requirements: 3.6, 17.2, 17.4, 17.6, 17.7, 17.8, 17.16_
  - [x] 9.6 Viết property test cho thứ tự kiểm tra Replay và Discard
    - **Property 20: Thứ tự kiểm tra Replay và Discard**
    - So sánh `replay_dead_letter.lua` / `discard_dead_letter.lua` trên fakeredis với `decide_replay` / `decide_discard`
    - **Validates: Requirements 13.6, 13.7, 13.8, 13.11, 13.13**
  - [x] 9.7 Viết property test cho lọc đúng và Request_Index nhất quán
    - **Property 26: Lọc đúng danh sách Request và Request_Index nhất quán**
    - **Validates: Requirements 15.2, 15.5, 15.11**
  - [x] 9.8 Viết unit test cho Lua script
    - `finalize` trả `REPLAY_PENDING`; `cancel_request` đưa replay chưa áp dụng về Pending; Request Failed mở lại với đủ field ở Req 13.2; `acquire_lease` trả `initial`/`takeover`/acquire lại bởi chính agent; `index_request` trên dữ liệu ghi bởi script v1; `finished_at` bị ghi đè sau Replay
    - _Requirements: 3.3, 13.2, 13.15, 14.2, 15.10_

- [x] 10. RequestMachine và RequestDriver
  - [x] 10.1 Mở rộng RequestMachine với Replay và usage
    - `das/manager/machine.py`: `limit(record) = record.attempt_limit or cfg.max_attempts` trong `_on_result`; event `ReplayRequested(dead_letter_id, job_id, attempt)` và handler `_on_replay` (áp dụng hoặc `dead_letter_reverts` kèm warning, luôn `replays_applied`); reset Job Skipped theo `skipped_to_reset`; `JobResultReceived(result, llm_meta)` sinh UsageRecord vào `w.new_usage` chỉ cho Job_Result được chấp nhận
    - `TransitionBatch` thêm `usage`, `tag`, `replays_applied`, `dead_letter_reverts`; `apply_decision` mô hình hóa đúng `apply_transition.lua`
    - _Requirements: 9.2, 9.5, 13.4, 13.5, 13.10, 13.15_
  - [x] 10.2 Viết property test cho áp dụng Replay trong RequestMachine
    - **Property 22: Áp dụng Replay trong RequestMachine**
    - **Validates: Requirements 13.4, 13.15**
  - [x] 10.3 Cập nhật driver cho Replay, reopen và usage
    - `das/manager/driver.py`: `_inbox_event` hiểu `kind == "replay"`; `_plan` gọi `reopen_request` khi Request Pending đã có plan; `_write_final` xử lý `REPLAY_PENDING` bằng một vòng `_drain_inbox` mới; truyền UsageRecord của Planner/Summarizer vào `commit_plan`/`finalize`
    - `Planner`: `PlanOutcome.usages`, `PlanLLMFailed(error, usages)`; `SummarizationFailed.usages`; bỏ UsageRecord khi lệnh ghi trả `LEASE_LOST`/`REQUEST_TERMINAL`
    - `request_release()`: dừng ở ranh giới event kế tiếp rồi `keeper.release()`
    - _Requirements: 9.1, 9.3, 9.4, 13.2, 13.4, 13.5, 13.15, 17.5_
  - [x] 10.4 Thêm metrics và Span vào driver
    - `das/manager/driver.py`: `das_requests_finished_total`/`das_request_duration_seconds` khi `finalize` trả `OK` (duration từ giá trị script trả, âm → 0); `das_job_transitions_total` và `das_job_attempts` chỉ khi `WriteResult.ok`, đọc từ `Decision.changes`
    - Span `manager.request` (parent là `stored.trace_context` hoặc gốc mới, không ghi lại vào Request), `manager.plan` qua hook `on_attempt`, `manager.dispatch` (envelope mang context của Span này), `manager.summarize`; `fail(code)` với `LEASE_LOST`, `REQUEST_TERMINAL`, `PlanRejected`; control command gắn với Request lấy `tracer.current_context()`
    - _Requirements: 2.2, 2.4, 2.5, 2.7, 6.2, 6.3, 6.4, 6.8, 6.10, 6.12_

- [x] 11. Thành phần Manager_Agent
  - [x] 11.1 Cập nhật ResultIngest và thêm `DeadLetterRecorder`
    - `das/manager/ingest.py`: `ResultIngest` đọc header `x-llm-provider`/`x-llm-model` (thiếu → `unknown`) và truyền làm `meta` vào `record_inbox`; Span `manager.ingest_result` với parent từ envelope
    - `DeadLetterRecorder` tiêu thụ destination `dlq`: identity → dlid → reason → `build_record` → `create_dead_letter`; `CREATED` tăng `das_dlq_records_created_total` rồi ACK; `EXISTS` ACK; exception → warning (reason, message_id) và `REQUEUE`; log chỉ chứa dlid, reason, request_id, job_id
    - _Requirements: 3.8, 6.7, 9.2, 12.1, 12.2, 12.3, 12.10, 12.11_
  - [x] 11.2 Thêm sampler queue, registry gauge và metric Lease
    - `das/transport/`: protocol tùy chọn `QueueInspector.ready_count(destination)`; RabbitMQ adapter dùng passive `queue_declare` lấy `message_count`
    - `das/manager/samplers.py`: `QueueDepthSampler` (`results`, `dlq`, `jobs/<cap>` của mọi Capability được quảng bá, timeout 5 s mỗi queue, giữ giá trị cũ, `das_queue_depth_sample_errors_total`, warning 60 s mỗi queue); `RegistryGauges` (đủ 6 tổ hợp role/status, gauge Capability = 0 khi không có Worker Available, timeout 2 s, `das_registry_sample_errors_total`)
    - `das/manager/claim.py` và LeaseKeeper: `das_lease_acquisitions_total{kind}` theo `kind` của script, `das_lease_losses_total{reason}` trong `_mark_lost`, `das_leases_held` qua `set_function`
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.10_
  - [x] 11.3 Hiện thực `RetentionSweeper` và `IndexBackfill`
    - `das/manager/retention.py`: mỗi `retention.sweep_interval` chọn ứng viên từ `finished`, `inbox_purge`, `dlq:terminal` theo `server_time()` và `batch_size` rồi gọi script tương ứng; cộng `das_retention_deleted_total{kind}` theo reply `DELETED`; lỗi Redis bỏ phần còn lại của lần sweep, warning 60 s; không chạy khi `retention.enabled=false`
    - `IndexBackfill`: lock `SET NX PX 60000` gia hạn mỗi 20 s, `SCAN MATCH das:req:* TYPE hash` bỏ key có hậu tố, `index_request` cho từng key, rồi `SET das:requests:index:ready 1`
    - _Requirements: 14.3, 14.6, 14.7, 14.8, 14.9, 14.10, 14.12, 14.13, 15.10_
  - [x] 11.4 Wiring các loop mới vào ManagerAgent
    - `das/manager/agent.py`: khởi tạo `ManagerMetrics`, Tracer, `LLMMetricsObserver` (kèm Price_Table của agent), `DeadLetterRecorder`, `QueueDepthSampler`, `RegistryGauges`, `RetentionSweeper`, `IndexBackfill`; loop không phải Critical_Loop tự khởi chạy lại sau lỗi
    - _Requirements: 3.1, 3.6, 12.1, 14.3, 14.13, 15.10_
  - [x] 11.5 Viết property test cho đếm Job transition tối đa một lần
    - **Property 1: Job transition được đếm tối đa một lần**
    - RuleBasedStateMachine, 2-10 Manager, fakeredis, chèn mất reply sau khi ghi thành công
    - **Validates: Requirements 2.4, 2.5, 2.6**
  - [x] 11.6 Viết property test cho kế toán idempotent
    - **Property 13: Kế toán idempotent**
    - Pipeline `ResultIngest` → inbox → `RequestMachine.step` → `apply_transition.lua`
    - **Validates: Requirements 9.2, 9.5, 9.11**
  - [x] 11.7 Viết property test cho loại bỏ Job_Result cũ sau Replay
    - **Property 23: Loại bỏ Job_Result cũ sau Replay**
    - **Validates: Requirements 13.10**
  - [x] 11.8 Viết property test cho mỗi dead letter đúng một record
    - **Property 18: Mỗi dead letter đúng một Dead_Letter_Record**
    - 1-5 `DeadLetterRecorder` dùng chung fakeredis, chèn lỗi Redis ngẫu nhiên
    - **Validates: Requirements 3.8, 12.2, 12.3**
  - [x] 11.9 Viết property test cho tính loại trừ của Replay và Discard
    - **Property 21: Tính loại trừ của Replay và Discard**
    - **Validates: Requirements 13.9, 13.12**
  - [x] 11.10 Viết property test cho an toàn của retention
    - **Property 24: An toàn của retention**
    - RuleBasedStateMachine với `FakeRedisTime`, 1-10 Manager
    - **Validates: Requirements 14.3, 14.4, 14.5, 14.7, 14.8, 14.9**
  - [x] 11.11 Viết property test cho liveness của retention và đếm đúng một lần
    - **Property 25: Liveness của retention và đếm đúng một lần**
    - **Validates: Requirements 14.6, 14.10**
  - [x] 11.12 Viết unit test cho Manager
    - Điểm ghi metric: finalize OK và `LEASE_LOST`, duration âm → 0, `das_job_attempts` = 0 cho Job Skipped/Cancelled trước dispatch, `reason` lease loss; sampler giữ giá trị cũ khi lỗi; chuỗi Span gồm takeover cùng trace ID với `InMemorySpanExporter`; context được persist khi `sample_ratio=0`; Request v1 không có context; DLQ `REQUEUE` khi Redis lỗi; retention tắt thì không xóa gì
    - _Requirements: 2.2, 2.7, 3.2, 3.4, 6.2, 6.12, 12.11, 14.12_

- [x] 12. Worker_Agent
  - [x] 12.1 Thêm metrics, Span và header provider/model vào Worker
    - `das/worker/worker.py`: Span `worker.job` với parent từ envelope; `das_job_execution_seconds` đo bằng `clock.monotonic` khi tạo `JobResultPayload` (không ghi lại khi publish lại); `das_worker_jobs_in_flight` qua `set_function`; `das_worker_concurrency_limit`
    - `das/worker/executor.py`: publish Job_Result kèm header `x-llm-provider`, `x-llm-model` và `trace_context` của `worker.job` với mọi status; usage đã chuẩn hóa ở biên adapter
    - _Requirements: 2.8, 2.9, 6.5, 9.2, 9.6, 20.5_
  - [x] 12.2 Viết unit test cho Worker
    - Execution seconds không ghi lại khi publish lại; header có mặt trên Job_Result; Job_Result Failed vẫn mang context; gauge in-flight trong khoảng 0..concurrency
    - _Requirements: 2.8, 2.9, 6.5_

- [x] 13. Gateway_API
  - [x] 13.1 Hiện thực phân quyền Admin_API và audit
    - `das/gateway/auth.py`: dependency dùng `classify_key` cho router `/v1` (API hoặc ADMIN, cùng response) và router `/v1/admin` (401 `unauthorized`, 403 `forbidden`, 403 `admin_disabled`), chạy trước handler nên từ chối không chạm Redis hay gửi command
    - `das/gateway/audit.py`: `AuditMiddleware` phát đúng một log `audit` mức info trước khi gửi response (`at`, `action`, `target`, `http_status`, `error_code`, `key_fingerprint`)
    - `das/gateway/admin.py`: khung router admin gắn auth và audit
    - _Requirements: 11.2, 11.3, 11.4, 11.5, 11.6, 11.7, 11.8_
  - [x] 13.2 Mở rộng endpoint v1 và thêm HTTP metrics
    - `das/gateway/app.py`: submit tạo Span `gateway.submit`, truyền `traceparent` vào `create_request`, tăng `das_requests_submitted_total` chỉ khi `OK`; cancel ghi metric Cancelled chỉ khi `OK`
    - `GET /v1/requests/{id}` thêm `token_usage`, `estimated_cost` (null khi không có Price_Table), `replay_history`, request transition entry; 503 khi đọc usage lỗi; giữ nguyên mọi field v1
    - `GET /v1/agents` thêm `status` Draining, `in_flight`, `drained`
    - `HttpMetricsMiddleware` chỉ gắn vào app `http_port`, route template hoặc `_unmatched`, method chuẩn hóa
    - _Requirements: 2.1, 2.3, 2.10, 2.13, 6.1, 9.8, 9.12, 9.13, 10.3, 10.7, 17.7, 19.6, 19.10_
  - [x] 13.3 Hiện thực `GET /v1/requests`
    - `parse_list_query` thuần (status 1-5 giá trị khử trùng, `created_after`/`created_before` ISO 8601 có múi giờ → UTC ms, after < before, `limit` 1..100, `decode_cursor(endpoint="requests", filters)`)
    - Khóa HMAC: `gateway.cursor_secret` hoặc khóa 32 byte ở `das:gateway:cursor_key` (`SET NX`)
    - Kiểm tra `index_ready` (503 `index_not_ready`), `list_index` lấy `limit+1`, pipeline HMGET, bỏ mục đã bị xóa, `next_cursor` khi có mục thứ `limit+1`; 400 `invalid_query{parameter}`; 503 `service_unavailable`
    - _Requirements: 14.11, 15.1, 15.2, 15.3, 15.4, 15.7, 15.8, 15.9, 15.10, 15.12, 15.13, 19.8, 19.9, 20.1_
  - [x] 13.4 Hiện thực endpoint DLQ của Admin_API
    - `das/gateway/admin.py`: `GET /v1/admin/dlq` (zset nguồn theo `status`/`reason`/`index`, filter còn lại trong script, cursor endpoint `dlq`, giới hạn độ dài `request_id` và `capability`), `GET /v1/admin/dlq/{id}` (record, envelope hoặc `raw_b64` + `truncated`, `request_status`, `job_state`, `job_attempt`, `request_exists`), `POST .../replay` (202 hoặc 409 kèm `current_state`), `POST .../discard` (200, idempotent, 409)
    - Tăng `das_dlq_admin_actions_total{action, outcome}` theo outcome của handler; 401/403 không ghi; 404 `not_found`; 503 `service_unavailable`
    - _Requirements: 3.9, 12.4, 12.5, 12.7, 12.8, 12.9, 12.12, 13.1, 13.2, 13.3, 13.6, 13.7, 13.8, 13.11, 13.12, 13.13, 13.16, 14.14_
  - [x] 13.5 Viết property test cho phân quyền Admin_API và Audit_Entry
    - **Property 17: Phân quyền Admin_API và Audit_Entry**
    - **Validates: Requirements 11.2, 11.3, 11.4, 11.5, 11.7, 11.8**
  - [x] 13.6 Viết property test cho phân trang DLQ đầy đủ
    - **Property 19: Phân trang DLQ đầy đủ**
    - **Validates: Requirements 12.4, 12.5, 12.6**
  - [x] 13.7 Viết property test cho phân trang danh sách Request đầy đủ
    - **Property 27: Phân trang danh sách Request đầy đủ**
    - **Validates: Requirements 15.1, 15.4, 15.6, 15.7, 15.12**
  - [x] 13.8 Viết property test cho từ chối Page_Cursor không hợp lệ
    - **Property 29: Từ chối Page_Cursor không hợp lệ**
    - **Validates: Requirements 12.9, 15.8, 19.9**
  - [x] 13.9 Viết unit test cho Gateway
    - Submit 202/401/400/503 và counter; cancel `TERMINAL` không đổi metric; golden snapshot response v1 giữ đủ key; `estimated_cost` null, bảng rỗng và `0.000000`; 503 khi đọc usage lỗi; field của mục danh sách không có nội dung; 400 cho từng parameter; `index_not_ready`; inspect DLQ với `request_exists` true/false/null và sau khi Request bị xóa; outcome của admin counter
    - _Requirements: 2.1, 2.13, 3.9, 9.12, 10.3, 10.7, 12.7, 14.14, 15.3, 15.8, 15.10, 19.6_

- [x] 14. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 15. Ops server và probe
  - [x] 15.1 Hiện thực `LoopSupervisor`, watchdog và Ops app
    - `das/health/supervisor.py`: `LoopSupervisor` (`register`, `ended`, `mark_shutting_down`), `EventLoopWatchdog` tick mỗi 1 s bằng Clock, `stale(now)` khi quá 5 s
    - `das/health/server.py`: `create_ops_app(...)` với `GET /metrics` (404 khi tắt, 500 khi `render()` lỗi kèm warning 60 s, bỏ qua `X-API-Key`), `GET /livez` (loops và event_loop, không phụ thuộc kết nối), `GET /readyz` (`startup`, `registered_available`, `transport:<name>`, `state_store`, `not_draining`, `not_shutting_down`; mỗi check timeout 2 s chạy song song, tổng 3 s; trường `llm_provider` từ `CachedCheck` v1; Gateway chỉ có `startup` và `state_store`); giữ `/healthz` v1
    - _Requirements: 1.1, 1.3, 1.7, 1.9, 16.1, 16.2, 16.3, 16.4, 16.5, 16.6, 16.7, 16.8, 16.9, 16.10_
  - [x] 15.2 Wiring Ops server, Tracer và metrics trong CLI và agent
    - `das/cli.py`: dựng `MetricsRecorder`/`NoopRecorder` và `build_tracer` cho mọi role; chạy Ops server trên `health_port` (Gateway tách khỏi `http_port`, ops path trên `http_port` trả 404); không bind được Ops_Port → exit 2 kèm số cổng; flush Span khi shutdown không đổi exit code
    - `das/manager/agent.py` và Worker agent: đăng ký mọi Critical_Loop (ClaimLoop, ResultIngest, DeadLetterIngest, DeadLetterRecorder, RegistrySweeper, Heartbeat; Worker consume loop, Heartbeat) với `LoopSupervisor`, gọi `mark_shutting_down` khi shutdown
    - _Requirements: 1.2, 1.8, 8.2, 8.8, 8.12, 16.2, 16.8, 16.9, 16.11_
  - [x] 15.3 Viết unit test cho Ops server
    - `/metrics` 200/404/500; `/livez` với loop bị kill, event loop quá hạn (FakeClock), shutdown, startup; `/readyz` bật/tắt từng điều kiện, timeout 2 s, `llm_provider` không gọi completion; `/healthz` v1 không đổi; Gateway trả 404 cho ops path trên `http_port`; `NoopTracer` không mở socket
    - _Requirements: 1.2, 1.7, 1.9, 8.2, 16.2, 16.4, 16.6, 16.7, 16.8, 16.9, 16.10_

- [x] 16. Drain, undrain và CLI
  - [x] 16.1 Hiện thực drain/undrain trên Manager_Agent
    - `das/manager/claim.py`: `ClaimLoop.pause()` / `resume()`
    - `das/manager/agent.py`: `drain(release_leases)` (pause, `set_status(Draining)` trong 1 s, heartbeat `desired_status=Draining`, `in_flight=len(drivers.owned)`; `release_leases=True` gọi `request_release()` cho mọi driver, release trong 5 s), `undrain()`; handler `agent_control` trên RPC destination `agent_id`, idempotent, trả `AgentControlReplyMessage`; RetentionSweeper tiếp tục khi Draining
    - _Requirements: 17.4, 17.5, 17.8, 17.9, 17.13, 17.16_
  - [x] 16.2 Hiện thực drain/undrain trên Worker_Agent
    - `das/worker/worker.py`: `drain()` dừng poll loop trong 1 s, `acknowledge(REQUEUE)` dispatch chưa vào `JobExecutor` mà không publish Job_Result, Job đang chạy tiếp tục và heartbeat với Draining; `undrain()` mở lại poll loop; handler `agent_control` bỏ qua `release_leases`
    - _Requirements: 17.2, 17.3, 17.6, 17.8, 17.9, 17.13, 17.15, 17.16_
  - [x] 16.3 Hiện thực endpoint drain/undrain của Admin_API
    - `das/gateway/admin.py`: `POST /v1/admin/agents/{id}/drain` (body `{release_leases?: bool}`, 400 `invalid_request{parameter:"release_leases"}`) và `/undrain`; 404 khi không có trong registry; 409 `agent_state` khi Unavailable, role gateway hoặc chính Gateway; `request_reply(..., timeout=5)` → 202 hoặc 504 `agent_timeout` không ghi registry và không gửi lại; 503 khi Gateway không có RPC_Transport
    - _Requirements: 17.1, 17.8, 17.10, 17.11, 17.12, 17.15_
  - [x] 16.4 Thêm lệnh CLI `das drain` và `das undrain`
    - `das/cli.py`: subcommand nhận `<gateway_url> <agent_id>`, `--release-leases`, `--wait`, `--timeout` (1..86.400, mặc định 600); đọc `DAS_ADMIN_API_KEY` (thiếu → exit 2 nêu tên biến); 202 → exit 0, status khác → exit 1 kèm HTTP status và mã lỗi; `--wait` gọi `GET /v1/agents` mỗi 2 s bằng Clock tới `drained=true` hoặc timeout (exit 1); mọi output qua Redactor, không in khóa
    - _Requirements: 17.14, 17.17_
  - [x] 16.5 Viết unit test cho drain
    - Worker trả lại dispatch chưa bắt đầu và hoàn tất Job đang chạy; Manager dừng acquire và `release_leases` cho Manager khác acquire; idempotent; 404, 409, 504 không gửi lại, 400 `release_leases`; `drained` trong `/v1/agents`; khởi động lại thì Available; CLI exit code và `--wait` với `respx` và FakeClock
    - _Requirements: 17.2, 17.3, 17.4, 17.5, 17.7, 17.9, 17.10, 17.11, 17.12, 17.14, 17.15, 17.16, 17.17_

- [x] 17. Cập nhật import-linter
  - [x] 17.1 Mở rộng contract cấm import và thêm test kiến trúc
    - `pyproject.toml`: thêm `prometheus_client` và `opentelemetry` vào `forbidden_modules` của contract `das.manager` / `das.worker`
    - `tests/unit/test_import_contracts.py`: `lint-imports` exit 0 trên repo; exit khác 0 khi thêm tạm module vi phạm import `prometheus_client` hoặc `opentelemetry` trong `das.manager` và `das.worker` (dọn module tạm sau test)
    - _Requirements: 19.4_

- [x] 18. Compose profile `observability` và tài liệu
  - [x] 18.1 Thêm profile `observability` và cấu hình đi kèm
    - `deploy/docker-compose.yml`: service `prometheus` (`prom/prometheus:v3.15.1`), `grafana` (`grafana/grafana:13.2.4`), `jaeger` (`jaegertracing/jaeger:2.22.0`) với `profiles: [observability]`, port publish `127.0.0.1:9090`, `127.0.0.1:3000`, `127.0.0.1:16686`, không publish OTLP; agent env `DAS__TRACING__ENABLED: ${DAS__TRACING__ENABLED:-false}`, `DAS__TRACING__OTLP_ENDPOINT: http://jaeger:4317`, `DAS__TRACING__OTLP_PROTOCOL: grpc`; Gateway `health_port` 8081
    - Grafana: `GF_SECURITY_ADMIN_USER=${GRAFANA_ADMIN_USER:-}`, `GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_ADMIN_PASSWORD:-}`, `GF_AUTH_ANONYMOUS_ENABLED=false`, entrypoint kiểm tra hai biến và exit 1 nêu tên biến thiếu
    - `deploy/prometheus/prometheus.yml`: `scrape_interval: 15s`, `scrape_timeout: 10s`, job `gateway`, `manager`, `worker` với `dns_sd_configs` (type A, port 8081, `refresh_interval: 15s`; worker gồm `worker-research`, `worker-review`)
    - `deploy/grafana/provisioning/datasources/das.yaml` (Prometheus, Jaeger), `deploy/grafana/provisioning/dashboards/das.yaml`, `deploy/grafana/dashboards/das-overview.json` với 13 panel của Req 18.5
    - `deploy/rabbitmq/definitions.json`: policy `das-dlq` (`delivery-limit: -1`) cho `das.dlq`
    - `deploy/.env.example`: `GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD`, `DAS__TRACING__ENABLED`, `DAS__ADMIN_API_KEYS`
    - _Requirements: 12.11, 18.1, 18.2, 18.3, 18.5, 18.7, 18.8, 18.10, 18.11, 20.6_
  - [x] 18.2 Cập nhật README
    - `README.md`: lệnh khởi động/dừng profile `observability`; địa chỉ và cổng Prometheus, Grafana, Jaeger; biến `deploy/.env` cho Grafana và tracing; endpoint trên Ops_Port; Admin_API; CLI drain; Retention_Policy; cấu hình Price_Table (khuyến nghị giá dạng chuỗi); lưu ý chỉ dùng Replay và drain sau khi mọi agent đã nâng cấp
    - _Requirements: 18.9_
  - [x] 18.3 Viết smoke test tĩnh cho deploy
    - Parse `docker-compose.yml`: tag ghim chính xác (không `latest`, không chỉ major), port bind `127.0.0.1`, `profiles`, tập service không có profile giống v1; parse `prometheus.yml` (15 s / 10 s, `dns_sd_configs`); parse dashboard JSON: đủ 13 panel và mọi tên metric trong `expr` (bỏ hậu tố `_bucket`/`_sum`/`_count`) thuộc catalog; README có các mục của Req 18.9; repo không có Kubernetes manifest hay Helm chart
    - _Requirements: 18.1, 18.2, 18.3, 18.5, 18.6, 18.7, 18.9, 20.6_

- [x] 19. Integration và end-to-end test
  - [x] 19.1 Viết integration test với testcontainers RabbitMQ
    - `ready_count` khớp số message ready; policy `das-dlq` giữ Message sau hơn 20 lần requeue; Dead_Letter_Record được tạo trong ≤ 5 s và Message rời `das.dlq`; header `x-llm-provider`/`x-llm-model` đi qua broker
    - _Requirements: 3.1, 12.1, 12.11, 13.14_
  - [x] 19.2 Viết e2e test với Compose profile `observability`
    - `tests/integration/e2e/`: mọi target Prometheus `up` trong 120 s; `--scale worker-research=2` → replica mới `up` trong 60 s; Jaeger `/api/traces?service=das-gateway` trả đúng một trace mỗi Request với đủ Span của Req 18.12 trong 30 s; Grafana từ chối anonymous; thiếu `GRAFANA_ADMIN_PASSWORD` → container exit nêu tên biến; mixed fleet v1/v2 khi tắt tracing không dead-letter `malformed`
    - _Requirements: 18.4, 18.7, 18.10, 18.11, 18.12, 19.7_

- [x] 20. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Sub-task có dấu `*` là test tùy chọn, có thể bỏ qua để ra MVP nhanh hơn; task hiện thực không bao giờ là tùy chọn.
- Mỗi task tham chiếu tiêu chí requirement cụ thể. Mỗi trong 30 correctness property của design ứng với đúng một sub-task property test.
- Property test đặt trong `tests/property/` theo bảng file ở Testing Strategy của design, mỗi test có comment `# Feature: das-observability-operations, Property {n}: {title}`, chạy ≥ 100 example qua profile Hypothesis.
- Dependency mới được ghim chính xác như design §15; dependency bắc cầu ghim trong `uv.lock`.
- Bound thời gian, hành vi broker, Jaeger và Grafana được kiểm bằng integration/e2e test (19.1, 19.2), không bằng property test.
- Nếu khi hiện thực phát hiện requirement thiếu hoặc mâu thuẫn (ví dụ `attempt > attempt count` khi Replay, identity của malformed bytes trùng nhau), quay lại requirements trước khi đổi design.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["2.1", "2.4", "3.1", "3.3", "3.4", "3.6", "4.1", "4.2"] },
    { "id": 2, "tasks": ["2.2", "2.3", "2.5", "2.6", "3.2", "3.5", "4.3", "5.1"] },
    { "id": 3, "tasks": ["3.7", "4.4", "4.5", "4.6", "5.2", "5.3", "6.1", "6.4", "6.7", "9.1"] },
    { "id": 4, "tasks": ["6.2", "6.3", "6.5", "6.6", "6.8", "6.9", "7.1", "9.2", "9.5", "10.1"] },
    { "id": 5, "tasks": ["7.2", "9.3", "10.2"] },
    { "id": 6, "tasks": ["7.3", "9.4", "9.6", "10.3", "11.1", "12.1", "13.1"] },
    { "id": 7, "tasks": ["9.7", "9.8", "10.4", "11.2", "11.3", "12.2", "13.2", "13.4"] },
    { "id": 8, "tasks": ["11.4", "11.5", "11.6", "11.7", "11.8", "11.9", "11.10", "11.11", "11.12", "13.3", "13.5", "13.6"] },
    { "id": 9, "tasks": ["13.7", "13.8", "13.9", "15.1"] },
    { "id": 10, "tasks": ["15.2", "15.3"] },
    { "id": 11, "tasks": ["16.1", "16.2", "16.3", "16.4"] },
    { "id": 12, "tasks": ["16.5", "17.1", "18.1"] },
    { "id": 13, "tasks": ["18.2", "18.3"] },
    { "id": 14, "tasks": ["19.1", "19.2"] }
  ]
}
```
