# Baseline Phase 3 (Requirement 11.6, 11.7, 11.8)

Chụp trên code trước mọi sửa đổi của Phase 3 trong `das/`, `tests/` và `pyproject.toml`. Lúc chụp, task 1.1 đã thêm `tests/fixtures/phase3_baseline/` (`__init__.py`, `capture.py`, `codec_corpus.jsonl`, `contract.json`). Các file này không có tên `test_*.py`, nên pytest không thu thập chúng; số đếm bên dưới không bao gồm test nào từ thư mục đó (đã kiểm: số thu thập của Fast_Test_Suite là 1645 cả trước và sau khi các file đó xuất hiện). Chưa có test mới nào của Phase 3.

## Môi trường chạy

| Mục | Giá trị |
|---|---|
| Thời điểm | 2026-10-28, chiều (+07); các suite chạy lần lượt, không song song với nhau |
| Máy | MacBook Apple M3, 8 core, 16 GiB RAM |
| OS | macOS 27.0.1 (Darwin arm64) |
| Python | 3.12.15 (qua `uv run`) |
| uv | 0.8.4 (bản cài trên máy local; chưa phải bản ghim 0.13.6 của Phase 3) |
| Hypothesis profile | `default` (không đặt `HYPOTHESIS_PROFILE`) |
| Docker | Có: Docker client/server 29.9.2, daemon chạy được |

## Số test theo suite

Số thu thập lấy bằng `--collect-only -q`. Số skip/xfail lấy từ một lần chạy đầy đủ với `-q -rsxX`.

| Suite | Lệnh | Thu thập | Pass | Fail | Skip | Xfail/Xpass | Thời gian |
|---|---|---|---|---|---|---|---|
| Fast_Test_Suite | `uv run pytest tests/unit tests/property` | 1645 (unit 1547, property 98) | 1645 | 0 | 0 | 0 / 0 | 148 s |
| Integration_Test_Suite | `uv run pytest tests/integration --ignore=tests/integration/e2e` | 10 | 9 | 1 | 0 | 0 / 0 | 50 s (pytest), cộng thời gian khởi động container |
| E2E_Test_Suite | `uv run pytest tests/integration/e2e` | 14 | 14 | 0 | 0 | 0 / 0 | 202 s |

Ngưỡng cho Req 11.7 (so ở task 13.1): số thu thập mỗi suite không nhỏ hơn 1645 / 10 / 14; số skip+xfail mỗi suite không lớn hơn 0 / 0 / 0.

Ghi chú về lần chạy Integration_Test_Suite: test fail là `tests/integration/test_rabbitmq_broker.py::test_reconnect_restores_subscriptions` (Flaky_Test của Requirement 10), với `AssertionError: timed out waiting for delivery after reconnect` ở `tests/integration/test_rabbitmq_broker.py:411`, sau khi `worker.connected` đã là True và `len(worker.subscriptions) == 3` đã đúng. Log có `WARNING das.transport.rabbitmq: rabbitmq connection lost; reconnecting`. Dấu hiệu này khớp giả thuyết G3 trong design (mục 9). Fail này là hành vi baseline; nó không phải skip hay xfail nên không ảnh hưởng ngưỡng skip/xfail.

Danh sách test thu thập của Integration_Test_Suite:

```
tests/integration/test_grpc_two_process.py::test_request_reply_between_two_processes
tests/integration/test_rabbitmq_broker.py::test_capability_queues_route_by_capability
tests/integration/test_rabbitmq_broker.py::test_unacked_dispatch_redelivered_after_connection_drop
tests/integration/test_rabbitmq_broker.py::test_delivery_limit_dead_letters_after_max_attempts[1]
tests/integration/test_rabbitmq_broker.py::test_delivery_limit_dead_letters_after_max_attempts[3]
tests/integration/test_rabbitmq_broker.py::test_reconnect_restores_subscriptions
tests/integration/test_rabbitmq_operations.py::test_ready_count_matches_ready_messages
tests/integration/test_rabbitmq_operations.py::test_dlq_policy_keeps_message_past_twenty_requeues
tests/integration/test_rabbitmq_operations.py::test_broker_dead_letter_recorded_within_five_seconds
tests/integration/test_rabbitmq_operations.py::test_llm_headers_cross_the_broker
```

Danh sách test thu thập của E2E_Test_Suite:

```
tests/integration/e2e/test_compose_e2e.py::test_stack_ready_within_120s
tests/integration/e2e/test_compose_e2e.py::test_prompt_to_final_result
tests/integration/e2e/test_compose_e2e.py::test_cancel_running_request_within_2s
tests/integration/e2e/test_compose_e2e.py::test_manager_kill_takeover_within_10s
tests/integration/e2e/test_compose_e2e.py::test_scale_up_and_down
tests/integration/e2e/test_compose_e2e.py::test_dependency_unreachable_exits_nonzero[rabbitmq-override0]
tests/integration/e2e/test_compose_e2e.py::test_dependency_unreachable_exits_nonzero[redis-override1]
tests/integration/e2e/test_observability_e2e.py::test_all_prometheus_targets_up_within_120s
tests/integration/e2e/test_observability_e2e.py::test_grafana_rejects_anonymous
tests/integration/e2e/test_observability_e2e.py::test_grafana_refuses_start_without_admin_password[absent]
tests/integration/e2e/test_observability_e2e.py::test_grafana_refuses_start_without_admin_password[empty]
tests/integration/e2e/test_observability_e2e.py::test_jaeger_one_trace_per_request
tests/integration/e2e/test_observability_e2e.py::test_scaled_worker_scraped_within_60s
tests/integration/e2e/test_observability_e2e.py::test_mixed_fleet_v1_peer_tracing_disabled
```

## Property test trong `tests/property/` (Req 11.6)

`tests/property/` có 69 file `test_*.py`, 96 test (hàm hoặc `TestCase` stateful) và 98 item được thu thập: `test_config_validator_phase2.py::test_unmutated_config_is_accepted` tham số hóa thành 3 item `[0]`, `[1]`, `[200]`; mọi test khác là 1 item. Giá trị `max_examples` lấy theo cách sau:

- Số ghi trong cột `max_examples` là giá trị tường minh trong `settings(...)` đặt trên test đó, kèm số dòng của lời gọi `settings(` trong file.
- `profile (100)`: test không đặt `max_examples` (không có `@settings`, hoặc `@settings` không có `max_examples`, hoặc `TestCase.settings` không có `max_examples`), nên dùng profile Hypothesis đang nạp. Profile được đăng ký ở `tests/conftest.py`: `default` có `max_examples=100` (dòng 26), `ci` có `max_examples=200` (dòng 31); chọn bằng `HYPOTHESIS_PROFILE`, mặc định `default`.
- `—`: test không dùng Hypothesis (test ví dụ thường đặt trong file property).

Có 40 chỗ đặt `max_examples` tường minh trong `tests/property/*.py`. (Design ghi "77 chỗ đặt"; con số đó có vẻ đếm cả kết quả grep trong `__pycache__/*.pyc`. Đếm trên mã nguồn: 40 trong `tests/property/`, thêm 2 trong `tests/conftest.py`, 7 trong `tests/unit/` và 1 trong `tests/fixtures/phase3_baseline/capture.py` của task 1.1, tổng 50.)

| File | Test (node ID sau `::`) | `max_examples` | Chỗ đặt |
|---|---|---|---|
| test_admin_api_authorization.py | test_admin_api_authorization_and_audit | profile (100) | |
| test_admin_api_authorization.py | test_classify_key_compares_every_key | profile (100) | |
| test_agent_listing.py | test_agent_listing_completeness | profile (100) | |
| test_attempt_progression.py | test_attempt_progression_retry_and_dead_lettering | 200 | dòng 166 |
| test_backoff_schedule.py | test_backoff_formula | profile (100) | |
| test_backoff_schedule.py | test_machine_schedules_retry_at_backoff | profile (100) | |
| test_capability_wait.py | test_capability_wait_timeout | 300 | dòng 83 |
| test_codec_bytes_transport_v1.py | test_encoding_without_trace_context_is_v1_identical | profile (100) | |
| test_codec_bytes_transport_v1.py | test_noop_tracer_messages_have_no_trace_context | profile (100) | |
| test_codec_bytes_transport_v1.py | test_wire_bytes_equal_encode_on_every_transport | profile (100) | |
| test_codec_lenient_trace_context.py | test_decode_is_lenient_about_trace_context | profile (100) | |
| test_codec_lenient_trace_context.py | test_unsupported_schema_version_is_rejected | profile (100) | |
| test_codec_malformed.py | test_decode_rejects_malformed_input_atomically | profile (100) | |
| test_codec_roundtrip.py | test_round_trip_and_canonical_encoding | profile (100) | |
| test_codec_size_limit.py | test_decode_rejects_oversized_input | profile (100) | |
| test_codec_size_limit.py | test_encode_respects_limit | profile (100) | |
| test_config_precedence.py | test_resolve_prefers_env_then_file_then_absent | profile (100) | |
| test_config_precedence.py | test_validated_config_uses_env_then_file_then_default | profile (100) | |
| test_config_secrets.py | test_credentials_in_config_file_are_never_echoed | profile (100) | |
| test_config_validation.py | test_base_config_is_valid | — | |
| test_config_validation.py | test_config_validation_reports_every_bad_field | profile (100) | |
| test_config_validator_phase2.py | test_unmutated_config_is_accepted[0], [1], [200] | — | |
| test_config_validator_phase2.py | test_validator_reports_exactly_the_mutated_new_fields | 200 | dòng 309 |
| test_correlation_propagation.py | test_request_messages_and_logs_carry_request_id | 100 | dòng 304 |
| test_correlation_propagation.py | test_worker_handlers_use_inbound_correlation_id | 100 | dòng 408 |
| test_cost_additivity.py | test_estimated_cost_additivity_and_format | profile (100) | |
| test_cost_monotonicity.py | test_adding_a_usage_record_never_decreases_estimated_cost | 200 | dòng 20 |
| test_dead_letter_record_once.py | test_each_dead_letter_has_exactly_one_record | 100 | dòng 208 |
| test_dispatch_readiness.py | test_dispatch_readiness_and_dependency_inputs | 200 | dòng 130 |
| test_dlq_pagination.py | test_dlq_pagination_is_complete | 100 | dòng 203 |
| test_envelope_trace_roundtrip.py | test_envelope_roundtrip_with_trace_context | 300 | dòng 78 |
| test_failure_propagation.py | test_failure_propagation_skips_exactly_the_transitive_dependents | 300 | dòng 175 |
| test_final_result.py | test_final_result_completeness | profile (100) | |
| test_gateway_query_shape.py | test_query_response_shape | profile (100) | |
| test_gateway_rejections.py | test_rejected_calls_are_non_mutating | profile (100) | |
| test_job_transition_count.py | TestJobTransitionCountedAtMostOnce::runTest (stateful) | 200 | dòng 334, `JobTransitionMachine.TestCase.settings` (`stateful_step_count=80`) |
| test_lease_fencing.py | TestLeaseMutualExclusionAndFencing::runTest (stateful) | profile (100) | dòng 214, `LeaseMachine.TestCase.settings` chỉ đặt `stateful_step_count=30` |
| test_llm_metrics_consistency.py | test_llm_metrics_consistency | 200 | dòng 232 |
| test_llm_retry.py | test_llm_retry_policy | 200 | dòng 61 |
| test_log_format.py | test_log_entry_format_and_context | profile (100) | |
| test_log_redaction.py | test_new_secrets_are_redacted_in_every_output | profile (100) | |
| test_log_redaction.py | test_rendered_log_lines_contain_no_secret | profile (100) | |
| test_log_trace_correlation.py | test_log_entries_correlate_with_active_sampled_span | 150 | dòng 152 |
| test_log_trace_correlation.py | test_noop_tracer_logs_never_carry_trace_ids | 50 | dòng 187 |
| test_machine_cancelled.py | test_cancelled_request_resume_is_inert | 200 | dòng 199 |
| test_machine_cancelled.py | test_cancelled_request_step_is_inert | 300 | dòng 180 |
| test_machine_replay.py | test_replay_applied_or_reverted | 500 | dòng 251 |
| test_malformed_rejection.py | test_result_ingest_rejects_malformed_without_state_change | profile (100) | dòng 314, `@settings(deadline=None)` |
| test_malformed_rejection.py | test_worker_rejects_malformed_dispatch_without_side_effects | profile (100) | dòng 239, `@settings(deadline=None)` |
| test_manager_resume.py | test_resume_redispatches_exactly_unfinished_in_flight_jobs | 200 | dòng 168 |
| test_metrics_cardinality.py | test_cardinality_bound_and_catalog_conformance | 150 | dòng 144 |
| test_metrics_no_observation_loss.py | test_no_observation_lost_on_overflow | 150 | dòng 102 |
| test_page_cursor_rejection.py | test_decode_rejects_every_invalid_cursor | profile (100) | |
| test_page_cursor_rejection.py | test_endpoints_reject_invalid_cursor | 100 | dòng 277 |
| test_page_cursor_roundtrip.py | test_page_cursor_round_trip | profile (100) | |
| test_plan_roundtrip.py | test_job_plan_round_trip | profile (100) | |
| test_plan_roundtrip.py | test_schema_validator_rejects_violations | — | |
| test_plan_validator.py | test_validator_matches_oracle | profile (100) | |
| test_planner_attempts.py | test_bounded_planning_attempts | 150 | dòng 58 |
| test_prompt_validation.py | test_every_whitespace_character_alone_is_empty | profile (100) | |
| test_prompt_validation.py | test_validate_prompt_matches_specification | profile (100) | |
| test_prop_retention_liveness.py | test_retention_liveness_and_exactly_once_count | profile (100) | |
| test_publish_retry_bound.py | test_publish_retry_bound | 300 | dòng 168 |
| test_registry_availability.py | TestRegistryAvailability::runTest (stateful) | 100 | dòng 166, `RegistryAvailabilityMachine.TestCase.settings` (`stateful_step_count=30`) |
| test_replay_discard_check_order.py | test_discard_check_order_matches_decide_discard | profile (100) | |
| test_replay_discard_check_order.py | test_replay_check_order_matches_decide_replay | profile (100) | |
| test_replay_discard_exclusivity.py | test_replay_discard_exclusive | profile (100) | |
| test_request_index_consistency.py | test_listing_filters_and_request_index_consistent | profile (100) | |
| test_request_list_pagination.py | test_request_list_pagination_is_complete | 100 | dòng 256 |
| test_result_idempotency.py | test_result_handling_is_idempotent | 300 | dòng 141 |
| test_retention_safety.py | test_retention_safety::runTest (stateful) | profile (100) | dòng 520, `RetentionSafety.TestCase.settings` chỉ đặt `stateful_step_count=50` |
| test_routing_capability.py | test_empty_selection_despite_capable_worker | — | |
| test_routing_capability.py | test_partial_worker_excludes_only_its_queue | — | |
| test_routing_capability.py | test_routing_capability_selection_is_sound | 300 | dòng 43 |
| test_routing_capability.py | test_unavailable_partial_worker_is_ignored | — | |
| test_span_attribute_allowlist.py | test_finished_spans_only_carry_allowlisted_secret_free_attributes | 200 | dòng 85 |
| test_span_export_accounting.py | test_span_export_accounting | profile (100) | |
| test_stale_result_after_replay.py | test_stale_results_discarded_after_replay | 150 | dòng 193 |
| test_state_cancel.py | test_cancel_unknown_request_is_not_found | — | |
| test_state_cancel.py | test_cancellation_semantics | profile (100) | |
| test_submission_uniqueness.py | test_submission_creates_unique_pending_requests | profile (100) | |
| test_summarization_trigger.py | test_summarization_trigger_and_inputs | 300 | dòng 178 |
| test_transition_log.py | TestTransitionLog::runTest (stateful) | profile (100) | dòng 186, `TransitionLogMachine.TestCase.settings` chỉ đặt `stateful_step_count=20` |
| test_transport_loader_errors.py | test_complete_adapter_loads | profile (100) | |
| test_transport_loader_errors.py | test_incomplete_adapter_names_exactly_missing_operations | profile (100) | |
| test_transport_loader_errors.py | test_unregistered_name_raises_not_installed_naming_it | profile (100) | |
| test_transport_reconnect.py | test_reconnect_policy_schedule | 200 | dòng 43 |
| test_transport_reconnect.py | test_reconnect_restores_every_subscription_before_delivery | 100 | dòng 80 |
| test_usage_idempotent.py | test_usage_accounting_is_idempotent | 150 | dòng 296 |
| test_usage_normalization.py | test_missing_total_is_sum_without_problems | 200 | dòng 69 |
| test_usage_normalization.py | test_normalize_usage | 300 | dòng 38 |
| test_usage_summary.py | test_empty_records_give_all_zero_summary | — | |
| test_usage_summary.py | test_token_usage_summary_invariants | profile (100) | |
| test_worker_concurrency.py | test_worker_never_exceeds_concurrency_limit | 100 | dòng 97 |
| test_worker_execution.py | test_worker_execution_contract | 100 | dòng 154 |
| test_worker_subscriptions.py | test_worker_subscriptions_match_capabilities | 100 | dòng 78 |

Số dòng là vị trí của lời gọi `settings(` ở baseline; Phase 3 có thể làm lệch số dòng (ví dụ khi Ruff sắp lại import). Khi so ở task 13.1, khớp theo file và tên test, không theo số dòng.

## Mục `[tool.importlinter]` của `pyproject.toml` ở baseline (Req 11.8)

Bản sao nguyên văn (kể cả comment ngay phía trên, dòng 67–87 của `pyproject.toml`):

```toml
# Requirement 15.4: Manager and Worker logic talk to the network only through the
# Transport interface and MessageCodec, never directly through broker/RPC/Redis clients.
# Indirect imports (e.g. via das.state or das.transport adapters) are allowed.
[tool.importlinter]
root_packages = ["das"]
include_external_packages = true

[[tool.importlinter.contracts]]
name = "Manager and Worker logic do not import protocol client libraries"
type = "forbidden"
source_modules = ["das.manager", "das.worker"]
forbidden_modules = [
    "aio_pika",
    "aiormq",
    "pamqp",
    "grpc",
    "redis",
    "prometheus_client",
    "opentelemetry",
]
allow_indirect_imports = true
```

Baseline có đúng một contract (loại `forbidden`), không có `ignore_imports` hay ngoại lệ nào khác.

## Sau Phase 3

Kiểm ở task 13.1 (Property 2; Req 4.6, 10.7, 11.4, 11.6, 11.7, 11.8), sau khi mọi sửa đổi của Phase 3 đã xong.

### Môi trường chạy

| Mục | Giá trị |
|---|---|
| Thời điểm | 2026-10-29, 13:36–13:46 (+07); ba pytest suite chạy lần lượt, không song song với nhau; `lint-imports` chạy trong lúc Fast_Test_Suite đang chạy (chỉ đọc mã nguồn) |
| Máy | MacBook Apple M3, 8 core, 16 GiB RAM (cùng máy với baseline) |
| OS | macOS 27.0.1 (Darwin arm64, build 26A434) |
| Python / pytest | 3.12.15 / 9.1.1 (qua `uv run`) |
| uv | 0.8.4 (bản cài trên máy local) |
| Hypothesis profile | `default` (không đặt `HYPOTHESIS_PROFILE`) |
| Docker | Docker client/server 29.9.2, Docker Compose v5.6.1 |
| `DAS_REQUIRE_DOCKER` | không đặt (như baseline) |

### Số test theo suite (Req 11.7)

Cùng cách đếm với baseline: số thu thập lấy bằng `--collect-only -q`, số pass/skip/xfail lấy từ một lần chạy đầy đủ với `-q -rsxX` (thêm `-p no:cacheprovider` để không ghi `.pytest_cache`; cờ này không đổi tập test).

| Suite | Thu thập (baseline → sau) | Pass | Fail | Skip (baseline → sau) | Xfail/Xpass (baseline → sau) | Exit | Thời gian |
|---|---|---|---|---|---|---|---|
| Fast_Test_Suite | 1645 → 1978 (unit 1547 → 1579, property 98 → 399) | 1978 | 0 | 0 → 0 | 0/0 → 0/0 | 0 | 177 s |
| Integration_Test_Suite | 10 → 10 | 10 | 0 | 0 → 0 | 0/0 → 0/0 | 0 | 41 s (pytest) |
| E2E_Test_Suite | 14 → 14 | 14 | 0 | 0 → 0 | 0/0 → 0/0 | 0 | 230 s |
| Import_Contract_Check | `uv run lint-imports`: 1 contract | 1 kept, 0 broken | | | | 0 | |

Kết luận: số thu thập mỗi suite không nhỏ hơn ngưỡng 1645 / 10 / 14, số skip+xfail mỗi suite không lớn hơn 0 / 0 / 0, và mọi suite kết thúc với exit code 0.

- Fast_Test_Suite tăng 333 item, tất cả là test mới của Phase 3. Ở `tests/property/` chỉ có một file mới, `test_codec_baseline_bytes.py` (301 item, tham số hóa theo corpus codec của baseline, không dùng Hypothesis nên không có `max_examples`). Ở `tests/unit/` có 32 item mới (ví dụ `test_phase3_baseline.py` 6 item, `test_require_docker_hook.py`, `test_dlq_lost_reply.py`).
- Integration_Test_Suite: danh sách 10 test thu thập trùng khớp danh sách baseline ở trên. Flaky_Test `test_reconnect_restores_subscriptions` (fail ở baseline) pass ở lần chạy này.
- E2E_Test_Suite: danh sách 14 test thu thập trùng khớp danh sách baseline ở trên. Sau lần chạy không còn container nào của project `das-e2e`.
- Fast_Test_Suite có 3 cảnh báo `PytestDeprecationWarning` (`asyncio_default_fixture_loop_scope` chưa đặt) phát ra từ pytest con mà `tests/unit/test_require_docker_hook.py` chạy qua `pytester`; đây là cảnh báo, không phải skip hay xfail.

Test baseline của Phase 3 chạy riêng: `uv run pytest tests/unit/test_phase3_baseline.py tests/property/test_codec_baseline_bytes.py tests/unit/test_deploy_static.py::test_delivery_limit_matches_manager_max_attempts` cho 308 passed (6 + 301 + 1).

### Property test của Phase 1 và Phase 2 (Req 11.6)

So khớp theo file và node ID với bảng ở mục "Property test trong `tests/property/`" (không theo số dòng):

- Cả 98 item baseline (96 test, 69 file) vẫn được thu thập với cùng node ID, kể cả 3 item `test_unmutated_config_is_accepted[0]`, `[1]`, `[200]` và 6 test stateful `...::runTest`. Không item baseline nào bị thiếu hay đổi tên.
- Cả 98 item pass trong lần chạy Fast_Test_Suite ở trên (0 fail, 0 skip, 0 xfail).
- `tests/property/` hiện có 70 file `test_*.py`: 69 file baseline cộng `test_codec_baseline_bytes.py`.

`max_examples` (`grep -n max_examples tests/property/*.py tests/conftest.py`):

- `tests/property/` vẫn có đúng 40 chỗ đặt tường minh, đều ở các test baseline, và mỗi giá trị bằng đúng giá trị trong bảng baseline (không chỗ nào giảm, không chỗ nào bị xóa). Số dòng chỉ lệch ở các chỗ `settings(` viết nhiều dòng, vì grep trả dòng chứa `max_examples` thay vì dòng `settings(`: `test_dead_letter_record_once.py` 209, `test_job_transition_count.py` 335, `test_registry_availability.py` 167, `test_stale_result_after_replay.py` 194, `test_usage_idempotent.py` 297.
- Các test ghi `profile (100)` ở baseline vẫn không đặt `max_examples`, nên vẫn dùng profile. `tests/conftest.py` vẫn đăng ký `default` với `max_examples=100` (nay ở dòng 28) và `ci` với `max_examples=200` (nay ở dòng 33); số dòng lệch 2 so với baseline do Phase 3 sửa phần đầu `tests/conftest.py` (hook `DAS_REQUIRE_DOCKER`), giá trị không đổi. Profile mặc định vẫn là `default`.
- `test_codec_baseline_bytes.py` không có `settings` hay `max_examples`.

### Mục `[tool.importlinter]` (Req 11.8)

Trích mục hiện tại của `pyproject.toml` (comment ngay phía trên cộng `[tool.importlinter]` và `[[tool.importlinter.contracts]]`, nay ở dòng 104–124 thay vì 67–87, vì Phase 3 thêm `[tool.ruff]`, `[tool.mypy]` phía trên) và diff với bản sao nguyên văn ở mục baseline: **giống hệt**, diff rỗng. `pyproject.toml` có đúng hai header chứa `importlinter` (`[tool.importlinter]` và một `[[tool.importlinter.contracts]]`), nên vẫn chỉ có một contract: cùng tên, loại `forbidden`, cùng `source_modules`, cùng 7 `forbidden_modules`, `allow_indirect_imports = true`, không có `ignore_imports` hay ngoại lệ mới.

`uv run lint-imports`: "Analyzed 153 files, 722 dependencies", contract "Manager and Worker logic do not import protocol client libraries" KEPT, "Contracts: 1 kept, 0 broken.", exit 0.

### Kiểm âm tính `delivery-limit` (Req 4.6)

Sao lưu `deploy/rabbitmq/definitions.json` ra `/tmp`, sửa tạm, chạy `uv run pytest tests/unit/test_deploy_static.py::test_delivery_limit_matches_manager_max_attempts`:

| Sửa tạm | Kết quả |
|---|---|
| Policy `das-jobs`: `delivery-limit` 2 → 5 | 1 failed: `assert 5 == (3 - 1)` ở `tests/unit/test_deploy_static.py:119` |
| Xóa hẳn policy `das-jobs` | 1 failed: `StopIteration` (test không tìm thấy policy) |
| Khôi phục từ bản sao lưu | `cmp` với bản sao lưu: giống hệt từng byte; test pass |

Test không bị skip hay xfail, và nằm trong Fast_Test_Suite, nên lệch `delivery-limit` làm Fast_Test_Suite (và Fast_Job) fail.

Ghi chú: lần sửa tạm đầu tiên được hoàn tác sau khoảng một phút vì Integration_Test_Suite đang chạy song song và `test_rabbitmq_operations.py` nạp file này lúc setup fixture. File đã được khôi phục (kiểm bằng `cmp`) trước khi module đó bắt đầu (lúc đó mới xong 6/10 item, thuộc `test_grpc_two_process.py` và `test_rabbitmq_broker.py`, không đọc file này). Kiểm âm tính được làm lại sau khi Integration_Test_Suite kết thúc; kết quả trong bảng là của lần làm lại.
