# Implementation Plan: DAS Hardening và CI (Phase 3)

## Overview

Hiện thực Phase 3 bằng Python 3.12 trên nền DAS Phase 1 và Phase 2, không thêm hành vi runtime. Thứ tự: chụp baseline cho Requirement 11 trước mọi sửa đổi trong `das/`; thêm tooling (ruff, mypy, stub, pytest-repeat) và tạo lại `uv.lock` bằng uv 0.13.6; sửa lint rồi sửa lỗi kiểu theo chính sách ưu tiên của design; hook `DAS_REQUIRE_DOCKER` và `DAS_E2E_LOG_DIR`; test mất reply cho DLQ; điều tra và sửa Flaky_Test; workflow `.github/workflows/ci.yml`; README và test tĩnh đối chiếu workflow với README; sửa spec Phase 1/Phase 2 và ghi Decision_Record; cuối cùng đối chiếu với baseline. Property 1 (bytes của Message_Codec) và Property 2 (mọi property cũ vẫn đúng) là bắt buộc. Mọi sửa đổi trong `das/` chỉ thuộc bốn loại ở Req 11.4; sửa nào đổi hành vi quan sát được thì ghi theo Req 11.5 ngay trong mô tả task.

## Tasks

- [x] 1. Chụp baseline cho Requirement 11 (trước mọi sửa đổi trong `das/`)
  - [x] 1.1 Viết script chụp baseline và sinh file baseline
    - `tests/fixtures/phase3_baseline/__init__.py` và `capture.py` (chạy bằng `uv run python -m tests.fixtures.phase3_baseline.capture`)
    - `codec_corpus.jsonl`: ít nhất 300 Message sinh bằng strategy trong `tests/strategies.py` với seed cố định, phủ mọi message type, có và không có Trace_Context (Envelope_V2 và v1); mỗi dòng gồm `type`, `message` (JSON của model), `bytes_b64` do `MessageCodec().encode` tạo
    - `contract.json`: `metrics` (tên, type, label từ catalog `das.observability.metrics`), `config_schema` (`AgentConfig.model_json_schema()`), `env_vars` (`DAS__...` mà loader chấp nhận), `routes` (method, path của Gateway gồm Admin_API và Ops server), `exit_codes` (từ `das/errors.py`)
    - Ghi file theo thứ tự xác định (sort key) để diff đọc được; chạy script một lần trên code baseline và commit kết quả
    - _Requirements: 11.1, 11.2, 11.3_

  - [x] 1.2 Ghi `.kiro/specs/das-hardening-ci/baseline.md`
    - Số test thu thập (`--collect-only -q`) và số skip/xfail (một lần chạy đầy đủ) của Fast_Test_Suite, Integration_Test_Suite, E2E_Test_Suite; ghi rõ máy chạy và có Docker hay không
    - Danh sách file và tên property test trong `tests/property/` cùng giá trị `max_examples` theo từng chỗ đặt
    - Bản sao nguyên văn mục `[tool.importlinter]` của `pyproject.toml` ở baseline
    - Chạy trước khi thêm bất kỳ test mới nào của Phase 3 để số đếm phản ánh đúng baseline
    - _Requirements: 11.6, 11.7, 11.8_

  - [x] 1.3 Viết test so contract với baseline
    - `tests/unit/test_phase3_baseline.py`: so metric catalog, `AgentConfig.model_json_schema()`, biến `DAS__`, route HTTP của Gateway/Admin_API/Ops server và exit code hiện tại với `contract.json`; in khác biệt dạng diff khi fail
    - _Requirements: 11.1, 11.3_

  - [x] 1.4 Viết property test cho bytes của Message_Codec so với baseline
    - **Property 1: Bytes của Message_Codec giữ nguyên so với baseline**
    - `tests/property/test_codec_baseline_bytes.py`, comment tag `# Feature: das-hardening-ci, Property 1: Bytes của Message_Codec giữ nguyên so với baseline`
    - Duyệt mọi dòng của `codec_corpus.jsonl`: dựng lại Message bằng `TypeAdapter(TypedMessage).validate_python(...)`, so `encode` với bytes baseline từng byte, so `decode(bytes baseline)` với Message đã ghi
    - **Validates: Requirements 11.2**

- [x] 2. Thêm tooling và cấu hình
  - [x] 2.1 Ghim dependency mới và tạo lại `uv.lock`
    - `pyproject.toml` group `dev`: `ruff==0.17.3`, `mypy==2.4.0`, `types-PyYAML==6.0.12.20260906`, `types-grpcio==1.84.0.20261021`, `types-protobuf==7.35.1.20260906`, `types-aioboto3==15.5.0`, `botocore-stubs==<bản 1.40.x khớp botocore 1.40.61>`, `pytest-repeat==0.9.4`; kiểm lại phiên bản trên PyPI trước khi ghim
    - Tạo lại `uv.lock` bằng đúng uv `0.13.6`; xác nhận `uv sync --locked` thành công và `uv.lock` ghim `types-aiobotocore`, `types-s3transfer`
    - Kiểm tương thích `pytest-repeat` với pytest 9.1.1 bằng `--count 2` trên một unit test; nếu không tương thích, dừng và áp phương án `--das-repeat N` của design (ID `[k-N]`), ghi lại lựa chọn trong task
    - `.gitignore`: thêm `.mypy_cache/`, `.ruff_cache/`
    - _Requirements: 5.1, 6.1, 10.4, 10.8_

  - [x] 2.2 Cấu hình Ruff
    - `[tool.ruff]`: `target-version = "py312"`, `line-length = 120`, `extend-exclude = ["das/transport/proto"]`, `force-exclude = true`
    - `[tool.ruff.lint]`: `select = ["E", "F"]`; không có `ignore`, không có `per-file-ignores`, không bật `fix`
    - _Requirements: 5.2, 5.3, 5.6_

  - [x] 2.3 Cấu hình Mypy
    - `[tool.mypy]`: `python_version = "3.12"`, `files = ["das"]`, `plugins = ["pydantic.mypy"]`, `warn_unused_ignores = true`, `enable_error_code = ["ignore-without-code"]`; không `strict`, không `ignore_missing_imports` toàn cục, không `follow_imports` toàn cục
    - `[[tool.mypy.overrides]]` cho `das.transport.proto`, `das.transport.proto.*` với `ignore_errors = true`
    - Chỉ thêm override `ignore_missing_imports` cho riêng module bên thứ ba nếu có thư viện thiếu cả type hint lẫn stub; ghi tên module vào task
    - Nếu plugin `pydantic.mypy` làm tăng lỗi sai khó giải thích, bỏ plugin và ghi lý do
    - _Requirements: 6.2, 6.3, 6.4, 6.6, 6.8_

- [x] 3. Sửa code cho tới khi Lint_Check pass
  - [x] 3.1 Sửa vi phạm Ruff
    - Sửa 4 dòng E501 trong `tests/` (`tests/integration/e2e/test_compose_e2e.py`, `tests/integration/e2e/test_observability_e2e.py`, `test_metrics_cardinality.py`, `test_manager_summarizer.py`) bằng xuống dòng hoặc tách chuỗi, không đổi giá trị chuỗi
    - Sửa mọi vi phạm khác Ruff báo; trong `das/` chỉ dùng các loại sửa ở Req 11.4
    - Xác nhận không còn `# noqa` không kèm mã rule ngoài Generated_Code
    - `uv run ruff check` exit 0, không báo vi phạm, không sửa file
    - _Requirements: 5.4, 5.5, 11.4_

  - [x] 3.2 Viết test cho cấu hình lint
    - `tests/unit/test_lint_config.py`: gọi ruff qua subprocess với `--stdin-filename`, không ghi file vào repo
    - `das/transport/proto/_probe.py` có `import os` không dùng → exit 0, không output vi phạm
    - `das/_probe.py` có tên chưa định nghĩa → exit khác 0, output có `das/_probe.py:<dòng>:` và `F821`
    - `das/_probe.py` có lỗi cú pháp → exit khác 0, output có đường dẫn và số dòng
    - _Requirements: 5.3, 5.7, 5.8_

- [x] 4. Sửa code cho tới khi Type_Check pass
  - [x] 4.1 Sửa lỗi kiểu bằng annotation và xóa ignore thừa
    - Literal trạng thái agent trong `das/.../registry.py`, `das/worker...`, `das/manager/agent.py`; `dict[str, Any]` cho kwargs constructor `prometheus_client` trong `das/observability/metrics.py`; annotation cho 3 lambda không suy ra được kiểu
    - Xóa 3 `# type: ignore` không còn cần trong `das/gateway/admin.py`, `das/manager/driver.py`
    - Theo thứ tự ưu tiên của design: annotation → thu hẹp kiểu → xóa ignore thừa → `# type: ignore[<mã>]` kèm comment lý do
    - Ghi danh sách file và loại sửa vào mô tả task
    - _Requirements: 6.5, 6.6, 11.4_

  - [x] 4.2 Sửa lỗi kiểu bằng thu hẹp kiểu và sửa codec
    - Thu hẹp `Optional` trong `das/cli.py`, `das/manager/driver.py` bằng `is None`, `isinstance` hoặc `assert` theo luồng hiện tại
    - Lỗi keyword `payload` của `_EnvelopeFields` trong `das/codec/envelope.py`, `das/codec/message_codec.py`: sửa annotation hoặc cách gọi mà không đổi output; chạy `tests/property/test_codec_baseline_bytes.py` và mọi property test codec ngay sau khi sửa
    - Overload `Session.client` của `types-aioboto3` trong `das/llm/adapters/bedrock.py`: `# type: ignore[<mã>]` kèm comment nếu stub sai
    - _Requirements: 6.5, 11.2, 11.4_

  - [x] 4.3 Xử lý hai lỗi kiểu có thể là lỗi thật
    - `das/manager/planner.py`: gọi `CompletionClient.complete(operation=...)` mà protocol không khai báo `operation`; đọc protocol và mọi adapter để xác định đây là thiếu khai báo protocol hay lỗi runtime
    - `das/llm/adapters/bedrock.py`: `await` một giá trị có thể là `None`; xác định luồng nào tạo ra `None`
    - Nếu cách sửa đổi hành vi quan sát được, ghi vào task: đường dẫn, số dòng, mã lỗi Mypy, hành vi trước và sau (Req 11.5)
    - Nếu cách sửa làm đổi route, status code, mã lỗi, body, bytes codec, metric, config, biến `DAS__` hoặc exit code: dừng và hỏi người dùng trước khi sửa
    - `uv run mypy` exit 0 và báo đã kiểm mọi file trong `das/` ngoài Generated_Code
    - _Requirements: 6.5, 11.1, 11.3, 11.4, 11.5_

  - [x] 4.4 Kiểm âm tính cho Mypy
    - Thêm tạm một `# type: ignore` không mã và một lỗi kiểu vào một module trong `das/`, chạy `uv run mypy`, xác nhận exit khác 0 và output có `file:line` cùng mã lỗi, rồi hoàn tác; ghi kết quả vào task
    - Xác nhận output không có lỗi nào trong `das/transport/proto/`
    - _Requirements: 6.3, 6.6, 6.7_
    - Kết quả: thêm tạm vào cuối `das/health/supervisor.py` hai dòng `_neg_check_ignore: int = "x"  # type: ignore` (171) và `_neg_check_error: int = "y"` (172); `uv run mypy` exit 1 với `das/health/supervisor.py:171: error: "type: ignore" comment without error code ... [ignore-without-code]`, `das/health/supervisor.py:172: error: Incompatible types in assignment ... [assignment]`, `Found 2 errors in 1 file (checked 100 source files)`; 0 dòng chứa `das/transport/proto`. Khôi phục từ bản sao lưu (`cmp` giống hệt byte), chạy lại `uv run mypy` exit 0: `Success: no issues found in 100 source files`

- [x] 5. Checkpoint - Lint, kiểu và Fast_Test_Suite
  - Chạy `uv run ruff check`, `uv run mypy`, `uv run lint-imports`, `uv run pytest tests/unit tests/property`; tất cả exit 0. Ensure all tests pass, ask the user if questions arise.

- [x] 6. Hạ tầng test cho CI
  - [x] 6.1 Hiện thực hook `DAS_REQUIRE_DOCKER`
    - `tests/conftest.py`: `pytest_runtest_makereport` dạng hookwrapper; khi `DAS_REQUIRE_DOCKER=1` và report skipped có lý do khớp `\bdocker\b` (không phân biệt hoa thường), đổi `outcome` thành `failed` với `longrepr` nêu lý do skip gốc
    - Skip do cổng bận (lý do không nhắc Docker) vẫn là skip; không đổi lời gọi `pytest.skip` hiện có
    - _Requirements: 8.6_

  - [x] 6.2 Viết test cho hook `DAS_REQUIRE_DOCKER`
    - `tests/unit/test_require_docker_hook.py` dùng `pytester`: file test giả skip "Docker is not available; ..." trong fixture scope module
    - Có biến → failed/error, exit code 1; không có biến → skipped, exit 0; có biến nhưng lý do là cổng bận → skipped
    - _Requirements: 8.6_

  - [x] 6.3 Thêm `DAS_E2E_LOG_DIR` vào fixture e2e
    - `tests/integration/e2e/test_compose_e2e.py`, `tests/integration/e2e/test_observability_e2e.py`: trong `finally` của fixture `stack`, ghi log Compose vào `Path(DAS_E2E_LOG_DIR) / f"{PROJECT}.log"` (tạo thư mục nếu chưa có) khi biến được đặt; ngược lại giữ `tmp_path_factory`
    - Không đổi gì khác trong fixture
    - _Requirements: 8.8_

  - [x] 6.4 Bổ sung assertion deploy tĩnh
    - `tests/unit/test_deploy_static.py`: `DAS__ADMIN_API_KEYS` được khai báo trong `environment` của service `gateway` chỉ bằng tên (không literal, không mặc định); `deploy/.env.example` khai báo biến với giá trị rỗng
    - Giữ nguyên `test_delivery_limit_matches_manager_max_attempts`, xác nhận không bị skip hay xfail
    - _Requirements: 4.1, 4.2, 4.5, 4.6_

- [x] 7. Test cho counter dead letter khi mất reply
  - [x] 7.1 Hiện thực `ReplyDroppingRedis`
    - Trong `tests/unit/test_dlq_lost_reply.py`: lớp con của `fakeredis.aioredis.FakeRedis` chạy thật `EVALSHA` của `create_dead_letter.lua` trên `FakeServer` rồi ném `redis.exceptions.TimeoutError` ở lần đầu; đếm `executed`, `dropped`; callback `on_drop` tùy chọn
    - `target_sha` lấy từ `StateStore(client)._scripts["create_dead_letter"].sha`; xác nhận `AsyncScript.__call__` đi qua `client.evalsha`, nếu không thì override `execute_command` và so tên lệnh `EVALSHA` cùng SHA
    - _Requirements: 3.1, 3.4_

  - [x] 7.2 Viết ba trường hợp của test mất reply
    - Mỗi trường hợp dùng `FakeServer` mới, `MemoryBroker`/`MemoryTransport` của `tests/fakes/memory_transport.py`, Message `dispatch` có header `x-death-reason: delivery_limit`, bọc trong `asyncio.timeout(3)`
    - (a) giao lại cho cùng Recorder A; (b) `on_drop` gọi `recorder_a.stop()`, giao lại cho Recorder B dùng `FakeRedis` thường trên cùng server và xác nhận B đã gọi script; đối chứng không có fault
    - Assertion fault có hiệu lực trước mọi assertion khác: `executed >= 1`, `dropped == 1`, record tồn tại ngay sau lần chặn, log `dead_letter_record_failed` có `error="TimeoutError"`; nếu không thì fail với "fault injection had no effect"
    - Sau `broker.settle()`: `pending("dlq") == []`, `unacked_count("dlq") == 0`; đúng 1 record với `dead_letter_id == dlid`; tổng `das_dlq_records_created_total` của mọi recorder ≤ 1 ở (a), (b) và == 1 ở đối chứng
    - Ba trường hợp cùng chạy dưới 10 giây, không cần Docker
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6_

- [x] 8. Điều tra và sửa Flaky_Test
  - [x] 8.1 Điều tra nguyên nhân gốc
    - Chạy `test_reconnect_restores_subscriptions` với `--count 50 -o log_cli=true --log-cli-level=DEBUG` cho `das.transport.rabbitmq` và `aio_pika`, ghi log ra file; instrumentation tạm (không commit): `worker._poll_task`, `inbox.job_ids()`, JSON `/api/connections` tại mỗi lần drop
    - Chạy riêng node ID 10 lần, mỗi lần một phiên pytest với container mới (giả thuyết G4)
    - Nếu chưa thấy fail, áp lần lượt các monkeypatch tái hiện G1–G4 của design và đối chiếu dấu hiệu
    - Ghi lại vị trí (file và hàm), chuỗi sự kiện và bằng chứng (đoạn log hoặc bước tái hiện) cho Root_Cause_Note
    - Nếu không tìm được nguyên nhân gốc: dừng, báo lại log đã thu và các giả thuyết đã loại, hỏi người dùng trước khi làm tiếp
    - _Requirements: 10.1_
    - Kết quả (2026-10-29, macOS 27.0.1, Docker 29.9.2, `rabbitmq:4.1-management`, Python 3.12.15, aio-pika 10.1.1, aiormq 7.2.2):
      - **Vị trí:** `das/transport/rabbitmq.py::RabbitMQTransport.poll_once`, nhánh `except Exception as exc` quanh `queue.get(...)` (dòng 792): `isinstance(exc, _CONNECTION_ERRORS | TimeoutError)`. `_CONNECTION_ERRORS` là một `tuple`, nên `tuple | type` raise `TypeError: unsupported operand type(s) for |: 'tuple' and 'type'` ngay trong except. Lỗi này không được bắt ở `poll_once` lẫn `RabbitMQTransport._poll_loop`, nên poll task kết thúc; `RabbitMQTransport._reconnect` không gọi `_ensure_poller()` (chỉ `subscribe` gọi), nên sau reconnect không còn ai `basic.get`. Mọi exception từ `queue.get` đều đi vào nhánh này, kể cả `TimeoutError` khi `basic.get` chậm quá `_GET_TIMEOUT` = 5 s.
      - **Chuỗi sự kiện:** (1) `drop_connections` gọi `DELETE /api/connections/{name}`; (2) poll task đang chờ `basic.get` (3 subscription, mỗi vòng tới 3 RPC, `poll_interval` 0.05 s) khi frame `Connection.Close` (320 CONNECTION_FORCED) tới; aiormq hủy RPC và `queue.get` raise `aiormq.exceptions.ConnectionClosed` (thuộc `_CONNECTION_ERRORS`, lẽ ra được nuốt và `return False`); (3) phép `|` trong `isinstance` raise `TypeError`, thoát `_poll_loop`, task `done()` với exception; (4) `_on_lost` → `_reconnect` mở connection mới, `registry.restore` khôi phục đủ 3 queue, `connected` = True, gate mở, `len(worker.subscriptions) == 3` đúng; (5) không có poller nên "during"/"after-second" không bao giờ được lấy → `AssertionError: timed out waiting for delivery after reconnect` (drop thứ nhất) hoặc `timed out waiting for <lambda>` ở `wait_for("after-second" in ...)` (drop thứ hai). Nếu lúc `Connection.Close` tới poll task đang ở `_idle()` thì không có exception và lần chạy pass, nên test chỉ fail theo xác suất. Dấu hiệu trùng với lần fail gốc ghi ở `baseline.md` (timeout "delivery after reconnect" sau khi `worker.connected` đã True và đủ 3 subscription).
      - **Bằng chứng 1, Repeat_Run_Check có instrumentation** (`--count 50 -v -o log_cli=true --log-cli-level=DEBUG`, cùng một container): `36 failed, 14 passed in 945.93s`; 20 lần fail với `timed out waiting for delivery after reconnect` (drop thứ nhất), 16 lần fail ở `wait_for` của "after-second" ngay sau `wait_for 'second reconnect' ok` (drop thứ hai); không lần fail nào có dấu hiệu khác. Ở mọi lần fail (36/36), snapshot lúc timeout có `poll_task=<Task finished ... _poll_loop() done ...> exception=TypeError("unsupported operand type(s) for |: 'tuple' and 'type'")`, `connected=True`, `gate=True`, `queues=3`, cùng traceback: `aiormq.exceptions.ConnectionClosed: [Errno 320] CONNECTION_FORCED - das integration test: forced drop` → "During handling of the above exception, another exception occurred" → `rabbitmq.py", line 816, in _poll_loop` → `line 792, in poll_once: if not isinstance(exc, _CONNECTION_ERRORS | TimeoutError)` → `TypeError`.
      - **Bằng chứng 1b, code nguyên trạng, không instrumentation, không log_cli** (`.venv/bin/python -m pytest "tests/integration/test_rabbitmq_broker.py::test_reconnect_restores_subscriptions" --count 20 -v`): `9 failed, 11 passed in 315.75s`, fail ở `[1-20]`, `[2-20]`, `[3-20]`, `[9-20]`, `[10-20]`, `[11-20]`, `[14-20]`, `[15-20]`, `[16-20]`; 5 lần ở `test_rabbitmq_broker.py:411` ("delivery after reconnect"), 4 lần ở `:423` ("after-second"). Không có dòng log `rabbitmq basic.get failed` nào, tức `log.exception` trong nhánh except chưa từng chạy được. Tỉ lệ fail ở máy này (45–72%) cao hơn mô tả "fail một lần ở đầu lần chạy" của OD 3.4; tỉ lệ phụ thuộc vào xác suất `basic.get` đang chờ lúc `Connection.Close` tới.
      - **Bằng chứng 2, tái hiện tất định không cần Docker** (từ thư mục gốc repo, `.venv/bin/python`): tạo `RabbitMQTransport(url="amqp://unused/")`, đặt `_connected = True`, thêm một `Subscription("jobs/x", handler, SubscribeOptions(), on_cancel=None)` vào `registry`, gán `_queues[sub.id]` là object có `async def get(**kw): raise aiormq.exceptions.ConnectionClosed(320, "CONNECTION_FORCED")` và `_in_flight[sub.id] = 0`; `await t.poll_once()` raise `TypeError: unsupported operand type(s) for |: 'tuple' and 'type'` (context `ConnectionClosed`) thay vì trả `False`; chạy `asyncio.create_task(t._poll_loop())` thì sau 0.3 s task `done()` với cùng `TypeError`. Kiểm nhanh: `isinstance(ValueError(), (aiormq.exceptions.AMQPError,) | TimeoutError)` raise `TypeError` trên Python 3.12.
      - **Giả thuyết đã loại:** G1 (ACK của "before" chưa tới broker): không snapshot `job_ids` nào có job trùng, không lần fail nào ở `assert sorted(...)`. Khe thời gian của G1 có tồn tại (5/75 snapshot `before-drop` có `in_flight=1`, tức ACK chưa settle lúc snapshot), nhưng ACK vẫn tới broker trước khi `GET`+`DELETE` của `drop_connections` xong, nên khe này không gây lần fail nào quan sát được. G2 (management API liệt kê connection đã đóng): mọi `DELETE` trả 204, không có 404, không có "second drop detected" timeout; snapshot `/api/connections` không có connection `closed`/`closing`. G4 (khởi động lạnh): chạy riêng node ID 10 phiên pytest, mỗi phiên một container mới, có instrumentation: 7/10 phiên fail nhưng cả 7 đều mang dấu hiệu ở trên (`exc=TypeError`, 5 lần "delivery after reconnect", 2 lần "after-second"); ở cả 10 phiên `_wait_ready`/`_wait_amqp` không lỗi và `wait_for` đầu tiên (`job_ids() == ["before"]`) xong trong 0.05–0.10 s, nên không có timeout nào do broker vừa khởi động. G3 đúng ở hệ quả (poll task chết, `_reconnect` không khởi động lại poller) nhưng sai ở cơ chế: exception thoát ra là `TypeError` do phép `|` trên tuple, không phải `asyncio.CancelledError` (aiormq bọc `CancelledError` thành `ConnectionClosed`, vốn thuộc `_CONNECTION_ERRORS`). Vì đã thấy fail ở bước baseline, không cần áp monkeypatch G1–G4.
      - **Gợi ý cho 8.2/8.3:** sửa phép kiểm thành `isinstance(exc, (*_CONNECTION_ERRORS, TimeoutError))` (hoặc tuple tương đương) trong `poll_once`; có thể thêm theo design G3 `_ensure_poller()` sau `registry.restore` trong `_reconnect` và guard một vòng trong `_poll_loop` làm lớp phòng vệ, interface công khai không đổi. `tests/unit/test_rabbitmq_transport.py` hiện chưa có test nào cho `queue.get` raise; unit test 8.3 nên cho `queue.get` raise `ConnectionClosed` một lần rồi kiểm delivery tiếp tục sau reconnect.
      - Instrumentation tạm trong `tests/integration/test_rabbitmq_broker.py` đã gỡ (khôi phục từ bản sao lưu, `cmp` giống hệt byte); `das/transport/rabbitmq.py` không bị sửa (`cmp` với bản sao lưu giống hệt byte). Log và script tái hiện nằm ngoài repo ở `/tmp/flaky-8.1/`.

  - [x] 8.2 Sửa Flaky_Test theo nguyên nhân gốc
    - Sửa ở đúng vị trí: trong `tests/integration/test_rabbitmq_broker.py` (ví dụ G1: chờ `worker.in_flight() == 0` trước mỗi lần drop; G2: `drop_connections` coi 404 là đã đóng, chỉ xóa connection `running`, giữ chữ ký vì `test_rabbitmq_operations.py` import), hoặc trong `das/transport/rabbitmq.py` (G3: `_ensure_poller()` sau `registry.restore` trong `_reconnect`, guard lỗi một vòng trong `_poll_loop`) mà không đổi interface công khai
    - Giữ ba subscription và mọi assertion; không rerun, skip, xfail; không tăng timeout để che race
    - Mỗi nguyên nhân có bằng chứng riêng thì sửa và ghi riêng
    - _Requirements: 10.2, 10.6, 10.7_
    - Kết quả (2026-10-29): chỉ sửa `das/transport/rabbitmq.py`; `tests/integration/test_rabbitmq_broker.py` không đổi (`cmp` với bản sao lưu của 8.1 giống hệt byte). 8.1 chỉ tìm thấy một nguyên nhân gốc, nên có một sửa chính và hai lớp phòng vệ theo G3.
      - **Sửa chính, `RabbitMQTransport.poll_once` (dòng 794):** `isinstance(exc, _CONNECTION_ERRORS | TimeoutError)` → `isinstance(exc, (*_CONNECTION_ERRORS, TimeoutError))`. Phép kiểm giờ là một tuple hợp lệ, nên lỗi từ `queue.get` không còn biến thành `TypeError`: lỗi connection (`ConnectionClosed`, ...) và `TimeoutError` được nuốt im lặng, lỗi khác được `log.exception("rabbitmq basic.get failed")`, và cả hai trường hợp đều `return False`. Poll task không còn chết khi `Connection.Close` tới giữa `basic.get`, nên sau reconnect vẫn còn poller lấy "during"/"after-second". Đây là chỗ đã sinh ra mọi lần fail ở 8.1 (36/36 và 9/20).
      - **Phòng vệ 1, `RabbitMQTransport._reconnect` (dòng 684–685):** sau khi `_open()` và `registry.restore` thành công, gọi `self._ensure_poller()` nếu `registry.active()` không rỗng. Nếu poll task đã kết thúc vì bất kỳ lý do nào, nó được tạo lại sau reconnect. Khi không có subscription thì không tạo task, giống trước đây. `_ensure_poller` không làm gì nếu task còn chạy.
      - **Phòng vệ 2, `RabbitMQTransport._poll_loop` (dòng 815–827):** bọc `await self.poll_once()` bằng `try/except Exception`. Nếu `_closing` thì `return`; nếu không thì `log.exception("rabbitmq poll pass failed")`, coi vòng đó là không lấy được message và `_idle()`. `asyncio.CancelledError` là `BaseException` nên không bị bắt; `close()` vẫn hủy được task.
      - **Interface công khai không đổi (Req 10.7):** không đổi chữ ký, thuộc tính công khai, tham số khởi tạo hay export nào. Không đổi route, status code, metric, trường Agent_Config, biến `DAS__` hay exit code (Req 11.1, 11.3).
      - **Thay đổi hành vi quan sát được (ghi theo Req 11.5):** đây là sửa bug bên trong adapter, không phải sửa theo Ruff hay Mypy, nên không có mã rule hay mã lỗi (Ruff và Mypy đều không báo dòng 794 ở baseline). `das/transport/rabbitmq.py:794`, trước: mọi exception từ `basic.get` (kể cả lỗi connection khi broker đóng connection, và `TimeoutError` sau `_GET_TIMEOUT` = 5 s) làm poll task kết thúc với `TypeError`, và sau reconnect không subscription nào nhận message nữa dù `connected` là True. Sau: `poll_once` trả `False`, poll task tiếp tục chạy và delivery tiếp tục sau reconnect, đúng hành vi đã đặc tả ở v1 Req 8.6. Lỗi không phải lỗi connection giờ được log ở mức ERROR với logger `das.transport.rabbitmq` (`rabbitmq basic.get failed`); `_poll_loop` có thêm thông điệp `rabbitmq poll pass failed` cho lỗi thoát ra khỏi một vòng (dòng 824).
      - **Rà soát cách dùng `|` sai trong `das/`:** grep `isinstance(...|...)` và `except ...|...` trong mọi file `.py` dưới `das/`. Chỉ có dòng 794 dùng `|` với một tuple. Các chỗ còn lại đều là `type | type` hợp lệ trên Python 3.12: `das/transport/rabbitmq.py:249`, `:253`; `das/transport/base.py:279`, `:300`; `das/codec/message_codec.py:174`; `das/observability/logging.py:158`, `:168`, `:174`, `:178`; `das/observability/tracing.py:91`. `das/codec/message_codec.py:208` (`ENVELOPE_FIELDS | ENVELOPE_V2_OPTIONAL_FIELDS`) là phép hợp hai `frozenset`, hợp lệ. Không sửa gì khác.
      - **Kiểm tra:** `uv run ruff check` "All checks passed!"; `uv run mypy` "Success: no issues found in 100 source files"; `uv run pytest tests/unit/test_rabbitmq_transport.py` 30 passed. Tái hiện tất định của 8.1 (`.venv/bin/python /tmp/flaky-8.1/repro_poll_crash.py`): `poll_once returned False`, `poll task done: False exception: None` (trước khi sửa: `TypeError` ở cả hai). Kiểm tay guard (`/tmp/flaky-8.2/repro_guard.py`, `poll_once` raise `TypeError` ở vòng đầu): task vẫn chạy thêm 18 vòng trong 0.2 s, `task.cancel()` vẫn làm task kết thúc với `cancelled() == True`. `uv run pytest "tests/integration/test_rabbitmq_broker.py::test_reconnect_restores_subscriptions" --count 5 -v`: 5 passed in 55.61s (Docker 29.9.2, `rabbitmq:4.1-management`). Repeat_Run_Check 20 lần thuộc task 8.4; unit test cho adapter thuộc task 8.3.

  - [x] 8.3 Viết unit test cho adapter (chỉ khi sửa trong `das/transport/rabbitmq.py`)
    - `tests/unit/test_rabbitmq_transport.py` với connect giả: sau một vòng poll gặp lỗi rồi reconnect, delivery vẫn tiếp tục
    - Bỏ qua task này và ghi lý do nếu cách sửa chỉ nằm trong test
    - _Requirements: 10.7_
    - Kết quả (2026-10-29): task áp dụng vì 8.2 sửa `das/transport/rabbitmq.py`. Thêm vào `tests/unit/test_rabbitmq_transport.py` hàng đợi `FakeBroker.get_failures` (mỗi phần tử là `(exception, drop)`; `FakeQueue.get` lấy một phần tử, nếu `drop` thì gọi `broker.drop()` trước rồi raise) và 5 test: `test_get_connection_error_returns_false_and_poller_survives`, `test_get_timeout_returns_false_without_logging`, `test_delivery_resumes_after_poll_error_and_reconnect` (kịch bản chính: `basic.get` raise `ConnectionClosed` đúng lúc connection bị đóng, reconnect qua connect giả, Message publish và Message có sẵn trong queue đều được giao), `test_reconnect_restarts_ended_poller`, `test_poll_loop_survives_failed_pass_and_stays_cancellable` (limiter raise `RuntimeError` ở lần `try_acquire` đầu). `das/` không đổi. Với bản adapter trước khi sửa (`/tmp/flaky-8.2/rabbitmq.py.orig`, thay tạm rồi khôi phục, `cmp` giống hệt byte): cả 5 test fail (2 test đầu với `TypeError: unsupported operand type(s) for |: 'tuple' and 'type'`, 3 test sau với `TimeoutError` của `wait_until` vì không còn poller); 30 test cũ pass. Với bản đã sửa: `uv run pytest tests/unit/test_rabbitmq_transport.py -v` 35 passed; `uv run ruff check tests/unit/test_rabbitmq_transport.py` "All checks passed!"; `uv run mypy` "Success: no issues found in 100 source files".

  - [x] 8.4 Chạy Repeat_Run_Check và các suite sau khi sửa
    - `uv run pytest "tests/integration/test_rabbitmq_broker.py::test_reconnect_restores_subscriptions" --count 20 -v` pass 20/20 trên máy local; ghi số lần pass, ngày, OS, phiên bản Docker
    - Fast_Test_Suite và Integration_Test_Suite pass
    - _Requirements: 10.3, 10.7_
    - Kết quả (2026-10-29): `das/` không đổi trong task này; `tests/` chỉ đổi `tests/unit/test_dlq_lost_reply.py` (sửa cô lập test, xem dòng Fast_Test_Suite).
      - **Môi trường:** macOS 27.0.1 (build 26A434, arm64), Python 3.12.15, Docker Desktop 29.9.2 (client và server 29.9.2, kernel `7.0.14-linuxkit`), image `rabbitmq:4.1-management` (RabbitMQ 4.1.8, `sha256:bf215c927c885b6d28bc52e02b0f2e1c033f2b439ec946c643834be93312ec4d`), pytest 9.1.1, pytest-repeat 0.9.4.
      - **Repeat_Run_Check:** chạy đúng lệnh `uv run pytest "tests/integration/test_rabbitmq_broker.py::test_reconnect_restores_subscriptions" --count 20 -v`: **20/20 passed**, 0 failed, trong 204.45 s (log `/tmp/das-8.4-repeat.log`). Trước khi sửa: 9/20 (8.1).
      - **Integration_Test_Suite** (`uv run pytest tests/integration --ignore=tests/integration/e2e`, có Docker): **10 passed, 0 failed, 0 skipped** trong 38.39 s (`test_grpc_two_process.py` 1, `test_rabbitmq_broker.py` 5, `test_rabbitmq_operations.py` 4).
      - **Fast_Test_Suite** (`uv run pytest tests/unit tests/property`): **1970 passed, 0 failed, 0 skipped** trong 154.42 s (log `/tmp/fast_suite.log`). Trước khi sửa: hai lần chạy đều 1 failed, 1969 passed, ở `tests/unit/test_dlq_lost_reply.py::test_lost_reply_redelivered_to_other_manager`, assert `dead_letter_record_exists`; lỗi tất định, phụ thuộc thứ tự (fail khi chạy sau `tests/unit/test_cli.py` hoặc `tests/unit/test_config_phase2.py`).
      - **Sửa cô lập test (được user duyệt):** chỉ sửa `tests/unit/test_dlq_lost_reply.py`. Nguyên nhân: hai file trên gọi `configure_logging(...)` mức INFO (fixture autouse `tests/unit/test_cli.py:61`), đặt global `wrapper_class=make_filtering_bound_logger(INFO)` (`das/observability/logging.py:326`); `capture_logs` chỉ thay processors nên event DEBUG `dead_letter_record_exists` (`das/manager/ingest.py:308`) bị lọc. Thay đổi: thêm fixture autouse `_default_structlog_config` lưu `structlog.get_config()`, gọi `structlog.reset_defaults()` cho mỗi test và khôi phục config cũ sau test. Không bỏ hay nới assertion nào (giữ cả `client_b.executed == 1` lẫn log event, và các kiểm "fault injection had no effect"). `dead_letter_record_failed` là `log.warning` nên không bị ảnh hưởng ở mức INFO. Kiểm lại: `uv run pytest tests/unit/test_cli.py tests/unit/test_dlq_lost_reply.py` 16 passed, `uv run pytest tests/unit/test_config_phase2.py tests/unit/test_dlq_lost_reply.py` 19 passed, `uv run ruff check` All checks passed.
      - **Test đã biết `tests/property/test_worker_execution.py::test_worker_execution_contract`:** pass ở cả hai lần chạy Fast_Test_Suite đầy đủ, không tái hiện lần fail `TimedOut` trước đó.

- [x] 9. Checkpoint - Fast_Test_Suite và Integration_Test_Suite
  - Chạy Fast_Test_Suite và Integration_Test_Suite (cần Docker); xác nhận Flaky_Test ổn định. Ensure all tests pass, ask the user if questions arise.

- [x] 10. CI_Pipeline
  - [x] 10.1 Viết workflow với Fast_Job
    - `.github/workflows/ci.yml`: trigger `push` (`branches: ["**"]`), `pull_request` (`opened`, `synchronize`, `reopened`), `workflow_dispatch`; `permissions: contents: read`; `concurrency` theo `event_name` và `head_ref || ref` với `cancel-in-progress: true`; `env` `UV_VERSION: "0.13.6"`, `PYTHON_VERSION: "3.12"`
    - Job `fast` (`ubuntu-24.04`, `timeout-minutes: 15`): checkout, setup-uv (`version`, `python-version`, `enable-cache`, `cache-dependency-glob: uv.lock`), step `uv sync --locked` với `id: install`, bốn step `uv run ruff check`, `uv run mypy`, `uv run lint-imports`, `uv run pytest tests/unit tests/property` theo đúng thứ tự, mỗi step có `if: ${{ !cancelled() && steps.install.outcome == 'success' }}`
    - Step Fast_Test_Suite sinh seed, in `Hypothesis seed: <n>` và lệnh tái hiện, đưa seed vào `PYTEST_ADDOPTS`; không cache `.hypothesis`
    - Kiểm lại SHA của `actions/checkout` và `astral-sh/setup-uv` bằng `git ls-remote --tags` trước khi ghim; mỗi `uses:` có comment `# vX.Y.Z`; không tham chiếu `secrets.`
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 7.7, 7.8, 7.9, 9.1, 9.2, 9.3, 9.4, 9.5, 9.6, 9.8, 9.9_

  - [x] 10.2 Thêm Integration_Job và E2E_Job vào workflow
    - Cả hai `needs: fast`, `if: ${{ github.event_name == 'workflow_dispatch' || (github.event_name == 'push' && github.ref == 'refs/heads/main') }}`, `ubuntu-24.04`, `timeout-minutes: 30`, `DAS_REQUIRE_DOCKER: "1"`, không phụ thuộc lẫn nhau
    - `integration`: step `uv run pytest tests/integration --ignore=tests/integration/e2e` (`timeout-minutes: 15`), step Repeat_Run_Check riêng (`if: !cancelled() && install success`, `timeout-minutes: 12`) chạy đúng lệnh của task 8.4
    - `e2e`: `DAS_E2E_LOG_DIR: ${{ runner.temp }}/e2e-logs`; step kiểm Docker Compose ≥ 2.30 (in phiên bản tìm thấy, `::error::` và exit 1 khi thấp hơn hoặc không xác định); step `uv run pytest tests/integration/e2e` (`timeout-minutes: 24`); step thu log container theo label `com.docker.compose.project` cho `das-e2e`, `das-e2e-obs` và step upload artifact (`if: failure() || cancelled()`); step teardown `down -v --remove-orphans` và xóa volume theo label (`if: always()`)
    - Lấy SHA của `actions/upload-artifact` bản mới nhất bằng `git ls-remote --tags https://github.com/actions/upload-artifact` và ghim kèm comment tag
    - _Requirements: 8.1, 8.2, 8.3, 8.4, 8.5, 8.6, 8.7, 8.8, 8.9, 8.10, 8.11, 9.1, 9.2, 9.6, 9.7, 10.4, 10.5_

- [x] 11. README và test tĩnh của workflow
  - [x] 11.1 Cập nhật README.md
    - Mục Development: mỗi lệnh một code block, nguyên văn như trong workflow: `uv sync --locked`, `uv run ruff check`, `uv run mypy`, `uv run lint-imports`, `uv run pytest tests/unit tests/property`, `uv run pytest tests/integration --ignore=tests/integration/e2e`, `uv run pytest tests/integration/e2e`, lệnh Repeat_Run_Check; thay lệnh cũ `uv sync` và `uv run pytest tests/integration/e2e -s`
    - Yêu cầu: Python 3.12, uv 0.13.6, Docker cho Integration/E2E/Repeat_Run_Check, Docker Compose ≥ 2.30 cho E2E; bốn lệnh đầu không cần Docker; ý nghĩa `DAS_REQUIRE_DOCKER=1`
    - Cách lấy dòng `Hypothesis seed: <n>` từ log Fast_Job và lệnh chạy lại với `--hypothesis-seed=<n>`
    - Generated_Code bị loại khỏi ruff và mypy; lệnh `grpc_tools.protoc` sinh lại và bước sửa import tương đối
    - Mục Continuous integration: ba job, lệnh, sự kiện kích hoạt, Integration_Job/E2E_Job chỉ chạy sau Fast_Job success; các bước Manual_Trigger (Actions → CI → Run workflow → Use workflow from → Run workflow)
    - Giữ câu về `delivery-limit` ở mục Configuration
    - _Requirements: 4.4, 10.8, 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 12.7_

  - [x] 11.2 Viết test tĩnh cho workflow và README
    - `tests/unit/test_ci_workflow_static.py` (parse bằng `yaml.safe_load`): mọi `uses:` khớp `owner/repo@<40 hex>` kèm comment `# vX.Y.Z`; `UV_VERSION` dạng `X.Y.Z` và mọi setup-uv dùng nó; `permissions == {"contents": "read"}` và không job nào khai báo quyền khác; không chuỗi `secrets.`; `timeout-minutes` 15/30/30; `needs`, `if:` của `integration`, `e2e`; `cancel-in-progress`; trigger đúng
    - Thứ tự và tên bốn step của `fast`, mỗi step có `!cancelled()` và `steps.install.outcome == 'success'`
    - Lệnh `uv run ...` của mỗi step kiểm tra (kể cả Repeat_Run_Check) xuất hiện nguyên văn trong một code block của mục Development, và mọi lệnh `uv run ruff|mypy|lint-imports|pytest` trong mục đó có trong workflow
    - `tests/integration/**` không dùng `allow_module_level=True`
    - _Requirements: 7.1, 7.2, 7.3, 7.7, 7.8, 8.1, 8.2, 8.3, 8.7, 8.9, 9.1, 9.2, 9.5, 9.6, 12.1, 12.7_

- [x] 12. Sửa spec và ghi Decision_Record
  - [x] 12.1 Sửa Phase2_Requirements
    - `.kiro/specs/das-observability-operations/requirements.md`: v2 Req 18.12 (`/api/v3/traces` với `query.service_name=das-gateway`), 17.15 (`parameter:"body"` và `parameter:"release_leases"`), 13.7 (`stale_attempt` khi attempt lớn hơn), 12.12 mới cuối Requirement 12 (malformed cùng body → một record), 3.8 (counter ≤ số record, bằng khi mọi reply tới), 3.3 (Lease không có `last_owner` → `initial`), 2.7 (chỉ transition do Manager ghi, bỏ "hoặc Cancelled trước khi được dispatch"), 17.5 (reply không chờ nhả Lease, giới hạn 5 giây giữ, `das drain --wait`); viết theo EARS
    - Không đánh lại số, không xóa tiêu chí; grep xác nhận không còn `/api/traces?service=das-gateway` và số tiêu chí mỗi Requirement không giảm
    - Kết quả: tiêu chí malformed mới là v2 Req 12.13 (không phải 12.12) vì 12.12 đã tồn tại (503 khi đọc DLQ); `open-decisions.md` OD 1.4 đã ghi 12.13
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8, 2.11_

  - [x] 12.2 Sửa Phase2_Design
    - `.kiro/specs/das-observability-operations/design.md`: D6, §6 `acquire_lease.lua`, §8 Drain, §10 bảng endpoint, §14 `decide_replay`, Property 18, "Unit và example test" (metric và Drain), "Integration và smoke test", đoạn cuối Testing Strategy theo bảng của design Phase 3
    - Bảng "Property tests": Property 1 → `tests/property/test_job_transition_count.py`, 24 → `test_retention_safety.py`, 25 → `test_prop_retention_liveness.py`, các dòng còn lại theo tag `# Feature: das-observability-operations, Property N` trong `tests/property/`; xác nhận mọi file trong bảng tồn tại
    - Grep xác nhận không còn `/api/traces?service=das-gateway`, `test_prop_transition_metrics.py`, `test_prop_retention.py`, và không còn câu nói tổng `das_dlq_records_created_total` luôn bằng số record
    - _Requirements: 2.9, 2.10_

  - [x] 12.3 Thêm đoạn "Redis deployment scope" vào Phase1_Design
    - `.kiro/specs/distributed-agent-system/design.md`, ngay sau bullet "Redis Lua scripts", viết tiếng Anh: chỉ một Redis primary; Sentinel và Cluster ngoài phạm vi vì Lua script truy cập nhiều key không cùng hash slot; HA cho Redis cần phase riêng
    - _Requirements: 4.3_

  - [x] 12.4 Ghi Decision_Record cho mọi mục trừ OD 3.4
    - `.kiro/specs/open-decisions.md`: đổi `[ ]` thành `[x]` trong tiêu đề OD 1.1–1.8, 2.1, 3.1, 3.2, 3.3, 3.5, 4.1, 4.2; thêm đúng một Decision_Record (`**Quyết định:**`, `**Ngày:**`, `**Áp dụng tại:**`) theo bảng của design, OD 1.6 ghi đúng lựa chọn (a), OD 1.5 tham chiếu cả v2 Req 3.8 và `tests/unit/test_dlq_lost_reply.py`, OD 4.2 tham chiếu `tests/unit/test_deploy_static.py::test_delivery_limit_matches_manager_max_attempts`
    - Không sửa ký tự nào trong phần Hiện trạng, Lựa chọn, Đề xuất và phần 5
    - _Requirements: 1.1, 1.2, 1.3, 1.5, 1.6, 1.7, 1.8_

  - [x] 12.5 Ghi Decision_Record của OD 3.4 và kiểm Decision_Log
    - Đổi `[ ]` thành `[x]` cho OD 3.4; Decision_Record theo mẫu của design với `**Nguyên nhân gốc:**` (file::hàm, chuỗi sự kiện, bằng chứng), `**Cách sửa:**` (file đã sửa, thay đổi, vì sao loại bỏ nguyên nhân), `**Repeat_Run_Check:**` (20/20, ngày, môi trường) lấy từ task 8.1, 8.2, 8.4; `**Áp dụng tại:**` gồm `tests/integration/test_rabbitmq_broker.py` (và `das/transport/rabbitmq.py` nếu sửa ở adapter)
    - Kiểm: grep không còn tiêu đề `[ ]` ở phần 1–4; mỗi mục có đúng một `**Quyết định:**`; diff với bản cũ chỉ có dòng thêm và đổi `[ ]`→`[x]`; mọi đường dẫn trong `**Áp dụng tại:**` tồn tại
    - _Requirements: 1.1, 1.4, 1.5, 1.6, 1.7, 10.1, 10.2, 10.3_

- [x] 13. Đối chiếu với baseline
  - [x] 13.1 Kiểm mọi property của Phase 1 và Phase 2 vẫn đúng
    - **Property 2: Mọi property của Phase 1 và Phase 2 vẫn đúng**
    - Chạy Fast_Test_Suite, Integration_Test_Suite, E2E_Test_Suite, Import_Contract_Check; so với `baseline.md`: mọi property test baseline còn tồn tại và pass, `max_examples` không giảm, số test thu thập không giảm, số skip/xfail không tăng ở mỗi suite
    - Diff mục `[tool.importlinter]` với bản baseline: không đổi tên, loại, layer, module hay ngoại lệ
    - Kiểm âm tính `delivery-limit`: sửa tạm `deploy/rabbitmq/definitions.json`, xác nhận `test_delivery_limit_matches_manager_max_attempts` fail, rồi hoàn tác
    - Ghi kết quả vào `baseline.md` (phần "Sau Phase 3")
    - **Validates: Requirements 4.6, 10.7, 11.4, 11.6, 11.7, 11.8**

- [x] 14. Final checkpoint - Toàn bộ suite và baseline
  - Lint_Check, Type_Check, Import_Contract_Check, Fast_Test_Suite, Integration_Test_Suite, E2E_Test_Suite và Repeat_Run_Check đều exit 0; kết quả đối chiếu baseline ở task 13.1 đạt. Ensure all tests pass, ask the user if questions arise.
  - **Kết quả (2026-10-29):** E2E_Test_Suite (`uv run pytest tests/integration/e2e -v -s`, `DAS_E2E_LOG_DIR=/tmp/e2e-logs-r1|r2`) chạy lại hai lần liên tiếp, cả hai `14 passed`, exit 0: lần 1 `14 passed in 235.15s` (query_s n=136, max 0.2555 s, median 0.0063 s; takeover_after_expiry_s 0.072), lần 2 `14 passed in 208.16s` (query_s n=144, max 0.1764 s, median 0.0054 s; takeover_after_expiry_s 0.043). Trước khi chạy không còn container, network hay volume `das-e2e*` nào; load average của host 5.3–13 trên 8 CPU, có container `omniroute` không liên quan đang chạy. Lần chạy trước đó (`/tmp/das_e2e_final.log`) cho `3 failed, 11 passed in 165.09s`: `test_manager_kill_takeover_within_10s` qua bước takeover nhưng một lần query mất 0.840 s (giới hạn `< 0.5`, `test_compose_e2e.py:151`); `test_scale_up_and_down` thấy 3 container mới thay vì 2 (`:400`); `test_dependency_unreachable_exits_nonzero[rabbitmq-override0]` thấy thêm đúng 3 agent của 3 container đó (`:490`). Nguyên nhân có khả năng (chưa xác nhận): lần query chậm do host tải cao; hai fail sau là hệ quả dây chuyền, vì test kill fail trước bước `compose up --scale manager=2` nên test scale bắt đầu với 1 manager và `--scale manager=3` tạo 2 manager + 1 worker, rồi test scale fail trước bước scale down nên 3 agent đó đăng ký trong lúc test dependency chạy. Lời giải thích này khớp với code test và log nhưng chưa được tái hiện. Đường query của Gateway chỉ đọc Redis, không đi qua `das/transport/rabbitmq.py`.

## Notes

- Tasks có `*` là optional, có thể bỏ qua cho MVP. Property 1, Property 2 và các test baseline của Requirement 11 là bắt buộc.
- Task 1.1 và 1.2 phải xong trước mọi sửa đổi trong `das/`; nếu đã lỡ sửa, chụp baseline từ trạng thái trước sửa đổi (ví dụ bản sao lưu) chứ không từ code đã sửa.
- Mọi sửa đổi trong `das/` ở task 3, 4, 8 chỉ thuộc bốn loại của Req 11.4; sửa nào đổi hành vi quan sát được thì ghi theo Req 11.5.
- Hành vi chỉ kiểm được trên GitHub (cancel, skipped, status check, artifact, seed trong log) cần một lần kiểm tay sau khi repository lên GitHub; workspace hiện chưa là git repository nên việc này nằm ngoài danh sách task.
- Nếu `pytest-repeat` không dùng được, lệnh Repeat_Run_Check trong task 8.4, 10.2, 11.1 đổi cùng lúc.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["1.3", "1.4"] },
    { "id": 2, "tasks": ["2.1"] },
    { "id": 3, "tasks": ["2.2"] },
    { "id": 4, "tasks": ["2.3", "3.1"] },
    { "id": 5, "tasks": ["3.2", "4.1"] },
    { "id": 6, "tasks": ["4.2"] },
    { "id": 7, "tasks": ["4.3"] },
    { "id": 8, "tasks": ["4.4", "6.1", "6.3", "6.4", "7.1"] },
    { "id": 9, "tasks": ["6.2", "7.2", "8.1"] },
    { "id": 10, "tasks": ["8.2"] },
    { "id": 11, "tasks": ["8.3"] },
    { "id": 12, "tasks": ["8.4"] },
    { "id": 13, "tasks": ["10.1", "11.1", "12.1", "12.2", "12.3", "12.4"] },
    { "id": 14, "tasks": ["10.2"] },
    { "id": 15, "tasks": ["11.2", "12.5"] },
    { "id": 16, "tasks": ["13.1"] }
  ]
}
```
