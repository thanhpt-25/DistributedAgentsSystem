# Các vấn đề cần lựa chọn / xác nhận

Phạm vi: Phase 1 (`distributed-agent-system`) và Phase 2 (`das-observability-operations`).

Nguồn: báo cáo của các subagent trong lần chạy Phase 2 gần nhất (task 11.1 trở đi), cùng các điểm còn mở được ghi trong `design.md` của hai spec. Tôi không có báo cáo chi tiết của Phase 1 và của các task Phase 2 đã làm trong phiên trước (1–7, 9.1–9.6, 10.1–10.3, 12.1). Vì vậy mục Phase 1 chỉ gồm những gì còn thấy được trong spec và code.

Mỗi mục ghi: hiện trạng, các lựa chọn, đề xuất. Ô `[ ]` để đánh dấu khi đã quyết.

---

## 1. Phase 2: lệch giữa spec và hiện thực

### 1.1 [x] Đường dẫn API của Jaeger (Req 18.12)
- **Hiện trạng:** spec ghi `/api/traces?service=das-gateway`. Image ghim `jaegertracing/jaeger:2.22.0` trả 404 ở đường dẫn này, vì chỉ còn API v3. Test e2e (`tests/integration/e2e/test_observability_e2e.py`) đang dùng `/api/v3/traces?query.service_name=das-gateway`.
- **Lựa chọn:**
  - (a) Sửa Req 18.12 và design theo API v3.
  - (b) Đổi image Jaeger sang bản còn hỗ trợ API v1.
- **Đề xuất:** (a). Hiện thực và test đã chạy được với v3.
- **Quyết định:** (a) Sửa Req 18.12 và design theo API v3.
  - **Ngày:** 2026-10-29
  - **Áp dụng tại:** v2 Req 18.12; Phase2_Design mục "Integration và smoke test"; `tests/integration/e2e/test_observability_e2e.py`

### 1.2 [x] Body drain không phải JSON object
- **Hiện trạng:** Req 17.15 chỉ định nghĩa lỗi `invalid_request{parameter:"release_leases"}`. Khi body không phải JSON object (ví dụ `[]` hoặc JSON hỏng), code trả 400 `invalid_request` với `parameter:"body"`, giá trị này spec không nhắc tới.
- **Lựa chọn:**
  - (a) Bổ sung `parameter:"body"` vào Req 17.15.
  - (b) Đổi code để trả `parameter:"release_leases"` cho mọi body lỗi.
- **Đề xuất:** (a). Thông báo lỗi chính xác hơn.
- **Quyết định:** (a) Bổ sung `parameter:"body"` vào Req 17.15.
  - **Ngày:** 2026-10-29
  - **Áp dụng tại:** v2 Req 17.15; Phase2_Design §10 (bảng endpoint của Gateway_API)

### 1.3 [x] Replay với `attempt > attempt hiện tại của Job`
- **Hiện trạng:** requirement không nêu trường hợp này. Design chọn trả `stale_attempt` (design §ops, ghi chú ở cuối Testing Strategy).
- **Lựa chọn:**
  - (a) Xác nhận `stale_attempt` và ghi vào Req 13.7.
  - (b) Thêm một mã lỗi riêng.
- **Đề xuất:** (a).
- **Quyết định:** (a) Xác nhận `stale_attempt` và ghi vào Req 13.7.
  - **Ngày:** 2026-10-29
  - **Áp dụng tại:** v2 Req 13.7; Phase2_Design §14 (`decide_replay`)

### 1.4 [x] Hai Message malformed giống hệt nhau từng byte (D6)
- **Hiện trạng:** Dead_Letter_ID của Message malformed tính từ `sha256(body)`. Hai Message khác nhau nhưng trùng byte chỉ tạo **một** Dead_Letter_Record.
- **Lựa chọn:**
  - (a) Chấp nhận và ghi rõ trong requirements.
  - (b) Đưa thêm thông tin của broker (delivery tag, timestamp) vào identity. Cách này làm mất tính idempotent khi Message được giao lại.
- **Đề xuất:** (a).
- **Quyết định:** (a) Chấp nhận và ghi rõ trong requirements.
  - **Ngày:** 2026-10-29
  - **Áp dụng tại:** v2 Req 12.13 (tiêu chí mới cuối Requirement 12); Phase2_Design D6

### 1.5 [x] `das_dlq_records_created_total` đếm thiếu khi mất reply
- **Hiện trạng:** nếu Redis đã ghi Dead_Letter_Record nhưng reply bị mất (timeout), lần giao lại gặp `EXISTS` và không tăng counter. Kết quả: số record vẫn đúng, nhưng counter thấp hơn 1. Property 18 chỉ chèn lỗi xảy ra *trước* khi ghi, nên không bắt được trường hợp này.
- **Lựa chọn:**
  - (a) Chấp nhận: counter là "≤ số record", và ghi điều này vào Req 3.8.
  - (b) Đếm theo trạng thái trong Redis, ví dụ dùng cờ `counted` trong record (phức tạp hơn).
- **Đề xuất:** (a), kèm bổ sung một test cho trường hợp mất reply.
- **Quyết định:** (a) Chấp nhận: counter là "≤ số record", và ghi điều này vào Req 3.8.
  - **Ngày:** 2026-10-29
  - **Áp dụng tại:** v2 Req 3.8; `tests/unit/test_dlq_lost_reply.py`; Phase2_Design Property 18

### 1.6 [x] Lease do v1 ghi được tính là `initial` thay vì `takeover` (Req 3.3)
- **Hiện trạng:** `acquire_lease.lua` chỉ dựa vào `last_owner` để quyết định `takeover`. Lease do v1 ghi không có `last_owner`, nên lần acquire đầu tiên của Manager khác sau khi nâng cấp được đếm là `initial`.
- **Lựa chọn:**
  - (a) Chấp nhận (chỉ ảnh hưởng một lần, trong lúc nâng cấp).
  - (b) Script dùng thêm `owner` cũ còn trong hash khi không có `last_owner`.
- **Đề xuất:** (a), hoặc (b) nếu cần số liệu chính xác ngay trong lúc nâng cấp.
- **Quyết định:** (a) Chấp nhận (chỉ ảnh hưởng một lần, trong lúc nâng cấp).
  - **Ngày:** 2026-10-29
  - **Áp dụng tại:** v2 Req 3.3; Phase2_Design §6 (`acquire_lease.lua`)

### 1.7 [x] "Job Cancelled trước dispatch → attempts = 0" (Req 2.7) không quan sát được ở Manager
- **Hiện trạng:** Job chỉ bị Cancelled trước dispatch qua `cancel_request` của Gateway. Đó không phải lần ghi của Manager, nên Manager không ghi `das_job_attempts` cho Job này. Test hiện kiểm "Manager không ghi gì" thay cho điều kiện trên.
- **Lựa chọn:**
  - (a) Sửa Req 2.7: chỉ áp dụng cho transition do Manager ghi.
  - (b) Cho Gateway ghi `das_job_attempts` khi cancel. Cách này trái với việc metric Job thuộc về Manager.
- **Đề xuất:** (a).
- **Quyết định:** (a) Sửa Req 2.7: chỉ áp dụng cho transition do Manager ghi.
  - **Ngày:** 2026-10-29
  - **Áp dụng tại:** v2 Req 2.7; Phase2_Design mục "Unit và example test"

### 1.8 [x] Reply drain của Manager không chờ Lease được nhả
- **Hiện trạng:** với `release_leases=true`, Manager trả reply ngay sau khi *yêu cầu* mọi driver nhả Lease. Việc nhả xảy ra ở event boundary kế tiếp; test cho thấy xong trong vòng 5 s. Cách này giữ reply trong giới hạn timeout 5 s của Gateway.
- **Lựa chọn:**
  - (a) Xác nhận cách hiện tại. Operator dùng `das drain --wait` để chờ `drained=true`.
  - (b) Chờ nhả xong rồi mới reply. Có nguy cơ vượt 5 s và nhận 504.
- **Đề xuất:** (a).
- **Quyết định:** (a) Xác nhận cách hiện tại. Operator dùng `das drain --wait` để chờ `drained=true`.
  - **Ngày:** 2026-10-29
  - **Áp dụng tại:** v2 Req 17.5; Phase2_Design §8 (Drain)

---

## 2. Phase 2: deploy

### 2.1 [x] `DAS__ADMIN_API_KEYS` trong service `gateway` của Compose
- **Hiện trạng:** task 18.1 chỉ cho phép thêm biến tracing và `health_port` vào các service v1. Subagent đã thêm `DAS__ADMIN_API_KEYS:` (truyền qua theo tên) vào service `gateway`. Thiếu dòng này thì biến khai báo trong `.env.example` không bao giờ tới được Gateway. Khi biến không được đặt, Admin_API bị tắt, giống v1.
- **Lựa chọn:**
  - (a) Giữ dòng này.
  - (b) Xóa dòng này và ghi trong README cách tự bật Admin_API.
- **Đề xuất:** (a).
- **Quyết định:** (a) Giữ dòng này.
  - **Ngày:** 2026-10-29
  - **Áp dụng tại:** `deploy/docker-compose.yml`; `deploy/.env.example`

---

## 3. Chất lượng code và tooling (cả hai phase)

### 3.1 [x] Có áp dụng cấu hình ruff hay không
- **Hiện trạng:** `pyproject.toml` không có cấu hình ruff. Chạy với rule mặc định của ruff 0.17.3 thì có 196 cảnh báo, gần như toàn bộ là style: BLE001 (51), RUF100 (36), I001 (32), TRY004 (10)… Với nhóm rule lỗi thật (E4/E7/E9/F) chỉ còn 1 lỗi; lỗi này nằm trong file proto sinh tự động (xem 3.3).
- **Lựa chọn:**
  - (a) Thêm `[tool.ruff]` với bộ rule cụ thể rồi sửa theo.
  - (b) Chỉ bật E/F.
  - (c) Không dùng ruff.
- **Đề xuất:** (b) trước, mở rộng dần nếu cần. Ngoài ra nên thêm `ruff` vào dev dependency: trong lần chạy này `.venv` không có ruff, phải chạy qua `uvx`.
- **Quyết định:** (b) Chỉ bật E/F.
  - **Ngày:** 2026-10-29
  - **Áp dụng tại:** `pyproject.toml` (`[tool.ruff]`, `[tool.ruff.lint]`)

### 3.2 [x] mypy
- **Hiện trạng:** `.venv` không có mypy, nên chưa lần nào kiểm kiểu toàn bộ code.
- **Đề xuất:** quyết định có đưa mypy vào dev dependency và CI hay không, kèm mức độ (`--strict` hay cơ bản).
- **Quyết định:** Đưa mypy vào dependency group `dev` và vào Fast_Job của CI, cấu hình cơ bản (không bật `strict`), chỉ áp dụng cho package `das/`.
  - **Ngày:** 2026-10-29
  - **Áp dụng tại:** `pyproject.toml` (`[tool.mypy]`); `.github/workflows/ci.yml`

### 3.3 [x] Import thừa trong code gRPC sinh tự động
- **Hiện trạng:** `das/transport/proto/das_pb2_grpc.py` có `import warnings` không dùng.
- **Lựa chọn:**
  - (a) Loại `das/transport/proto/` khỏi lint.
  - (b) Sửa tay. Sẽ mất khi sinh lại code.
- **Đề xuất:** (a).
- **Quyết định:** (a) Loại `das/transport/proto/` khỏi lint.
  - **Ngày:** 2026-10-29
  - **Áp dụng tại:** `pyproject.toml` (`extend-exclude` của `[tool.ruff]`, `[[tool.mypy.overrides]]` cho `das.transport.proto`)

### 3.4 [x] Test có thể flaky: `test_reconnect_restores_subscriptions`
- **Hiện trạng:** `tests/integration/test_rabbitmq_broker.py::test_reconnect_restores_subscriptions` (Phase 1) fail một lần ở đầu lần chạy, các lần sau đều pass.
- **Đề xuất:** quyết định có theo dõi riêng (chạy lặp, ví dụ `--count 20`) để tìm nguyên nhân hay không.
- **Quyết định:** Tìm nguyên nhân gốc và sửa; theo dõi bằng Repeat_Run_Check (20 lần) trong Integration_Job.
  - **Ngày:** 2026-10-29
  - **Nguyên nhân gốc:** `das/transport/rabbitmq.py::RabbitMQTransport.poll_once`. Nhánh `except Exception as exc` quanh `queue.get(...)` kiểm `isinstance(exc, _CONNECTION_ERRORS | TimeoutError)`; `_CONNECTION_ERRORS` là tuple nên phép `|` raise `TypeError: unsupported operand type(s) for |: 'tuple' and 'type'` ngay trong except. Chuỗi sự kiện: (1) test xóa connection qua `DELETE /api/connections/{name}`; (2) nếu poll task đang chờ `basic.get` khi `Connection.Close` (320 CONNECTION_FORCED) tới, `queue.get` raise `aiormq.exceptions.ConnectionClosed`; (3) phép kiểm trên biến lỗi này thành `TypeError`, lỗi thoát khỏi `RabbitMQTransport._poll_loop` và poll task kết thúc; (4) `RabbitMQTransport._reconnect` mở connection mới và khôi phục đủ 3 subscription (`connected` là True), nhưng không khởi động lại poller (chỉ `subscribe` gọi `_ensure_poller()`); (5) không còn ai `basic.get`, test timeout ở `timed out waiting for delivery after reconnect` (drop thứ nhất) hoặc ở `wait_for` của "after-second" (drop thứ hai). Nếu `Connection.Close` tới lúc poll task đang ở `_idle()` thì không có exception và lần chạy pass, nên test chỉ fail theo xác suất. Bằng chứng: trước khi sửa, chạy `uv run pytest "tests/integration/test_rabbitmq_broker.py::test_reconnect_restores_subscriptions" --count 20 -v` cho 9 failed, 11 passed (macOS 27.0.1, Docker 29.9.2); với `-o log_cli=true --log-cli-level=DEBUG` và `--count 50` cho 36 failed, 14 passed, và cả 36 lần fail có cùng traceback `ConnectionClosed: [Errno 320] CONNECTION_FORCED` → "During handling of the above exception, another exception occurred" → `poll_once: if not isinstance(exc, _CONNECTION_ERRORS | TimeoutError)` → `TypeError`, với poll task đã `done()`, `connected=True`, đủ 3 queue. Tái hiện tất định không cần Docker: tạo `RabbitMQTransport(url="amqp://unused/")`, đặt `_connected = True`, thêm một `Subscription` vào `registry`, gán `_queues[sub.id]` là object có `async def get(**kw): raise aiormq.exceptions.ConnectionClosed(320, "CONNECTION_FORCED")` và `_in_flight[sub.id] = 0`; trên code cũ `await t.poll_once()` raise `TypeError` thay vì trả `False`, và `asyncio.create_task(t._poll_loop())` kết thúc với cùng lỗi sau 0.3 s. Đã loại: G1 (ACK của "before" chưa tới broker), G2 (management API còn liệt kê connection đã đóng), G4 (broker khởi động lạnh); 7/10 phiên pytest với container mới đều fail với dấu hiệu `TypeError` ở trên.
  - **Cách sửa:** `das/transport/rabbitmq.py`: trong `RabbitMQTransport.poll_once` đổi phép kiểm thành `isinstance(exc, (*_CONNECTION_ERRORS, TimeoutError))`; thêm hai lớp phòng vệ: `RabbitMQTransport._reconnect` gọi `_ensure_poller()` sau `registry.restore` khi còn subscription, và `RabbitMQTransport._poll_loop` bọc mỗi vòng `poll_once()` bằng `try/except Exception` (log `rabbitmq poll pass failed`, `CancelledError` vẫn hủy được task). Cách sửa loại bỏ nguyên nhân ở trên vì lỗi connection và `TimeoutError` từ `basic.get` giờ được nuốt và `poll_once` trả `False`, nên poll task không còn chết khi broker đóng connection giữa `basic.get`; nếu poll task kết thúc vì lý do khác, `_reconnect` tạo lại nó. Interface công khai không đổi; `tests/integration/test_rabbitmq_broker.py` giữ nguyên (ba subscription, mọi assertion, không rerun/skip/xfail, không tăng timeout). Thêm 5 unit test trong `tests/unit/test_rabbitmq_transport.py` (connect giả, `basic.get` raise `ConnectionClosed` đúng lúc connection bị đóng rồi reconnect, kiểm delivery tiếp tục); cả 5 fail trên code cũ và pass trên code đã sửa.
  - **Repeat_Run_Check:** 20/20 pass, 2026-10-29, máy local của Developer (macOS 27.0.1 arm64, Docker 29.9.2, image `rabbitmq:4.1-management` RabbitMQ 4.1.8, Python 3.12.15), lệnh `uv run pytest "tests/integration/test_rabbitmq_broker.py::test_reconnect_restores_subscriptions" --count 20 -v`, 204.45 s. Trước khi sửa: 9/20.
  - **Áp dụng tại:** `das/transport/rabbitmq.py`; `tests/integration/test_rabbitmq_broker.py`; `tests/unit/test_rabbitmq_transport.py`; `.github/workflows/ci.yml` (bước Repeat_Run_Check của Integration_Job); README mục Development

### 3.5 [x] Tên file property test lệch với design
- **Hiện trạng:**
  - Property 1 nằm ở `tests/property/test_job_transition_count.py`; bảng file trong design ghi `test_prop_transition_metrics.py`.
  - Property 24 và 25 nằm ở hai file riêng: `test_retention_safety.py` và `test_prop_retention_liveness.py`; design ghi chung `test_prop_retention.py`.
- **Lựa chọn:**
  - (a) Cập nhật bảng trong design theo tên file thực tế.
  - (b) Đổi tên file theo design.
- **Đề xuất:** (a).
- **Quyết định:** (a) Cập nhật bảng trong design theo tên file thực tế.
  - **Ngày:** 2026-10-29
  - **Áp dụng tại:** Phase2_Design bảng "Property tests"; `tests/property/test_job_transition_count.py`; `tests/property/test_retention_safety.py`; `tests/property/test_prop_retention_liveness.py`

---

## 4. Phase 1: điểm còn mở theo spec

### 4.1 [x] Chỉ hỗ trợ một Redis primary
- **Hiện trạng:** design Phase 1 ghi rằng v1 nhắm tới một Redis primary duy nhất; khóa nhiều node kiểu Redlock nằm ngoài phạm vi. Mọi cơ chế lease, fencing, retention và DLQ của Phase 2 đều dựa trên giả định này.
- **Đề xuất:** xác nhận giữ giả định này. Nếu cần HA cho Redis (Sentinel hoặc Cluster), cần một phase riêng, vì các Lua script hiện đụng tới nhiều key không cùng hash slot.
- **Quyết định:** Giữ giả định một Redis primary.
  - **Ngày:** 2026-10-29
  - **Áp dụng tại:** Phase1_Design mục "Redis deployment scope" (ngay sau mục "Redis Lua scripts")

### 4.2 [x] Đồng bộ `delivery-limit` với `max_attempts`
- **Hiện trạng:** policy `das-jobs` trong `deploy/rabbitmq/definitions.json` phải được đặt tay bằng `limits.max_attempts - 1`. Đã có test kiểm tra điều này với file config mẫu, và README có ghi chú.
- **Lựa chọn:**
  - (a) Giữ việc đồng bộ thủ công.
  - (b) Cho agent tự đặt hoặc tự kiểm tra policy khi khởi động.
- **Đề xuất:** (a), trừ khi thường xuyên đổi `max_attempts`.
- **Quyết định:** (a) Giữ việc đồng bộ thủ công.
  - **Ngày:** 2026-10-29
  - **Áp dụng tại:** `tests/unit/test_deploy_static.py::test_delivery_limit_matches_manager_max_attempts`; README mục Configuration

---

## 5. Lưu ý không cần quyết định ngay

- **Thứ tự nâng cấp:** README ghi chỉ dùng Replay và drain sau khi mọi agent đã lên v2. Khi tắt tracing, fleet hỗn hợp v1/v2 vẫn tương thích (đã kiểm bằng e2e với một peer v1 mô phỏng, vì repo không có bản build v1).
- **Redactor che chuỗi con 8 ký tự của khóa:** nếu Admin_API_Key tình cờ chứa chuỗi trùng với nội dung log (ví dụ tên route), phần đó trong log sẽ bị che. Với khóa sinh ngẫu nhiên thì không xảy ra trong thực tế.
- **`FakeClock` và `within()`:** khi test dùng `FakeClock`, mỗi lần gọi `within()` đẩy thời gian giả lên tới mức timeout dù lệnh gọi xong ngay. Test cần thời gian giả chính xác phải tính tới điều này.
