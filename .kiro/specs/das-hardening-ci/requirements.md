# Requirements Document

## Introduction

Tính năng này là Phase 3 "Hardening và CI" của Distributed Agent System (DAS). Phase 1 (spec `.kiro/specs/distributed-agent-system/`) và Phase 2 (spec `.kiro/specs/das-observability-operations/`) đã được triển khai. Phase 3 không thêm tính năng runtime mới. Mục tiêu là:

1. Đóng mọi mục còn mở trong `.kiro/specs/open-decisions.md` (mục 1.1–1.8, 2.1, 3.1–3.5, 4.1, 4.2) theo lựa chọn được đề xuất của từng mục, và cập nhật văn bản spec Phase 2 ở những chỗ lựa chọn đó là sửa spec.
2. Thêm lint bằng ruff (bộ rule E và F) và kiểm tra kiểu bằng mypy (cấu hình cơ bản, không strict); codebase hiện tại phải qua cả hai.
3. Thêm một CI_Pipeline trên GitHub Actions chạy lint, kiểm tra kiểu, import contract và Fast_Test_Suite cho mọi push và pull request; chạy Integration_Test_Suite và E2E_Test_Suite trên nhánh `main` hoặc khi được kích hoạt thủ công.
4. Tìm nguyên nhân và sửa test flaky `tests/integration/test_rabbitmq_broker.py::test_reconnect_restores_subscriptions`.

Quy ước tham chiếu: "v1 Req X.Y" là Requirement X, tiêu chí Y trong spec Phase 1; "v2 Req X.Y" là Requirement X, tiêu chí Y trong spec Phase 2; "OD X.Y" là mục X.Y trong `.kiro/specs/open-decisions.md`. Mọi thuật ngữ của Phase 1 và Phase 2 (Request, Job, Lease, State_Store, Dead_Letter_Record, Admin_API, Drain_Command, ...) giữ nguyên nghĩa đã định nghĩa trong glossary của hai spec đó.

Giả định của tài liệu này:

- Workspace hiện chưa là git repository và chưa có thư mục `.github/`. CI_Pipeline giả định repository sẽ được đặt trên GitHub và dùng GitHub Actions.
- Type_Check áp dụng cho package `das/`, không áp dụng cho `tests/`. Generated_Code được loại khỏi cả Lint_Check và Type_Check.
- Ngoài các sửa đổi cần thiết để qua Lint_Check và Type_Check, Phase 3 không thay đổi hành vi runtime của Gateway_API, Manager_Agent, Worker_Agent, Message_Codec hay Transport (Requirement 11).

Chỗ áp dụng property-based testing: Phase 3 không thêm logic thuần mới. Test bổ sung duy nhất cho runtime (Requirement 3) là test dựa trên fault injection với vài ví dụ cụ thể; tiêu chí "FOR ALL" ở Requirement 11 tái sử dụng các property test hiện có của Phase 1 và Phase 2.

## Glossary

- **Decision_Log**: File `.kiro/specs/open-decisions.md`, liệt kê các quyết định còn mở của Phase 1 và Phase 2, mỗi mục có ô đánh dấu `[ ]` hoặc `[x]`.
- **Decision_Record**: Phần ghi nhận quyết định của một mục trong Decision_Log, gồm lựa chọn đã chọn, ngày quyết định và tham chiếu tới nơi quyết định được áp dụng (tiêu chí spec, file design, test hoặc file deploy).
- **Phase2_Requirements**: File `.kiro/specs/das-observability-operations/requirements.md`.
- **Phase2_Design**: File `.kiro/specs/das-observability-operations/design.md`.
- **Phase1_Design**: File `.kiro/specs/distributed-agent-system/design.md`.
- **Generated_Code**: Mọi file trong thư mục `das/transport/proto/`, được sinh bởi `grpcio-tools`.
- **Ruff**: Công cụ lint Python `ruff`, khai báo trong dependency group `dev` của `pyproject.toml`.
- **Mypy**: Công cụ kiểm tra kiểu tĩnh `mypy`, khai báo trong dependency group `dev` của `pyproject.toml`.
- **Lint_Check**: Lệnh `uv run ruff check` chạy tại thư mục gốc của repository với cấu hình `[tool.ruff]` trong `pyproject.toml`.
- **Type_Check**: Lệnh `uv run mypy` chạy tại thư mục gốc của repository với cấu hình `[tool.mypy]` trong `pyproject.toml`.
- **Import_Contract_Check**: Lệnh `uv run lint-imports` kiểm tra contract import-linter đã có trong `pyproject.toml` (v1 Req 15.4).
- **Fast_Test_Suite**: Lệnh `uv run pytest tests/unit tests/property`, không cần Docker.
- **Integration_Test_Suite**: Lệnh `uv run pytest tests/integration --ignore=tests/integration/e2e`, dùng testcontainers.
- **E2E_Test_Suite**: Lệnh `uv run pytest tests/integration/e2e`, dựng toàn bộ Compose stack với LLM giả.
- **CI_Pipeline**: Tập workflow GitHub Actions trong thư mục `.github/workflows/` của repository.
- **Fast_Job**: Job của CI_Pipeline chạy Lint_Check, Type_Check, Import_Contract_Check và Fast_Test_Suite.
- **Integration_Job**: Job của CI_Pipeline chạy Integration_Test_Suite.
- **E2E_Job**: Job của CI_Pipeline chạy E2E_Test_Suite.
- **Manual_Trigger**: Sự kiện `workflow_dispatch` của GitHub Actions, do người dùng kích hoạt thủ công.
- **Pinned_Action**: Tham chiếu tới một GitHub Action dạng `owner/repo@<commit SHA 40 ký tự hex>`, kèm comment nêu tag phiên bản tương ứng.
- **Flaky_Test**: Test `tests/integration/test_rabbitmq_broker.py::test_reconnect_restores_subscriptions` (kiểm v1 Req 8.6).
- **Repeat_Run_Check**: Việc chạy Flaky_Test 20 lần liên tiếp trong cùng một phiên pytest và cùng một môi trường; check đạt khi cả 20 lần đều pass.
- **Root_Cause_Note**: Đoạn mô tả nguyên nhân gốc của Flaky_Test, ghi trong Decision_Record của OD 3.4.
- **Developer**: Người phát triển chạy các lệnh kiểm tra trên máy local.

## Requirements

### Requirement 1: Ghi nhận quyết định trong Decision_Log

**User Story:** Là developer, tôi muốn mọi mục còn mở của Phase 1 và Phase 2 được quyết định và ghi lại, để không còn chỗ mơ hồ giữa spec và code khi bắt đầu phase sau.

#### Acceptance Criteria

1. THE Decision_Log SHALL có ô `[x]` thay cho ô `[ ]` trong tiêu đề của mỗi mục OD 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 2.1, 3.1, 3.2, 3.3, 3.4, 3.5, 4.1 và 4.2, và SHALL không còn ô `[ ]` nào trong tiêu đề của các mục thuộc phần 1 đến phần 4.
2. THE Decision_Record của mỗi mục OD 1.1–1.8, 2.1, 3.1, 3.3, 3.5, 4.1 và 4.2 SHALL ghi lựa chọn đã chọn trùng với lựa chọn được đề xuất trong Decision_Log, nêu cả ký hiệu của lựa chọn và nội dung của lựa chọn đó như trong Decision_Log: (a) cho OD 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 2.1, 3.3, 3.5 và 4.2; (b) cho OD 3.1; và "giữ giả định một Redis primary" cho OD 4.1. Với OD 1.6, nơi phần đề xuất nêu "(a), hoặc (b)", Decision_Record SHALL ghi đúng một lựa chọn là (a).
3. THE Decision_Record của OD 3.2 SHALL ghi quyết định đưa Mypy vào dependency group `dev` và vào Fast_Job của CI_Pipeline, với cấu hình không bật `strict` và chỉ áp dụng cho package `das/` (Requirement 6).
4. THE Decision_Record của OD 3.4 SHALL ghi Root_Cause_Note, cách sửa đã áp dụng (gồm đường dẫn các file đã sửa và mô tả thay đổi), và kết quả Repeat_Run_Check gồm số lần pass trên tổng 20 lần, ngày chạy và môi trường chạy (máy local của Developer hoặc Integration_Job) (Requirement 10).
5. THE Decision_Record của mỗi mục SHALL tham chiếu tới ít nhất một nơi áp dụng quyết định: số tiêu chí đã sửa trong Phase2_Requirements, mục đã sửa trong Phase2_Design hoặc Phase1_Design, đường dẫn test, hoặc đường dẫn file cấu hình hay file deploy. Tối thiểu: OD 1.1–1.8 tham chiếu số tiêu chí "v2 Req X.Y" đã sửa theo Requirement 2; OD 3.5 tham chiếu bảng file property test trong Phase2_Design; OD 4.1 tham chiếu đoạn đã sửa trong Phase1_Design; OD 2.1 tham chiếu `deploy/docker-compose.yml`; OD 3.1, 3.2 và 3.3 tham chiếu `pyproject.toml`; OD 3.4 tham chiếu `tests/integration/test_rabbitmq_broker.py`; OD 4.2 tham chiếu đường dẫn của test kiểm `delivery-limit` ở Requirement 4. Mỗi đường dẫn được tham chiếu SHALL tồn tại trong repository, và mỗi tiêu chí hay mục được tham chiếu SHALL chứa nội dung của quyết định đó.
6. THE Decision_Log SHALL giữ nguyên từng ký tự phần Hiện trạng, Lựa chọn và Đề xuất của mỗi mục, cùng toàn bộ phần 5 "Lưu ý không cần quyết định ngay", để lý do của quyết định vẫn đọc được; thay đổi duy nhất trong tiêu đề của mỗi mục là `[ ]` thành `[x]`.
7. THE Decision_Log SHALL có đúng một Decision_Record trong mỗi mục OD được liệt kê ở tiêu chí 1, nằm giữa tiêu đề của mục đó và tiêu đề của mục kế tiếp, và mỗi Decision_Record SHALL gồm lựa chọn đã chọn, ngày quyết định dạng `YYYY-MM-DD` và các tham chiếu theo tiêu chí 5.
8. THE Decision_Record của OD 1.5 SHALL tham chiếu tới cả tiêu chí v2 Req 3.8 đã sửa và đường dẫn của test mất reply ở Requirement 3, vì phần đề xuất của OD 1.5 gồm cả việc bổ sung test đó.

### Requirement 2: Sửa spec Phase 2 theo quyết định

**User Story:** Là developer, tôi muốn Phase2_Requirements và Phase2_Design mô tả đúng hành vi đã triển khai, để spec là nguồn tham chiếu đáng tin cho review và test.

#### Acceptance Criteria

1. THE Phase2_Requirements SHALL nêu ở v2 Req 18.12 rằng trace được kiểm qua API v3 của trace backend tại đường dẫn `/api/v3/traces` với tham số `query.service_name=das-gateway`, và SHALL không còn chứa chuỗi `/api/traces?service=das-gateway` ở bất kỳ tiêu chí nào (OD 1.1).
2. THE Phase2_Requirements SHALL nêu ở v2 Req 17.15 rằng Gateway_API trả HTTP status 400 với mã lỗi `invalid_request` và không gửi command nào trong hai trường hợp: `parameter:"body"` khi body của cuộc gọi drain không parse được thành JSON hoặc parse được nhưng không phải JSON object (ví dụ `[]`), và `parameter:"release_leases"` khi body là JSON object có `release_leases` không phải boolean (OD 1.2).
3. THE Phase2_Requirements SHALL nêu ở v2 Req 13.7 rằng Replay một Dead_Letter_Record có attempt number lớn hơn attempt count hiện tại của Job bị từ chối với HTTP status 409, mã lỗi `stale_attempt` và không thay đổi state nào, cùng vị trí trong thứ tự kiểm tra với trường hợp attempt number nhỏ hơn attempt count hiện tại (OD 1.3).
4. THE Phase2_Requirements SHALL nêu ở v2 Req 12.2, hoặc ở một tiêu chí mới thêm vào cuối Requirement 12, rằng hai Message có Dead_Letter_Reason `malformed` với body giống hệt nhau từng byte tạo đúng một Dead_Letter_Record, kể cả khi hai Message đó được publish riêng biệt (OD 1.4).
5. THE Phase2_Requirements SHALL thay ở v2 Req 3.8 mệnh đề "tổng counter trên toàn Manager_Group bằng số Dead_Letter_Record đã tạo" bằng mệnh đề: tổng `das_dlq_records_created_total` trên toàn Manager_Group nhỏ hơn hoặc bằng số Dead_Letter_Record đã tạo, và bằng số đó khi mọi reply của lệnh ghi State_Store tạo Dead_Letter_Record tới được Manager_Agent (OD 1.5).
6. THE Phase2_Requirements SHALL nêu ở v2 Req 3.3 rằng lần acquire đầu tiên trên một Lease do agent v1 ghi (Lease không có trường `last_owner`) được đếm với `kind="initial"`, kể cả khi agent ID của owner trước khác Manager_Agent đang acquire (OD 1.6).
7. THE Phase2_Requirements SHALL nêu ở v2 Req 2.7 rằng histogram `das_job_attempts` chỉ ghi observation cho transition do lệnh ghi State_Store của Manager_Agent tạo ra, SHALL bỏ cụm "hoặc Cancelled trước khi được dispatch" khỏi trường hợp observation bằng 0 và giữ trường hợp Job bị Skipped, và SHALL nêu rằng Job bị Cancelled trước dispatch qua cancel của Gateway_API không tạo observation nào (OD 1.7).
8. THE Phase2_Requirements SHALL nêu ở v2 Req 17.5 rằng với `release_leases=true`, Manager_Agent trả reply cho Drain_Command sau khi yêu cầu mọi driver nhả Lease mà không chờ việc nhả hoàn tất, rằng giới hạn 5 giây cho việc release Lease kể từ khi nhận Drain_Command vẫn giữ nguyên, và rằng operator dùng `das drain --wait` để chờ `drained=true` (OD 1.8).
9. THE bảng file property test trong Phase2_Design SHALL ghi Property 1 ở `tests/property/test_job_transition_count.py`, Property 24 ở `tests/property/test_retention_safety.py`, Property 25 ở `tests/property/test_prop_retention_liveness.py`, và mỗi file được ghi trong bảng cho ba property này SHALL tồn tại trong repository; THE Phase2_Design SHALL không còn chứa chuỗi `test_prop_transition_metrics.py` hay `test_prop_retention.py` (OD 3.5).
10. WHEN một tiêu chí của Phase2_Requirements được sửa theo các tiêu chí 1 đến 8, THE Phase2_Design SHALL được cập nhật ở mọi đoạn mô tả cùng hành vi đó (bao gồm mô tả component, mục error handling, Correctness Properties và Testing Strategy), để không đoạn nào của Phase2_Design mô tả hành vi trái với tiêu chí đã sửa; cụ thể Phase2_Design SHALL không còn chứa chuỗi `/api/traces?service=das-gateway` và SHALL không còn nêu rằng tổng `das_dlq_records_created_total` luôn bằng số Dead_Letter_Record đã tạo.
11. THE Phase2_Requirements SHALL giữ nguyên số thứ tự của mọi Requirement và mọi tiêu chí hiện có, SHALL không xóa tiêu chí nào, và SHALL chỉ thêm tiêu chí mới vào cuối Requirement tương ứng, để các tham chiếu "v2 Req X.Y" trong code, test và tài liệu khác vẫn đúng.

### Requirement 3: Test cho counter dead letter khi mất reply

**User Story:** Là developer, tôi muốn có test cho trường hợp Redis đã ghi Dead_Letter_Record nhưng reply bị mất, để hành vi "counter nhỏ hơn hoặc bằng số record" được kiểm chứ không chỉ được ghi trong spec.

#### Acceptance Criteria

1. THE Fast_Test_Suite SHALL có một test fault injection mô phỏng kịch bản sau: State_Store thực thi xong lệnh ghi tạo Dead_Letter_Record cho một Message, reply của lệnh ghi đó bị chặn để Manager_Agent nhận lỗi timeout, sau đó Message được giao lại. Test SHALL kiểm rằng Manager_Agent đã thật sự nhận lỗi timeout ở lần ghi đầu, và SHALL fail nếu fault injection không có hiệu lực, tức là lệnh ghi không được thực thi hoặc reply không bị chặn.
2. WHEN mỗi trường hợp của kịch bản ở tiêu chí 1 kết thúc, THE test SHALL kiểm rằng số Dead_Letter_Record của Message đó trong State_Store bằng đúng 1.
3. WHEN mỗi trường hợp của kịch bản ở tiêu chí 1 kết thúc, THE test SHALL kiểm rằng tổng `das_dlq_records_created_total` trên mọi Manager_Agent tham gia trường hợp đó nhỏ hơn hoặc bằng 1, với giá trị counter đọc sau khi lần giao lại đã được Manager_Agent xử lý xong.
4. THE test ở tiêu chí 1 SHALL nằm trong `tests/unit` hoặc `tests/property` để thuộc Fast_Test_Suite. Test SHALL chạy được khi không có Docker và không có Redis hay RabbitMQ thật, và SHALL hoàn tất cả hai trường hợp ở tiêu chí 5 cùng trường hợp đối chứng ở tiêu chí 6 trong tối đa 10 giây.
5. THE test ở tiêu chí 1 SHALL chạy kịch bản thành hai trường hợp riêng: (a) Message được giao lại cho chính Manager_Agent đã nhận timeout; (b) Message được giao lại cho một Manager_Agent khác dùng chung State_Store đó.
6. WHEN reply của lệnh ghi tạo Dead_Letter_Record tới được Manager_Agent mà không có fault injection, THE test SHALL kiểm rằng State_Store chứa đúng 1 Dead_Letter_Record cho Message đó và tổng `das_dlq_records_created_total` bằng đúng 1. Trường hợp đối chứng này kiểm vế "bằng" của v2 Req 3.8 và ngăn assertion ở tiêu chí 3 pass khi counter không bao giờ tăng.

### Requirement 4: Xác nhận quyết định về deploy và giả định Phase 1

**User Story:** Là operator, tôi muốn các giả định về Redis, RabbitMQ và Compose được ghi rõ trong tài liệu, để biết giới hạn của hệ thống khi triển khai.

#### Acceptance Criteria

1. THE service `gateway` trong `deploy/docker-compose.yml` SHALL tiếp tục khai báo `DAS__ADMIN_API_KEYS` trong mục `environment` dưới dạng truyền theo tên (chỉ có tên biến, không có giá trị literal và không có giá trị mặc định), để giá trị chỉ đến từ môi trường hoặc `deploy/.env` của operator (OD 2.1).
2. THE `deploy/.env.example` SHALL tiếp tục khai báo `DAS__ADMIN_API_KEYS` với giá trị rỗng, để Admin_API ở trạng thái tắt khi operator không đặt giá trị cho biến này (OD 2.1).
3. THE Phase1_Design SHALL có một đoạn nêu ba điểm: (a) DAS chỉ hỗ trợ đúng một Redis primary; (b) Redis Sentinel và Redis Cluster nằm ngoài phạm vi, vì các Lua script truy cập nhiều key không cùng hash slot; (c) muốn có HA cho Redis thì cần một phase riêng (OD 4.1).
4. THE README.md SHALL tiếp tục nêu, trong mục mô tả cấu hình, rằng khi đổi `limits.max_attempts` thì operator phải sửa tay `delivery-limit` của policy `das-jobs` trong `deploy/rabbitmq/definitions.json` thành `limits.max_attempts - 1` (OD 4.2).
5. WHEN Fast_Test_Suite chạy, THE test hiện có `tests/unit/test_deploy_static.py::test_delivery_limit_matches_manager_max_attempts` SHALL được thu thập, SHALL chạy mà không bị đánh dấu skip hay xfail, và SHALL pass, với file config mẫu là `deploy/config/manager.yaml` và file definitions là `deploy/rabbitmq/definitions.json`.
6. IF `delivery-limit` của policy `das-jobs` trong `deploy/rabbitmq/definitions.json` khác giá trị `limits.max_attempts - 1` lấy từ `deploy/config/manager.yaml`, hoặc policy `das-jobs` không có trong file definitions, THEN THE test ở tiêu chí 5 SHALL fail, và Fast_Job SHALL kết thúc với trạng thái failure.

### Requirement 5: Lint bằng Ruff

**User Story:** Là developer, tôi muốn một lint check bắt lỗi thật (tên chưa định nghĩa, import thừa, lỗi cú pháp), để lỗi được phát hiện trước khi chạy test.

#### Acceptance Criteria

1. THE `pyproject.toml` SHALL khai báo Ruff trong dependency group `dev` với đúng một phiên bản ghim chính xác (`==`), và `uv.lock` SHALL chứa đúng phiên bản Ruff đó, để `uv sync --locked` cài được Ruff mà không báo lỗi lock không khớp.
2. THE `pyproject.toml` SHALL có mục `[tool.ruff]` đặt `target-version` là `py312` và đặt `line-length` tường minh là một số nguyên trong khoảng 88 đến 120, và mục `[tool.ruff.lint]` SHALL đặt `select` gồm đúng hai bộ rule `E` và `F`, không có bộ rule nào khác.
3. THE cấu hình Ruff SHALL loại Generated_Code khỏi Lint_Check (OD 3.3), sao cho một vi phạm rule `E` hoặc `F` nằm trong một file của Generated_Code không làm Lint_Check báo vi phạm hay kết thúc với exit code khác 0.
4. WHEN Developer chạy Lint_Check tại thư mục gốc của repository, THE Lint_Check SHALL kiểm mọi file `.py` trong `das/` và `tests/` ngoài Generated_Code, kết thúc với exit code 0, không báo vi phạm nào và không sửa nội dung file nào.
5. IF một dòng code cần bỏ qua một rule, THEN THE dòng đó SHALL dùng `# noqa: <mã rule>` với ít nhất một mã rule cụ thể (ví dụ `# noqa: F401`), và codebase ngoài Generated_Code SHALL không chứa comment `# noqa` nào không kèm mã rule.
6. THE cấu hình Ruff SHALL không chứa danh sách `ignore` toàn cục và không chứa mục `per-file-ignores` nào bỏ qua một rule thuộc bộ `F`.
7. IF một file trong `das/` hoặc `tests/` (ngoài Generated_Code) có vi phạm rule `E` hoặc `F`, THEN THE Lint_Check SHALL kết thúc với exit code khác 0 và nêu đường dẫn file, số dòng và mã rule của từng vi phạm.
8. IF một file trong `das/` hoặc `tests/` (ngoài Generated_Code) có lỗi cú pháp Python 3.12, THEN THE Lint_Check SHALL kết thúc với exit code khác 0 và nêu đường dẫn file và số dòng của lỗi cú pháp.

### Requirement 6: Kiểm tra kiểu bằng Mypy

**User Story:** Là developer, tôi muốn kiểm tra kiểu tĩnh cho package `das/`, để lỗi sai kiểu giữa các module được phát hiện trước khi chạy.

#### Acceptance Criteria

1. THE `pyproject.toml` SHALL khai báo Mypy trong dependency group `dev` với phiên bản ghim chính xác (`==`), cùng một stub package ghim chính xác (`==`) cho mỗi thư viện bên thứ ba mà `das/` import và có stub package được phát hành riêng (ví dụ `types-PyYAML` cho `yaml`), và `uv.lock` SHALL được cập nhật tương ứng, để `uv sync --locked` cài được đúng các phiên bản đó mà không báo lỗi lock không khớp.
2. THE `pyproject.toml` SHALL có mục `[tool.mypy]` đặt `python_version` là 3.12, đặt `files` chỉ gồm package `das` (không gồm `tests/`), bật `warn_unused_ignores`, và không bật `strict`.
3. THE cấu hình Mypy SHALL loại Generated_Code khỏi Type_Check hoặc đặt `ignore_errors` cho các module trong Generated_Code, sao cho output của Type_Check không chứa lỗi nào có đường dẫn file nằm trong `das/transport/proto/`.
4. WHERE một thư viện bên thứ ba mà `das/` import không có type hint và không có stub package được phát hành, THE cấu hình Mypy SHALL đặt `ignore_missing_imports` cho riêng module của thư viện đó qua một mục `[[tool.mypy.overrides]]`, và SHALL không đặt `ignore_missing_imports` ở mức toàn cục.
5. WHEN Developer chạy Type_Check tại thư mục gốc của repository sau `uv sync --locked`, THE Type_Check SHALL kết thúc với exit code 0, không báo lỗi nào, và báo đã kiểm mọi file `.py` trong `das/` ngoài Generated_Code.
6. WHERE một dòng code cần bỏ qua một lỗi kiểu, THE dòng đó SHALL dùng `# type: ignore[<mã lỗi>]` với mã lỗi cụ thể, và cấu hình Mypy SHALL bật mã lỗi `ignore-without-code`, để một `# type: ignore` không có mã lỗi làm Type_Check báo lỗi.
7. IF một module trong `das/` (ngoài Generated_Code) có lỗi kiểu theo cấu hình ở tiêu chí 2, THEN THE Type_Check SHALL kết thúc với exit code khác 0 và nêu đường dẫn file, số dòng và mã lỗi của từng lỗi.
8. THE cấu hình Mypy SHALL không đặt `ignore_errors` cho module nào ngoài Generated_Code, SHALL không đặt `follow_imports` là `skip` hoặc `silent` ở mức toàn cục, và SHALL không đặt `ignore_missing_imports` hay `ignore_errors` cho bất kỳ module nào trong package `das` ngoài Generated_Code.

### Requirement 7: CI_Pipeline cho mọi push và pull request

**User Story:** Là developer, tôi muốn mọi thay đổi được kiểm tự động bằng lint, kiểm tra kiểu, import contract và test nhanh, để lỗi không lọt vào nhánh chính.

#### Acceptance Criteria

1. WHEN một commit được push lên bất kỳ nhánh nào của repository, hoặc một pull request được mở, được push thêm commit hay được mở lại, THE CI_Pipeline SHALL chạy Fast_Job trên commit đầu của nhánh hoặc của pull request đó.
2. THE Fast_Job SHALL chạy Lint_Check, Type_Check, Import_Contract_Check và Fast_Test_Suite theo đúng thứ tự này, mỗi lệnh là một step riêng có tên nêu lệnh đó, để log và trạng thái pass/fail của từng lệnh xem được riêng trong giao diện GitHub Actions.
3. WHEN bước cài dependency của Fast_Job kết thúc với trạng thái success, THE Fast_Job SHALL chạy cả bốn step ở tiêu chí 2, kể cả khi một step đứng trước kết thúc với exit code khác 0, để mọi lỗi của một lần chạy được báo cùng lúc.
4. IF bất kỳ lệnh nào ở tiêu chí 2 kết thúc với exit code khác 0, THEN THE Fast_Job SHALL kết thúc với trạng thái failure, và step của lệnh đó SHALL được đánh dấu failure.
5. THE Fast_Job SHALL chạy trên runner GitHub-hosted Ubuntu với Python phiên bản 3.12.x, và SHALL cài dependency, gồm cả dependency group `dev`, bằng `uv sync --locked` để dùng đúng các phiên bản trong `uv.lock`.
6. IF `uv.lock` không có trong repository hoặc không khớp với dependency khai báo trong `pyproject.toml`, THEN THE Fast_Job SHALL kết thúc với trạng thái failure ở bước cài dependency, SHALL không chạy step nào ở tiêu chí 2, và SHALL không sửa `uv.lock`.
7. THE Fast_Job SHALL có giới hạn thời gian chạy (`timeout-minutes`) là 15 phút; IF Fast_Job chạy quá 15 phút, THEN THE CI_Pipeline SHALL dừng Fast_Job và Fast_Job SHALL không được báo trạng thái success.
8. WHEN một commit mới được push lên cùng nhánh, hoặc lên nhánh nguồn của cùng pull request, trong khi một lần chạy CI_Pipeline cũ do cùng loại sự kiện (push hoặc pull request) của nhánh đó còn đang chạy, THE CI_Pipeline SHALL hủy lần chạy cũ, đặt trạng thái lần chạy cũ là cancelled, và SHALL tiếp tục lần chạy cho commit mới.
9. WHEN Fast_Job kết thúc, THE CI_Pipeline SHALL báo trạng thái của Fast_Job (success, failure hoặc cancelled) như một status check trên commit tương ứng, và trên pull request nếu lần chạy được kích hoạt bởi pull request.

### Requirement 8: CI_Pipeline cho integration và e2e test

**User Story:** Là developer, tôi muốn integration test và e2e test chạy tự động trên nhánh `main` và chạy được khi cần, để phát hiện lỗi tích hợp với RabbitMQ, Redis và gRPC mà không làm chậm mọi pull request.

#### Acceptance Criteria

1. WHEN một commit được push lên nhánh `main` (bao gồm commit tạo ra khi merge một pull request vào `main`), THE CI_Pipeline SHALL chạy Fast_Job, Integration_Job và E2E_Job trên commit đó.
2. WHEN người dùng kích hoạt CI_Pipeline bằng Manual_Trigger trên một nhánh bất kỳ, THE CI_Pipeline SHALL chạy Fast_Job, Integration_Job và E2E_Job trên commit đầu (HEAD) của nhánh đó tại thời điểm kích hoạt.
3. WHEN Fast_Job của một lần chạy được kích hoạt theo tiêu chí 1 hoặc tiêu chí 2 kết thúc với trạng thái success, THE CI_Pipeline SHALL bắt đầu Integration_Job và E2E_Job của cùng lần chạy đó; hai job này SHALL không phụ thuộc vào nhau, để kết quả failure của job này không ngăn hay hủy job kia.
4. THE Integration_Job SHALL chạy Integration_Test_Suite trên runner `ubuntu` có Docker, với Python 3.12 và cài dependency bằng `uv sync --locked` như Fast_Job (Requirement 7, tiêu chí 5).
5. THE E2E_Job SHALL chạy E2E_Test_Suite trên runner `ubuntu` có Docker và Docker Compose phiên bản 2.30 trở lên; IF phiên bản Docker Compose trên runner thấp hơn 2.30 hoặc không xác định được, THEN THE E2E_Job SHALL kết thúc với trạng thái failure trước khi chạy E2E_Test_Suite, với log nêu phiên bản Docker Compose tìm thấy.
6. IF Integration_Test_Suite hoặc E2E_Test_Suite kết thúc với exit code khác 0, hoặc báo ít nhất một test bị skip với lý do Docker không có sẵn, THEN THE job tương ứng SHALL kết thúc với trạng thái failure, để việc thiếu Docker trên runner không bị che bởi kết quả pass.
7. THE Integration_Job SHALL có `timeout-minutes` là 30 phút và THE E2E_Job SHALL có `timeout-minutes` là 30 phút, để job chạy quá 30 phút bị GitHub Actions dừng và không được báo là success.
8. IF E2E_Job kết thúc với trạng thái failure hoặc bị dừng do vượt `timeout-minutes`, THEN THE E2E_Job SHALL lưu log của mọi container trong Compose stack, kể cả container đã dừng, làm artifact của lần chạy đó.
9. IF lần chạy CI_Pipeline được kích hoạt bởi một pull request hoặc bởi một push lên nhánh khác `main`, THEN THE CI_Pipeline SHALL chỉ chạy Fast_Job và SHALL không chạy Integration_Job và E2E_Job trong lần chạy đó.
10. IF Fast_Job của một lần chạy kết thúc với trạng thái khác success (failure, cancelled hoặc vượt `timeout-minutes`), THEN THE CI_Pipeline SHALL không bắt đầu Integration_Job và E2E_Job của lần chạy đó, và SHALL báo hai job này với trạng thái skipped.
11. WHEN E2E_Job kết thúc với bất kỳ trạng thái nào (success, failure hoặc bị dừng do vượt `timeout-minutes`), THE E2E_Job SHALL dừng và xóa Compose stack cùng mọi volume của stack đó, sau bước lưu log ở tiêu chí 8 nếu bước đó được chạy.

### Requirement 9: Tính tái lập và an toàn của CI_Pipeline

**User Story:** Là developer, tôi muốn CI_Pipeline cho kết quả giống nhau giữa các lần chạy và không bị thay đổi ngầm bởi bên thứ ba, để lỗi CI phản ánh đúng thay đổi trong code.

#### Acceptance Criteria

1. THE CI_Pipeline SHALL tham chiếu mọi GitHub Action trong mọi giá trị `uses:` của mọi file workflow dưới dạng Pinned_Action, và SHALL không có giá trị `uses:` nào trỏ tới GitHub Action bằng tag, tên nhánh hoặc commit SHA ngắn hơn 40 ký tự hex.
2. THE CI_Pipeline SHALL cài uv với một phiên bản ghim chính xác dạng `X.Y.Z` (không dùng `latest`, khoảng phiên bản hay ký tự đại diện), và SHALL dùng cùng phiên bản uv đó cho Fast_Job, Integration_Job và E2E_Job.
3. THE CI_Pipeline SHALL cache thư mục cache của uv với cache key chứa hệ điều hành của runner và hash của `uv.lock`, để cache key đổi khi nội dung `uv.lock` đổi và giữ nguyên khi nội dung `uv.lock` giữ nguyên.
4. IF cache của uv không có sẵn cho cache key hiện tại hoặc việc khôi phục cache thất bại, THEN THE job tương ứng SHALL tiếp tục cài dependency bằng `uv sync --locked` từ đầu, và SHALL không kết thúc với trạng thái failure chỉ vì thiếu cache.
5. THE CI_Pipeline SHALL đặt quyền `GITHUB_TOKEN` ở mức `contents: read` cho mọi job, và SHALL không cấp cho job nào quyền `GITHUB_TOKEN` nào khác ngoài `contents: read`.
6. THE CI_Pipeline SHALL chạy được đến khi Fast_Job, Integration_Job và E2E_Job kết thúc với trạng thái success mà không tham chiếu tới secret nào của repository hay organization, kể cả khi lần chạy được kích hoạt bởi pull request từ fork.
7. THE E2E_Job SHALL dùng LLM giả đã có trong E2E_Test_Suite thay cho mọi LLM provider thật, và SHALL chỉ dùng giá trị credential giả được ghi trực tiếp trong file workflow hoặc file test và chỉ hợp lệ trong Compose stack của test.
8. WHEN Fast_Job chạy Fast_Test_Suite, THE CI_Pipeline SHALL ghi giá trị seed Hypothesis của lần chạy vào log của step Fast_Test_Suite, và SHALL không khôi phục thư mục `.hypothesis` từ lần chạy trước, để lệnh `uv run pytest tests/unit tests/property --hypothesis-seed=<seed lấy từ log>` trên máy local chạy với cùng seed.
9. IF một property test trong Fast_Test_Suite fail, THEN THE log của step Fast_Test_Suite SHALL chứa tên test bị fail, giá trị seed Hypothesis của lần chạy và falsifying example mà Hypothesis báo.

### Requirement 10: Sửa Flaky_Test

**User Story:** Là developer, tôi muốn Flaky_Test pass ổn định, để kết quả fail của Integration_Job luôn chỉ ra lỗi thật.

#### Acceptance Criteria

1. THE Decision_Record của OD 3.4 SHALL chứa Root_Cause_Note gồm: (a) vị trí của nguyên nhân gốc, ghi bằng đường dẫn file và tên hàm, trong code của Flaky_Test hoặc trong code của RabbitMQ Transport adapter; (b) mô tả chuỗi sự kiện dẫn tới lần fail của Flaky_Test; (c) ít nhất một bằng chứng cho nguyên nhân đó, là đoạn log, trace hoặc danh sách bước tái hiện mà Developer làm theo được trên máy local.
2. THE Root_Cause_Note SHALL nêu cách sửa đã áp dụng loại bỏ nguyên nhân gốc ở mục (a) của tiêu chí 1 như thế nào, để người đọc đối chiếu được cách sửa với nguyên nhân.
3. WHEN Developer chạy Repeat_Run_Check trên máy local sau khi cách sửa Flaky_Test được áp dụng, THE Flaky_Test SHALL pass cả 20 lần, và THE Decision_Record của OD 3.4 SHALL ghi số lần pass trên tổng 20 lần và ngày chạy của kết quả đó.
4. WHEN Integration_Job chạy, THE Integration_Job SHALL chạy Repeat_Run_Check trong một step riêng, tách khỏi step chạy Integration_Test_Suite, và cả Repeat_Run_Check lẫn Integration_Test_Suite SHALL kết thúc trong giới hạn `timeout-minutes` 30 phút của Integration_Job.
5. IF có ít nhất 1 trong 20 lần chạy của Repeat_Run_Check fail, THEN THE Integration_Job SHALL kết thúc với trạng thái failure, và log của step đó SHALL cho thấy những lần chạy nào đã fail.
6. THE cách sửa Flaky_Test SHALL giữ nguyên điều mà Flaky_Test kiểm (v1 Req 8.6: sau khi mất kết nối, adapter kết nối lại và khôi phục mọi subscription). THE cách sửa SHALL không dùng cơ chế tự chạy lại test khi fail, không đánh dấu Flaky_Test là skip hay xfail, không xóa assertion nào của Flaky_Test, và không giảm số subscription mà Flaky_Test kiểm là đã được khôi phục.
7. IF nguyên nhân gốc nằm trong code của RabbitMQ Transport adapter thay vì trong test, THEN THE cách sửa SHALL chỉ thay đổi code bên trong module của RabbitMQ Transport adapter, SHALL giữ nguyên interface công khai mà các module khác dùng, và mọi test hiện có của Fast_Test_Suite và Integration_Test_Suite SHALL tiếp tục pass.
8. THE README.md SHALL nêu một lệnh chạy Repeat_Run_Check trên máy local mà Developer chạy được nguyên văn, không cần sửa lệnh, và lệnh đó SHALL kết thúc với exit code khác 0 khi có ít nhất 1 trong 20 lần chạy fail.

### Requirement 11: Giữ nguyên hành vi runtime

**User Story:** Là operator, tôi muốn Phase 3 không thay đổi hành vi của hệ thống đang chạy, để có thể nâng cấp mà không cần kiểm lại API hay giao thức Message.

#### Acceptance Criteria

1. THE Phase 3 SHALL giữ nguyên mọi route HTTP (method và đường dẫn), status code, mã lỗi và body response của Gateway_API và Admin_API so với codebase trước sửa đổi đầu tiên của Phase 3 (gọi là baseline); các tiêu chí của Phase2_Requirements được sửa theo Requirement 2 chỉ mô tả lại hành vi đã có trong baseline và SHALL không dẫn tới thay đổi code của Gateway_API hay Admin_API.
2. WHEN Message_Codec serialize cùng một Message đầu vào, THE Phase 3 SHALL cho ra chuỗi bytes giống hệt từng byte với chuỗi bytes do Message_Codec của baseline tạo ra, với cùng định dạng envelope và cùng schema version; WHEN Message_Codec deserialize một chuỗi bytes do baseline tạo ra, THE Phase 3 SHALL trả về Message bằng với Message mà baseline trả về.
3. THE Phase 3 SHALL giữ nguyên so với baseline: tên, tập label và ý nghĩa của mọi metric; tên, kiểu và giá trị mặc định của mọi trường Agent_Config; tên và cách diễn giải giá trị của mọi biến môi trường `DAS__`; và giá trị số cùng điều kiện phát sinh của mọi exit code.
4. WHERE một sửa đổi trong `das/` là cần thiết để qua Lint_Check hoặc Type_Check, THE sửa đổi đó SHALL chỉ thuộc một trong các loại: xóa import hoặc biến không dùng, thêm hoặc sửa type annotation, thu hẹp kiểu bằng kiểm tra tường minh (`isinstance`, so sánh với `None`, `assert`), hoặc sửa một lỗi mà Lint_Check hay Type_Check chỉ ra; mọi sửa đổi thuộc bất kỳ loại nào SHALL vẫn thỏa tiêu chí 1, 2 và 3.
5. IF một lỗi được sửa theo tiêu chí 4 làm thay đổi hành vi quan sát được so với baseline, THEN THE mô tả của task tương ứng SHALL ghi đường dẫn file, số dòng, mã rule của Ruff hoặc mã lỗi của Mypy, hành vi trước khi sửa và hành vi sau khi sửa.
6. WHEN mọi sửa đổi của Phase 3 hoàn tất, THE mọi property test của Phase 1 và Phase 2 có trong `tests/property/` ở baseline SHALL vẫn tồn tại, SHALL pass, và SHALL có giá trị `max_examples` của Hypothesis lớn hơn hoặc bằng giá trị đã cấu hình ở baseline (property giữ nguyên hành vi).
7. WHEN mọi sửa đổi của Phase 3 hoàn tất, THE Fast_Test_Suite, Integration_Test_Suite, E2E_Test_Suite và Import_Contract_Check SHALL kết thúc với exit code 0, số test được pytest thu thập trong mỗi suite SHALL lớn hơn hoặc bằng số test thu thập ở baseline, và số test bị skip hoặc xfail trong mỗi suite SHALL nhỏ hơn hoặc bằng số đó ở baseline.
8. THE mọi contract import-linter có trong `pyproject.toml` ở baseline SHALL được giữ nguyên tên, loại contract, danh sách layer hoặc module và các ngoại lệ đã khai báo; Phase 3 SHALL không xóa contract nào và không thêm ngoại lệ nào vào các contract đó.

### Requirement 12: Tài liệu cho Developer

**User Story:** Là developer, tôi muốn README nêu đủ các lệnh kiểm tra mà CI chạy, để có thể tái hiện kết quả CI trên máy local trước khi push.

#### Acceptance Criteria

1. THE mục Development của README.md SHALL liệt kê, mỗi lệnh trong một code block riêng, lệnh cài dependency `uv sync --locked` và lệnh của Lint_Check, Type_Check, Import_Contract_Check, Fast_Test_Suite, Integration_Test_Suite, E2E_Test_Suite và Repeat_Run_Check, với văn bản lệnh giống hệt lệnh mà step tương ứng trong CI_Pipeline chạy.
2. THE mục Development của README.md SHALL nêu rằng Integration_Test_Suite, E2E_Test_Suite và Repeat_Run_Check cần Docker đang chạy trên máy local, rằng E2E_Test_Suite cần Docker Compose phiên bản 2.30 trở lên, và rằng Lint_Check, Type_Check, Import_Contract_Check và Fast_Test_Suite chạy được không cần Docker.
3. THE README.md SHALL mô tả từng job Fast_Job, Integration_Job và E2E_Job của CI_Pipeline, gồm: các lệnh job đó chạy; sự kiện kích hoạt job (push lên bất kỳ nhánh nào và pull request cho Fast_Job; push lên `main` và Manual_Trigger cho Integration_Job và E2E_Job); và việc Integration_Job và E2E_Job chỉ chạy sau khi Fast_Job của cùng lần chạy kết thúc với trạng thái success.
4. THE README.md SHALL mô tả các bước kích hoạt Manual_Trigger trên giao diện GitHub, gồm tên workflow cần chọn và bước chọn nhánh để chạy.
5. THE README.md SHALL nêu rằng Generated_Code (thư mục `das/transport/proto/`) được loại khỏi cả Lint_Check và Type_Check, và SHALL nêu lệnh để sinh lại Generated_Code bằng `grpcio-tools`.
6. THE README.md SHALL mô tả cách lấy seed Hypothesis từ log của một lần chạy Fast_Job bị fail và lệnh để chạy lại Fast_Test_Suite trên máy local với seed đó (Requirement 9, tiêu chí 8 và 9).
7. IF một lệnh trong một step của CI_Pipeline được thêm, đổi hoặc xóa, THEN THE README.md SHALL được cập nhật trong cùng thay đổi đó, để mọi lệnh kiểm tra trong CI_Pipeline xuất hiện nguyên văn trong mục Development của README.md và README.md không chứa lệnh kiểm tra nào mà CI_Pipeline đã bỏ.
