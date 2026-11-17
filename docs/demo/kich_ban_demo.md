# Demo kỹ thuật pipeline và vận hành — 6 phút 40 giây

Bản kịch bản A6 cập nhật ngày 17/11/2026, dữ liệu tới batch 17/11 và run
`acceptance_A_20261117_r2`. A3 đã nghiệm thu 66/66 check PASS. Video dùng kết
quả đã chạy trước, không nộp pipeline mới trong lúc quay. Các lượt thao tác
tự động và video được ghi ở phần cuối; phần nói vẫn cần Hoàng tự diễn tập.

## Chuẩn bị trước khi bắt đầu

Ở thư mục repo, PowerShell với `.venv` đã cài dependencies. Nếu A3 đã chạy
nghiệm thu, dùng run mới trong JSON. Hiện dùng run 17/11 đã nghiệm thu và mở
read mode:

```powershell
.\.venv\Scripts\python.exe -m src.orchestration.environment read
docker compose --profile orchestration up -d --wait --wait-timeout 180 airflow
```

Lệnh trên mở UI và không nộp run mới. DAG cần giữ paused trong lúc minh họa;
không bật lịch khi đang chờ CSV. Lượt chạy pipeline và fault probe mới thuộc
A3, thực hiện riêng khi dữ liệu đã chốt.

Mở Airflow localhost:8082, Superset localhost:8088, MinIO localhost:9001;
đăng nhập sẵn. Chọn run đúng trong `docs/validation/integration_airflow_run.json`
và run lỗi ở `airflow_failure_gate.json`. Chuẩn bị SQL Lab kết nối Trino.

## Các cảnh quay và lời trình bày

| Thời gian | Màn hình và thao tác | Nội dung nói / đầu ra cần thấy |
|---|---|---|
| 0:00–0:45 | Sơ đồ `docs/architecture.md` | CSV → Bronze Parquet trên MinIO → Silver/Gold Iceberg → Trino → Superset. Airflow điều phối; PostgreSQL lưu metadata ứng dụng, SQLite là catalog hiện tại. |
| 0:45–1:25 | MinIO, bucket Bronze, nguồn và batch thật | Chỉ file Parquet; giải thích phân nguồn/batch, manifest SHA256 giúp truy nguyên và phát hiện CSV cũ bị sửa. |
| 1:25–2:25 | Airflow Graph, run thành công đã chuẩn bị | Chỉ nhóm acquisition/Bronze/Silver/Gold và task verify. Đọc số task, thời gian trực tiếp từ run. Giải thích input đầy đủ rồi mới đi tiếp. |
| 2:25–3:15 | Airflow Graph của run fault probe | Chỉ task lỗi đầu tiên và downstream `upstream_failed`; đây là thử nghiệm cố ý với batch không có, không sửa trạng thái task bằng tay. |
| 3:15–4:20 | SQL Lab, truy vấn lifecycle bên dưới | Đọc một kết quả thật. “Vắng hai batch” là vắng quan sát trong mẫu crawl, không phải đã bán; historical `NOT_APPLICABLE`. |
| 4:20–5:40 | SQL Lab, sandbox Iceberg | Chỉ snapshot ban đầu và dữ liệu hiện tại sau MERGE; id=1 đổi 100→120, thêm id=3, time travel vẫn đọc hai dòng cũ. |
| 5:40–6:20 | Dashboard tổng quan | Đọc KPI thật, chọn một tỉnh, Apply; chỉ kết quả thay đổi. Nhận xét tĩnh đầu trang được tính lúc provision. |
| 6:20–6:40 | Giữ dashboard | Tóm tắt khả năng truy nguyên, lịch sử, kiểm định; nêu Batdongsan xuất thủ công và local tách ghi Spark/đọc Trino. |

SQL lifecycle:

```sql
SELECT source, lifecycle_status, COUNT(*) AS so_tin
FROM iceberg.gold.fact_listing_lifecycle
GROUP BY source, lifecycle_status
ORDER BY source, lifecycle_status;
```

Trước khi quay, lấy `demo_table` và `original_snapshot_id` từ
`docs/validation/iceberg_features.json`. Chuyển prefix `lakehouse` thành
`iceberg` khi dùng Trino; giữ nguyên namespace `smoke_test` và tên bảng.

```sql
-- Thay <table> bằng tên sandbox trong JSON, <snapshot_id> bằng ID trong JSON.
SELECT * FROM iceberg.smoke_test.<table> ORDER BY id;
SELECT * FROM iceberg.smoke_test.<table>
FOR VERSION AS OF <snapshot_id> ORDER BY id;
```

Không chạy lại `MERGE` vào bảng nghiệp vụ để minh họa. Nếu cần tạo sandbox mới,
chuyển write mode khi không có job đang chạy, dùng
`./scripts/run_spark.ps1 src/quality/demo_iceberg_features.py`, rồi chuyển read
mode. Script tự tạo tên sandbox mới và SQL Spark đã thực thi trong
`docs/demo/iceberg_features.sql`; cú pháp time travel Spark khác Trino.

## Diễn tập và phương án dự phòng

Thực hiện hai lượt bấm tab/query/filter, ghi thời lượng và các lỗi đã sửa vào
ghi chú diễn tập thực tế. Quay một video trọn vẹn sau lượt thứ hai; lưu đường
dẫn/video cho C, không commit video nặng vào repo. Nếu dịch vụ lỗi khi bảo vệ,
chiếu bản quay và nói rõ đó là kết quả đã ghi trước.

Đã có công cụ `scripts/record_demo.py` để đăng nhập trước khi quay, sau đó
ghi màn hình browser, thực thi SELECT lifecycle và time travel trên dữ liệu
thật, xem hai run đã lưu và đổi bộ lọc dashboard. Không quay phần nhập mật
khẩu, không kích hoạt DAG hoặc ghi bảng Silver/Gold. Cài dependency riêng:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-demo.txt
.\.venv\Scripts\python.exe scripts/record_demo.py --output outputs/demo_luot_moi --hold-seconds 6
```

Mỗi lần gọi tạo hai lượt thao tác; output phải là thư mục mới để giữ video
cũ. Công cụ lấy run/batch/snapshot từ JSON và đọc số hàng lifecycle, KPI mặc
định/TP.HCM từ Trino. Lượt hiện tại có 15 hàng lifecycle, 5 hàng time travel,
KPI 133.495 → 84.639. Kết quả này chỉ chứng minh luồng thao tác tự động;
không thay cho hai lần Hoàng trình bày có bấm giờ.

Kết quả thực thi sau A3 ngày 17/11: hai lượt tự động PASS trong **71,54 giây**
và **70,56 giây**. Các video browser đã finalize, kích thước 1600×900, được kiểm
tra khung hình graph lỗi, SQL lifecycle/time travel và dashboard. Bằng chứng
ở [demo_rehearsals.json](../validation/demo_rehearsals.json); video dự phòng:
`outputs/demo_pipeline_A_20261117_nghiemthu_luot1.webm` và
`outputs/demo_pipeline_A_20261117_nghiemthu_luot2.webm`.
Video nằm local trong outputs, không theo dõi trên Git. Chuyển video cho C
khi tích hợp tài liệu; lời mở đầu nói rõ dùng run lịch sử.

## Lời mở đầu và kết thúc

“Dạ em chào thầy. Em là Võ Đức Hoàng, phụ trách pipeline và vận hành. Em trình
bày cách hệ thống lưu dữ liệu nguồn, chuẩn hóa, theo dõi lịch sử và đưa dữ liệu
lên dashboard. Phần minh họa dùng dữ liệu tới 17/11 và run
acceptance_A_20261117_r2 đã kiểm chứng. Bộ nghiệm thu kỹ thuật gồm 66 check
đều PASS.”

Khi chỉ graph thành công: “Các nhóm xử lý hoàn tất theo thứ tự, các bước verify
kiểm tra trước khi đi tiếp. Đây là run lịch sử có 24 task thành công. Run thử
lỗi bên cạnh dùng batch không có dữ liệu: task chuẩn bị đầu tiên thất bại và
23 task phía sau bị chặn, nên pipeline không tiếp tục xử lý input chưa đạt.”

Khi chỉ time travel: “Sau MERGE, id 1 có giá 120 và xuất hiện thêm id 3. Em
truy vấn lại snapshot ban đầu vẫn thấy hai dòng, id 1 có giá 100. Bảng này
thuộc namespace sandbox riêng, dùng để minh họa lịch sử snapshot mà không
thay đổi dữ liệu nghiệp vụ trong buổi demo.”

“Hệ thống đã có khả năng truy nguyên input, kiểm định, lưu lịch sử và cung cấp
dữ liệu cho truy vấn. Tin vắng trong mẫu crawl chưa xác nhận đã bán. Môi trường
local hiện tách cửa sổ Spark ghi và Trino đọc, Batdongsan vẫn cần xuất CSV từ
extension. Khi thêm dữ liệu hoặc sửa code, nhóm sẽ chạy lại kiểm chứng trước
khi chốt phát hành.”
