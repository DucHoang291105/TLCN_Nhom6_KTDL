# Vận hành lịch sử listing trên Iceberg

## Hợp đồng và phạm vi

`lakehouse.silver.listing_history` có grain `(source_id, version_number)`.
Chỉ bắt đầu phiên bản mới khi `record_hash` nghiệp vụ thay đổi giữa hai lần
quan sát liên tiếp; chuỗi A → B → A giữ đủ ba phiên bản. `valid_from` ưu tiên
ngày snapshot, rồi thời gian crawl/ingest; `valid_to` là `valid_from` của phiên
bản tiếp theo, khoảng hiệu lực `[valid_from, valid_to)`. Phiên bản cuối có
`valid_to=NULL` và `is_current_version=true`. Đây là lịch sử đã quan sát,
không phải xác nhận tin còn đăng hoặc đã bán.

Partition của history là identity `snapshot_date`. Các bảng Silver Core và
Gold khác vẫn rebuild; **không tuyên bố toàn pipeline đã incremental**.

## MERGE và phục hồi

1. Đọc bảng `listing_observation` đã deduplicate, kiểm tra khóa và thời gian.
2. Tính signature theo `source_id`, bao gồm toàn bộ cột đầu vào history,
   cả metadata. So sánh với `listing_history_state` để tìm ID bị ảnh hưởng.
3. Với mỗi ID đó, tính lại timeline từ **toàn bộ** quan sát của ID, không chỉ
   batch mới. Vì vậy xử lý được batch đến muộn, sửa dữ liệu và xóa quan sát.
   `version_number` là thứ tự timeline, có thể được đánh lại khi sửa lịch sử;
   Gold repricing được rebuild sau đó, không giữ khóa phiên bản cũ bất biến.
4. `MERGE INTO` theo khóa history: insert/update phiên bản cần thiết, delete
   phiên bản cũ không còn hợp lệ. So sánh null-safe để tránh update vô ích.
5. MERGE bảng state **sau** khi history đã commit, sau đó ghi snapshot ID
   history vào property `pipeline.history-snapshot-id` của state.

Iceberg không có transaction nguyên tử giữa hai bảng. Nếu job dừng giữa các
commit, checkpoint không khớp history thì lần chạy lại reconcile tất cả ID.
Chạy tuần tự một writer (Airflow `max_active_runs=1`, không chạy thủ công
song song). Không expire snapshot thủ công trong lúc job đang chạy.

Không có thay đổi: trả `NO_CHANGE`, không tạo data snapshot history mới.
Đổi partition spec bằng `ADD PARTITION FIELD` là thay đổi metadata; file cũ
vẫn mang spec cũ tới khi được viết lại. `--mode rebuild` ghi lại toàn bộ bảng
với partition hiện hành và là đường đối chứng/khôi phục có chủ đích.

## Lệnh chạy

Chạy tại root repository sau khi bật Docker:

```powershell
# Cần đủ ba CSV cho batch yêu cầu; không crawl lại batch đã có.
python -c "from src.orchestration.pipeline import validate_snapshots; validate_snapshots('20261023')"
./scripts/run_spark.ps1 src/bronze/ingest_snapshots_bronze.py --skip-existing
./scripts/run_silver_pipeline.ps1 -HistoryMode incremental
./scripts/run_gold_pipeline.ps1

# Hoặc Airflow chạy toàn chuỗi, history mặc định incremental:
./scripts/run_airflow_pipeline.ps1 -BatchId 20261023 -HistoryMode incremental

# Chỉ khi chủ động yêu cầu rebuild history:
./scripts/run_spark.ps1 src/silver/build_listing_history.py --mode rebuild

# Đối chiếu MERGE với rebuild và chạy các tình huống lỗi/đến muộn:
./scripts/run_spark.ps1 src/quality/verify_history_incremental.py --baseline-batch 20261016

# Demo thật, tạo bảng sandbox riêng, không đổi schema production:
./scripts/run_spark.ps1 src/quality/demo_iceberg_features.py
python -m pytest -q
```

Bronze `--skip-existing` chỉ tái sử dụng batch có `_SUCCESS`; batch chưa hoàn
thành sẽ báo lỗi. Job cũng tự đối chiếu SHA256 từng CSV với manifest PASS.
Luôn chạy manifest preflight trước ingest: snapshot cũ đã
ghi nhận mà đổi SHA256 sẽ bị chặn, không im lặng bỏ qua CSV mới sửa. Nếu thật
sự sửa dữ liệu nguồn cũ, cần kiểm tra và reload Bronze có chủ đích, rồi cập
nhật manifest đã được review; không sửa manifest để ép PASS.

`bronze_snapshot_ingestion_summary.json.total_output_rows` là số dòng **mới
ghi trong lần chạy đó**, không phải tổng Bronze. Khi tái sử dụng toàn bộ batch,
giá trị 0 là bình thường; tổng dữ liệu nằm trong `bronze_state_audit.json`.

## Xem kết quả ở đâu

- `docs/validation/history_incremental_verification.json`: số dòng, SHA256
  của toàn bộ multiset row hash, `EXCEPT ALL` hai chiều, thời gian thực đo,
  retry và các ca kiểm thử SCD2. Không loại bất kỳ cột history nào khỏi hash.
- `docs/validation/silver_history_summary.json`: lần chạy history gần nhất.
- `docs/validation/silver_final_verification.json`: kiểm định toàn Silver,
  gồm interval, current-version, grain, partition và checkpoint history.
- `docs/demo/iceberg_features.sql`: chính các câu Spark SQL đã thực thi,
  có snapshot ID thật để minh chứng `VERSION AS OF`.
- `docs/validation/iceberg_features.json`: output time travel, MERGE, thêm/
  đổi tên cột, kiểm tra thêm cột không viết lại data file, partition.
- GitHub Actions workflow `Tests`: pytest trên Python 3.10 và 3.12; không
  giả lập rằng CI đã chạy Spark. Spark/Iceberg integration chạy bằng Docker
  tại máy và lưu bằng chứng JSON riêng.

Benchmark là một lần đo trên máy local, cùng input đã cache, tính cả kiểm tra,
signature và ghi history/state. MERGE có thể chậm hơn rebuild trên tập nhỏ;
điều kiện bắt buộc là kết quả giống nhau và retry an toàn, không ép kết quả
thời gian để chứng minh nhanh hơn.

Spark mặc định 16 shuffle partition cho cụm local 2 core; có thể đổi bằng
`SPARK_SHUFFLE_PARTITIONS` trong môi trường của driver khi mở rộng tài nguyên.

Các bảng benchmark/fixture ở namespace `lakehouse.smoke_test`; không union
dữ liệu tổng hợp vào Silver/Gold production.

Tham khảo kỹ thuật: [Iceberg Spark writes](https://iceberg.apache.org/docs/latest/spark-writes/),
[Spark DDL](https://iceberg.apache.org/docs/latest/spark-ddl/),
[Spark queries](https://iceberg.apache.org/docs/latest/spark-queries/).
Badge CI dùng hành vi mặc định của [GitHub workflow status badge](https://docs.github.com/en/actions/how-tos/monitor-workflows/add-a-status-badge),
không gắn cứng với nhánh feature sẽ bị xóa sau merge.
