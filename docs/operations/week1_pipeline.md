# Pipeline và vận hành — tuần 16–22/10/2026

## Nguồn dữ liệu và ngày batch

Pipeline xử lý đủ Batdongsan, Guland và Nhadatvui. Guland/Nhadatvui có crawler Python; Batdongsan được xuất CSV bằng Chrome extension. Đặt CSV vào:

```text
data/incoming/snapshots/batdongsan/YYYYMMDD/batdongsan_raw.csv
data/incoming/snapshots/guland/YYYYMMDD/guland_raw.csv
data/incoming/snapshots/nhadatvui/YYYYMMDD/nhadatvui_raw.csv
```

Airflow dùng lại batch đã có. Chỉ crawl Guland/Nhadatvui khi thiếu CSV của đúng ngày hiện tại theo giờ Việt Nam; không tạo lại batch quá khứ. Mặc định giữ phạm vi 200 trang như batch 16/10. Có thể cấu hình `GULAND_CRAWL_PAGES` và `NHADATVUI_CRAWL_PAGES` trong `.env`; phải ghi rõ thay đổi phạm vi khi so sánh các batch. Crawler từ chối ghi đè file CSV đã tồn tại. Batch 16/10 được dùng lại, không crawl lại.

Batdongsan: trước 22:00 ngày chạy, xuất file từ extension và đặt vào thư mục trên. Sensor chờ tối đa 2 giờ. Thiếu file thì pipeline dừng trước Bronze. Cách thu thập bằng extension không ảnh hưởng khả năng xử lý CSV tự động.

## Khởi động và chạy

```powershell
docker compose --profile orchestration up -d --build
docker exec tlcn_airflow airflow jobs check --job-type SchedulerJob --local
docker exec tlcn_airflow airflow dags list-import-errors --output json
.\scripts\run_airflow_pipeline.ps1 -BatchId 20261016
```

Nếu Airflow vừa khởi động và API chưa sẵn sàng, chờ health của scheduler rồi chạy lại lệnh trigger. Giao diện: http://localhost:8082. Tài khoản local mặc định là `admin`; mật khẩu do Airflow sinh và lưu bền trong volume `airflow_home`. Không commit mật khẩu vào Git:

```powershell
docker exec tlcn_airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

DAG `lakehouse_pipeline` chạy **22:00 thứ 3 và thứ 6**, múi giờ `Asia/Ho_Chi_Minh`, `catchup=False`, tối đa một DAG run và một task đang chạy. Ngày batch của scheduled run lấy theo ngày lịch chạy ở Việt Nam; khi chạy thủ công có thể truyền `batch_id`.

`batch_id` xác định batch bắt buộc phải có ở đầu vào, không phải bộ lọc tái dựng dữ liệu tại một thời điểm quá khứ. Ingest và rebuild hiện xử lý toàn bộ snapshot đang có.

```text
Chuẩn bị Guland → chuẩn bị Nhadatvui → chờ CSV Batdongsan → manifest
→ Bronze ingest → audit Bronze
→ Silver Core → History → Location → Feature → verify Silver
→ Gold dimensions → fact/BQ → lifecycle → verify Gold → lưu minh chứng
```

Task kiểm định FAIL hoặc tiến trình Spark trả exit code khác 0 sẽ làm task thất bại; task phía sau dùng `all_success`, không tiếp tục ghi các lớp sau. Các summary PASS phải được sinh từ kết quả thật. Airflow image được build từ Dockerfile và dependency được cài ở bước build; khởi động lại container không phải cài lại. DAG điều khiển Spark qua Docker socket trên máy phát triển local.

## Bảng vòng đời tin đăng

`lakehouse.gold.fact_listing_lifecycle`: một dòng cho mỗi `source_id` từng được Silver chấp nhận, gồm cả historical để giữ khả năng đối chiếu đủ tập ID. `source_id` đã chứa namespace nguồn; không hợp nhất vòng đời theo nhóm nghi trùng cross-source.

| Trường | Định nghĩa |
|---|---|
| `first_seen_date`, `last_seen_date` | Ngày snapshot đầu/cuối quan sát thấy |
| `first_seen_batch_id`, `last_seen_batch_id` | Batch đầu/cuối có listing |
| `observation_count` | Số batch phân biệt có listing sau loại trùng trong batch |
| `listed_days` | Chênh lệch last_seen và first_seen; khoảng quan sát, không phải số ngày tới khi bán |
| `price_change_count` | Số lần hai giá quan sát liền nhau đều dương và khác nhau; không nối qua giá thiếu/0 |
| `latest_source_batch_id`, `previous_source_batch_id` | Hai batch mới nhất của chính nguồn đó trong Bronze audit PASS |
| `missed_latest_batch_count` | Vắng mặt trong bao nhiêu batch thuộc hai batch cuối |
| `is_disappeared` | TRUE khi vắng cả hai batch cuối và nguồn có ít nhất hai batch |
| `lifecycle_status` | OBSERVED_LATEST / NOT_OBSERVED_LATEST / NOT_OBSERVED_2_BATCHES / INSUFFICIENT_BATCHES / NOT_APPLICABLE |

Nguồn historical có `is_disappeared=NULL`, `lifecycle_status=NOT_APPLICABLE`. Listing xuất hiện trở lại sẽ được đánh giá lại theo hai batch cuối. Lịch batch lấy từ audit Bronze để vẫn nhận diện batch có toàn bộ dòng bị quarantine. Không dùng ngày batch mới nhất của một nguồn để đánh giá nguồn khác.

`is_disappeared` chỉ phản ánh không thấy trong phạm vi crawl. Listing có thể đổi vị trí, rời top 200 trang, bị gỡ hoặc bị giới hạn hiển thị; không được diễn giải là đã bán. Số ngày quan sát không thay thế ngày đăng thực.

Trong Spark dùng `lakehouse.gold.fact_listing_lifecycle`; trong Trino dùng `iceberg.gold.fact_listing_lifecycle` vì tên catalog lấy theo `iceberg.properties`. Xem các truy vấn minh họa ở [lifecycle_demo.sql](lifecycle_demo.sql).

## Kiểm thử và minh chứng

```powershell
python -m pytest -q --junitxml=docs/validation/pytest.xml
.\scripts\run_spark.ps1 src\quality\verify_lifecycle_fixtures.py
.\scripts\run_spark.ps1 src\gold\build_listing_lifecycle.py
.\scripts\run_spark.ps1 src\gold\verify_gold.py
```

Các file kiểm chứng nằm trong `docs/validation/`: `snapshot_manifest.json`, `bronze_state_audit.json`, `silver_final_verification.json`, `gold_listing_lifecycle_summary.json`, `gold_verification.json`, `lifecycle_fixture_tests.json`, `airflow_pipeline_run.json`, `airflow_task_states.json`.

Trong Airflow: mở DAG → chọn run thành công → Graph/Grid để chụp trạng thái task; mở `gold.listing_lifecycle` và `gold.verify_gold` để chụp log kết quả. Lưu ảnh vào `docs/evidence/week1_20261016/`. File JSON là nguồn số liệu, không nhập thủ công các con số vào báo cáo.

## Khi chạy lỗi

- Thiếu Batdongsan: xuất CSV từ extension rồi chạy lại task sensor/downstream hoặc tạo manual run mới cùng `batch_id`.
- LFS pointer: chạy `git lfs pull` trước khi nạp Bronze.
- Crawl có trang lỗi: xem `crawl_summary.json`, giữ file cũ làm audit và chuẩn bị lại batch có ghi nhận nguồn gốc; không ép PASS file crawl một phần.
- Audit Bronze WARN: xử lý nguồn/batch thiếu hoặc unreadable trước khi chạy Silver.
- Không chạy đồng thời script rebuild PowerShell và DAG vì chúng ghi cùng bảng Iceberg.

`MERGE INTO`, time travel/schema evolution demo và CI thuộc tuần sau; pipeline tuần này vẫn rebuild từ toàn bộ snapshot Bronze.
