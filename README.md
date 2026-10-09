# TLCN Nhóm 6 - Data Lakehouse bất động sản Việt Nam

Đề tài: **Xây dựng Data Lakehouse phục vụ phân tích dữ liệu thị trường bất động sản tại Việt Nam**.

Thành viên:

- 23133024 - Võ Đức Hoàng
- 23133029 - Vương Đức Huy
- 23133040 - Nguyễn Lê Hoàng Kiệt

Repository hiện phản ánh tiến độ đến **tuần 6: Silver Feature, Gold nền và Gold theo Business Question (BQ1–BQ3)**. Toàn bộ đã được kiểm chứng end-to-end và rebuild deterministic. Báo cáo tiến độ: [`docs/report/Bao_cao_Silver_Feature_Gold.docx`](docs/report/Bao_cao_Silver_Feature_Gold.docx).

## Kiến trúc hiện tại

```text
6 nguồn historical ─┐
                    ├─> Bronze: MinIO + Parquet
3 nguồn crawl ──────┘            │
                                 ↓
                     Spark normalize + DQ + dedup
                                 │
                                 ↓
                     Silver: Apache Iceberg
                     ├── listing_observation
                     ├── listing_dq_quarantine
                     ├── crawl_current_27
                     ├── historical_current_27
                     ├── silver_listings_current_27
                     ├── listing_history
                     ├── listing_location
                     └── listing_feature
                                 │
                                 ↓
                     Gold: Apache Iceberg (star schema)
                     ├── 9 dimension (dim_source, dim_date, dim_location, ...)
                     ├── fact_listing
                     └── bảng BQ: price benchmark (BQ2), budget trade-off (BQ1),
                         area substitution (BQ3), DQ KPI, repricing, market overview
                                 │
                                 ↓
                     Trino → Dashboard (giai đoạn tiếp theo)
```

Silver hợp nhất đủ 9 nguồn:

- Crawl: `batdongsan`, `guland`, `nhadatvui`.
- Historical: `chotot`, `mogi`, `alonhadat`, `luachonnhadat`, `muaban`, `homedy`.

Mỗi tầng có bucket MinIO riêng: `lakehouse-bronze` (Parquet), `lakehouse-silver` và `lakehouse-gold` (bảng Iceberg dưới `real_estate/<bảng>`). Bucket `warehouse` chỉ là thư mục gốc mặc định bắt buộc của Iceberg catalog và luôn trống; chi tiết ở [`docs/architecture/lakehouse_architecture.md`](docs/architecture/lakehouse_architecture.md).

Silver chỉ đọc Bronze Parquet trên MinIO, không đọc tắt CSV crawler. Bảng downstream chính là `lakehouse.silver.silver_listings_current_27`; hai bảng crawl/historical current được giữ để kiểm chứng lineage.

## Kết quả hiện tại

Số liệu dưới đây là lần rebuild đầy đủ ngày 09/10/2026 từ dữ liệu trong repo. So với tuần 5, Bronze có thêm batch `20260929` và `20261002` của `batdongsan` và `guland` (+26.000 dòng snapshot), nên mọi count downstream đều tăng.

### Bronze State Audit

| Nhánh dữ liệu | Số nguồn | Số bản ghi |
|---|---:|---:|
| Historical | 6 | 30.317 |
| Snapshot crawl | 3 | 164.824 |
| Tổng cộng | 9 | 195.141 |

Các count trên là trạng thái được đọc trực tiếp từ MinIO khi chạy audit. Job ingest snapshot chỉ nhận thư mục batch đúng dạng `YYYYMMDD`; các thư mục crawl tạm như `20261003_150728` không được ingest.

### Data Quality và dedup

| Chỉ số | Số dòng |
|---|---:|
| Normalized | 195.141 |
| Accepted trước dedup | 194.477 |
| Duplicate bị loại | 3.960 |
| Listing observation | 190.517 |
| Quarantine | 664 |
| PASS | 114.750 |
| WARN | 75.767 |
| REJECT | 664 |

WARN được giữ lại kèm lý do để tránh làm mất dữ liệu thật. Chỉ lỗi định danh nghiêm trọng mới được đưa vào quarantine.

### Các bảng Silver Iceberg

| Bảng | Số dòng | Vai trò |
|---|---:|---|
| `listing_observation` | 190.517 | Observation hợp lệ sau dedup theo batch |
| `listing_dq_quarantine` | 664 | Record không đạt điều kiện tối thiểu |
| `crawl_current_27` | 93.651 | Current của 3 nguồn crawl |
| `historical_current_27` | 30.317 | Current của 6 nguồn historical |
| `silver_listings_current_27` | 123.968 | Final current đủ 9 nguồn, đúng 27 cột |
| `listing_history` | 131.416 | Phiên bản khi business `record_hash` thay đổi |
| `listing_location` | 123.968 | Một dòng location cho mỗi `source_id` current |
| `listing_feature` | 123.968 | `model_category`, cờ đặc điểm từ title, cờ `*_known` |

History có 7.448 phiên bản thay đổi bổ sung và không có hai version liên tiếp trùng `record_hash`. Location có 103.061 dòng WARN, chủ yếu do dữ liệu nguồn thiếu tọa độ hoặc cần fallback vị trí, không phải lỗi thực thi pipeline.

`listing_feature` map được `model_category` cho 99,73% listing (`khong_ro` 332 dòng). Tỷ lệ title nhắc đặc điểm: pháp lý 10,1%, nội thất 8,5%, mặt tiền 19,1%, thang máy 4,2%, ô tô vào 12,7%. `title_has_* = FALSE` chỉ có nghĩa title không đề cập.

### Gold nền (star schema)

| Bảng | Số dòng | Vai trò |
|---|---:|---|
| `dim_source` | 10 | 9 nguồn + dòng `-1` |
| `dim_date` | 2.694 | Ngày đăng/quan sát |
| `dim_location` | 362 | Tỉnh – quận/huyện theo `listing_location` |
| `dim_property_category` | 6 | 5 `model_category` + `-1` |
| `dim_price_band` / `dim_area_band` / `dim_unit_price_band` / `dim_room_band` | 9 / 7 / 8 / 6 | Band khai báo trong `config/gold_bands.csv` |
| `dim_dq_status` | 3 | PASS, WARN, `-1` |
| `fact_listing` | 108.611 | Một dòng cho mỗi tin **bán** current |

`fact_listing` = 123.968 current − 15.357 tin thuê (không có `is_rent` NULL hay REJECT). Mọi FK đều trỏ tới dimension; giá trị không map được dùng `-1` (ví dụ 39.426 tin thiếu số phòng, 2.235 tin giá thỏa thuận).

Dedup giữa các nguồn chỉ gắn cờ, không xóa: cùng location và category, diện tích lệch ≤ 2%, giá lệch ≤ 3%, Jaccard token title ≥ 0,35. Kết quả có 1.052 cặp, gom thành 606 nhóm, 1.582 tin nghi trùng (1,46%), nhiều nhất là cặp `guland` – `nhadatvui`. Bảng tổng hợp phải dùng `is_dup_representative = TRUE` (107.635 tin).

### Gold theo Business Question

| Bảng | Số dòng | Trả lời |
|---|---:|---|
| `agg_peer_group_benchmark` | 2.895 | BQ2: nhóm tương đồng (≥ 5 tin) ở 3 cấp |
| `fact_listing_price_assessment` | 107.635 | BQ2: vị trí giá thấp / hợp lý / cao của từng tin |
| `agg_budget_tradeoff` | 2.649 | BQ1: người mua được gì theo ngân sách × quận × loại hình |
| `fact_budget_pareto` | 105.397 | BQ1: 19.670 tin không bị trội |
| `agg_area_substitution` | 4.666 | BQ3: quận thay thế rẻ hơn, tương đồng đặc điểm ≥ 0,8 |
| `agg_dq_kpi` | 9 | Phễu dữ liệu Bronze → Gold theo nguồn |
| `fact_listing_price_change` | 642 | Lịch sử đổi giá (499 lần giảm) |
| `agg_market_overview` | 158 | Mặt bằng giá theo tỉnh × loại hình |

### Kiểm chứng

- Iceberg smoke test Spark → REST Catalog → MinIO: **PASS**.
- Silver final verification: **18/18 PASS** (12 check cũ + 5 check `listing_feature` + bảng nằm đúng bucket `lakehouse-silver`).
- Gold verification: **67/67 PASS** (dimension, band, FK, grain của mọi bảng BQ, nhóm benchmark ≥ 5 tin, tổng n khớp fact, khu vực thay thế cùng tỉnh và rẻ hơn, DQ KPI khớp Silver, mọi bảng nằm đúng bucket `lakehouse-gold`).
- Nguồn trong final current: **9/9**; final current đúng **27 cột canonical**.
- Unit tests: **122/122 PASS**.
- Rebuild deterministic: chạy toàn bộ Silver + Gold 2 lần, **26/26 bảng** cùng số dòng (`scripts/check_deterministic_rebuild.ps1`).

Kết quả máy đọc được nằm trong [`docs/validation`](docs/validation/).

## Công nghệ

- MinIO, Apache Spark 3.5.9, Apache Iceberg REST Catalog.
- Trino, PostgreSQL 16, Apache Airflow, Apache Superset.
- FastAPI, Docker Compose và Git LFS.

## Khởi động môi trường

Yêu cầu: Docker Desktop, Python 3.10+ và Git LFS.

```powershell
git lfs pull
Copy-Item .env.example .env
docker compose up -d minio minio-init iceberg-rest spark-master spark-worker
docker compose ps
```

| Dịch vụ | URL |
|---|---|
| MinIO Console | http://localhost:9001 |
| Spark Master | http://localhost:8080 |
| Spark Worker | http://localhost:8081 |
| Iceberg REST | http://localhost:8181/v1/config |

Không chạy `docker compose down -v` nếu muốn giữ dữ liệu trong Docker named volumes.

Dữ liệu historical (6 file `*_schema_chuan.csv`) không nằm trong Git; đặt vào `data/incoming/historical/<nguồn>/` rồi nạp Bronze:

```powershell
.\scripts\run_spark.ps1 "src\bronze\ingest_bronze.py"
.\scripts\run_spark.ps1 "src\bronze\ingest_snapshots_bronze.py"
```

Nếu `quay.io/minio/minio` không pull được, tạo `docker-compose.override.yml` cục bộ (không commit) dùng image `bitnamilegacy/minio:2025.7.23` với `user: root` và `entrypoint: ["minio"]` cho `minio` và `minio-init`.

## Chạy Silver và Gold

Chạy tự động toàn bộ pipeline và dừng ngay khi một bước lỗi:

```powershell
python -m pip install -r requirements-dev.txt
.\scripts\run_silver_pipeline.ps1
.\scripts\run_gold_pipeline.ps1
```

Sinh lại báo cáo Word (job xuất ví dụ chạy trên Spark, phần dựng file chạy trên máy):

```powershell
.\scripts\run_spark.ps1 "src\report\export_report_data.py"
python -m src.report.build_report
```

`run_spark.ps1` tự xóa thư mục ứng dụng đã chạy xong trong Spark worker; mỗi lần submit Spark chép khoảng 370 MB jar vào đó và Spark Standalone không tự dọn.

Hoặc chạy từng bước:

```powershell
.\scripts\run_spark.ps1 "src\bronze\audit_bronze_state.py"
.\scripts\run_spark.ps1 "src\silver\iceberg_smoke_check.py"
.\scripts\run_spark.ps1 "src\silver\build_listing_core_spark.py"
.\scripts\run_spark.ps1 "src\silver\build_listing_history.py"
.\scripts\run_spark.ps1 "src\silver\build_location.py"
.\scripts\run_spark.ps1 "src\silver\build_listing_feature.py"
.\scripts\run_spark.ps1 "src\silver\verify_silver.py"
.\scripts\run_spark.ps1 "src\gold\build_dimensions.py"
.\scripts\run_spark.ps1 "src\gold\build_fact_listing.py"
.\scripts\run_spark.ps1 "src\gold\verify_gold.py"
python -m pytest -q
```

## Cấu trúc quan trọng

```text
config/sources.yaml                 Danh mục 6 historical + 3 crawl
src/bronze/                         Ingest, audit và verify Bronze
src/common/spark_session.py         Cấu hình Spark, MinIO và Iceberg dùng chung
src/silver/build_listing_core.py    Transformer và quy tắc chuẩn hóa thuần Python
src/silver/build_listing_core_spark.py
                                     Xây Silver Foundation 9 nguồn
src/silver/build_listing_history.py  Xây history theo thay đổi record_hash
src/silver/build_location.py         Location enrichment
src/silver/feature_rules.py          Quy tắc feature thuần Python (category, cờ title)
src/silver/build_listing_feature.py  Xây listing_feature
src/silver/verify_silver.py          Kiểm chứng trực tiếp bảng Iceberg
config/gold_bands.csv                Ngưỡng band giá, diện tích, giá/m², số phòng
src/gold/gold_rules.py               Quy tắc Gold thuần Python (band, dedup cross-source)
src/gold/build_dimensions.py         Xây 9 dimension
src/gold/build_fact_listing.py       Xây fact_listing và cờ nghi trùng
src/gold/verify_gold.py              Kiểm chứng star schema Gold
tests/unit/                           Unit tests Silver và Gold
docs/data/                            Data Contract và Data Quality Rules
docs/validation/                      Bằng chứng audit/test/verification
```

## Phạm vi và bước tiếp theo

Đã hoàn thành: Silver Data Foundation, `listing_feature`, Gold nền (9 dimension + `fact_listing`) và các bảng Gold trả lời BQ1–BQ3 cùng DQ KPI, repricing và tổng quan thị trường.

Bước tiếp theo:

- Mô hình hedonic cho BQ2 (`pyspark.ml`) để tách phần chênh lệch giá do từng đặc điểm.
- Airflow DAG điều phối Bronze → Silver → Gold → verify; dashboard Superset qua Trino cho từng BQ.
- GIS point-in-polygon, Location Master tới mã huyện/xã, incremental `MERGE INTO` và dọn snapshot Iceberg cũ.
