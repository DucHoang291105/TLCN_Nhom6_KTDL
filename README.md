# TLCN Nhóm 6 - Data Lakehouse bất động sản Việt Nam

Đề tài: **Xây dựng Data Lakehouse phục vụ phân tích dữ liệu thị trường bất động sản tại Việt Nam**.

Thành viên:

- 23133024 - Võ Đức Hoàng
- 23133029 - Vương Đức Huy
- 23133040 - Nguyễn Lê Hoàng Kiệt

Repository hiện phản ánh tiến độ đến **tuần 6: Silver Feature, Gold nền và Gold theo Business Question (BQ1–BQ3)**, kèm đợt rà soát sửa lỗi ngày 09/10/2026. Báo cáo tiến độ: [`docs/report/Bao_cao_Silver_Feature_Gold.docx`](docs/report/Bao_cao_Silver_Feature_Gold.docx).

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

Số liệu dưới đây là lần rebuild đầy đủ ngày 09/10/2026 sau đợt rà soát (xem mục *Đợt rà soát 09/10/2026* bên dưới). Bronze gồm batch `20260929` và `20261002` của `batdongsan` và `guland`.

### Bronze State Audit

| Nhánh dữ liệu | Số nguồn | Số bản ghi |
|---|---:|---:|
| Historical | 6 | 30.317 |
| Snapshot crawl | 3 | 164.824 |
| Tổng cộng | 9 | 195.141 |

Các count trên là trạng thái được đọc trực tiếp từ MinIO khi chạy audit. Job ingest snapshot chỉ nhận thư mục batch đúng dạng `YYYYMMDD`.

### Data Quality và dedup theo batch

| Chỉ số | Số dòng |
|---|---:|
| Normalized | 195.141 |
| Accepted trước dedup | 194.477 |
| Duplicate bị loại | 3.960 |
| Listing observation | 190.517 |
| Quarantine (REJECT) | 664 |
| PASS / WARN | 114.872 / 75.645 |

### Các bảng Silver Iceberg

| Bảng | Số dòng | Vai trò |
|---|---:|---|
| `listing_observation` | 190.517 | Observation hợp lệ sau dedup theo batch; có `category_evidence` |
| `listing_dq_quarantine` | 664 | Record không đạt điều kiện tối thiểu |
| `crawl_current_27` / `historical_current_27` | 93.651 / 30.317 | Current theo nhánh |
| `silver_listings_current_27` | 123.968 | Bản ghi **mới nhất đã quan sát** của mỗi tin (không xác nhận tin còn hiển thị), đúng 27 cột |
| `listing_history` | 131.420 | Phiên bản khi business `record_hash` thay đổi |
| `listing_location` | 123.968 | Vị trí theo thứ tự bằng chứng; LQ05 → khoảng cách NULL |
| `listing_feature` | 123.968 | `model_category` + nguồn gốc nhãn, cờ mâu thuẫn, cờ đặc điểm từ title |

Loại hình: nhà phố 72.582, căn hộ 26.959, biệt thự/liền kề/shophouse 10.041, khác 7.106, đất 6.917, không rõ 363. 4.621 tin có nhãn nguồn mâu thuẫn với title (khác họ căn hộ/đất/nhà) được gắn cờ, không bị ghi đè. Title nêu pháp lý 10,1%, nội thất 8,5%, mặt tiền đường 16,8%, thang máy 4,2%, ô tô tiếp cận 12,7%; `title_has_* = FALSE` gộp "không nêu" và "nói không có".

### Gold nền (star schema)

| Bảng | Số dòng | Vai trò |
|---|---:|---|
| `dim_source` / `dim_date` / `dim_location` | 10 / 2.694 / 272 | Mỗi dimension có dòng `-1` |
| `dim_property_category` / `dim_dq_status` | 6 / 3 | |
| Band giá / diện tích / giá/m² / phòng | 9 / 7 / 8 / 6 | `config/gold_bands.csv` |
| `fact_listing` | 108.611 | Một dòng cho mỗi tin **bán** current |

`fact_listing` = 123.968 current − 15.357 tin thuê. Giá trị không map được dùng `-1` (ví dụ 39.426 tin thiếu số phòng); 16 tin có tỉnh mâu thuẫn tọa độ nhận `location_key = -1`.

Tin trùng giữa các nguồn chỉ được **nghi trùng** theo luật (cùng location và loại hình, diện tích ±2%, giá ±3%, Jaccard title ≥ 0,35, chỉ giữ cặp tốt nhất hai chiều): 850 nhóm, 1.738 tin; 3 nhóm có hai tin cùng nguồn không được gộp. Bảng tổng hợp dùng 107.733 **tin đại diện theo luật**, không bảo đảm một dòng cho mỗi bất động sản thực. Khóa dimension chỉ ổn định khi rebuild đồng bộ dimension và fact.

### Gold theo Business Question

| Bảng | Số dòng | Trả lời |
|---|---:|---|
| `agg_peer_group_benchmark` | 2.616 | BQ2: nhóm tương đồng (≥ 5 tin) ở 3 cấp |
| `fact_listing_price_assessment` | 107.733 | BQ2: vị trí giá/m² dưới P25 / trong P25–P75 / trên P75 (benchmark mô tả, không phải định giá) |
| `agg_budget_tradeoff` | 2.455 | BQ1: người mua được gì theo ngân sách × quận × loại hình |
| `fact_budget_pareto` | 105.495 | BQ1: 19.709 tin không bị trội theo tiêu chí đã chọn, trong cùng quận |
| `agg_area_substitution` | 3.485 | BQ3: quận cùng tỉnh có chi phí thấp hơn **cho cùng diện tích**, tương đồng đặc điểm ≥ 0,8 và không cờ nào lệch > 0,3 |
| `agg_dq_kpi` | 9 | Phễu Bronze → Gold theo nguồn (job duy nhất đọc Bronze) |
| `fact_listing_price_change` | 642 | Lịch sử đổi giá (499 lần giảm) |
| `agg_market_overview` | 143 | Mặt bằng giá theo tỉnh × loại hình |

### Kiểm chứng

- Silver verification: **22/22 PASS** (gồm so khớp tập `source_id` hai chiều và `category_evidence`).
- Gold verification: **81/81 PASS**; các check tính lại kết quả: tập khóa fact, band theo giá trị, vị trí giá từ P25/P75, nhãn Pareto (so từng cặp), điều kiện BQ3, xử lý LQ05 và nhóm nghi trùng.
- Unit tests: **PASS** (`python -m pytest -q`), gồm regression test cho từng lỗi trong đợt rà soát.
- Rebuild hai lần trên cùng Bronze: **PASS** trên 26 bảng, so số dòng, số khóa grain và hash nội dung (bỏ cột `*_built_at`) — `scripts/check_deterministic_rebuild.ps1`.

### Đợt rà soát 09/10/2026

| Chỉ số | Trước | Sau |
|---|---:|---:|
| Căn hộ / nhà phố (tin current) | 36.731 / 65.029 | 26.959 / 72.582 |
| LQ05 tỉnh mâu thuẫn tọa độ | 982 | 16 |
| LQ04 tỉnh suy từ tâm gần nhất | 27.772 | 0 |
| Nhóm nghi trùng lớn nhất | 30 | 5 |
| Cặp BQ3 | 4.666 | 3.485 |

Chi tiết lỗi, cách sửa và phần chưa kiểm chứng: [`docs/report/ban_ghi_sua_loi.md`](docs/report/ban_ghi_sua_loi.md) và Chương 7 của báo cáo.

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
