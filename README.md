# TLCN Nhóm 6 - Data Lakehouse bất động sản Việt Nam

Đề tài: **Xây dựng Data Lakehouse phục vụ phân tích dữ liệu thị trường bất động sản tại Việt Nam**.

Thành viên:

- 23133024 - Võ Đức Hoàng
- 23133029 - Vương Đức Huy
- 23133040 - Nguyễn Lê Hoàng Kiệt

Repository hiện phản ánh tiến độ đến **tuần 5: hoàn thiện Silver Data Foundation**. Silver đã được kiểm chứng end-to-end và đủ điều kiện để nhóm chuyển sang thiết kế Gold ở tuần 6.

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
                     └── listing_location
                                 │
                                 ↓
                     Gold / DWH / Dashboard
                     (giai đoạn tiếp theo)
```

Silver hợp nhất đủ 9 nguồn:

- Crawl: `batdongsan`, `guland`, `nhadatvui`.
- Historical: `chotot`, `mogi`, `alonhadat`, `luachonnhadat`, `muaban`, `homedy`.

Silver chỉ đọc Bronze Parquet trên MinIO, không đọc tắt CSV crawler. Bảng downstream chính là `lakehouse.silver.silver_listings_current_27`; hai bảng crawl/historical current được giữ để kiểm chứng lineage.

## Kết quả tuần 5

### Bronze State Audit

| Nhánh dữ liệu | Số nguồn | Số bản ghi |
|---|---:|---:|
| Historical | 6 | 30.317 |
| Snapshot crawl | 3 | 138.824 |
| Tổng cộng | 9 | 169.141 |

Các count trên là trạng thái được đọc trực tiếp từ MinIO khi chạy audit. File dữ liệu trong Git LFS có thể chứa thêm snapshot cục bộ chưa được ingest; không được dùng số lượng file Git thay cho trạng thái Bronze.

### Data Quality và dedup

| Chỉ số | Số dòng |
|---|---:|
| Normalized | 169.141 |
| Accepted trước dedup | 168.477 |
| Duplicate bị loại | 3.688 |
| Listing observation | 164.789 |
| Quarantine | 664 |
| PASS | 98.670 |
| WARN | 66.119 |
| REJECT | 664 |

WARN được giữ lại kèm lý do để tránh làm mất dữ liệu thật. Chỉ lỗi định danh nghiêm trọng mới được đưa vào quarantine.

### Các bảng Silver Iceberg

| Bảng | Số dòng | Vai trò |
|---|---:|---|
| `listing_observation` | 164.789 | Observation hợp lệ sau dedup theo batch |
| `listing_dq_quarantine` | 664 | Record không đạt điều kiện tối thiểu |
| `crawl_current_27` | 89.555 | Current của 3 nguồn crawl |
| `historical_current_27` | 30.317 | Current của 6 nguồn historical |
| `silver_listings_current_27` | 119.872 | Final current đủ 9 nguồn, đúng 27 cột |
| `listing_history` | 126.458 | Phiên bản khi business `record_hash` thay đổi |
| `listing_location` | 119.872 | Một dòng location cho mỗi `source_id` current |

History có 6.586 phiên bản thay đổi bổ sung và không có hai version liên tiếp trùng `record_hash`. Location bao phủ đủ final current; 99.510 dòng WARN chủ yếu do dữ liệu nguồn thiếu tọa độ hoặc cần fallback vị trí, không phải lỗi thực thi pipeline.

### Kiểm chứng

- Iceberg smoke test Spark → REST Catalog → MinIO: **PASS**.
- Final verification: **12/12 PASS**.
- Nguồn trong final current: **9/9**.
- Final current: đúng **27 cột canonical**, `source_id` không NULL và không trùng.
- Unit tests: **20/20 PASS**.
- Full deterministic rebuild: chạy lại cùng input không làm tăng row count.

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

## Chạy Silver

Chạy tự động toàn bộ pipeline và dừng ngay khi một bước lỗi:

```powershell
python -m pip install -r requirements-dev.txt
.\scripts\run_silver_pipeline.ps1
```

Hoặc chạy từng bước:

```powershell
.\scripts\run_spark.ps1 "src\bronze\audit_bronze_state.py"
.\scripts\run_spark.ps1 "src\silver\iceberg_smoke_check.py"
.\scripts\run_spark.ps1 "src\silver\build_listing_core_spark.py"
.\scripts\run_spark.ps1 "src\silver\build_listing_history.py"
.\scripts\run_spark.ps1 "src\silver\build_location.py"
.\scripts\run_spark.ps1 "src\silver\verify_silver.py"
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
src/silver/verify_silver.py          Kiểm chứng trực tiếp bảng Iceberg
tests/unit/                           Unit tests Silver
docs/data/                            Data Contract và Data Quality Rules
docs/validation/                      Bằng chứng audit/test/verification
```

## Phạm vi và bước tiếp theo

Silver Data Foundation đã hoàn thành theo phạm vi tuần 5. Các phần mở rộng chưa phải tiêu chí hoàn thành hiện tại gồm `silver_listing_feature`, GIS point-in-polygon, Location Master tới mã huyện/xã và incremental `MERGE INTO`.

Tuần 6 tập trung thiết kế Gold: market overview, price benchmark, repricing từ history, Data Quality KPI và chuẩn bị truy vấn qua Trino/PostgreSQL cho dashboard.
