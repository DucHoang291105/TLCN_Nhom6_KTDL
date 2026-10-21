# TLCN Nhóm 6 — Data Lakehouse bất động sản Việt Nam

Xây dựng Data Lakehouse phục vụ thu thập, chuẩn hóa, kiểm soát chất lượng và phân tích dữ liệu thị trường bất động sản tại Việt Nam.

Thành viên:

- 23133024 — Võ Đức Hoàng
- 23133029 — Vương Đức Huy
- 23133040 — Nguyễn Lê Hoàng Kiệt

## Chức năng đã triển khai

- Tích hợp 9 nguồn listing: 3 nguồn crawl và 6 nguồn historical.
- Nạp dữ liệu vào Bronze trên MinIO dưới dạng Parquet, giữ metadata nguồn và batch.
- Chuẩn hóa Silver về schema chung 27 cột; phân loại PASS/WARN/REJECT, loại trùng trong batch và lưu quarantine.
- Lưu các lần quan sát, phiên bản thay đổi và bản ghi mới nhất đã quan sát của từng listing.
- Chuẩn hóa vị trí, phân loại bất động sản và trích xuất các đặc điểm từ tiêu đề.
- Xây Gold gồm 9 dimension, bảng fact listing và các bảng phân tích theo Business Question.
- Theo dõi vòng đời quan sát: first/last seen, số lần quan sát, khoảng ngày quan sát, số lần đổi giá và trạng thái vắng hai batch.
- Điều phối bằng Airflow: chuẩn bị dữ liệu → Bronze → Silver → kiểm định → Gold → kiểm định; dừng downstream khi một bước thất bại.
- Lưu kết quả kiểm định dạng JSON và các hình minh chứng để đối chiếu kết quả chạy.

## Kiến trúc và công nghệ

```text
CSV snapshot / historical
          ↓
Bronze — MinIO + Parquet
          ↓
Silver — Spark + Apache Iceberg
  Observation · Current · History · Location · Feature
          ↓
Gold — Apache Iceberg
  Dimensions · Fact Listing · Lifecycle · Bảng phân tích
          ↓
Trino — truy vấn dữ liệu

Airflow điều phối các bước và kiểm định chất lượng.
```

Thành phần xử lý chính: Apache Spark 3.5.9, MinIO, Apache Iceberg REST Catalog, PostgreSQL 16, Apache Airflow 3.3.1, Trino và Docker Compose. Snapshot CSV được quản lý bằng Git LFS.

Repository có cấu hình Superset để kết nối lớp truy vấn; dashboard nghiệp vụ chưa được hoàn thiện.

Các bucket:

| Bucket | Nội dung |
|---|---|
| `lakehouse-bronze` | Parquet theo nguồn và batch |
| `lakehouse-silver` | Các bảng Silver Iceberg |
| `lakehouse-gold` | Các bảng Gold Iceberg |
| `warehouse` | Warehouse mặc định của Iceberg catalog |

Các bảng Silver/Gold sử dụng location riêng dưới bucket tương ứng.

## Nguồn dữ liệu

| Nhóm | Nguồn | Cách tiếp nhận |
|---|---|---|
| Crawl | Batdongsan | CSV xuất từ Chrome extension |
| Crawl | Guland, Nhadatvui | Crawler Python hoặc CSV đã thu thập |
| Historical | Chotot, Mogi, Alonhadat, Luachonnhadat, Muaban, Homedy | CSV đã chuẩn hóa schema đầu vào |

Cấu trúc snapshot:

```text
data/incoming/snapshots/
├── batdongsan/YYYYMMDD/batdongsan_raw.csv
├── guland/YYYYMMDD/guland_raw.csv
└── nhadatvui/YYYYMMDD/nhadatvui_raw.csv
```

Airflow dùng lại CSV nếu batch đã tồn tại. Guland/Nhadatvui chỉ được crawl khi thiếu batch của đúng ngày hiện tại theo giờ Việt Nam. Batdongsan được tiếp nhận qua sensor chờ CSV; sau khi có file, các bước xử lý tiếp theo chạy tự động.

Dữ liệu historical đặt trong `data/incoming/historical/<nguồn>/`, theo tên file cấu hình tại `src/bronze/ingest_bronze.py`.

## Kết quả đã kiểm chứng

Số liệu dưới đây thuộc lần chạy thành công trên dữ liệu tới batch **20261016**. Nguồn đối chiếu: [các JSON kiểm chứng](docs/validation/).

| Đầu ra | Số dòng |
|---|---:|
| Bronze historical | 30.317 |
| Bronze snapshot — 24 CSV, 8 batch mỗi nguồn | 241.991 |
| Tổng Bronze | 272.308 |
| Silver `listing_observation` | 263.331 |
| Silver `listing_dq_quarantine` | 664 |
| Silver `silver_listings_current_27` | 134.620 |
| Silver `listing_history` | 148.209 |
| Silver `listing_location` / `listing_feature` | 134.620 mỗi bảng |
| Gold `fact_listing` — tin bán | 119.263 |
| Gold `fact_listing_lifecycle` | 134.620 |
| Gold `fact_listing_price_change` — sự kiện đổi giá tin bán | 1.504 |

Kết quả kiểm định:

- Silver: **22/22 PASS**.
- Gold: **92/92 PASS**.
- Unit tests: **186 PASS**.
- Kiểm thử Spark cho lifecycle: **12/12 PASS**.
- Airflow: **23/23 task SUCCESS**, thời gian chạy khoảng **19,51 phút** trên môi trường đã kiểm chứng.
- Kiểm thử lỗi: một task thất bại, 22 task downstream bị chặn.
- Checksum của 24 CSV gốc giữ nguyên sau khi chạy pipeline.

## Các đầu ra phục vụ phân tích

| Bảng Gold | Nội dung |
|---|---|
| `fact_listing` và 9 dimension | Tin bán cùng khóa nguồn, thời gian, vị trí, loại hình và các nhóm giá/diện tích |
| `agg_budget_tradeoff`, `fact_budget_pareto` | BQ1: lựa chọn theo ngân sách, diện tích và đặc điểm |
| `agg_peer_group_benchmark`, `fact_listing_price_assessment` | BQ2: so sánh giá/m² với nhóm listing tương đồng |
| `agg_area_substitution` | BQ3: khu vực thay thế trong cùng tỉnh với chi phí thấp hơn cho cùng diện tích |
| `fact_listing_price_change` | Các sự kiện thay đổi giá đã quan sát |
| `fact_listing_lifecycle` | Vòng đời quan sát của listing theo từng nguồn |
| `agg_market_overview`, `agg_dq_kpi` | Tổng quan thị trường và chất lượng dữ liệu |

Silver Current là bản ghi **mới nhất đã quan sát**, không xác nhận listing vẫn còn hiển thị. Cờ vắng hai batch không đồng nghĩa đã bán; `listed_days` là khoảng thời gian quan sát, không phải thời gian tới khi bán. Nguồn historical có trạng thái lifecycle `NOT_APPLICABLE`.

Các nhóm nghi trùng cross-source được gắn cờ theo luật; chưa bảo đảm mỗi nhóm tương ứng đúng một bất động sản thực. BQ2 hiện là benchmark mô tả, không phải mô hình định giá hedonic. Pipeline hiện rebuild đầy đủ từ Bronze.

## Khởi động môi trường

Yêu cầu: Docker Desktop, Git LFS và Python 3.10+ để chạy script hoặc test trên máy.

```powershell
git lfs pull
# Chỉ thực hiện khi chưa có .env:
Copy-Item .env.example .env
```

Cấu hình thông tin kết nối và các khóa Airflow/Superset trong `.env` trước khi khởi động. Giữ nguyên `.env` đã cấu hình khi sử dụng lại môi trường.

Khởi động dịch vụ xử lý:

```powershell
docker compose up -d minio minio-init iceberg-rest spark-master spark-worker
```

Khởi động cả Airflow:

```powershell
docker compose --profile orchestration up -d --build --wait --wait-timeout 180
```

Dependency của Airflow được cài trong Docker image; dữ liệu MinIO, catalog và metadata dịch vụ được lưu trong named volume. Khởi động lại container sử dụng lại các thành phần đã cài. `docker compose down -v` sẽ xóa named volume và dữ liệu bên trong.

| Dịch vụ | Địa chỉ |
|---|---|
| MinIO Console | http://localhost:9001 |
| Spark Master / Worker | http://localhost:8080 / http://localhost:8081 |
| Airflow | http://localhost:8082 |
| Iceberg REST | http://localhost:8181/v1/config |
| Trino — profile `query` | http://localhost:8090 |
| Superset — profile `dashboard` | http://localhost:8088 |

## Chạy pipeline

Khi nạp dữ liệu lần đầu, chuẩn bị đủ historical CSV và snapshot rồi chạy:

```powershell
.\scripts\run_spark.ps1 src\bronze\ingest_bronze.py
.\scripts\run_spark.ps1 src\bronze\ingest_snapshots_bronze.py
```

Chạy toàn bộ pipeline qua Airflow với một batch đã có:

```powershell
.\scripts\run_airflow_pipeline.ps1 -BatchId 20261016
```

DAG `lakehouse_pipeline` có lịch **22:00 thứ 3 và thứ 6**, múi giờ `Asia/Ho_Chi_Minh`, không backfill tự động và chỉ chạy một run/task đồng thời. Có thể theo dõi Graph/Grid và log từng task tại Airflow.

`BatchId` xác định batch bắt buộc có ở đầu vào; pipeline vẫn xử lý toàn bộ snapshot đang lưu. Cần chuẩn bị CSV Batdongsan trước giờ chạy. Phạm vi crawler được cấu hình bằng `GULAND_CRAWL_PAGES` và `NHADATVUI_CRAWL_PAGES` trong `.env`, mặc định 200 trang mỗi nguồn.

Chạy Silver/Gold trực tiếp:

```powershell
python -m pip install -r requirements-dev.txt
.\scripts\run_silver_pipeline.ps1
.\scripts\run_gold_pipeline.ps1
```

Chạy script trực tiếp và DAG vào các thời điểm riêng để tránh ghi đồng thời cùng bảng Iceberg.

## Kiểm thử và xem kết quả

```powershell
python -m pytest -q
.\scripts\run_spark.ps1 src\quality\verify_lifecycle_fixtures.py
.\scripts\run_spark.ps1 src\silver\verify_silver.py
.\scripts\run_spark.ps1 src\gold\verify_gold.py
```

- JSON audit và kiểm định: [docs/validation](docs/validation/).
- Hình minh chứng xuất từ JSON/API: [docs/evidence](docs/evidence/).
- Hợp đồng dữ liệu và DQ Rules: [docs/data](docs/data/).
- SQL xem lifecycle bằng Trino: [lifecycle_demo.sql](docs/operations/lifecycle_demo.sql).

Tên bảng trong Spark là `lakehouse.gold.fact_listing_lifecycle`; trong Trino là `iceberg.gold.fact_listing_lifecycle`. Hai catalog cùng trỏ tới Iceberg REST Catalog.

## Cấu trúc mã nguồn

```text
config/                Nguồn dữ liệu, bảng tham chiếu và ngưỡng phân tích
src/ingestion/         Crawler Guland và Nhadatvui
src/bronze/            Ingest, audit và so sánh snapshot
src/silver/            Chuẩn hóa, DQ, history, location và feature
src/gold/              Dimension, fact, các bảng BQ và lifecycle
src/orchestration/     Chuẩn bị batch, gọi Spark và lưu kết quả Airflow
src/quality/           Kiểm thử Spark cho lifecycle
src/report/            Xuất dữ liệu, biểu đồ và báo cáo
dags/                  DAG Airflow
scripts/               Lệnh vận hành PowerShell
docker/                Cấu hình và Dockerfile cho các dịch vụ
tests/unit/            Unit tests
docs/                  Kiến trúc, hợp đồng dữ liệu và kết quả kiểm chứng
```
