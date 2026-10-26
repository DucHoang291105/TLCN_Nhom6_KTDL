# TLCN Nhóm 6 — Data Lakehouse bất động sản Việt Nam

[![Tests](https://github.com/DucHoang291105/TLCN_Nhom6_KTDL/actions/workflows/tests.yml/badge.svg)](https://github.com/DucHoang291105/TLCN_Nhom6_KTDL/actions/workflows/tests.yml)

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
- Cập nhật lịch sử SCD2 bằng Iceberg `MERGE INTO`, partition theo `snapshot_date`; hỗ trợ batch đến muộn, dữ liệu sửa lại và chạy lại không tạo phiên bản thừa.
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

Superset có 4 dashboard: tổng quan thị trường, lựa chọn theo ngân sách (BQ1), vị trí giá trong nhóm tương đồng (BQ2) và khu vực thay thế (BQ3). Dữ liệu đọc từ Gold qua Trino; script kiểm tra lại 29 biểu đồ và đối chiếu KPI.

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

Số liệu dưới đây được kiểm chứng sau tích hợp pipeline và dashboard, dữ liệu tới batch **20261023**. Lượt chạy: `integration_ac_20261024`. Nguồn đối chiếu: [kết quả Airflow](docs/validation/integration_airflow_run.json), [kiểm chứng tích hợp](docs/validation/pipeline_dashboard_integration.json) và [các JSON kiểm định](docs/validation/).

| Đầu ra | Số dòng |
|---|---:|
| Bronze historical | 30.317 |
| Bronze snapshot — 30 CSV (Batdongsan 10, Guland 9, Nhadatvui 11 batch) | 262.141 |
| Tổng Bronze | 292.458 |
| Silver `listing_observation` | 282.586 |
| Silver `listing_dq_quarantine` | 664 |
| Silver `silver_listings_current_27` | 137.732 |
| Silver `listing_history` | 151.982 |
| Silver `listing_history_state` — checkpoint theo listing | 137.732 |
| Silver `listing_location` / `listing_feature` | 137.732 mỗi bảng |
| Gold `fact_listing` — tin bán | 122.375 |
| Gold `fact_listing_lifecycle` | 137.732 |
| Gold `fact_listing_price_change` — sự kiện đổi giá tin bán | 1.732 |
| Gold `fact_listing_price_model` — dự đoán giá/m² của mô hình hedonic BQ2 | 68.666 |
| Gold `agg_price_model_coefficient` — hệ số mô hình hedonic | 223 |

Guland không có batch 20261020; pipeline giữ đúng lịch quan sát riêng của từng nguồn, không tạo batch thay thế.

Kết quả kiểm định:

- Silver: **30/30 PASS**.
- Gold: **108/108 PASS** (thêm 15 check cho mô hình hedonic BQ2 và 1 check khoảng cách BQ3, chạy lại ngày 26/10).
- Unit tests: **344 PASS tại máy local**; GitHub Actions chạy kiểm thử trên Python 3.10 và 3.12.
- Kiểm thử Spark SCD2: **11/11 PASS**; demo Iceberg: **6/6 PASS**.
- Kiểm thử Spark cho lifecycle: **12/12 PASS**.
- Airflow: **23/23 task SUCCESS**, thời gian chạy khoảng **12,48 phút**, run `integration_ac_20261024`. DAG hiện có 24 task (thêm `gold.price_model`), chưa chạy lại qua Airflow.
- Kiểm thử lỗi: một task thất bại, 22 task downstream bị chặn.
- Checksum 29 CSV đã có giữ nguyên; bổ sung đúng batch Nhadatvui 03/10, nâng tổng lên 30 CSV.

Đối chứng mới sau tích hợp: MERGE và full rebuild đều tạo **151.982 dòng history**, cùng SHA256 và không có khác biệt khi `EXCEPT ALL` hai chiều. MERGE chỉ tính lại timeline của **9.689/137.732 listing ID**, staging còn **4.398 dòng** thực sự thay đổi. Xem [kết quả history](docs/validation/history_incremental_verification.json) cho số đo thực tế; không khẳng định incremental luôn nhanh hơn rebuild. Retry không đổi input không tạo data snapshot history mới.

## An toàn vận hành và hiệu năng

- Manifest tái sử dụng kết quả parse CSV nhưng luôn SHA256 toàn bộ mọi file; chặn snapshot cũ bị sửa/xóa, CSV lỗi và LFS pointer chưa tải. Hash song song mặc định 4 worker.
- History chỉ MERGE các dòng thực sự đổi; có khóa writer local, kiểm tra location/schema, checkpoint phục hồi và fast path cho snapshot đã xác minh không đổi. Dùng `--force-verify` để bỏ fast path.
- Manifest, summary history và công cụ kiểm chứng ghi JSON nguyên tử, đọc được giữa UID Airflow/Spark. Runner không xóa thư mục app đang chạy.
- Giới hạn container master 1536 MiB để chừa bộ nhớ ngoài heap driver; Ivy cache nằm đúng named volume, có init quyền cho lần tạo volume đầu.
- Benchmark tối ưu trước tích hợp: manifest cached trung vị 3,64 giây (rehash 29 CSV); history MERGE 13,64 → 12,23 giây; lifecycle 2,89 → 2,66 giây. Đây là dataset đối chứng cố định cũ, không phải thời gian cam kết cho batch mới. Chỉ history incremental, không khẳng định cả pipeline nhanh theo cùng tỷ lệ.
- Đối chiếu tối ưu trước đây giữ nguyên nội dung 28/28 bảng. Sau tích hợp Silver có sửa giá/diện tích và thêm một batch thật nên dữ liệu thay đổi có chủ đích; kiểm chứng hiện tại đối chiếu số liệu Spark với Trino và dashboard, không ép hash về kết quả cũ.

Kết quả kỹ thuật: [đối chiếu 28 bảng](docs/validation/person_a_output_equivalence.json),
[benchmark history](docs/validation/person_a_history_optimization.json),
[benchmark manifest](docs/validation/person_a_manifest_benchmark.json).
Chi tiết lệnh và giới hạn vận hành: [Iceberg history](docs/operations/iceberg_history.md).

## Bảng dữ liệu phục vụ phân tích

| Bảng Gold | Nội dung |
|---|---|
| `fact_listing` và 9 dimension | Tin bán cùng khóa nguồn, thời gian, vị trí, loại hình và các nhóm giá/diện tích |
| `agg_budget_tradeoff`, `fact_budget_pareto` | BQ1: lựa chọn theo ngân sách, diện tích và đặc điểm |
| `agg_peer_group_benchmark`, `fact_listing_price_assessment` | BQ2: so sánh giá/m² với nhóm listing tương đồng |
| `fact_listing_price_model`, `agg_price_model_coefficient` | BQ2: mô hình hedonic giải thích giá/m² theo quận, diện tích, số phòng và 5 cờ đặc điểm; so với baseline trung vị nhóm |
| `agg_area_substitution` | BQ3: khu vực thay thế trong cùng tỉnh với chi phí thấp hơn cho cùng diện tích, không xa trung tâm tỉnh hơn quá 10 km |
| `fact_listing_price_change` | Các sự kiện thay đổi giá đã quan sát |
| `fact_listing_lifecycle` | Vòng đời quan sát của listing theo từng nguồn |
| `agg_market_overview`, `agg_dq_kpi` | Tổng quan thị trường và chất lượng dữ liệu |

Silver Current là bản ghi **mới nhất đã quan sát**, không xác nhận listing vẫn còn hiển thị. Cờ vắng hai batch không đồng nghĩa đã bán; `listed_days` là khoảng thời gian quan sát, không phải thời gian tới khi bán. Nguồn historical có trạng thái lifecycle `NOT_APPLICABLE`.

Các nhóm nghi trùng cross-source được gắn cờ theo luật; chưa bảo đảm mỗi nhóm tương ứng đúng một bất động sản thực. BQ2 gồm benchmark mô tả (P25–P75) và mô hình hedonic `hedonic_ols_v1`. Trên tập test, mô hình có R² log 0,73 so với 0,68 của baseline và MAE thấp hơn 3%, nhưng chỉ tốt hơn baseline ở 2/4 loại bất động sản. Hệ số là tương quan trên giá chào bán, không phải tác động nhân quả; chi tiết tại [tiêu chí chấp nhận BQ](docs/business/bq_acceptance_criteria.md). Backtest sơ bộ (`src/quality/bq2_backtest.py`, [kết quả](docs/validation/bq2_backtest.json)): tin trên P75 lúc mới đăng có tỷ lệ giảm giá 2,93% so với 2,03% của các tin còn lại, nhưng khác biệt không còn ý nghĩa ở mức 5% sau khi kiểm soát theo nguồn (p = 0,08); dữ liệu mới khoảng 1 tháng. Silver Core và Gold vẫn rebuild từ Bronze; riêng `listing_history` cập nhật incremental qua MERGE và bảng state phục hồi được. Bronze tái sử dụng các batch đã hoàn tất.

## Iceberg history và kiểm thử tự động

Hướng dẫn chạy, quy tắc SCD2, checkpoint/retry và cách kiểm chứng: [Vận hành Iceberg history](docs/operations/iceberg_history.md).

```powershell
./scripts/run_spark.ps1 src/silver/build_listing_history.py --mode incremental
./scripts/run_spark.ps1 src/quality/verify_history_incremental.py --baseline-batch 20261016
./scripts/run_spark.ps1 src/quality/demo_iceberg_features.py
python -m pytest -q
```

Workflow GitHub Actions `Tests` chạy pytest trên Python 3.10/3.12 khi push hoặc tạo pull request. Kiểm thử Spark/Iceberg chạy thực tế trong Docker và xuất [JSON kiểm chứng](docs/validation/); các bảng demo nằm riêng trong namespace `smoke_test`.

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

Catalog hiện dùng SQLite. **Tách cửa sổ Spark ghi và Trino đọc**: dừng Trino trước
khi chạy pipeline; Airflow sẽ chặn Spark nếu Trino còn hoạt động. Dashboard tạm
không truy vấn được trong cửa sổ này. Không chạy script Spark thủ công đồng thời
với Trino. Chưa chuyển catalog sang PostgreSQL hoặc chứng nhận đọc/ghi đồng thời.

```powershell
docker stop tlcn_superset tlcn_trino
```

Khi nạp dữ liệu lần đầu, chuẩn bị đủ historical CSV và snapshot rồi chạy:

```powershell
.\scripts\run_spark.ps1 src\bronze\ingest_bronze.py
python -c "from src.orchestration.pipeline import validate_snapshots; validate_snapshots('20261023')"
.\scripts\run_spark.ps1 src\bronze\ingest_snapshots_bronze.py --skip-existing
```

Chạy toàn bộ pipeline qua Airflow với một batch đã có:

```powershell
.\scripts\run_airflow_pipeline.ps1 -BatchId 20261023 -HistoryMode incremental
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

## Chạy dashboard và đánh giá

Sau khi pipeline đã hoàn tất:

```powershell
python -m pip install -r requirements.txt
docker compose --profile query --profile dashboard up -d trino superset
python -m src.dashboard.provision_superset
python -m src.quality.verify_pipeline_dashboard
```

Mở Superset tại http://localhost:8088, đăng nhập bằng tài khoản trong `.env`.
Dashboard: `/superset/dashboard/tlcn-tong-quan/`, `tlcn-bq1/`, `tlcn-bq2/`, `tlcn-bq3/`.
Provision chỉ dựng lại tài nguyên do script quản lý; sao lưu cấu hình Superset
trước khi chạy nếu đã chỉnh sửa dashboard. Biểu đồ truy vấn Gold; phần nhận xét
đầu trang được tính lúc provision và không tự đổi theo bộ lọc tương tác. Chạy lại
provision sau batch mới để cập nhật phần nhận xét.

- [Kiểm tra dashboard](docs/validation/superset_dashboard_check.json): 29 biểu đồ và KPI.
- [Benchmark truy vấn](docs/validation/query_benchmark.json): so sánh thời gian mô tả; truy vấn Gold/Silver không tương đương hoàn toàn nên không xem tỷ lệ thời gian là speedup có kiểm soát.
- [Hướng dẫn gán nhãn](docs/validation/labeling/README.md): 198 mẫu, hiện chưa có nhãn thủ công nên chưa có accuracy thực nghiệm. Không điền dự đoán làm nhãn thật.

`verify_pipeline_dashboard` dùng bằng chứng run tích hợp đã lưu, không tự chứng
nhận cho batch mới. Khi kiểm chứng một run mới cần export lại trạng thái Airflow,
chạy lại kiểm định và provision trước khi đọc kết quả này.

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
src/quality/           Kiểm thử Spark cho lifecycle/SCD2 và demo Iceberg
src/report/            Xuất dữ liệu, biểu đồ và báo cáo
dags/                  DAG Airflow
scripts/               Lệnh vận hành PowerShell
docker/                Cấu hình và Dockerfile cho các dịch vụ
tests/unit/            Unit tests
docs/                  Kiến trúc, hợp đồng dữ liệu và kết quả kiểm chứng
```
