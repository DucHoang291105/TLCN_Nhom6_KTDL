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

Superset có 5 dashboard: tổng quan thị trường, lựa chọn theo ngân sách (BQ1), vị trí giá trong nhóm tương đồng (BQ2), quãng đường đường bộ tới nơi làm/học và giá theo vành đai (BQ3) và chất lượng dữ liệu. Dữ liệu đọc từ Gold qua Trino; script kiểm tra lại 49 biểu đồ và đối chiếu KPI. Các bản đồ dùng nền OpenStreetMap.

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

Hai crawler web là các bước thu thập độc lập: một nguồn lỗi không ngăn nguồn kia
thu thập. Sensor CSV Batdongsan và các lớp xử lý chỉ tiếp tục khi cả hai nguồn
web hợp lệ. Cấu hình mặc định là **Guland 20 trang, Nhadatvui 200 trang**; có thể
đổi bằng `GULAND_CRAWL_PAGES` / `NHADATVUI_CRAWL_PAGES`. Mốc 20 trang Guland đã
chạy thực tế trong container; thử trang 200 hiện bị website trả HTTP 403.
Đây là phạm vi lấy mẫu, không phải toàn bộ tin trên website. So sánh vòng đời
giữa các batch cần xét cả thay đổi phạm vi lấy mẫu.

Crawler dừng khi website xác nhận hết dữ liệu, đạt số trang cấu hình, gặp chặn
HTTP 401/403, hết lượt chờ HTTP 429, lặp trang, hoặc lỗi liên tiếp đạt ngưỡng
(mặc định 2 trang lỗi). Request timeout mặc định 30 giây, tối đa 3 lần thử;
ngân sách mỗi lượt mặc định 2.100 giây. Các biến điều chỉnh là
`CRAWL_REQUEST_TIMEOUT`, `CRAWL_MAX_RETRIES`, `CRAWL_MAX_CONSECUTIVE_FAILURES`,
`CRAWL_RUN_TIMEOUT_SECONDS` và `CRAWL_DELAY_SECONDS`.

Pipeline cào vào thư mục tạm có tên bắt đầu bằng dấu chấm, kiểm tra summary,
phạm vi trang, schema và số dòng CSV, rồi mới công bố batch `YYYYMMDD` và lưu
SHA256. Lượt lỗi giữ lại dữ liệu tạm để kiểm tra, không được nạp như batch PASS.
CSV đã có được giữ nguyên; batch cào thành công chỉ được tái sử dụng khi checksum
và hợp đồng dữ liệu còn đúng. CSV lịch sử do người dùng cung cấp vẫn được tiếp
nhận theo quy trình manifest hiện có.

Có thể thử hai file chạy từ thư mục gốc mà không tạo batch sản xuất:

```powershell
$env:CRAWL_PAGES = "3"
$env:CRAWL_OUTPUT_DIR = "$PWD\outputs\crawl_try_guland"
.\.venv\Scripts\python.exe -m src.ingestion.guland.guland_raw_crawler
$env:CRAWL_OUTPUT_DIR = "$PWD\outputs\crawl_try_nhadatvui"
.\.venv\Scripts\python.exe -m src.ingestion.nhadatvui.nhadatvui_raw_crawler
Remove-Item Env:CRAWL_PAGES, Env:CRAWL_OUTPUT_DIR
```

Chọn thư mục output mới cho mỗi lần thử; crawler không ghi đè CSV đã tồn tại.
Summary `PASS` chỉ chứng nhận phạm vi trang đã yêu cầu hoặc điểm kết thúc được
website xác nhận. `FAIL` / `RUNNING` không đủ điều kiện cho bước nạp Bronze.

Dữ liệu historical đặt trong `data/incoming/historical/<nguồn>/`, theo hợp đồng tại `src/bronze/historical_contract.py`.

## Kết quả kiểm định hiện hành

Bản tích hợp đã nghiệm thu với run `official_integrated_clean_20261122`, batch `20261120`: **94/94 điều kiện PASS**, **597/597 unit test PASS**, DAG **26/26 SUCCESS**. Kiểm chứng được chạy lại sau khi ghép cập nhật và dọn mã nguồn.

Nguồn số liệu chính thức là [defense_verification.json](docs/validation/defense_verification.json):
run, batch, số điều kiện PASS, hash code/input và số dòng Silver/Gold. Kiểm tra
báo cáo còn đúng với code và dữ liệu trên máy bằng:

```powershell
.\.venv\Scripts\python.exe -m src.orchestration.status
```

Báo cáo của tag cũ chứng nhận đúng phiên bản cũ; thêm batch hoặc thay code cần
nghiệm thu lại. Tập dữ liệu nhận tới 20/11 gồm 51 snapshot CSV (325.191 dòng)
và 6 historical CSV (30.317 dòng). Số dòng đầu ra phải đọc từ lượt kiểm định
hiện hành, vì còn phụ thuộc DQ và dedup.

Suite chạy DAG 26 task; hai rebuild so schema, grain và multiset SHA256 trên
38 bảng; SCD2, lifecycle, Iceberg, truy vấn Trino, 49 chart/5 dashboard và bộ lọc.
Backtest, tái tạo mô hình và kiểm tra repost dùng Gold vừa dựng. Đề xuất chọn
trọng số trên dữ liệu mới được lưu riêng, không tự đổi cấu hình mô hình.
Kết quả kỹ thuật không thay thế nghiệm thu BQ hoặc nhãn thủ công G4.

Guland thiếu 20/10, 30/10; Nhadatvui thiếu 30/10, 10/11, đã được chấp nhận và
khai báo trong `config/source_gaps.json`. Quyết định chỉnh ngày Guland 10/11
và checksum được lưu ở `input_date_review.json`; chưa xác minh độc lập ngày
thu thập. Tin vắng hai batch chỉ là vắng trong mẫu crawl, chưa xác nhận đã bán.
Độ sâu từng nguồn và batch: [crawl_coverage.json](docs/validation/crawl_coverage.json).

### Số liệu đầu ra

Bảng này được xuất từ JSON của batch 20/11 sau hai rebuild trên bản tích hợp. Trạng thái nghiệm thu đầy đủ nằm trong `defense_verification.json`.

| Đầu ra | Số dòng |
|---|---:|
| Bronze historical | 30,317 |
| Bronze snapshot — 51 CSV | 325,191 |
| Tổng Bronze | 355,508 |
| Silver `listing_observation` | 343,465 |
| Silver `listing_dq_quarantine` | 664 |
| Silver Current / Location / Feature | 151,740 |
| Silver `listing_history` | 169,991 |
| Silver `listing_history_state` | 151,740 |
| Gold `fact_listing` | 136,383 |
| Gold `fact_listing_lifecycle` | 151,740 |
| Gold `fact_listing_price_change` | 3,221 |
| Gold `fact_listing_price_model` | 127,961 |
| Gold `agg_price_model_coefficient` | 972 |

Silver **30/30 PASS**; Gold **139/139 PASS**. Hai rebuild **38/38 bảng** giống schema, grain và SHA256 nội dung.

Kết quả chỉ áp dụng cho code và input đã kiểm chứng. Sau thay đổi phải chạy lại nghiệm thu; dữ liệu thô không phải số dòng sau DQ/dedup.

## An toàn vận hành và hiệu năng

- Manifest tái sử dụng kết quả parse CSV nhưng luôn SHA256 toàn bộ mọi file; chặn snapshot cũ bị sửa/xóa, CSV lỗi và LFS pointer chưa tải. Hash song song mặc định 4 worker.
- History chỉ MERGE các dòng thực sự đổi; có khóa writer local, kiểm tra location/schema, checkpoint phục hồi và fast path cho snapshot đã xác minh không đổi. Dùng `--force-verify` để bỏ fast path.
- Manifest, summary history và công cụ kiểm chứng ghi JSON nguyên tử, đọc được giữa UID Airflow/Spark. Runner không xóa thư mục app đang chạy.
- Giới hạn container master 1536 MiB để chừa bộ nhớ ngoài heap driver; Ivy cache nằm đúng named volume, có init quyền cho lần tạo volume đầu.
- Bộ kiểm chứng hiện hành đối chiếu đủ 38 bảng qua hai rebuild, gồm schema, grain và hash toàn bộ nội dung. Benchmark checksum một/bốn worker được lưu trong `identity_benchmark.json`; không dùng thời gian của bộ dữ liệu cũ để cam kết hiệu năng cho batch mới.

Kết quả kỹ thuật: [hai rebuild](docs/validation/deterministic_rebuild.json),
[history và retry](docs/validation/history_incremental_verification.json),
[benchmark checksum](docs/validation/identity_benchmark.json).
Chi tiết lệnh và giới hạn vận hành: [Iceberg history](docs/operations/iceberg_history.md).

## Bảng dữ liệu phục vụ phân tích

| Bảng Gold | Nội dung |
|---|---|
| `fact_listing` và 9 dimension | Tin bán cùng khóa nguồn, thời gian, vị trí, loại hình và các nhóm giá/diện tích |
| `agg_budget_tradeoff`, `fact_budget_pareto` | BQ1: lựa chọn theo ngân sách, diện tích và đặc điểm |
| `agg_peer_group_benchmark`, `fact_listing_price_assessment` | BQ2: so sánh giá/m² với nhóm listing tương đồng |
| `fact_listing_price_model`, `agg_price_model_coefficient` | BQ2: mô hình hedonic giải thích giá/m² theo quận, diện tích, số phòng, 5 cờ đặc điểm, khoảng cách tới trung tâm và phường, kết hợp với baseline trung vị nhóm |
| `agg_area_substitution` | BQ3 (bảng phụ, dashboard không dùng): khu vực thay thế trong cùng tỉnh với chi phí thấp hơn cho cùng diện tích, không xa trung tâm tỉnh hơn quá 10 km |
| `fact_listing_commute` | BQ3 theo nơi làm/học (dashboard dùng bảng này): mỗi tin bán đại diện × nơi đến có tên trong cùng tỉnh, quãng đường và thời gian đi đường bộ (OSRM, ô lưới 0,01°, `config/commute_road_distance.csv`, lấy lại bằng `python -m src.gold.commute_road fetch`), khoảng cách chim bay để so sánh, vành đai đường bộ (<5, 5–10, 10–15, 15–20, 20–30, ≥30 km) |
| `agg_commute_district` | BQ3 theo nơi làm/học: mỗi quận × nơi đến (`config/commute_destinations.csv` và trung tâm 34 tỉnh), quãng đường chim bay từ tâm quận và trung vị quãng đường đường bộ của tin trong quận, chi phí cho cùng diện tích, cờ không bị quận khác vừa gần hơn vừa rẻ hơn (theo chim bay và theo đường bộ) |
| `agg_commute_substitution` | BQ3 theo nơi làm/học (bảng phụ, dashboard không dùng): quận rẻ hơn với quãng đường chim bay tới nơi đến không xa hơn quá 2 km so với quận đang nhắm |
| `ev_budget_curve`, `ev_budget_pairs` | Bằng chứng BQ1: diện tích theo ngân sách và vùng khoảng cách; mỗi quận một cặp tin thật (tin B rẻ hơn ≥ 10%, rộng hơn ≥ 20%) |
| `ev_reprice`, `ev_reprice_examples` | Bằng chứng BQ2: vị trí giá lúc mới đăng và việc hạ giá 1–30% sau đó (H1), kèm tin thật |
| `ev_flag_check`, `ev_calibration` | Kiểm chứng mô hình BQ2: chênh giá thô theo cờ trong cùng quận–diện tích so với hệ số; giá ước tính so với giá thật trên tập test |
| `fact_listing_price_change` | Các sự kiện thay đổi giá đã quan sát |
| `fact_listing_lifecycle` | Vòng đời quan sát của listing theo từng nguồn |
| `agg_market_overview`, `agg_dq_kpi` | Tổng quan thị trường và chất lượng dữ liệu |

Silver Current là bản ghi **mới nhất đã quan sát**, không xác nhận listing vẫn còn hiển thị. Cờ vắng hai batch không đồng nghĩa đã bán; `listed_days` là khoảng thời gian quan sát, không phải thời gian tới khi bán. Nguồn historical có trạng thái lifecycle `NOT_APPLICABLE`.

Các nhóm nghi trùng cross-source được gắn cờ theo luật; chưa bảo đảm mỗi nhóm tương ứng đúng một bất động sản thực. BQ2 gồm benchmark mô tả (P25–P75) và mô hình hedonic kết hợp baseline (`hedonic_ols_blend`, tham số tại `config/price_model_blend.json`). Mô hình phủ 94,69% tin đại diện. Trên phần test trong khoảng trim, dữ liệu tới 17/11, R² log 0,720 so với 0,656 của baseline, MAE giá/m² thấp hơn 7,41% (41,8 so với 45,1 triệu đồng). So sánh sai số ghép cặp hiện cho cải thiện ở cả 4 loại, nhưng ở biệt thự chỉ khoảng 1,3%. Kết quả ở [price_model_summary.json](docs/validation/price_model_summary.json), giới hạn ở [BQ acceptance](docs/business/bq_acceptance_criteria.md). Độ bất định của hệ số hedonic còn cần tính lại từ sai số mô hình thuần trước khi dùng khoảng tin cậy để kết luận; hệ số thể hiện tương quan trên giá chào bán, không phải tác động nhân quả.

Backtest ([kết quả](docs/validation/bq2_backtest.json), dữ liệu 22/09–17/11) dùng định nghĩa giảm giá 1–30%: nhóm trên P75 lúc đầu có tỷ lệ giảm giá khoảng 4,09% so với 2,92% ở nhóm còn lại. Sau kiểm soát nguồn, p ≈ 0,047, chưa đạt ngưỡng Bonferroni 0,05/3; kết quả còn khác nhau theo tỉnh. H2 về phần dư mô hình chưa được khẳng định sau kiểm soát nguồn hoặc trên test riêng. Đây là phân tích hồi cứu dùng peer benchmark hiện tại; không trình bày như dự báo hoặc quan hệ nhân quả. Silver Core và Gold vẫn rebuild từ Bronze; riêng `listing_history` cập nhật incremental qua MERGE và bảng state phục hồi được. Bronze tái sử dụng các batch đã hoàn tất.

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

Yêu cầu: Docker Desktop đang chạy Linux containers, Git + Git LFS,
Python 3.10+ và PowerShell. Đây là quy trình chuẩn bị để thử trên máy sạch;
**chưa xác nhận tái lập hoặc đo thời gian trên máy thứ hai**.

```powershell
git clone https://github.com/DucHoang291105/TLCN_Nhom6_KTDL.git
Set-Location TLCN_Nhom6_KTDL
git lfs install
git lfs pull
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m src.orchestration.init_environment
docker compose config --quiet
```

`init_environment` tạo `.env` khi chưa có, sinh mật khẩu và secret riêng,
bao gồm Fernet key hợp lệ. File `.env` đã có được giữ nguyên. Thông tin đăng
nhập MinIO/Superset nằm trong `.env` local; Airflow standalone sinh tài khoản
ban đầu trong thư mục `/opt/airflow` của container. Không đổi password
PostgreSQL sau khi đã khởi tạo volume nếu chưa đồng bộ tài khoản trong DB.

Có thể dùng `./scripts/reproduce_environment.ps1 -Phase Prepare` thay cho các
bước LFS/venv/dependency/env sau khi clone; `-Phase Check` kiểm tra điều kiện
máy. Hướng dẫn nhận CSV và đo thời gian trên máy khác:
[nhận dữ liệu và tái lập](docs/operations/nhan_du_lieu_va_tai_lap.md).

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
.\.venv\Scripts\python.exe -m src.orchestration.environment write
```

Khi nạp lần đầu, chuẩn bị đủ historical và snapshot; chọn batch thật có đủ
ba nguồn. Ví dụ dưới dùng **20261027**, batch đầy đủ đã kiểm chứng. Thay ngày
khi nhận đủ CSV mới.

```powershell
.\.venv\Scripts\python.exe -m src.quality.crawl_coverage --as-of 20261027 --strict
.\scripts\run_spark.ps1 src\bronze\ingest_bronze.py
.\scripts\run_airflow_pipeline.ps1 -BatchId 20261027 -HistoryMode incremental
```

Runner nộp run rồi trả về; đợi Airflow kết thúc thành công trước khi chuyển
query mode. Dùng run ID được in khi nộp:

```powershell
# Thay RUN_ID bằng run thực tế:
docker exec tlcn_airflow python -m src.orchestration.airflow_run status --run-id RUN_ID
docker exec tlcn_airflow python -m src.orchestration.airflow_run export --run-id RUN_ID --output integration_airflow_run.json
```

Kiểm tra dữ liệu tới batch thực tế mới nhất bằng audit riêng. Các điều kiện còn thiếu
được ghi trong JSON, không tự tạo gap hoặc đổi ngày CSV:

```powershell
.\.venv\Scripts\python.exe -m src.quality.crawl_coverage --as-of 20261120 --strict
```

Nhận file xuất extension hoặc CSV do thành viên gửi bằng
`python -m src.orchestration.receive_snapshot --source batdongsan --batch-id YYYYMMDD --file PATH`.
Mặc định chỉ kiểm tra; thêm `--apply` để copy nguyên bytes vào cấu trúc chuẩn.
Chạy lại file giống hệt được tái sử dụng, file khác nội dung không bị ghi đè.

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
.\.venv\Scripts\python.exe -m src.orchestration.environment read
.\.venv\Scripts\python.exe -m src.dashboard.provision_superset
.\.venv\Scripts\python.exe -m src.quality.verify_pipeline_dashboard
```

Mở Superset tại http://localhost:8088, đăng nhập bằng tài khoản trong `.env`.
Dashboard: `/superset/dashboard/tlcn-tong-quan/`, `tlcn-bq1/`, `tlcn-bq2/`, `tlcn-bq3/`, `tlcn-chat-luong/`.
Provision chỉ dựng lại tài nguyên do script quản lý; sao lưu cấu hình Superset
trước khi chạy nếu đã chỉnh sửa dashboard. Thẻ kết luận truy vấn SQL theo bộ lọc
tỉnh, loại hình, ngân sách hoặc nơi làm/học. Riêng thẻ BQ2 tổng hợp theo tỉnh và
loại hình, không theo quận. Các biểu đồ ghi `(Cố định)` hoặc `(Tất cả…)` giữ phạm
vi so sánh riêng; tiêu đề nhận xét của chúng được tính khi provision. Chạy lại
provision sau batch mới để cập nhật cấu hình và tiêu đề nhận xét.

- [Kiểm tra dashboard](docs/validation/superset_dashboard_check.json): 49 biểu đồ và KPI.
- [Benchmark truy vấn](docs/validation/query_benchmark.json): so sánh thời gian mô tả; truy vấn Gold/Silver không tương đương hoàn toàn nên không xem tỷ lệ thời gian là speedup có kiểm soát.
- [Kịch bản demo kỹ thuật](docs/demo/kich_ban_demo.md): Airflow, fault gate, lifecycle, Iceberg và Trino.
- [Hướng dẫn gán nhãn](docs/validation/labeling/README.md): 198 mẫu có nhãn AI sơ bộ và một lượt rà soát; **0 mẫu hoàn tất hai lượt rà soát**, trạng thái `PARTIAL`. Các metric so với bộ nhãn hiện tại là kết quả tạm thời, chưa phải nghiệm thu nhãn thủ công độc lập.

`verify_pipeline_dashboard` dùng bằng chứng run tích hợp đã lưu, không tự chứng
nhận cho batch mới. Khi kiểm chứng một run mới cần export lại trạng thái Airflow,
chạy lại kiểm định và provision trước khi đọc kết quả này.

Dùng [kiểm định tích hợp hiện hành](docs/validation/pipeline_dashboard_integration.json) để đối chiếu 38 bảng Spark–Trino, biểu đồ và bộ lọc.

DAG được giữ paused trong cửa sổ đọc để tránh writer tự động trùng lúc Trino hoạt động. Khi có batch mới, chuyển write mode rồi nộp run theo hướng dẫn ở trên. Một tag version mới chỉ tạo sau nghiệm thu dữ liệu chốt và CI trên đúng commit; mọi tag cũ được giữ nguyên.

## Kiểm thử và xem kết quả

```powershell
python -m pytest -q
.\.venv\Scripts\python.exe -m src.quality.verify_report_numbers
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

## Công bố và phục hồi

Xem [hướng dẫn vận hành](docs/operations/publication_and_recovery.md) về khóa writer, hàng rào công bố, chống ngủ, backup/restore và image đã ghim digest.
