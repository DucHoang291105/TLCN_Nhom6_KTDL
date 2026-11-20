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

Dữ liệu tới batch **20261117**, ba nguồn crawl và sáu nguồn historical. Bộ
nghiệm thu `acceptance_A_20261117_r2` đã PASS trên code và input được đóng
băng trong suốt lượt chạy; không tạo batch thay thế cho những ngày thiếu.

| Đầu ra | Số dòng | JSON nguồn trong `docs/validation/` |
|---|---:|---|
| Bronze historical | 30.317 | `bronze_state_audit.json` |
| Bronze snapshot — 48 CSV | 316.241 | `snapshot_manifest.json` |
| Tổng Bronze | 346.558 | `bronze_state_audit.json` |
| Silver `listing_observation` | 334.820 | `silver_final_verification.json` |
| Silver `listing_dq_quarantine` | 664 | `silver_final_verification.json` |
| Silver Current / Location / Feature | 150.017 | `silver_final_verification.json` |
| Silver `listing_history` | 167.794 | `silver_final_verification.json` |
| Silver `listing_history_state` | 150.017 | `silver_final_verification.json` |
| Gold `fact_listing` | 134.660 | `gold_verification.json` |
| Gold `fact_listing_lifecycle` | 150.017 | `gold_verification.json` |
| Gold `fact_listing_price_change` | 3.021 | `gold_verification.json` |
| Gold `fact_listing_price_model` | 128.853 | `gold_verification.json` |
| Gold `agg_price_model_coefficient` | 971 | `gold_verification.json` |

- Nghiệm thu tổng: **66/66 PASS**, `code_unchanged = true`, toàn bộ 54 CSV đầu vào không đổi.
- Airflow: **24/24 task SUCCESS**, thời gian **14 phút 17.59 giây**.
- Silver **30/30 PASS**; Gold **113/113 PASS**.
- Unit tests trong lượt nghiệm thu: **466/466 PASS**, không failure/error/skip.
- Hai rebuild khớp đủ **30 bảng** về schema, grain, số dòng và SHA256 nội dung.
- MERGE/SCD2 **11/11**, lifecycle **12/12**, demo Iceberg **6/6**; thử lỗi chặn đúng **23 task downstream**.
- Trino đối chiếu đúng 30 bảng; **29 biểu đồ** và bộ lọc tỉnh khớp Gold.

Nguồn: [nghiệm thu tổng](docs/validation/defense_verification.json),
[Airflow](docs/validation/integration_airflow_run.json),
[Silver](docs/validation/silver_final_verification.json),
[Gold](docs/validation/gold_verification.json),
[hai rebuild](docs/validation/deterministic_rebuild.json),
[tích hợp dashboard](docs/validation/pipeline_dashboard_integration.json).
Kết quả chỉ áp dụng cho code và input đã kiểm chứng. Sau sửa code hoặc thêm
batch, chạy lại kiểm chứng trước khi công bố kết quả hoặc gắn tag mới.

Đối chứng history hiện tại: MERGE và rebuild cùng **167.794 dòng**, SHA256
bằng nhau và không có khác biệt hai chiều. Trong benchmark từ baseline
16/10, MERGE xử lý **26.668 ID**, staging **22.260 dòng**,
mất **17.74 giây**; rebuild **9.96 giây**. Retry không đổi
input giữ nguyên snapshot. Đây là kiểm chứng tính đúng; không khẳng định
MERGE luôn nhanh hơn rebuild. Xem [history](docs/validation/history_incremental_verification.json).

Guland thiếu **20/10 và 30/10**; Nhadatvui thiếu **30/10 và 10/11**, đã được
chấp nhận và khai báo trong `config/source_gaps.json`. CSV Guland batch 10/11
được chủ dữ liệu hiệu chỉnh timestamp từ 12/11 về 10/11 trước ingest; quyết
định và checksum trước/sau ở [input date review](docs/validation/input_date_review.json).
Ngày thu thập của file này chưa được xác minh độc lập; phân tích theo thời
gian cần công bố giới hạn đó.

Dữ liệu hiện có **108.415/119.700 tin crawl vắng hai batch (90,57%)**; đây là
vắng trong mẫu thu thập, không xác nhận đã bán hoặc đã gỡ. Xem
[điều tra độ phủ](docs/operations/crawl_coverage.md) và
[JSON đo](docs/validation/crawl_coverage.json).

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
| `fact_listing_price_model`, `agg_price_model_coefficient` | BQ2: mô hình hedonic giải thích giá/m² theo quận, diện tích, số phòng, 5 cờ đặc điểm, khoảng cách tới trung tâm và phường, kết hợp với baseline trung vị nhóm |
| `agg_area_substitution` | BQ3: khu vực thay thế trong cùng tỉnh với chi phí thấp hơn cho cùng diện tích, không xa trung tâm tỉnh hơn quá 10 km |
| `agg_commute_district` | BQ3 theo nơi làm/học: mỗi quận × nơi đến (`config/commute_destinations.csv` và trung tâm 34 tỉnh), quãng đường đường chim bay từ tâm quận, chi phí cho cùng diện tích, cờ không bị quận khác vừa gần hơn vừa rẻ hơn |
| `agg_commute_substitution` | BQ3 theo nơi làm/học: quận rẻ hơn với quãng đường tới nơi đến không xa hơn quá 2 km so với quận đang nhắm |
| `ev_budget_curve`, `ev_budget_pairs` | Bằng chứng BQ1: diện tích theo ngân sách và vùng khoảng cách; mỗi quận một cặp tin thật (tin B rẻ hơn ≥ 10%, rộng hơn ≥ 20%) |
| `ev_reprice`, `ev_reprice_examples` | Bằng chứng BQ2: vị trí giá lúc mới đăng và việc hạ giá 1–30% sau đó (H1), kèm tin thật |
| `ev_flag_check`, `ev_calibration` | Kiểm chứng mô hình BQ2: chênh giá thô theo cờ trong cùng quận–diện tích so với hệ số; giá ước tính so với giá thật trên tập test |
| `fact_listing_price_change` | Các sự kiện thay đổi giá đã quan sát |
| `fact_listing_lifecycle` | Vòng đời quan sát của listing theo từng nguồn |
| `agg_market_overview`, `agg_dq_kpi` | Tổng quan thị trường và chất lượng dữ liệu |

Silver Current là bản ghi **mới nhất đã quan sát**, không xác nhận listing vẫn còn hiển thị. Cờ vắng hai batch không đồng nghĩa đã bán; `listed_days` là khoảng thời gian quan sát, không phải thời gian tới khi bán. Nguồn historical có trạng thái lifecycle `NOT_APPLICABLE`.

Các nhóm nghi trùng cross-source được gắn cờ theo luật; chưa bảo đảm mỗi nhóm tương ứng đúng một bất động sản thực. BQ2 gồm benchmark mô tả (P25–P75) và mô hình hedonic kết hợp baseline (`hedonic_ols_blend`, tham số tại `config/price_model_blend.json`). Trên tập test dữ liệu tới 17/11, mô hình phủ 96.52% tin đại diện; R² log 0.732 so với 0.672 của baseline, MAE giá/m² thấp hơn 7.42%. Đây là kết quả đánh giá mô hình hiện tại trong [price_model_summary.json](docs/validation/price_model_summary.json); ý nghĩa và tiêu chí nghiệp vụ cần đọc theo [BQ acceptance](docs/business/bq_acceptance_criteria.md). Hệ số thể hiện tương quan trên giá chào bán, không phải tác động nhân quả. Backtest sơ bộ (`src/quality/bq2_backtest.py`, [kết quả](docs/validation/bq2_backtest.json), dữ liệu 22/09–27/10): tin trên P75 lúc mới đăng có tỷ lệ giảm giá 3,26% so với 2,15% của các tin còn lại, vẫn có ý nghĩa sau khi kiểm soát theo nguồn (p = 0,006); phần dư mô hình hedonic chưa cho tín hiệu có ý nghĩa. Với dữ liệu tới 23/10 kết quả H1 chưa đạt (p = 0,08), nên cần thêm batch để khẳng định. Silver Core và Gold vẫn rebuild từ Bronze; riêng `listing_history` cập nhật incremental qua MERGE và bảng state phục hồi được. Bronze tái sử dụng các batch đã hoàn tất.

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

Chuẩn bị nhận dữ liệu ngày 17/11 bằng audit riêng. Các điều kiện còn thiếu
được ghi trong JSON, không tự tạo gap hoặc đổi ngày CSV:

```powershell
.\.venv\Scripts\python.exe -m src.quality.crawl_coverage --as-of 20261117 --strict
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
Dashboard: `/superset/dashboard/tlcn-tong-quan/`, `tlcn-bq1/`, `tlcn-bq2/`, `tlcn-bq3/`.
Provision chỉ dựng lại tài nguyên do script quản lý; sao lưu cấu hình Superset
trước khi chạy nếu đã chỉnh sửa dashboard. Biểu đồ truy vấn Gold; phần nhận xét
đầu trang được tính lúc provision và không tự đổi theo bộ lọc tương tác. Chạy lại
provision sau batch mới để cập nhật phần nhận xét.

- [Kiểm tra dashboard](docs/validation/superset_dashboard_check.json): 29 biểu đồ và KPI.
- [Benchmark truy vấn](docs/validation/query_benchmark.json): so sánh thời gian mô tả; truy vấn Gold/Silver không tương đương hoàn toàn nên không xem tỷ lệ thời gian là speedup có kiểm soát.
- [Kịch bản demo kỹ thuật](docs/demo/kich_ban_demo.md): Airflow, fault gate, lifecycle, Iceberg và Trino.
- [Kết quả thử thao tác demo](docs/validation/demo_rehearsals.json): hai lượt browser tự động PASS trên dữ liệu đã lưu; có video local không lời thuyết minh.
- [Hướng dẫn gán nhãn](docs/validation/labeling/README.md): 198 mẫu, hiện chưa có nhãn thủ công nên chưa có accuracy thực nghiệm. Không điền dự đoán làm nhãn thật.

`verify_pipeline_dashboard` dùng bằng chứng run tích hợp đã lưu, không tự chứng
nhận cho batch mới. Khi kiểm chứng một run mới cần export lại trạng thái Airflow,
chạy lại kiểm định và provision trước khi đọc kết quả này.

Probe trước A3 từng phát hiện hai bảng mô hình cũ; [bằng chứng probe](docs/validation/predata_runtime_compatibility.json) được giữ để truy nguyên. Lần nghiệm thu mới đã rebuild và đối chiếu đúng toàn bộ 30 bảng qua Trino; dùng [kết quả hiện tại](docs/validation/pipeline_dashboard_integration.json) để đánh giá trạng thái đồng bộ.

DAG được giữ paused trong cửa sổ đọc để tránh writer tự động trùng lúc Trino hoạt động. Khi có batch mới, chuyển write mode rồi nộp run theo hướng dẫn ở trên. Tag `v1.1-bao-ve` chỉ tạo sau nghiệm thu dữ liệu chốt và CI trên đúng commit; tag cũ được giữ nguyên.

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
