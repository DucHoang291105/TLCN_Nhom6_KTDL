# Báo cáo pipeline và vận hành — tuần 16–22/10/2026

Phạm vi: phần A trong giai đoạn hoàn thiện; dữ liệu tới batch **20261016**. Số liệu được xuất từ JSON kiểm chứng và Airflow API, không nhập tay.

## Đã thực hiện

- Bổ sung và đưa lên Git LFS đủ 8 batch cho mỗi nguồn Batdongsan/Guland/Nhadatvui: 24 CSV.
- Nạp snapshot vào Bronze, kiểm tra manifest/checksum và audit đủ 9 nguồn.
- Rebuild Silver Core, History, Location, Feature; verify Silver.
- Rebuild Gold và xây `lakehouse.gold.fact_listing_lifecycle`, một dòng/source_id.
- Bổ sung kiểm định lifecycle độc lập, đối chiếu observation và repricing, fixture Spark và unit tests.
- Hoàn thiện Dockerfile Airflow, cấu hình bền qua restart, DAG 23 task, lịch 22:00 thứ 3/thứ 6 giờ Việt Nam.
- Dùng lại CSV sẵn có; chỉ tự crawl Guland/Nhadatvui khi thiếu batch đúng ngày hiện tại. Batdongsan được sensor tiếp nhận từ Chrome extension.
- Thử lỗi có chủ đích ở bước chuẩn bị dữ liệu: task đầu FAIL, toàn bộ 22 task sau bị chặn.

## Kết quả thu được

| Đầu ra | Kết quả |
|---|---:|
| CSV snapshot | 24 |
| Dòng Bronze snapshot | 241,991 |
| Tổng Bronze, gồm historical | 272,308 |
| Silver observation | 263,331 |
| Silver current | 134,620 |
| Silver history | 148,209 |
| Gold fact_listing (tin bán) | 119,263 |
| Gold fact_listing_lifecycle | 134,620 |
| Sự kiện đổi giá của tin bán | 1,504 |
| Silver verification | 22/22 PASS |
| Gold verification | 92/92 PASS |
| Unit tests | 186 PASS |
| Fixture Spark lifecycle | 12/12 PASS |
| Airflow run | week1_complete_20261016 |
| Airflow tasks | 23/23 SUCCESS |
| Thời gian chạy DAG thực tế | 19.51 phút |
| Raw CSV giữ nguyên checksum | True |

## Vòng đời theo nguồn

| Nguồn | Trạng thái | Số listing |
|---|---|---:|
| alonhadat | NOT_APPLICABLE | 1,278 |
| batdongsan | NOT_OBSERVED_2_BATCHES | 28,611 |
| batdongsan | NOT_OBSERVED_LATEST | 1,543 |
| batdongsan | OBSERVED_LATEST | 3,939 |
| chotot | NOT_APPLICABLE | 8,152 |
| guland | NOT_OBSERVED_2_BATCHES | 39,418 |
| guland | NOT_OBSERVED_LATEST | 3,868 |
| guland | OBSERVED_LATEST | 8,956 |
| homedy | NOT_APPLICABLE | 566 |
| luachonnhadat | NOT_APPLICABLE | 883 |
| mogi | NOT_APPLICABLE | 19,075 |
| muaban | NOT_APPLICABLE | 363 |
| nhadatvui | NOT_OBSERVED_2_BATCHES | 14,257 |
| nhadatvui | NOT_OBSERVED_LATEST | 113 |
| nhadatvui | OBSERVED_LATEST | 3,598 |

`listed_days` là khoảng thời gian từ lần quan sát đầu tới lần quan sát cuối. `NOT_OBSERVED_2_BATCHES` chỉ là không thấy trong hai batch cuối của chính nguồn đó, **không phải đã bán**. Các nguồn historical có `is_disappeared=NULL` và `NOT_APPLICABLE`.

## Minh chứng và cách chạy

- [Hướng dẫn vận hành](../operations/week1_pipeline.md).
- [Hình xuất từ kết quả thật](../evidence/week1_20261016/): 4 PNG và nguồn JSON. Đây là hình dựng từ kết quả, không phải ảnh chụp trình duyệt.
- [Airflow run](../validation/airflow_task_states.json), [failure gate](../validation/airflow_failure_gate.json).
- [Lifecycle summary](../validation/gold_listing_lifecycle_summary.json), [Gold verification](../validation/gold_verification.json).
- [Snapshot manifest](../validation/snapshot_manifest.json), [kiểm tra dùng lại dữ liệu](../validation/snapshot_reuse_verification.json).

```powershell
docker compose --profile orchestration up -d --build
.\scripts\run_airflow_pipeline.ps1 -BatchId 20261016
```

Airflow UI: http://localhost:8082. Quy trình tái chạy, lịch batch và file cần xuất từ extension được ghi trong hướng dẫn vận hành.

## Giới hạn và bước tiếp theo

- Chưa có ảnh chụp trực tiếp giao diện Airflow trong phiên này vì công cụ trình duyệt chưa kết nối; thư mục minh chứng có checklist vị trí cần chụp. Đã có hình xuất từ API và kết quả kiểm chứng thật.
- Nhánh Batdongsan cần thao tác export CSV của người vận hành. Pipeline tự động từ lúc file đã được đặt vào thư mục batch.
- Không suy luận đã bán từ việc vắng trong phạm vi crawl; cần giữ phạm vi crawl ổn định giữa các batch.
- Pipeline tuần này rebuild đầy đủ. Incremental MERGE INTO, time travel/schema evolution và CI thuộc tuần sau.
- Kết quả deterministic 26 bảng ngày 09/10 trong repo là bằng chứng của mốc cũ; chưa dùng để khẳng định deterministic cho bộ dữ liệu mới và bảng lifecycle.
