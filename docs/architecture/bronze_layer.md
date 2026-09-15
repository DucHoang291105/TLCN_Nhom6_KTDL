# Bronze Layer

## Mục đích

Bronze lưu dữ liệu gần với dữ liệu nguồn nhất. Các lỗi nghiệp vụ chưa được sửa tại lớp này.

## Historical sources

| Source | Rows |
|---|---:|
| Alonhadat | 1,278 |
| Chợ Tốt | 8,152 |
| Homedy | 566 |
| Lựa Chọn Nhà Đất | 883 |
| Mogi | 19,075 |
| Muaban | 363 |
| Tổng | 30,317 |

## Storage

Dữ liệu được lưu dưới dạng Parquet trên MinIO:

`s3a://lakehouse-bronze/real_estate/historical/`

Mỗi nguồn được lưu riêng theo batch.

## Metadata

Mỗi record được bổ sung:

- `_source_name`
- `_source_file`
- `_batch_id`
- `_ingestion_mode`
- `_ingested_at`

## Batch đầu tiên

`historical_backfill_v1`

Kết quả:

- Input: 30,317 records
- Output: 30,317 records
- Validation: PASS

Data Quality nghiệp vụ được xử lý ở Silver, không xử lý tại Bronze.
