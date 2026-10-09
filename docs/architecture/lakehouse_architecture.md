# Lakehouse Architecture

- Raw/incoming: CSV theo nguồn và batch.
- Bronze: MinIO + Parquet, giữ dữ liệu gần nguồn và metadata ingestion.
- Silver: Apache Iceberg qua REST Catalog, dữ liệu chuẩn hóa, DQ, current, history, location và feature.
- Gold: Apache Iceberg, star schema (9 dimension + `fact_listing`); các bảng theo Business Question ở giai đoạn tiếp theo.
- Serving: Trino, PostgreSQL/PostGIS, API và dashboard.

## Lưu trữ theo tầng

Mỗi tầng Medallion có một bucket MinIO riêng:

| Tầng | Bucket / đường dẫn | Định dạng | Catalog |
|---|---|---|---|
| Bronze | `s3://lakehouse-bronze/real_estate/{historical,snapshots}/<nguồn>/batch_id=...` | Parquet | Không |
| Silver | `s3://lakehouse-silver/real_estate/<bảng>` | Iceberg v2 | `lakehouse.silver` |
| Gold | `s3://lakehouse-gold/real_estate/<bảng>` | Iceberg v2 | `lakehouse.gold` |

Catalog `lakehouse` (Iceberg REST, JDBC/SQLite) vẫn cần một warehouse mặc định là `s3://warehouse/`. Đây là thuật ngữ của Iceberg cho thư mục gốc mặc định của catalog, không phải Data Warehouse. JDBC catalog bỏ qua `location` khai báo ở namespace, nên mọi job ghi bảng qua `write_iceberg_table()` trong `src/common/spark_session.py`: hàm này gán `location` tường minh theo tầng. Nếu bảng đang nằm ở vị trí khác (ví dụ warehouse cũ), hàm sẽ `DROP TABLE ... PURGE` rồi tạo lại đúng bucket. Bucket `warehouse` vì vậy luôn trống.

`verify_silver.py` và `verify_gold.py` kiểm tra mọi bảng nằm đúng bucket của tầng.

Credential MinIO được lấy từ biến môi trường; không hard-code trong code.
