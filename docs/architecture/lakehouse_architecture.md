# Lakehouse Architecture

- Raw/incoming: CSV theo nguồn và batch.
- Bronze: MinIO + Parquet, giữ dữ liệu gần nguồn và metadata ingestion.
- Silver: Apache Iceberg qua REST Catalog, dữ liệu chuẩn hóa, DQ, current, history và location.
- Gold: Apache Iceberg, chưa triển khai trong phạm vi hoàn thiện Silver.
- Serving: Trino, PostgreSQL/PostGIS, API và dashboard ở các giai đoạn sau.

Silver dùng catalog `lakehouse`, namespace `silver`, warehouse `s3://warehouse/`. Credential MinIO được lấy từ biến môi trường; không hard-code trong code.
