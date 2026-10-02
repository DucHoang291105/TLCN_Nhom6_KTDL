# TLCN Nhóm 6 – Data Lakehouse bất động sản

Đề tài xây dựng Data Lakehouse phục vụ thu thập, lưu trữ, xử lý và phân tích dữ liệu thị trường bất động sản Việt Nam.

## Kiến trúc công nghệ

Môi trường local được đóng gói bằng Docker Compose và gồm:

- Thu thập dữ liệu: Python, Requests, BeautifulSoup và crawler theo từng nguồn.
- Data Lake: MinIO với các vùng `lakehouse-bronze`, `lakehouse-silver`, `lakehouse-gold`.
- Xử lý dữ liệu: Apache Spark 3.5.9 và Spark MLlib.
- Table format/catalog: Apache Iceberg REST Catalog.
- Truy vấn hợp nhất: Trino 483 kết nối Iceberg và PostgreSQL.
- Điều phối pipeline: Apache Airflow 3.3.1.
- Data Warehouse và metadata database: PostgreSQL 16.
- API: FastAPI.
- Dashboard: Apache Superset 6.0.0.
- Giao diện web: Nginx Web UI.
- Hệ sinh thái Hadoop: HDFS và YARN 3.4.2.

## Cấu trúc chính

```text
config/                     Cấu hình pipeline
data/incoming/snapshots/    Snapshot raw được chọn để tái hiện và kiểm thử
docker/                     Cấu hình image và dịch vụ
src/ingestion/              Crawler theo nguồn
src/bronze/                 Nạp dữ liệu Bronze
src/silver/                 Chuẩn hóa Silver
src/gold/                   Tổng hợp Gold
scripts/                    Script vận hành Docker và Spark
dags/                       Airflow DAG
docker-compose.yml          Khai báo toàn bộ nền tảng
```

## Dữ liệu snapshot có trong repository

Các CSV snapshot được quản lý bằng Git LFS để repository không chứa trực tiếp các blob dữ liệu lớn:

| Nguồn | Ngày snapshot | Số dòng |
|---|---:|---:|
| Batdongsan | 2026-09-22 | 20.000 |
| Batdongsan | 2026-09-25 | 14.000 |
| Batdongsan | 2026-09-29 | 4.000 |
| Batdongsan | 2026-10-02 | 4.000 |
| Guland | 2026-09-22 | 3.862.228 |
| Guland | 2026-09-25 | 2.724.496 |
| Nhadatvui | 2026-09-22 | 16.860 |
| Nhadatvui | 2026-09-25 | 10.800 |

Khi clone repository trên máy mới, cần cài Git LFS và chạy `git lfs pull` để tải nội dung thật của các CSV.

## Chuẩn bị lần đầu

Yêu cầu duy nhất trên máy host là Docker Desktop. Sao chép file môi trường:

```powershell
Copy-Item .env.example .env
```

Sau đó thay các mật khẩu mẫu trong `.env`. File `.env` đã được ignore và không được commit.

## Khởi động nền tảng

Máy 16 GB RAM không nên chạy toàn bộ dịch vụ đồng thời. Hãy chạy core và chỉ bật profile đang cần:

```powershell
cd "D:\Code\TLCN_BDS_Lakehouse"

# MinIO + Spark + Iceberg REST + PostgreSQL
.\scripts\start_platform.ps1 core

# Các nhóm chức năng tùy chọn
.\scripts\start_platform.ps1 query
.\scripts\start_platform.ps1 application
.\scripts\start_platform.ps1 orchestration
.\scripts\start_platform.ps1 dashboard
.\scripts\start_platform.ps1 hadoop
```

Kiểm tra và dừng dịch vụ:

```powershell
.\scripts\status_platform.ps1
.\scripts\stop_platform.ps1
.\scripts\stop_platform.ps1 -All
```

Image và dependency được lưu local. Dữ liệu được giữ trong Docker named volumes, vì vậy có thể tắt/mở Docker Desktop rồi dùng tiếp mà không cài lại. Không chạy `docker compose down -v` nếu muốn giữ dữ liệu.

## Địa chỉ dịch vụ

| Dịch vụ | Địa chỉ |
|---|---|
| MinIO Console | http://localhost:9001 |
| Spark Master | http://localhost:8080 |
| Spark Worker | http://localhost:8081 |
| Iceberg REST | http://localhost:8181/v1/config |
| PostgreSQL | `localhost:5433` |
| Trino | http://localhost:8090 |
| Airflow | http://localhost:8082 |
| Superset | http://localhost:8088 |
| FastAPI Swagger | http://localhost:8000/docs |
| Web UI | http://localhost:3000 |
| HDFS NameNode | http://localhost:9870 |
| YARN ResourceManager | http://localhost:8089 |

Thông tin đăng nhập local được cấu hình trong `.env`.

## Chạy Spark job

```powershell
.\scripts\run_spark.ps1 "src\bronze\ingest_bronze.py"
```

Theo dõi Spark job tại http://localhost:8080 và kiểm tra dữ liệu đầu ra tại MinIO Console.

## Kiểm tra nhanh Docker Compose

```powershell
docker compose --profile full config --quiet
docker compose ps
```

Các container dài hạn dùng `restart: unless-stopped`. Những profile nặng như Airflow, Superset, Trino và Hadoop chỉ nên bật khi sử dụng.
