TLCN_Nhom6_KTDL

Đề tài

Xây dựng Data Lakehouse phục vụ phân tích dữ liệu thị trường bất động sản tại Việt Nam

Nhóm 6:

23133024 - Võ Đức Hoàng

23133029 - Vương Đức Huy

23133040 - Nguyễn Lê Hoàng Kiệt

1. Mục tiêu

Project xây dựng một Data Lakehouse cho dữ liệu bất động sản đa nguồn.

Luồng xử lý dự kiến:

Historical Data + Snapshot Data
            ↓
          Bronze
            ↓
          Silver
            ↓
           Gold
            ↓
   PostgreSQL / PostGIS
            ↓
        Dashboard

Người dùng hướng đến là nhân viên phân tích thị trường, môi giới hoặc bộ phận kinh doanh bất động sản.

Dashboard sau này cần hỗ trợ các câu hỏi như:

Mặt bằng giá chào bán tại một khu vực hiện đang ở mức nào?

Một listing đang thấp, nằm trong vùng phổ biến hay cao hơn nhóm tương đồng?

Khu vực hoặc loại bất động sản nào có nhiều tin điều chỉnh giá?

Lịch sử giá của một listing thay đổi như thế nào theo các lần quan sát?

Dữ liệu hiện tại có vấn đề gì về chất lượng hoặc độ đầy đủ?

Giá trong hệ thống là giá đăng / giá chào bán, không phải giá giao dịch thực tế.

2. Nguồn dữ liệu hiện tại

Historical data hiện có 6 nguồn:

Nguồn

Số dòng

Alonhadat

1,278

Chợ Tốt

8,152

Homedy

566

Lựa Chọn Nhà Đất

883

Mogi

19,075

Muaban

363

Tổng

30,317

Dữ liệu historical được đặt tại:

data/incoming/historical/
├── alonhadat/
├── chotot/
├── homedy/
├── luachonnhadat/
├── mogi/
└── muaban/

Dữ liệu snapshot mới sau này sẽ đặt tại:

data/incoming/snapshots/batdongsan/

Bộ dữ liệu 3.5 triệu dòng trước đây không còn nằm trong pipeline chính.

3. Công nghệ hiện tại

Docker Compose

Apache Spark 3.5.9

Hadoop 3.3.4 / S3A

MinIO

Apache Iceberg 1.11.0

Python / PySpark

Dự kiến thêm sau:

PostgreSQL

PostGIS

Apache Airflow

Python Dash / Plotly / Leaflet

4. Cấu trúc project

TLCN_BDS_Lakehouse/
│
├── config/
├── data/
│   ├── incoming/
│   └── reference/
├── docs/
│   ├── business/
│   ├── data/
│   └── architecture/
├── src/
│   ├── ingestion/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── common/
├── tests/
│   └── smoke/
├── scripts/
├── dags/
├── sql/
├── dashboard/
├── docker/
├── notebooks/
├── outputs/
├── docker-compose.yml
├── .env
├── .env.example
├── .gitignore
└── README.md

5. Chạy Docker

Mở PowerShell:

cd "D:\Code\TLCN_BDS_Lakehouse"

Khởi động:

docker compose up -d

Kiểm tra:

docker compose ps

Hiện tại cần có:

tlcn_minio
tlcn_spark_master
tlcn_spark_worker

Dừng hệ thống:

docker compose down

Xem log:

docker logs tlcn_minio --tail 50
docker logs tlcn_spark_master --tail 50
docker logs tlcn_spark_worker --tail 50

6. Các trang quản trị

MinIO Console

http://localhost:9001

Đăng nhập:

Username: admin
Password: 12345678

Các bucket:

lakehouse-bronze
lakehouse-silver
lakehouse-gold

Spark Master

http://localhost:8080

Dùng để xem Spark Master, worker, số core, memory và application.

Spark Worker

http://localhost:8081

7. Chạy Spark job

Script hỗ trợ:

scripts/run_spark.ps1

Cách chạy:

.\scripts\run_spark.ps1 "duong_dan_file_python"

Ví dụ:

.\scripts\run_spark.ps1 "src\bronze\ingest_bronze.py"

Hoặc:

.\scripts\run_spark.ps1 "src\bronze\verify_bronze.py"

Các package Spark hiện dùng:

org.apache.hadoop:hadoop-aws:3.3.4

org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0

8. Bronze Layer

Bronze lưu dữ liệu gần với nguồn nhất.

Đường dẫn:

s3a://lakehouse-bronze/real_estate/historical/

Cấu trúc:

real_estate/
└── historical/
    ├── alonhadat/
    │   └── batch_id=historical_backfill_v1/
    ├── chotot/
    │   └── batch_id=historical_backfill_v1/
    ├── homedy/
    │   └── batch_id=historical_backfill_v1/
    ├── luachonnhadat/
    │   └── batch_id=historical_backfill_v1/
    ├── mogi/
    │   └── batch_id=historical_backfill_v1/
    └── muaban/
        └── batch_id=historical_backfill_v1/

Metadata được bổ sung:

_source_name
_source_file
_batch_id
_ingestion_mode
_ingested_at

Batch đầu tiên:

historical_backfill_v1

Kết quả:

Input : 30,317 rows
Output: 30,317 rows
Status: SUCCESS

Bronze chưa xử lý Data Quality nghiệp vụ sâu. Các lỗi như ngày đăng sai, price/area không hợp lệ, chuẩn hóa loại bất động sản... sẽ xử lý tại Silver.

9. Iceberg

Iceberg đã được test thành công với Spark và MinIO.

Stack hiện tại:

Spark 3.5.9
Hadoop 3.3.4
Iceberg 1.11.0
MinIO

Iceberg sẽ được dùng chính ở Silver và Gold.

10. Data Quality

Rule nằm tại:

docs/data/data_quality_rules.md

Các nhóm kiểm tra chính:

source / source_id

title

price

area

duplicate

ngày đăng

location

property type

price_per_m2

Ví dụ đã phát hiện:

source_id: muaban_70944165
posted_at: 2030-10-15
scraped_at: 2026-06-03

Bronze giữ nguyên dữ liệu nguồn.

Silver sẽ xử lý theo hướng:

published_at = NULL
dq_status = WARN
dq_reason = INVALID_PUBLISHED_AT

11. Tiến độ hiện tại - Tuần 5/13

Tuần 1 - Khảo sát dữ liệu

Kiểm kê dữ liệu

Profiling

Kiểm tra schema

Kiểm tra null

Kiểm tra duplicate

Kiểm tra date range

Tuần 2 - Data Dictionary và Data Quality

Data Source Inventory

Canonical Listing Schema

Data Quality Rules

Tuần 3 - Chốt bài toán

Xác định người dùng

Xác định mục tiêu dashboard

Xác định các câu hỏi cần trả lời

Chốt hướng multi-source data

Tuần 4 - Hạ tầng

Docker Compose

MinIO

Spark Master

Spark Worker

Spark kết nối MinIO qua S3A

Kiểm tra Hadoop version

Tuần 5 - Bronze Layer

Nạp 6 nguồn historical

Gắn metadata

Lưu Parquet trên MinIO

Verify input/output khi ingest

Đủ 30,317 records

Test Spark -> MinIO

Test Iceberg

Dọn dữ liệu test

Chạy verify_bronze.py

Commit milestone Week 5

Hiện tại Bronze historical gần hoàn thành.

12. Kế hoạch tiếp theo

Tuần 6 - Silver Core + Data Quality

Bronze
  ↓
Union 6 sources
  ↓
Canonical Schema
  ↓
Cast Data Type
  ↓
Data Quality Rules
  ↓
Normalize Transaction Type
  ↓
Normalize Property Type
  ↓
Date Validation
  ↓
Duplicate Handling
  ↓
PASS / WARN / REJECT
  ↓
Silver Iceberg Table

Bảng dự kiến:

listing_core
listing_rejects

Tuần 7

Location normalization

Feature extraction

GIS / coordinate handling

Tuần 8

Snapshot ingestion

Listing history

Price history

Tuần 9

Gold Layer

Market overview

Price benchmark

Repricing summary

Data Quality summary

Tuần 10

PostgreSQL / PostGIS

Data Warehouse

Fact / Dimension

Tuần 11

Dashboard Python

Map

Price Benchmark

Repricing

Data Quality

Tuần 12

Tích hợp toàn bộ pipeline

Tuần 13

Kiểm thử

Hoàn thiện báo cáo

Slide

Demo

13. Git

Không commit:

.env
data/incoming/**
outputs/**

Trước khi commit:

git status

Sau đó:

git add .
git commit -m "Complete Week 5 Bronze ingestion and validation"
git push

14. Trạng thái hệ thống hiện tại

Historical Data       ✓
Profiling             ✓
Canonical Schema      ✓
Data Quality Rules    ✓
Docker                ✓
MinIO                 ✓
Spark Master/Worker   ✓
Spark -> MinIO        ✓
Bronze Ingestion      ✓
Bronze Validation     gần xong
Iceberg Test          ✓

Silver                chưa bắt đầu
Gold                  chưa bắt đầu
Data Warehouse        chưa bắt đầu
Dashboard             chưa bắt đầu

Project hiện đang ở Tuần 5/13 - hoàn thiện Bronze Layer.