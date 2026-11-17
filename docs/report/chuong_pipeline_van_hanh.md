# Kiến trúc, pipeline, vận hành và kiểm thử

Võ Đức Hoàng — 23133024. Chương kỹ thuật cập nhật ngày 17/11/2026 theo run
`acceptance_A_20261117_r2`, dữ liệu tới batch 17/11. Đây là đầu ra A7 để tích hợp báo cáo
chung; kết quả nghiệp vụ BQ và độ chính xác gán nhãn được đánh giá riêng.

## 1. Kiến trúc triển khai

Luồng dữ liệu là CSV → Bronze Parquet trên MinIO → Silver Iceberg → Gold
Iceberg → Trino → Superset. Airflow điều phối thứ tự xử lý và các cổng kiểm
định. Sơ đồ ở [architecture.md](../architecture.md), cấu hình thực thi trong
[Docker Compose](../../docker-compose.yml).

Spark xử lý dữ liệu; Iceberg REST quản lý metadata bảng, snapshot và schema.
Bảng nghiệp vụ nằm trên MinIO; catalog REST hiện dùng SQLite bền vững trên
Docker volume. PostgreSQL phục vụ metadata Airflow và Superset. Các thành
phần Hadoop/YARN và ứng dụng thử nghiệm không thuộc luồng bảo vệ này.

Môi trường local vận hành theo cửa sổ Spark ghi rồi Trino đọc. Writer bị chặn
nếu Trino còn hoạt động. Không tuyên bố đã nghiệm thu nhiều writer hoặc đọc/ghi
đồng thời trên catalog SQLite.

## 2. Thu thập, manifest và Bronze

Ba nguồn crawl: Batdongsan, Guland, Nhadatvui; sáu nguồn historical: Chotot,
Mogi, Alonhadat, Luachonnhadat, Muaban, Homedy. CSV snapshot có cấu trúc
`data/incoming/snapshots/<source>/YYYYMMDD/<source>_raw.csv`.

Batdongsan xuất bằng Chrome extension, Airflow chờ CSV rồi xử lý tiếp. Hai
nguồn web còn lại có crawler Python; batch đã có được tái sử dụng. Crawler
không được gọi để giả lập dữ liệu quá khứ bằng ngày mới. Lịch DAG là 22:00
thứ ba và thứ sáu, giờ Việt Nam, không tự backfill; một run/task tại một thời
điểm. Trong cửa sổ đọc, DAG được giữ paused; khi có input mới cần chuyển
write mode trước khi nộp run hoặc bật lịch.

Manifest SHA256 kiểm tra toàn bộ từng file; parse cache chỉ được dùng khi
bytes không đổi. File cũ bị sửa/xóa, LFS pointer, CSV rỗng hoặc lỗi cấu trúc
đều bị phát hiện. Inventory được kiểm trước ghi Bronze và cuối job. Công cụ
nhận snapshot mặc định dry run, chỉ copy khi `--apply`, không ghi đè nội dung
khác, publish đủ bytes atomically và giữ file gốc. Hướng dẫn ở
[nhận dữ liệu](../operations/nhan_du_lieu_va_tai_lap.md).

Bộ nghiệm thu có **48 CSV snapshot, 316.241 dòng** và **30.317 dòng historical**,
tổng Bronze **346.558 dòng**. Batdongsan/Guland/Nhadatvui có lần lượt 17/15/16
batch. Nhadatvui có batch 03/10 ngoài lịch thường lệ; file đã pin được khôi
phục nguyên bytes từ Git LFS. Nguồn:
[manifest](../validation/snapshot_manifest.json), [Bronze audit](../validation/bronze_state_audit.json).

Các ngày thiếu được chủ dữ liệu chấp nhận: Guland 20/10, 30/10; Nhadatvui
30/10, 10/11. Chúng được khai báo, không có batch thay thế. CSV Guland 10/11
ban đầu ghi 12/11; chủ dữ liệu hiệu chỉnh về 10/11 trước ingest. Checksum
trước/sau và quyết định được giữ trong
[input_date_review.json](../validation/input_date_review.json); chưa xác minh
độc lập ngày thu thập. Phân tích theo thời gian cần công bố giới hạn đó.

Bronze lưu Parquet phân nguồn và batch cùng metadata truy nguyên. Các batch
đã hoàn tất được giữ để retry; toàn pipeline không phải một giao dịch nguyên
tử xuyên Bronze–Silver–Gold. Input phải đóng băng trong lúc chạy.

## 3. Silver Core và quản lý lịch sử

Silver chuẩn hóa về hợp đồng listing 27 cột: giá, diện tích, phòng, loại hình,
thời gian, vị trí và metadata. PASS/WARN được giữ theo quy tắc chất lượng;
vi phạm tối thiểu vào quarantine. Observation giữ các lần quan sát sau dedup
batch, Current chọn bản mới nhất đã quan sát theo source_id. Hợp đồng và DQ
ở [docs/data](../data/).

| Bảng | Số dòng |
|---|---:|
| listing_observation | 334.820 |
| listing_dq_quarantine | 664 |
| crawl_current_27 | 119.700 |
| historical_current_27 | 30.317 |
| silver_listings_current_27 | 150.017 |
| listing_history | 167.794 |
| listing_history_state / location / feature | 150.017 mỗi bảng |

Nguồn: [Silver verification](../validation/silver_final_verification.json).
Dữ liệu thiếu hoặc chưa chắc chắn không được tự tạo để ép PASS.

History sử dụng SCD2, partition `snapshot_date`, MERGE các phiên bản thay đổi
của ID bị ảnh hưởng. State hỗ trợ checkpoint/retry; cơ chế kiểm tra timeline,
batch đến muộn, dữ liệu sửa lại, schema/location và khóa writer giảm nguy cơ
lịch sử bị sai. Core và các bảng Gold vẫn rebuild; không khẳng định toàn bộ
pipeline là incremental.

Đối chứng từ baseline 16/10: MERGE và full rebuild cùng **167.794 dòng**,
SHA256 multiset bằng nhau, EXCEPT ALL hai chiều bằng 0. MERGE tính lại
**26.668 ID**, staging **22.260 dòng**; mất **17.74 giây**,
rebuild **9.96 giây**. Retry không đổi input giữ nguyên snapshot.
Các số này chứng minh tính đúng và idempotency trong phạm vi test, không
chứng minh MERGE luôn nhanh hơn. Nguồn:
[history incremental verification](../validation/history_incremental_verification.json).

SCD2 là lịch sử nghiệp vụ của listing; Iceberg time travel là phiên bản bảng
ở mức commit vật lý. Hai cơ chế hỗ trợ các câu hỏi khác nhau.

## 4. Gold, lifecycle và giới hạn diễn giải

Gold có 9 dimension và các fact/bảng phân tích; tổng số bảng sản xuất gồm
9 Silver và 21 Gold. Fact listing có **134.660 dòng**;
lifecycle **150.017 dòng**; sự kiện đổi giá
**3.021 dòng**. Mô hình giá có **128.853 dòng**
và **971 dòng hệ số**. Nguồn:
[Gold verification](../validation/gold_verification.json).

Lifecycle tính first/last seen, số lần quan sát, khoảng ngày quan sát, số lần
đổi giá và trạng thái vắng theo hai batch gần nhất của từng nguồn. Historical
có NOT_APPLICABLE. Current không xác nhận listing vẫn đang đăng; listed_days
không phải thời gian tới khi bán. Giá trong hệ thống là giá chào bán.

Điều tra crawl ghi nhận độ sâu giảm từ 1.000 xuống khoảng 200 trang ở
Batdongsan, 1.000 xuống 30 ở Guland, 937 xuống 200 ở Nhadatvui. Ở cutoff
17/11, **108.415/119.700 tin crawl vắng hai batch, 90,57%**, so với 89,34% tại
27/10. Tập tin tích lũy tăng trong khi mẫu thu thập cuối kỳ vẫn giới hạn;
chưa xác minh URL trực tiếp để tách tin hết hạn, bị gỡ hay đổi thứ tự hiển thị.
Không diễn giải tỷ lệ vắng thành tỷ lệ đã bán. Phương pháp, nhóm theo độ sâu
và số liệu trước/sau: [crawl coverage](../operations/crawl_coverage.md),
[JSON](../validation/crawl_coverage.json).

## 5. Điều phối và xử lý sự cố

Run `acceptance_A_20261117_r2` hoàn tất **24/24 task SUCCESS** trong **14 phút 17.59 giây**.
Các nhóm acquisition → Bronze → Silver → verify → Gold → verify → publish
thực thi có thứ tự. Fault probe dùng batch không có dữ liệu: task chuẩn bị
đầu tiên failed, 23 task downstream upstream_failed. Nguồn:
[Airflow run](../validation/integration_airflow_run.json), [fault gate](../validation/airflow_failure_gate.json).

Lượt đầu phát hiện SQLite read transaction còn giữ trong pool REST sau cửa
sổ query. Khi chuyển write mode, hệ thống kiểm tra không có writer/run,
dừng readers, mở lại kết nối REST rồi chờ healthy; catalog volume giữ nguyên.
Không xóa database hoặc dữ liệu để chữa lỗi. Client Airflow retry thêm một
lần chỉ cho GET/HEAD bị 403 do clock jitter JWT iat; POST/PATCH/DELETE không
retry tự động. Server vẫn xác thực token. Run lỗi và probe phục hồi được giữ
ở [catalog_lock_recovery.json](../validation/catalog_lock_recovery.json).

## 6. Kiểm thử và tính tái lập

Bộ nghiệm thu tổng **66/66 PASS**, gồm code/input không đổi, manifest,
DAG/fault gate, Silver **30/30**, Gold **113/113**, hai rebuild đầy đủ **30 bảng**
giống schema/grain/số dòng/SHA256 multiset, **11 ca SCD2**, **12 ca lifecycle**,
**6 check Iceberg**, Trino/table counts, **29 chart** và bộ lọc tỉnh. Unit test
**466/466 PASS**, không error/failure/skip. Nguồn:
[defense verification](../validation/defense_verification.json),
[deterministic rebuild](../validation/deterministic_rebuild.json),
[dashboard integration](../validation/pipeline_dashboard_integration.json).

Kết quả gắn với batch, run và SHA256 code trong JSON; không tự chứng nhận lần
sửa code hoặc thêm input sau đó. CI trên commit chuẩn bị phát hành cần chạy
sau khi code được review/push. Không di chuyển tag cũ để trỏ sang code mới.

Script tái lập theo phase Prepare/Check/Dashboard có đo thời gian thực và
trả lỗi khi một bước thất bại. Host preflight máy Hoàng 8/8 PASS, đã xác nhận
54 CSV không còn LFS pointer. Đây là kiểm tra máy hiện tại; chưa chứng nhận
người khác clone và chạy trọn pipeline trên máy sạch. Hướng dẫn:
[README](../../README.md), [tái lập](../operations/nhan_du_lieu_va_tai_lap.md).

## 7. Bàn giao và phần cần người thực hiện

A1 đã có tiếp nhận/manifest/gap cho dữ liệu hiện tại; A2 có điều tra và chú
thích; A3 đã chạy bộ nghiệm thu đầy đủ; A5 cập nhật README và kiểm tra số liệu;
A7 là chương này cùng [10 câu hỏi bảo vệ](hoi_dap_bao_ve.md). A4 đã có công cụ
và hướng dẫn, còn người khác chạy trên máy Kiệt và ghi thời gian toàn lượt.
A6 có [kịch bản dưới 7 phút](../demo/kich_ban_demo.md), công cụ quay và video
thao tác dự phòng; Hoàng cần tự diễn tập phần nói hai lần.

Các batch 20/11 và 24/11 vẫn là mốc tiếp theo trong kế hoạch. Sau dữ liệu chốt,
chạy lại nghiệm thu và CI trên đúng commit trước tag v1.1-bao-ve. Code được bàn giao qua nhánh A và PR vào main; tag phát hành chỉ tạo sau
review, dữ liệu chốt và CI trên đúng commit. Kiểm định kỹ thuật không thay cho
G4 và tiêu chí nghiệp vụ B/C; dữ liệu gán nhãn còn phải hoàn thành riêng.
