# Xây dựng và vận hành pipeline Data Lakehouse

## Luồng dữ liệu

CSV của ba nguồn crawl và sáu nguồn historical được nạp vào Bronze Parquet trên
MinIO. Spark chuẩn hóa Silver về hợp đồng 27 cột, lưu observation, current và
history SCD2; Location/Feature bổ sung dữ liệu phục vụ phân tích. Gold gồm
9 dimension, các fact, lifecycle và bảng BQ1–BQ3. Trino đọc Iceberg để Superset
hiển thị năm dashboard. Airflow điều phối 26 task theo thứ tự và chặn downstream
khi một điều kiện đầu vào hoặc xử lý thất bại. PostgreSQL lưu metadata ứng dụng;
REST catalog hiện dùng SQLite bền vững trên volume.

## Dữ liệu và khả năng truy nguyên

Snapshot nằm theo source/YYYYMMDD, dùng Git LFS; historical có hợp đồng riêng
trong `src/bronze/historical_contract.py`. Tới đợt tiếp nhận 20/11 có 51 CSV
snapshot, 325.191 dòng và 6 CSV historical, 30.317 dòng. Số dòng đầu ra sau DQ và
dedup phải lấy từ JSON của lượt nghiệm thu, không suy ra bằng cộng raw.

Mỗi lượt manifest SHA256 toàn bộ CSV. Parse cache không bỏ qua hash bytes.
Snapshot cũ bị sửa/xóa, CSV lỗi hoặc LFS pointer chưa tải đều bị chặn. Historical
lưu SHA256/size/schema/count; bootstrap đối chiếu toàn bộ multiset dòng CSV với
Bronze bằng EXCEPT ALL hai chiều. Batch historical đã có không bị overwrite.
Bốn ngày thiếu đã khai báo trong source_gaps; không tạo dữ liệu thay thế. Quyết
định sửa ngày Guland trước ingest và giới hạn xác minh ngày nằm trong
`input_date_review.json`.

## Thu thập và điều phối

DAG chạy 22:00 thứ ba/thứ sáu, giờ Việt Nam; một active run/task. Batdongsan
vẫn cần xuất CSV từ Chrome extension. Guland/Nhadatvui có crawler độc lập,
chỉ cào batch thiếu của ngày hiện tại. CSV có sẵn được tái sử dụng; không đổi ngày
crawl hôm nay thành lịch sử. Dữ liệu mới được ghi staging, xác minh hợp đồng
trang/schema/count/checksum rồi mới rename vào batch chính. Timeout, retry,
delay và ngân sách lượt chạy được truyền đủ qua Compose vào Airflow.

## History, lifecycle và diễn giải

History lưu phiên bản thay đổi theo source_id bằng MERGE INTO và state hỗ trợ
retry/checkpoint. Đối chứng MERGE với rebuild kiểm tra schema, grain và multiset
SHA256, không chỉ số dòng. Không kết luận MERGE luôn nhanh hơn; đọc thời gian thực
tế trong `history_incremental_verification.json`. Core và nhiều bảng Gold vẫn
rebuild; không gọi toàn pipeline là incremental.

Lifecycle dùng lịch batch thực tế của từng nguồn, tính first/last seen, số lần
quan sát, số lần đổi giá và vắng hai batch. Historical không có chuỗi snapshot
được đánh dấu NOT_APPLICABLE. Vắng trong mẫu không có nghĩa đã bán; phải xét độ
sâu crawl từng nguồn/batch trong `crawl_coverage.json`.

## Hàng rào công bố và phục hồi

Runner chạy tay và Airflow dùng chung khóa catalog. Trước job ghi, publication
chuyển BUILDING và làm mất hiệu lực downstream. Chỉ đủ mọi bước và báo cáo PASS
trên cùng code/config/input mới PUBLISHED. Lỗi giữa Gold, đổi input hoặc sửa
verification khiến read window bị chặn. Đây là cơ chế điều phối an toàn, không
phải transaction xuyên nhiều bảng. Người có quyền chạy Docker trực tiếp vẫn có
thể đi ngoài runner; quy trình vận hành phải dùng entry point được cung cấp.

Chuyển write dừng readers và mở lại kết nối SQLite REST sau khi kiểm tra idle.
Chuyển read yêu cầu publication hợp lệ và giữ DAG paused. Không chứng nhận
catalog nhiều writer hoặc đọc/ghi đồng thời. Sai location làm job dừng kèm
location cũ/mới, không tự DROP PURGE.

Backup thực hiện offline đồng bộ MinIO, catalog, PostgreSQL, Superset, Airflow;
manifest lưu checksum archive. Restore-check giải nén vào volume riêng và đối
chiếu contents/mode/ownership, không đè volume gốc. Đây là kiểm tra phục hồi
bytes cục bộ; nghiệm thu end-to-end trên máy thành viên khác vẫn là điều kiện
riêng. Image ngoài và base build được ghim digest, MinIO được thử trên volume
mới. Script nghiệm thu chống laptop ngủ do không thao tác và khôi phục chính
sách thread khi kết thúc.

## Kiểm chứng và số liệu dùng khi báo cáo

Nguồn số liệu duy nhất: [defense_verification.json](../validation/defense_verification.json).
Lấy run_id, batch_id, tổng checks PASS, silver_counts và gold_counts từ file này.
Chạy `python -m src.orchestration.status` để xác nhận code/input hiện tại còn khớp.
Không dùng con số từ tag cũ như kết quả của code mới.

Suite chạy pytest, compileall, Compose; lỗi acquisition và downstream; DAG thật;
hai rebuild trên đủ 38 bảng; fixture SCD2/lifecycle/Iceberg; commit sandbox rồi
thử chặn đọc sau lỗi; Trino/Superset/49 chart/bộ lọc. Sau Gold, chạy lại backtest,
tái tạo mô hình và repost check của B. CV mới được lưu thành candidate, không
đổi cấu hình production của B. Kết quả chi tiết và thời gian nằm trong JSON/log.

PASS kỹ thuật không thay thế nghiệm thu BQ, nhãn G4 hoặc máy khác. Manifest đóng
hash tiêu chí nghiệp vụ, cấu hình mô hình, nhãn và prediction; không tự ghi nhận
nhãn đã được con người duyệt lại. Gắn tag mới chỉ sau commit/push và CI xanh của
đúng commit. Tag cũ không bị di chuyển.

Hướng dẫn thao tác: [công bố và phục hồi](../operations/publication_and_recovery.md).
