# Câu hỏi bảo vệ — pipeline và vận hành

Số liệu hiện hành đọc từ `defense_verification.json`; chạy
`python -m src.orchestration.status` trước khi trình bày. Không thuộc lòng số
của một tag cũ khi code hoặc input đã thay đổi.

1. **Vì sao Lakehouse?** Object storage lưu raw và bảng Iceberg, Spark xử lý,
   Trino truy vấn. Phù hợp mục tiêu đồ án; chưa chứng minh vượt mọi Data Warehouse.
2. **PostgreSQL chứa listing không?** Listing nằm trên MinIO. PostgreSQL lưu
   metadata Airflow/Superset, REST catalog dùng SQLite trên volume.
3. **Chặn sửa input thế nào?** SHA256 toàn bộ CSV mỗi lượt; cache chỉ giảm parse.
   Historical còn so multiset raw với Bronze; batch cũ không bị overwrite.
4. **Tự động thu thập tới đâu?** Hai nguồn web có crawler có giới hạn; Batdongsan
   xuất CSV thủ công rồi Airflow tiếp nhận. Không tự tạo batch lịch sử còn thiếu.
5. **Lỗi giữa Gold thì sao?** Giữ cửa sổ đọc đóng, publication FAILED; cần chạy
   lại đủ bước phụ thuộc và kiểm định. Không có transaction nhiều bảng.
6. **Hai writer cùng lúc?** Airflow và lệnh chạy tay dùng cùng khóa; run khác bị
   từ chối khi đang có build. Không chứng nhận người chạy ngoài runner.
7. **Lịch sử đúng không, MERGE nhanh hơn không?** So hai chiều MERGE/rebuild,
   schema/grain/SHA256 và retry. Đọc thời gian thật từ JSON; không hứa luôn nhanh hơn.
8. **Tin vắng hai batch đã bán chưa?** Chưa. Đó là vắng trong mẫu, phụ thuộc độ
   sâu crawl và lịch riêng từng nguồn. Historical có trạng thái NOT_APPLICABLE.
9. **Dữ liệu thiếu có bị bịa để PASS?** Không. NULL/WARN được giữ theo hợp đồng,
   reject nghiêm trọng vào quarantine, ngày thiếu được khai báo.
10. **Spark/Trino chạy cùng lúc được không?** Local đang dùng write/read window
    riêng. Read yêu cầu đủ publication mới nhất và DAG paused.
11. **Đổi bucket có mất bảng không?** Runner dừng khi location không khớp;
    không tự DROP PURGE. Migration cần backup, đối chiếu và kế hoạch riêng.
12. **Restart Docker có phải backup?** Không. Có offline backup năm volume,
    checksum và restore-check trên volume mới. Máy vật lý khác phải nghiệm thu riêng.
13. **PASS là xong cả đồ án?** PASS kỹ thuật chỉ đúng code/input/run đã đo.
    G4, kết luận BQ, nhãn người duyệt, máy khác và CI có trạng thái riêng.
14. **Mô hình đổi khi thêm dữ liệu không?** Gold được dựng lại theo cấu hình của
    B; CV mới xuất candidate để B xem, không tự áp dụng trọng số khác.
15. **Vì sao không sửa tag cũ?** Tag là mốc bất biến. Bản mới cần proof mới và
    CI xanh của chính commit đó, rồi chọn version mới.

Nguồn: [nghiệm thu](../validation/defense_verification.json),
[Airflow](../validation/integration_airflow_run.json),
[history](../validation/history_incremental_verification.json),
[rebuild](../validation/deterministic_rebuild.json),
[vận hành](../operations/publication_and_recovery.md).
