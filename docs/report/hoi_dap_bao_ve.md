# Câu hỏi bảo vệ — pipeline và vận hành

Võ Đức Hoàng, cập nhật batch 17/11 theo run `acceptance_A_20261117_r2`.

1. **Vì sao Lakehouse?** Raw/Parquet và bảng Iceberg nằm trên object storage,
   hỗ trợ xử lý lại bằng Spark và truy vấn Trino. Đây là lựa chọn phù hợp mục
   tiêu; chưa có benchmark chứng minh tốt hơn mọi Data Warehouse.
2. **PostgreSQL lưu dữ liệu BĐS không?** Dữ liệu nằm trên MinIO. Catalog REST
   dùng SQLite; PostgreSQL phục vụ metadata Airflow/Superset. Xem Compose và
   `docs/architecture.md`.
3. **Phát hiện input bị sửa thế nào?** SHA256 mọi file mỗi lượt, cache chỉ
   giảm parse; file cũ mất/đổi bytes bị chặn. Hiện có 48 snapshot CSV, 316.241
   dòng, cộng 30.317 historical. Chỉnh ngày Guland trước ingest được ghi rõ
   tại `input_date_review.json`; sau khi đóng băng, 54 file không đổi.
4. **Batdongsan tự động tới đâu?** Xuất CSV từ extension vẫn thủ công. Sensor
   chờ file rồi pipeline tự xử lý tiếp. Guland/Nhadatvui có crawler và dùng
   lại batch đã có; không thu thập hôm nay rồi tự tạo batch quá khứ.
5. **Task lỗi có tiếp tục xử lý không?** Fault probe ghi 1 failed và 23
   upstream_failed. Run thật 24/24 SUCCESS trong 857.59 giây. Bronze đã commit
   theo batch được giữ để retry; không có transaction toàn pipeline.
6. **MERGE có luôn nhanh hơn không?** Không. Đối chứng hiện tại MERGE
   17.74 giây, rebuild 9.96 giây; cùng 167.794 dòng, cùng SHA256.
   Xem `history_incremental_verification.json`.
7. **Chứng minh lịch sử đúng thế nào?** So MERGE/rebuild hai chiều, schema,
   grain, full-row multiset hash, 11 fixture SCD2 và retry giữ nguyên snapshot.
   SCD2 là lịch sử listing; time travel là phiên bản vật lý của bảng Iceberg.
8. **90,57% tin vắng nghĩa là đã bán?** Chưa đủ bằng chứng. Đây là
   108.415/119.700 tin crawl không có trong hai batch mới nhất của nguồn;
   Guland chỉ thu 30 trang cuối kỳ so với 1.000 lúc đầu. Không xác nhận đã
   bán/hết hạn nếu chưa kiểm tra URL. Xem `crawl_coverage.json`.
9. **Spark và Trino chạy đồng thời được không?** Môi trường hiện tách write
   và read window, writer bị chặn khi Trino còn chạy. REST được mở lại kết
   nối sau khi readers dừng để tránh pooled SQLite read lock. Chưa nghiệm thu
   catalog đồng thời nhiều writer/reader.
10. **PASS có nghĩa đồ án hoàn thành mọi mặt?** Bộ kỹ thuật 66/66 PASS và
    466 unit test chỉ chứng nhận code/input/run này. G4, đánh giá nghiệp vụ,
    máy sạch của thành viên khác, review/CI và diễn tập phần nói có điều kiện
    riêng. Thêm data hoặc sửa code cần kiểm định lại.

Nguồn: [nghiệm thu](../validation/defense_verification.json),
[Airflow](../validation/integration_airflow_run.json),
[history](../validation/history_incremental_verification.json),
[rebuild](../validation/deterministic_rebuild.json).
