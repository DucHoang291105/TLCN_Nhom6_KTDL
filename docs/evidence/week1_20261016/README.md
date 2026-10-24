# Minh chứng tuần 1 — Pipeline và vận hành

Các ảnh `01_pipeline_verification.png`, `02_listing_lifecycle.png`, `03_airflow_task_states.png`, `04_airflow_failure_gate.png` được dựng từ JSON kiểm chứng và API Airflow thật bằng `python -m src.report.build_week1_evidence`. Đây là **hình xuất từ kết quả chạy**, không phải ảnh chụp giao diện. `figure_sources.json` ghi nguồn dữ liệu.

Ảnh chụp giao diện nên bổ sung khi có kết nối trình duyệt:

1. http://localhost:8082 → DAG `lakehouse_pipeline` → run thành công → Graph/Grid: đủ task xanh.
2. Task `gold.listing_lifecycle` → Logs: số dòng và summary của bảng vòng đời.
3. Task `gold.verify_gold` → Logs: các check PASS.
4. Run `week1_failure_gate_verified_20261016`: task đầu failed và downstream upstream_failed, chứng minh dừng khi lỗi.

Không chụp file password hoặc environment chứa secret. Các con số dùng cho báo cáo lấy từ `docs/validation/`.
