# Kết quả hoàn thiện pipeline và Iceberg

Sinh từ JSON kiểm chứng ngày 2026-10-24T10:22:47.122613+00:00. Không nhập tay số liệu.

## Đã thực hiện

- Tiếp nhận 29 CSV snapshot, gồm dữ liệu mới 20/10 và 23/10.
- Chuẩn hóa tên CSV Nhadatvui theo hợp đồng đầu vào; không sửa nội dung.
- Bronze tái sử dụng batch hoàn tất và preflight kiểm tra checksum bất biến.
- History SCD2 dùng MERGE theo `(source_id, version_number)`, partition `snapshot_date`.
- Có bảng state/checkpoint; retry, dữ liệu đến muộn, sửa/xóa quan sát đều có kiểm thử thực chạy.
- Thực thi demo time travel và schema evolution trên bảng sandbox, không đổi schema production.
- Tích hợp chế độ incremental/rebuild vào Airflow và PowerShell; thêm CI pytest Python 3.10/3.12.

## Kết quả thu được

| Chỉ tiêu | Kết quả |
|---|---:|
| Snapshot CSV | 29 |
| CSV mới sau baseline 20261016 | 5 |
| Dòng raw mới trong các CSV đó | 16,550 |
| Dòng snapshot Bronze | 258,541 |
| Silver observation | 278,987 |
| Silver current | 137,732 |
| Silver history | 151,972 |
| Gold fact listing | 122,375 |
| Silver checks | 30/30 PASS |
| Gold checks | 92/92 PASS |
| Unit tests | 199 PASS |
| SCD2 fixture | 11 PASS |
| Iceberg feature demo | 6 PASS |
| Airflow task | 23/23 SUCCESS |
| Thời gian thực chạy Airflow | 12.94 phút |

MERGE tính lại 9,689/137,732 listing ID;
đầu ra 151,972 dòng, cùng SHA256 với full rebuild:
`1ffde96c9a09bb0f2b1129532e9103a41beb13a4d6eba5ae5064d996852930d7`.
EXCEPT ALL hai chiều: 0/0 dòng khác.
Thời gian thực đo: MERGE 19.3666 giây, rebuild 11.388 giây.
Đây là một lần đo local, không khẳng định MERGE luôn nhanh hơn rebuild.

CI: [Tests](https://github.com/DucHoang291105/TLCN_Nhom6_KTDL/actions/runs/40085426425), commit `8d398619d42ad7d75c773e1c215a3318fdb57740`.
Airflow run: `incremental_verified_20261023`, chế độ history `incremental`.
29 checksum CSV giữ nguyên sau xử lý.

## Ảnh minh chứng

- [01 — MERGE so với full rebuild](01_merge_vs_rebuild.png).
- [02 — Các kiểm thử Iceberg/SCD2](02_iceberg_checks.png).
- [03 — Tổng hợp kiểm định pipeline](03_pipeline_validation.png).
- [04 — Trạng thái 23 task Airflow](04_airflow_tasks.png).

## Lưu ý khi báo cáo

- Guland không có batch 20/10; không giả tạo hoặc lấy batch 23/10 đổi ngày thành 20/10.
- Chỉ history incremental; Silver Core và Gold vẫn rebuild. Không làm thay phần mô hình BQ/dashboard.
- `valid_to=NULL` không chứng minh tin vẫn đang đăng; vắng hai batch không đồng nghĩa đã bán.
- Các PNG trong thư mục này là hình tổng hợp từ JSON/API thật, **không phải ảnh chụp UI**.
- Giữ cả log các lượt chưa thành công: batch 20/10 thiếu Guland nên bị chặn;
  lượt 23/10 đầu tiên bị chặn ở cleanup smoke test do namespace đã có bảng demo.
  Đã sửa cleanup chỉ xóa bảng riêng và chạy lại, không đổi task lỗi thành SUCCESS bằng tay.
- Log trước sửa: [thiếu batch](../../validation/airflow_missing_batch_20261020.json),
  [smoke cleanup](../../validation/airflow_smoke_cleanup_before_fix.json).
- [Hướng dẫn vận hành](../../operations/iceberg_history.md), [SQL demo](../../demo/iceberg_features.sql).
