# Minh chứng vai C – tuần 1 và tuần 2 (24/10/2026)

Dữ liệu: snapshot đến 23/10/2026, pipeline sau khi gộp `main` (`e76366a`) và sửa lỗi Silver (mục 5): verify_silver 30/30, verify_gold 92/92, pytest 292/292.

## 1. Dashboard Superset (tuần 1: Tổng quan, BQ2; tuần 2: BQ1, BQ3)

Dựng bằng code, chạy lại được: `python -m src.dashboard.provision_superset` (cần `docker compose --profile dashboard --profile query up -d`). Superset đọc Gold qua Trino.

Mỗi dashboard trả lời **một câu hỏi** theo cùng khung: (1) đầu trang là câu hỏi và 3 kết luận có số liệu, **tính lại từ Gold mỗi lần dựng** (`src/dashboard/insights.py`, không gõ tay); (2) hàng KPI; (3) biểu đồ chứng minh từng kết luận; (4) bảng chi tiết ở cuối.

| Ảnh | Câu hỏi | Biểu đồ chính |
|---|---|---|
| `01_tlcn-tong-quan.png` | Thị trường đang chào bán gì, ở đâu, giá bao nhiêu? | Số tin theo tỉnh, cơ cấu loại hình, median giá/m² theo tỉnh × loại hình, số dòng qua từng tầng Bronze → Gold |
| `02_tlcn-bq1.png` | Với 3–5 tỷ, mua nhà phố ở TP.HCM được gì, đánh đổi gì? | Diện tích theo quận **xếp theo khoảng cách tới trung tâm** (xa hơn → rộng hơn, r = 0,86), giá/m² theo quận, tỷ lệ tin nêu ô tô tiếp cận, danh sách tin không bị trội |
| `03_tlcn-bq2.png` | Tin nào rao cao/thấp bất thường so với căn tương đồng? | Tỷ lệ vị trí giá theo loại hình, phân bố mức lệch so với median nhóm, quận có nhiều căn hộ rao cao, tin rao cao nhất |
| `04_tlcn-bq3.png` | Không mua được ở quận này thì xem quận nào? | % chi phí thấp hơn kèm số km xa thêm, chênh chi phí (tỷ) cho căn điển hình, quận gốc có nhiều lựa chọn |

Lưu ý thiết kế: BQ2 không dùng "% trên P75" làm kết luận vì theo định nghĩa tứ phân vị luôn xấp xỉ 25%; thay vào đó dùng **mức lệch** (rao cao hơn median nhóm ≥ 25%, rao gấp đôi).

Kiểm chứng: `docs/validation/superset_dashboard_check.json` – **29/29 chart PASS**; mỗi chart lấy dữ liệu qua chart data API, KPI chính được đối chiếu với truy vấn Trino; kết luận đầu trang lưu ở trường `insights` của file này.

Ảnh do `python -m src.dashboard.screenshot_dashboards` chụp bằng trình duyệt headless (Playwright) sau khi đăng nhập; script dừng nếu bị chuyển về trang đăng nhập.

## 2. Benchmark Gold so với Silver (tuần 2)

`python -m src.quality.query_benchmark` → `docs/validation/query_benchmark.json`. Mỗi câu hỏi: 1 lần khởi động + median 5 lần chạy qua Trino.

| Câu hỏi | Gold (giây) | Tính từ Silver (giây) | Nhanh hơn |
|---|---:|---:|---:|
| Mặt bằng giá theo loại hình, TP.HCM | 0,096 | 0,752 | 7,8× |
| BQ1 – Nhà phố 3–5 tỷ theo quận, TP.HCM | 0,102 | 0,450 | 4,4× |
| BQ2 – Vị trí giá của căn hộ TP.HCM trong nhóm | 0,149 | 0,945 | 6,3× |
| BQ3 – Quận thay thế cho nhà phố 50–80 m² | 0,105 | 0,666 | 6,3× |

Giới hạn: truy vấn Silver chỉ **xấp xỉ** logic Gold (không dedup giữa nguồn, không Pareto, percentile xấp xỉ, không có các cấp nhóm dự phòng), nên số dòng khác nhau (BQ2: 17.223 so với 8.605) và thời gian Silver là **cận dưới** chi phí thật. Trino chạy một node trong Docker, dữ liệu khoảng 135 nghìn tin; tỷ lệ có thể khác ở quy mô lớn hơn.

## 3. Đo độ chính xác bằng tập nhãn (tuần 1–2)

- Mẫu: `docs/validation/labeling/labeling_sheet.csv` – 198 tin (22 × 9 nguồn), chọn cố định bằng hash (`src/quality/build_labeling_sample.py`).
- Hướng dẫn: `docs/validation/labeling/README.md`.
- Đánh giá: `python -m src.quality.evaluate_labels` → `docs/validation/label_accuracy.json` (accuracy loại hình, precision/recall/F1 từng cờ, ma trận nhầm lẫn).

**Trạng thái: chờ gán nhãn thủ công.** Kết quả chỉ được dùng khi người gán đã điền đủ và ghi tên ở cột `labeler`.

## 4. Vấn đề vận hành và dữ liệu còn lại

1. **Catalog Iceberg dùng SQLite bị khóa (`SQLITE_BUSY`)** khi Trino đọc (dashboard đang mở, benchmark) trong lúc Spark commit; một commit lỗi giữ khóa đến khi khởi động lại `iceberg-rest`. Khi chạy pipeline: tắt Superset (`docker stop tlcn_superset`), không chạy truy vấn song song. Về lâu dài nên chuyển catalog sang PostgreSQL.
2. Trên `main` còn **thiếu batch guland 20/10** (bổ sung dữ liệu sau).
3. **175 tin rao > 10× median nhóm** sau khi sửa phần lớn là outlier thật (mặt tiền trung tâm, đất ven biển trong nhóm so sánh gộp vùng ven) – giữ lại theo DQ11, dashboard hiển thị riêng.

## 5. Sửa lỗi lớp Silver (phát hiện qua dashboard)

Mỗi lỗi được tái hiện trên dữ liệu thật, sửa ở nguyên nhân gốc và có regression test (`tests/unit/test_silver_value_fixes.py`, `tests/unit/test_feature_rules.py`). Quy tắc ghi trong `docs/data/data_quality_rules.md` (DQ14–DQ16, quy tắc parse giá) và `docs/data/silver_location_schema.md`.

| Lỗi | Nguyên nhân gốc | Cách sửa | Trước | Sau |
|---|---|---|---:|---:|
| Tên quận TP.HCM không chuẩn ("Bình Tân", "Tân Phú", "Phường Gò Vấp", "Bình Thạnh cũ)") | Chỉ nhận tên có tiền tố "Quận/Huyện" | `normalize_district_field`: tên trần, phường 2025 trùng tên quận cũ, hậu tố "cũ" (`CORE_BARE_NAME`) | ~700 tin, 2 nhãn cho cùng một quận | 0 |
| "Unknown" và mảnh văn bản làm tên quận | Giữ nguyên mọi chuỗi nguồn | Bỏ giá trị giữ chỗ/mảnh văn bản rồi suy luận từ địa chỉ | 43 + 4 | 0 |
| "9.630 tỷ", "5800 tỷ", "3150000000 tỷ", "2,424 tỷ" | Dấu phân cách và đơn vị ghi sai | Thử cách đọc theo giá/m² hợp lý (DQ14) | 37 tin ≥ 1.000 tỷ | 270 giá được đọc lại |
| Diện tích 1 m² (nhadatvui), 11 thay cho "11.000m2" | Dấu phân cách/trường sai ở crawler | Lấy diện tích ghi trong title (DQ15) | 67 tin < 10 m² | 108 diện tích được sửa |
| Giá trị không thể có (giá < 50 triệu, giá/m² ngoài [20 nghìn; 2 tỷ]) | Lỗi nhập/parse không sửa được | Đặt NULL, giữ `price_str` (DQ16) | 25 giá < 50 triệu; 98 giá/m² > 2 tỷ; 6 giá/m² < 20 nghìn (có thể trùng nhau) | 0 (304 giá trị được NULL) |
| muaban "52m2 giá 2tỷ360triệu" → 52 tỷ | Parser lấy số đầu tiên trong chuỗi (diện tích) | Lấy số đứng trước đơn vị; hỗ trợ "2 tỷ 290", "2tỷ550", "11 triệu 500 nghìn"; đối chiếu giá tính sẵn của file historical (`reconcile_shipped_price`) | 11 tin | 0 |
| "Nhà 6 tầng gần chung cư SCREC" xếp thành căn hộ | "chung cư" sau từ chỉ vị trí bị coi là loại hình | Bỏ qua cụm "gần/ngay/đối diện… + loại hình" (feature rules v4) | 2 tin ví dụ | 0 |
| Nhánh historical gắn DQ08 khi thiếu diện tích | Nhầm mã | Đúng mã DQ05 | – | – |

Không sửa: 23 tin batdongsan rao tròn 1 tỷ/m² ("250 tỷ" cho "250 m²", nguồn ghi "1 tỷ/m²") – là giá thật của nhà mặt phố trung tâm.

Sau khi sửa: pipeline chạy lại toàn bộ, verify_silver 30/30, verify_gold 92/92; mẫu gán nhãn được sinh lại theo feature rules v4 (chưa có nhãn nên không mất dữ liệu gán nhãn).
