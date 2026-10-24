# Hướng dẫn gán nhãn thủ công

Mục đích: đo độ chính xác của `listing_feature` (loại hình và 5 cờ đặc điểm) bằng một mẫu do người kiểm tra, thay vì chỉ dựa vào unit test.

## File

| File | Vai trò |
|---|---|
| `labeling_sheet.csv` | **File bạn điền.** 198 tin (22 tin × 9 nguồn), chọn ngẫu nhiên cố định bằng hash. |
| `labeling_predictions.csv` | Dự đoán của hệ thống. **Không mở file này trước khi gán nhãn xong**, để tránh bị ảnh hưởng. |
| `sample_manifest.json` | Cách chọn mẫu (quần thể, số tin mỗi nguồn, salt). |

Sinh lại mẫu (chỉ khi chưa gán nhãn; script từ chối ghi đè file đã có nhãn):

```powershell
.\scripts\run_spark.ps1 "src\quality\build_labeling_sample.py"
```

## Cách điền

Mở `labeling_sheet.csv` bằng Excel hoặc Google Sheets, đọc `title`, `price`, `area`, `address` và mở `url` khi cần. Điền:

| Cột | Giá trị | Ý nghĩa |
|---|---|---|
| `true_category` | `nha_pho`, `can_ho`, `biet_thu`, `dat`, `phong_tro_khac`, `khong_ro` | Loại hình **thực tế** của bất động sản, đánh giá từ title, giá, diện tích và trang tin |
| `true_legal` | `1` / `0` / `?` | `1` nếu **title** nêu pháp lý (sổ hồng, sổ đỏ, SHR, công chứng…) |
| `true_furnished` | `1` / `0` / `?` | `1` nếu title nêu có nội thất (full nội thất, NTCC…) |
| `true_frontage` | `1` / `0` / `?` | `1` nếu title nêu BĐS **nằm mặt tiền đường/phố**. "MT 5m" (chiều ngang) hoặc "mặt tiền hẻm" → `0` |
| `true_elevator` | `1` / `0` / `?` | `1` nếu title nêu có thang máy |
| `true_car_access` | `1` / `0` / `?` | `1` nếu title nêu ô tô tiếp cận được (hẻm/ngõ ô tô, ô tô vào nhà, đỗ cửa) |
| `labeler` | tên hoặc MSSV | Người gán |
| `labeled_at` | `YYYY-MM-DD` | Ngày gán |
| `note` | tự do | Ghi chú cho trường hợp khó |

Quy ước loại hình:

- `nha_pho`: nhà phố, nhà riêng, nhà hẻm, nhà mặt tiền, nhà cấp 4.
- `can_ho`: căn hộ chung cư, officetel, condotel, penthouse, duplex, studio, tập thể.
- `biet_thu`: biệt thự, nhà vườn, liền kề, shophouse.
- `dat`: đất nền, đất thổ cư, lô đất, đất nông nghiệp.
- `phong_tro_khac`: phòng trọ, căn hộ dịch vụ (cả tòa), kho xưởng, văn phòng, mặt bằng, khách sạn, trang trại.
- `khong_ro`: không đủ thông tin để xác định.

Cờ đặc điểm đo việc **title có nêu** hay không (vì hệ thống chỉ đọc title), không phải BĐS thực tế có hay không. Dùng `?` khi không quyết định được; dòng `?` không được tính.

## Đánh giá

Sau khi điền xong (hoặc một phần), chạy:

```powershell
python -m src.quality.evaluate_labels
```

Kết quả nằm ở `docs/validation/label_accuracy.json`: độ chính xác loại hình (tổng và theo nguồn), precision, recall, F1 cho từng loại hình và từng cờ, ma trận nhầm lẫn. `status` chỉ là `COMPLETE` khi mọi dòng có loại hình hợp lệ, tên người gán và đủ 5 cờ (chấp nhận `?` cho cờ chưa xác định, không đưa cờ đó vào metric). Trùng/mất sample ID bị chặn; script sinh mẫu không ghi đè khi bất kỳ ô gán nhãn, tên, ngày hoặc ghi chú đã được điền.
