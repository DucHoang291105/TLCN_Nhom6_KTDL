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

## Quy trình: gán sơ bộ + rà soát (từ 16/11/2026)

Quy tắc chi tiết kèm ví dụ cho người review: [`QUY_TAC_GAN_NHAN.md`](QUY_TAC_GAN_NHAN.md).

Để kịp tiến độ, nhãn được **gán sơ bộ bởi trợ lý AI (Claude)** chỉ từ `title`, `price`, `area`, `address`, **không mở `labeling_predictions.csv`**. Kế hoạch ban đầu là hai lượt review độc lập, sau đó một người gán lại 50 tin cách ít nhất hai ngày để đo độ nhất quán (kappa).

**Trạng thái thực tế (21/11/2026): G4 dừng ở mức sơ bộ, `status = PARTIAL`.**

- Lượt 1: đủ 198/198 tin. Hai thành viên chia nhau, **mỗi tin chỉ một người xem**. Người rà soát sửa 6/1.188 ô (0,5%), cả 6 đều ở cột loại hình.
- Lượt 2 và phép gán lại để tính kappa: **không thực hiện** do hết thời gian. Cột `review_2` để trống; không điền bù.
- Hệ quả khi trình bày: nhãn là "nhãn sơ bộ + 1 lượt người rà soát", không phải ground truth đã chốt. Tỷ lệ sửa thấp có thể do người rà soát bị bản sơ bộ dẫn dắt. Trên tập test 76 tin, số ca dương tính của từng cờ ít (thang máy 2, nội thất 7, ô tô 11, pháp lý 15, mặt tiền 21), nên precision/recall 100% chỉ có nghĩa trên các ca này.
- Số liệu và số ca lấy từ `label_accuracy.json` (`review_rounds`, `review_changes`) và `label_accuracy_rules_compare.json` (`*_support`, `*_labelled`).

| File / cột | Vai trò |
|---|---|
| `labeling_sheet_prelabel.csv` | Bản gán sơ bộ, **không sửa**. Dùng để đếm số nhãn người review đã thay đổi |
| `labeling_sheet.csv` | Bản review: sửa trực tiếp vào các cột `true_*` |
| `review_1`, `review_2` | MSSV người review lượt 1 và lượt 2; chỉ điền sau khi đã xem **cả 198 dòng** |
| `note` | Ghi lý do khi sửa nhãn; các dòng mơ hồ đã có sẵn ghi chú của bản sơ bộ |

Cách review:

1. Đọc title; với dòng có `note` hoặc `khong_ro`, mở `url` để chốt loại hình.
2. Sửa nhãn nếu không đồng ý. Không mở `labeling_predictions.csv` cho tới khi cả hai lượt review xong.
3. Quy ước bản sơ bộ đã dùng (nhóm có thể đổi, nhưng phải đổi đồng loạt): "thổ cư", "hoàn công" tính là nêu pháp lý; "gần ô tô" → ô tô = 0; "hẻm xe tải" → ô tô = 1; "mặt tiền hẻm", "sát mặt tiền", "mặt tiền bờ sông" → mặt tiền = 0; title chỉ có "mặt tiền/lô góc" mà không rõ nhà hay đất → `khong_ro`.

`evaluate_labels` chỉ báo `status = COMPLETE` khi mọi dòng có đủ `review_1` và `review_2` của **hai người khác nhau** (cùng một tên ở cả hai ô không được tính), và ghi `review_changes` (số dòng, số ô đã sửa so với bản sơ bộ) vào `label_accuracy.json`. Khi báo cáo G4 phải ghi rõ quy trình này và tỷ lệ sửa: nếu người review gần như không sửa gì, cần nói rõ đó là giới hạn (người review có thể bị bản sơ bộ dẫn dắt).
