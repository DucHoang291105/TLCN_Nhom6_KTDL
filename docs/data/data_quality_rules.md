# Data Quality Rules

Các quy tắc này áp dụng khi chuyển dữ liệu từ Bronze sang Silver. Silver ưu tiên bảo toàn dữ liệu và lineage: chỉ quarantine khi không thể định danh record; dữ liệu thiếu hoặc bất thường nhưng còn giá trị sử dụng được giữ lại với trạng thái WARN.

## 1. Mã quy tắc

| Rule | Điều kiện | Chuẩn hóa | Trạng thái |
|---|---|---|---|
| DQ01 | Thiếu `source` | Không thể xác định nguồn | REJECT |
| DQ02 | Thiếu `ad_id` | Không thể tạo khóa ổn định | REJECT |
| DQ03 | Thiếu/rỗng `title` | Không đủ khả năng định danh nội dung tin | REJECT |
| DQ04 | Giá thỏa thuận, thiếu, không parse được hoặc <= 0 | `price = NULL`, giữ `price_str` | WARN |
| DQ05 | Diện tích thiếu, không parse được hoặc <= 0 | `area = NULL` | WARN |
| DQ06 | Không xác định được bán/thuê từ field hoặc endpoint đã xác minh | `is_rent = NULL` | WARN |
| DQ07 | Category chưa map | Giữ raw category trong metadata; canonical category NULL | WARN |
| DQ08 | Nguồn có cung cấp thời gian đăng nhưng giá trị `posted_at > scraped_at`, rỗng hoặc không tin cậy | `posted_at = NULL` | WARN |
| DQ09 | Nguồn có cung cấp tọa độ nhưng thiếu một phía, `(0,0)` hoặc ngoài phạm vi | `lat = lon = NULL`, `has_coord = FALSE` | WARN |
| DQ10 | `price_per_m2` nguồn lệch lớn với `price / area` | Dùng giá trị canonical `price / area` | WARN |
| DQ11 | Giá/diện tích có dấu hiệu outlier | Không tự động xóa; gắn cờ audit | WARN |
| DQ12 | Text chứa HTML/control character/khoảng trắng thừa | Chuẩn Unicode, loại control character và gom khoảng trắng | PASS sau chuẩn hóa |
| DQ13 | `batch_id` snapshot không đúng `YYYYMMDD` | Không ghi vào observation chính | REJECT |

## 2. DQ Status và quarantine

- `PASS`: đạt các rule chính sau chuẩn hóa.
- `WARN`: vẫn được giữ trong Silver nhưng có ít nhất một vấn đề không nghiêm trọng.
- `REJECT`: không vào canonical observation/current; ghi sang `silver_dq_quarantine` cùng lý do và lineage Bronze.

Một record có nhiều lỗi thì `dq_reasons` chứa toàn bộ mã rule. Mức cuối cùng lấy theo thứ tự `REJECT > WARN > PASS`.

Một field được Data Contract đánh dấu `UNAVAILABLE` cho toàn bộ nguồn không phải lỗi riêng của từng record, nên không phát sinh DQ08/DQ09. Field vẫn để NULL/FALSE và được phản ánh trong `completeness_score`. Ví dụ card Batdongsan hiện không có `published_info_text`, `lat` và `lon`; chỉ khi crawler bắt đầu cung cấp các cột này mà giá trị của record rỗng/sai thì mới gắn WARN.

## 3. Duplicate và grain

- Grain observation: một `(source, ad_id, batch_id)` sau dedup.
- Duplicate trong cùng batch được chọn theo: `completeness_score DESC`, `scraped_at DESC`, `page_fetched ASC`.
- Cùng `source_id` ở ngày khác là repeated observation và phải được giữ để theo dõi lịch sử.
- `record_hash` chỉ dùng field nghiệp vụ chuẩn hóa; không dùng batch, page, scraped time hoặc ingestion time.
- Record hash giống batch trước: cập nhật metadata quan sát; hash khác: tạo version nghiệp vụ mới trong history.
- Không quan sát lại listing không đồng nghĩa listing đã bán.
- ID khác nhưng nghi cùng bất động sản chỉ ghi match candidate, không tự động merge.

## 4. Giá, diện tích và phòng

- Giá canonical dùng VND.
- `price_str` giữ text nguồn để audit.
- `price_m = price / 1_000_000` khi price hợp lệ.
- `price_per_m2 = price / area` khi price và area hợp lệ; ngược lại là NULL.
- Không suy đoán giá từ description nếu nguồn không công bố giá chính thức.
- Không thay area hoặc rooms thiếu bằng 0.
- Chỉ trích rooms từ description khi regex thể hiện rõ phòng ngủ, ví dụ `3PN` hoặc `3 phòng ngủ`.

## 5. Location và tọa độ

- `ward`, `district_id`, `district_name`, `lat`, `lon` có thể NULL.
- Thiếu location/tọa độ không làm record bị loại.
- Cần cả lat và lon hợp lệ mới đặt `has_coord = TRUE`.
- Tọa độ bổ sung phải ghi nguồn: `original`, `geocoded`, `ward_centroid`, `district_centroid` hoặc `missing`.
- Không coi centroid là vị trí thật của bất động sản.
- Với dữ liệu toàn quốc, khoảng cách phải tính tới trung tâm tỉnh/thành tương ứng, không mặc định dùng trung tâm TP.HCM.

## 6. Timestamp

- Chuẩn hóa timestamp về cùng timezone nghiệp vụ `Asia/Ho_Chi_Minh` trước khi ghi.
- Epoch milliseconds của NhaDatVui phải chuyển đúng đơn vị.
- Thời gian tương đối như `2 giờ trước` chỉ được suy ra dựa trên `scraped_at` khi parser chắc chắn; nếu không thì `posted_at = NULL`.
- Không dùng `scraped_at` để giả làm `posted_at` khi nguồn không cung cấp ngày đăng.

## 7. Feature từ văn bản

Bronze giữ nguyên title/description. Silver Feature có thể trích:

- `title_has_frontage`
- `title_has_car_access`
- `title_has_elevator`
- `title_has_furnished`
- `title_has_legal`

Chỉ đặt TRUE khi văn bản có bằng chứng rõ. Không đề cập không đồng nghĩa với FALSE; khi BQ cần phân biệt phải dùng `*_known` hoặc NULL. Feature phải ghi `feature_extracted_from` để audit.

## 8. Outlier và BQ

Silver không kết luận tin “đắt”, “rẻ” hoặc “đáng mua”. Silver chỉ chuẩn hóa và gắn cờ chất lượng. Benchmark giá, mức lệch so với bất động sản tương đồng, trade-off ngân sách và xếp hạng khu vực thay thế thuộc Gold/ML.
