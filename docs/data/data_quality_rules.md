# Data Quality Rules

Các quy tắc này được áp dụng khi chuyển dữ liệu từ Bronze sang Silver.

## 1. Quy tắc bắt buộc

| Rule | Điều kiện | Xử lý |
|---|---|---|
| DQ01 | source_name bị thiếu | REJECT |
| DQ02 | source_listing_id bị thiếu | REJECT |
| DQ03 | title bị thiếu hoặc rỗng | REJECT |
| DQ04 | price không parse được hoặc <= 0 | REJECT |
| DQ05 | area không parse được hoặc <= 0 | REJECT |
| DQ06 | transaction_type không xác định được | REJECT |
| DQ07 | property_type không map được | WARN |

## 2. Quy tắc thời gian

### DQ08 - Published date không hợp lệ

Nếu:

published_at > observed_at

thì:

- không đoán lại ngày;
- đặt published_at = NULL ở Silver;
- dq_status = WARN.

Ví dụ đã phát hiện:

source_id: muaban_70944165  
posted_at: 2030-10-15  
scraped_at: 2026-06-03

Đây được xem là dữ liệu ngày đăng không hợp lệ.

## 3. Duplicate

Historical:

(source_name, source_listing_id)

không được trùng trong cùng một batch.

Khi có dữ liệu snapshot:

(source_name, source_listing_id, observed_at)

là khóa quan sát.

Không được xóa các bản ghi của cùng một listing ở các ngày khác nhau vì các bản ghi này được dùng để tạo lịch sử giá.

## 4. Location

district_name, ward_name, latitude và longitude có thể thiếu.

Thiếu tọa độ không làm record bị loại.

Nếu không có tọa độ:

has_coord = false  
coordinate_source = missing

Nếu sau này bổ sung tọa độ thì phải ghi rõ nguồn:

- original
- geocoded
- centroid

Không được coi tọa độ centroid là tọa độ thật của bất động sản.

## 5. Rooms

rooms là trường không bắt buộc.

Không được tự động thay giá trị thiếu bằng 0.

NULL có nghĩa là không có thông tin, trong khi 0 là một giá trị thực.

## 6. Price per m2

Nếu price và area hợp lệ:

price_per_m2 = price / area

Có thể so sánh giá trị tính lại với price_per_m2 từ nguồn.

Nếu sai lệch lớn thì gắn WARN để kiểm tra.

## 7. Description

description không phải trường bắt buộc.

Historical data hiện tại gần như không có description.

Với dữ liệu mới, nếu có description thì Bronze giữ nguyên nội dung và Silver có thể trích thêm các feature sau nếu rule đủ rõ:

- is_frontage
- is_alley
- car_access
- has_elevator
- has_furniture
- has_legal_info

Không suy diễn feature nếu nội dung không đủ rõ.

## 8. DQ Status

Mỗi record Silver có một trong ba trạng thái:

PASS  
Dữ liệu đạt các rule chính.

WARN  
Record vẫn được giữ nhưng có vấn đề không nghiêm trọng như thiếu tọa độ hoặc ngày đăng không hợp lệ.

REJECT  
Record không đủ điều kiện dùng cho phân tích chính, ví dụ không có ID, giá hoặc diện tích hợp lệ.
