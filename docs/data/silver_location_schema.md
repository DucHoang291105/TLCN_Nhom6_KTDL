# Silver Location Schema

`listing_location` là bảng mở rộng vị trí trên phạm vi toàn Việt Nam của
`listings_current_27`. Grain của bảng là **một dòng cho mỗi `source_id`**.
Job chỉ đọc Silver Core trên MinIO, không đọc trực tiếp CSV crawler hoặc Bronze.

Đường dẫn đầu ra:

`s3a://lakehouse-silver/real_estate/location/listing_location`

Danh mục tham chiếu được quản lý tại
`config/vietnam_province_centers.csv`, gồm 34 tỉnh/thành có hiệu lực từ
01/07/2025. Các tên tỉnh/thành cũ được ánh xạ về đơn vị hiện hành để dữ liệu
lịch sử và dữ liệu crawl mới có thể dùng chung một schema.

## Các cột

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| source_id | string | Khóa listing, duy nhất trong bảng current |
| source | string | Nguồn listing |
| address | string | Địa chỉ đã làm sạch từ Silver Core |
| ward | string | Phường/xã từ Silver Core; không tự điền khi thiếu |
| district_id | string | Mã quận/huyện nếu Core có |
| district_name_raw | string | Tên quận/huyện lấy từ Core |
| district_name_model | string | Nhãn khu vực cấp quận/huyện phục vụ phân tích |
| district_mapping_method | string | Cách tạo nhãn quận/huyện |
| district_model_version | string | Phiên bản quy tắc quận/huyện |
| province_name_raw | string | Tên tỉnh/thành tìm thấy trong dữ liệu nguồn |
| province_name_model | string | Tỉnh/thành chuẩn theo mô hình 34 đơn vị |
| province_mapping_method | string | `ADDRESS_COMPONENT`, `HCMC_DISTRICT`, `ADMIN_PREFIX_TEXT`, `FREE_TEXT`, `NEAREST_CENTER` hoặc `UNMAPPED` (xem thứ tự ưu tiên bên dưới) |
| province_model_version | string | Phiên bản danh mục tỉnh/thành |
| lat, lon | double | Tọa độ hợp lệ từ Silver Core |
| has_coord | boolean | Có đủ cặp tọa độ hợp lệ |
| location_precision | string | `COORDINATE`, `WARD`, `DISTRICT`, `PROVINCE_OR_ADDRESS` hoặc `UNKNOWN` |
| center_name | string | Tên trung tâm tham chiếu của tỉnh/thành đã map |
| center_type | string | Loại tâm tham chiếu dùng cho phân tích |
| center_lat, center_lon | double | Tọa độ tâm tham chiếu |
| distance_to_center_km | double | Khoảng cách Haversine đến tâm của đúng tỉnh/thành; NULL nếu thiếu tọa độ, chưa map tỉnh, hoặc tỉnh xung đột với tọa độ (`LQ05`) |
| location_dq_status | string | `PASS`, `WARN` hoặc `REJECT` |
| location_dq_reasons | array<string> | Các mã cảnh báo location |
| location_hash | string | SHA-256 để theo dõi thay đổi location |

## Quy tắc quan trọng

- Không mặc định mọi listing về TP.HCM. Mỗi listing được map về tỉnh/thành của
  chính nó rồi mới tính `distance_to_center_km`.
- Tỉnh/thành được chọn theo thứ tự bằng chứng:
  1. `ADDRESS_COMPONENT`: thành phần **cuối** của địa chỉ (hoặc thành phần có tiền
     tố "Tỉnh/Thành phố/TP") trùng đúng tên tỉnh. Với NhaDatVui, Core ghép
     `ward_name` và `province_name` có cấu trúc vào `address`.
  2. `HCMC_DISTRICT`: quận/huyện của TP.HCM trong trường quận hoặc địa chỉ.
  3. `ADMIN_PREFIX_TEXT`: tên tỉnh có tiền tố hành chính trong địa chỉ, title, URL.
  4. `FREE_TEXT`: tên tỉnh đứng trần, so khớp **có dấu** ("Vĩnh" ≠ "Vinh",
     "Huê" ≠ "Huế"), bỏ qua khi đứng sau từ chỉ đường/phường/xã/người
     ("Xa lộ Hà Nội", "Phường Phú Thọ Hòa", "Hồ Văn Huê"); nếu có tọa độ thì tỉnh
     phải nhất quán với tọa độ.
  5. `NEAREST_CENTER`: không có text đủ tin cậy nhưng có tọa độ; chỉ là suy luận,
     mang cảnh báo `LQ04`. Tâm gần nhất không phải ranh giới hành chính.
- Khi tỉnh theo text mâu thuẫn với tọa độ (`LQ05`), `distance_to_center_km` để
  NULL; Gold gán `location_key = -1` nên dòng này không được tính như một vị trí
  đã xác nhận.
- Không có đủ bằng chứng thì giữ NULL; Silver không tự bịa tỉnh, quận hoặc tọa độ.
- Tọa độ tâm trong file cấu hình là điểm tham chiếu phân tích có version, không
  phải ranh giới pháp lý hay cam kết là vị trí chính xác của trụ sở hành chính.
- Quy tắc quận/huyện kế thừa project R chỉ được dùng để tương thích phân tích
  cho TP.HCM. Với tỉnh/thành khác, tên quận/huyện có sẵn được giữ nguyên.

## Data Quality

| Mã | Điều kiện |
|---|---|
| LQ00 | Thiếu `source_id`; REJECT |
| LQ01 | Không có thông tin vị trí sử dụng được |
| LQ02 | Không xác định được tỉnh/thành |
| LQ03 | Thiếu cặp tọa độ hợp lệ |
| LQ04 | Tỉnh/thành được suy luận bằng tâm gần nhất thay vì text |
| LQ05 | Tọa độ cách tâm tỉnh/thành đã map hơn 350 km: tỉnh theo text xung đột với tọa độ; khoảng cách để NULL |

## Nguồn và phiên bản

- Mô hình hành chính: `vn_province_34_effective_2025_07_01_v1`.
- Cấu hình 34 tỉnh/thành dựa trên Nghị quyết 202/2025/QH15 về sắp xếp đơn vị
  hành chính cấp tỉnh; file CSV nội bộ giữ toàn bộ alias cũ cần cho pipeline.
