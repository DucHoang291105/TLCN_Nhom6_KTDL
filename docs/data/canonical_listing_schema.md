# Canonical Listing Schema

Đây là schema chuẩn dùng cho Silver Core. Bronze vẫn giữ nguyên toàn bộ dữ liệu gốc của từng nguồn.

| Field | Kiểu dự kiến | Bắt buộc | Nguồn/Rule | Ý nghĩa |
|---|---|---|---|---|
| listing_id | string | Có | source_name + source_listing_id | Khóa ổn định của listing |
| source_name | string | Có | source | Tên nguồn dữ liệu |
| source_listing_id | string | Có | source_id | ID tin tại nguồn |
| ad_id | string | Không | ad_id | ID gốc nếu có |
| listing_url | string | Không | ad_url | URL tin đăng |
| source_url | string | Không | source_url | URL nguồn |
| title | string | Có | title | Tiêu đề tin |
| description | string | Không | nguồn mới nếu có | Mô tả tin đăng |
| transaction_type | string | Có | is_rent | sale / rent |
| property_type | string | Có | category_name | Loại BĐS đã chuẩn hóa |
| address | string | Không | address | Địa chỉ |
| ward_name | string | Không | ward | Phường/xã |
| district_name | string | Không | district_name | Quận/huyện |
| price | double | Có | price | Giá chào bán/cho thuê |
| area | double | Có | area | Diện tích m2 |
| price_per_m2 | double | Không | price_per_m2 hoặc tính lại | Giá/m2 |
| rooms | integer | Không | rooms | Số phòng nếu có |
| latitude | double | Không | lat | Vĩ độ |
| longitude | double | Không | lon | Kinh độ |
| has_coord | boolean | Có | has_coord | Có tọa độ hay không |
| coordinate_source | string | Không | derived | original / geocoded / centroid / missing |
| published_at | timestamp | Không | posted_at | Thời gian đăng tin |
| observed_at | timestamp | Có | scraped_at | Thời điểm hệ thống quan sát tin |
| ingested_at | timestamp | Có | pipeline | Thời điểm nạp vào Lakehouse |
| batch_id | string | Có | pipeline | Mã lần nạp |
| dq_status | string | Có | DQ pipeline | PASS / WARN / REJECT |

## Mapping từ historical schema hiện tại

- `source` -> `source_name`
- `source_id` -> `source_listing_id`
- `ad_id` -> `ad_id`
- `ad_url` -> `listing_url`
- `source_url` -> `source_url`
- `title` -> `title`
- `is_rent` -> `transaction_type`
- `category_name` -> `property_type`
- `address` -> `address`
- `ward` -> `ward_name`
- `district_name` -> `district_name`
- `price` -> `price`
- `area` -> `area`
- `price_per_m2` -> `price_per_m2`
- `rooms` -> `rooms`
- `lat` -> `latitude`
- `lon` -> `longitude`
- `has_coord` -> `has_coord`
- `posted_at` -> `published_at`
- `scraped_at` -> `observed_at`

## Các cột chỉ giữ ở Bronze

Các trường sau vẫn được giữ nguyên ở Bronze nhưng chưa cần đưa vào Silver Core:

- source_group
- price_str
- district_id
- category_id
- image
- page_fetched
- price_m

## Ghi chú

`description` vẫn có trong schema Silver để dùng cho dữ liệu mới, nhưng không phải trường bắt buộc vì dữ liệu historical hiện tại gần như không có trường mô tả.

Giá trong hệ thống là giá đăng/giá chào bán, không phải giá giao dịch thực tế.
