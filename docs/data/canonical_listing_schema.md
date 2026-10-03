# Canonical Listing Schema

Đây là Data Contract chính thức của lớp Silver. Bronze giữ dữ liệu gần với nguồn; Silver chuẩn hóa 6 nguồn historical và 3 nguồn crawl mới về cùng schema 27 cột tương thích với project R `phantichnhadathcm`.

Không duy trì đồng thời một canonical schema thứ hai với tên cột khác. Các trường kỹ thuật, DQ, lineage và feature phục vụ BQ được lưu ngoài view 27 cột.

## 1. Schema nghiệp vụ 27 cột

Thứ tự cột dưới đây là thứ tự chuẩn khi ghi `silver_listings_current_27` và khi UNION với historical.

| # | Field | Kiểu Silver | Bắt buộc | Quy tắc |
|---:|---|---|---|---|
| 1 | source | string | Có | Tên nguồn viết thường, ví dụ `guland` |
| 2 | source_group | string | Không | Nhóm sản phẩm/giao dịch gốc của nguồn |
| 3 | source_id | string | Có | `source + "_" + ad_id` |
| 4 | ad_id | string | Có | ID gốc tại website; luôn cast sang string |
| 5 | title | string | Có | Tiêu đề đã chuẩn Unicode, bỏ control character và gom khoảng trắng |
| 6 | price | double | Không | Giá chào bán/cho thuê theo VND; thỏa thuận hoặc không xác định thì NULL |
| 7 | price_str | string | Không | Chuỗi giá gốc để hiển thị và audit |
| 8 | area | double | Không | Diện tích m²; không hợp lệ hoặc <= 0 thì NULL |
| 9 | rooms | integer | Không | Số phòng ngủ; thiếu thì NULL, không thay bằng 0 |
| 10 | address | string | Không | Địa chỉ/mô tả vị trí đã làm sạch |
| 11 | ward | string | Không | Phường/xã canonical nếu ánh xạ được |
| 12 | district_id | string | Không | Mã quận/huyện canonical |
| 13 | district_name | string | Không | Tên quận/huyện canonical |
| 14 | category_id | string | Không | Mã loại bất động sản canonical |
| 15 | category_name | string | Không | Tên loại bất động sản canonical |
| 16 | lat | double | Không | Vĩ độ hợp lệ trong `[-90, 90]` |
| 17 | lon | double | Không | Kinh độ hợp lệ trong `[-180, 180]` |
| 18 | image | string | Không | URL ảnh đại diện đầu tiên |
| 19 | ad_url | string | Không | URL chi tiết tin đăng |
| 20 | source_url | string | Không | URL phục vụ truy vết; ưu tiên URL tin, fallback URL crawl |
| 21 | posted_at | timestamp | Không | Thời điểm đăng đáng tin cậy; không chắc chắn thì NULL |
| 22 | scraped_at | timestamp | Có | Thời điểm crawler quan sát listing |
| 23 | page_fetched | integer | Không | Trang crawl phát hiện listing |
| 24 | price_m | double | Không | `price / 1_000_000` khi price hợp lệ |
| 25 | price_per_m2 | double | Không | `price / area` khi price và area hợp lệ |
| 26 | has_coord | boolean | Có | TRUE khi có đủ cặp lat/lon hợp lệ |
| 27 | is_rent | boolean | Có | Bán = FALSE, thuê = TRUE |

Giá trong hệ thống là giá đăng/giá chào, không phải giá giao dịch thực tế.

## 2. Metadata kỹ thuật của Silver Observation

`silver_listing_observation` chứa 27 cột trên và các cột kỹ thuật sau:

| Field | Kiểu | Ý nghĩa |
|---|---|---|
| batch_id | string | Batch Bronze; snapshot dùng `YYYYMMDD` |
| snapshot_date | date | Ngày quan sát suy ra từ batch hợp lệ |
| bronze_ingested_at | timestamp | Thời điểm nạp vào Bronze |
| bronze_source_file | string | Tên file Bronze đã tiếp nhận |
| bronze_path | string | Đường dẫn lineage về Bronze |
| record_hash | string | SHA-256 của các trường nghiệp vụ đã chuẩn hóa |
| dq_status | string | `PASS`, `WARN` hoặc `REJECT` |
| dq_reasons | array<string> | Danh sách mã DQ áp dụng cho record |
| completeness_score | double | Điểm đầy đủ trong `[0, 1]` |

`record_hash` không chứa `batch_id`, `snapshot_date`, `page_fetched`, `scraped_at` hoặc thời điểm ingestion.

## 3. Grain và khóa

- Grain của `silver_listing_observation`: một listing của một source trong một batch, sau khi loại duplicate kỹ thuật trong batch.
- Khóa dedup cùng batch: `(source, ad_id, batch_id)`.
- Khóa nghiệp vụ ổn định: `source_id`.
- Nếu trùng cùng batch, ưu tiên `completeness_score` cao hơn, sau đó `scraped_at` mới hơn, cuối cùng `page_fetched` nhỏ hơn.
- Cùng `source_id` ở các batch khác nhau là repeated observation, không được xóa như duplicate.
- Grain của `silver_listings_current_27`: một dòng mới nhất cho mỗi `source_id`, chỉ gồm đúng 27 cột nghiệp vụ.
- Listing không còn xuất hiện không đồng nghĩa đã bán.

## 4. Quy tắc NULL

- Thiếu `source`, `ad_id` hoặc `title`: REJECT và chuyển quarantine.
- Giá thỏa thuận/không xác định: `price = NULL`, giữ `price_str`, record ở trạng thái WARN.
- `area <= 0` hoặc không parse được: `area = NULL` và WARN.
- Không có bằng chứng rõ về phòng ngủ: `rooms = NULL`, không thay bằng 0.
- Price hoặc area không hợp lệ: `price_m`/`price_per_m2` tương ứng là NULL.
- Nguồn có cung cấp tọa độ nhưng cặp tọa độ thiếu, `(0,0)` hoặc ngoài phạm vi: `lat = lon = NULL`, `has_coord = FALSE` và WARN.
- Nguồn có cung cấp thời gian đăng nhưng `posted_at` rỗng, không tin cậy hoặc lớn hơn `scraped_at`: `posted_at = NULL` và WARN.
- Field được mapping đánh dấu `UNAVAILABLE` cho toàn bộ nguồn không tạo WARN trên từng record; vẫn để NULL/FALSE và trừ vào `completeness_score`.
- Category/location chưa map: giữ record, để canonical field NULL và WARN; không dùng chuỗi `Không rõ` thay cho NULL.
- Outlier không bị xóa tự động tại Silver; giữ record và gắn DQ flag.

Chi tiết mã lỗi nằm trong `docs/data/data_quality_rules.md`.

## 5. Feature phục vụ Business Questions

Các feature sau không thuộc 27 cột và được ghi ở `silver_listing_feature`:

- `model_category`
- `distance_to_center_km`
- `center_type`
- `location_precision`
- `title_has_legal`
- `title_has_furnished`
- `title_has_frontage`
- `title_has_elevator`
- `title_has_car_access`
- `rooms_known`
- `legal_info_known`
- `feature_completeness_score`
- `feature_extracted_from`

Cờ `title_has_* = FALSE` chỉ có nghĩa văn bản không đề cập, không khẳng định bất động sản không có đặc điểm đó. Khi cần phân biệt phải dùng thêm cờ `*_known` hoặc để NULL.
