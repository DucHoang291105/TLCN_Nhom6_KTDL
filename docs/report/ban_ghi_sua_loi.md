# Bản ghi sửa lỗi – đợt rà soát ngày 09/10/2026

Phạm vi: các lỗi ảnh hưởng tới kết quả Silver/Gold và mức kiểm chứng mà báo cáo nêu ra. Đợt này không làm lại kiến trúc và không bổ sung ML. Số liệu "trước" lấy từ `docs/validation/review_before_fix.json`; số liệu "sau" là lần rebuild cuối.

## 1. Lỗi đã sửa và file thay đổi

| # | Lỗi (đã tái hiện) | Nguyên nhân | Cách sửa | File |
|---|---|---|---|---|
| 1 | "Nhà 5 tầng…", "Bán nhà 3 tầng…" được xếp thành căn hộ | Core gán mặc định căn hộ cho mọi tin batdongsan không nhận diện được loại hình (cho rằng endpoint `/ban-can-ho-chung-cu` chỉ có căn hộ). Thực tế 24.225/42.000 dòng rơi vào mặc định này, trong khi `category_id` và URL của tin cho thấy nhiều loại hình khác | Dùng taxonomy của site: `category_id` (dự phòng: tiền tố URL) cho batdongsan, `product_slug` cho nhadatvui. Không đủ bằng chứng thì để NULL, không gán mặc định | `src/silver/build_listing_core.py` |
| 1 | "Căn hộ Đất Xanh" được xếp thành đất | Khớp chuỗi con trên nhiều trường ghép chung, ưu tiên "đất" | Khớp theo từng trường, theo ranh giới từ; xét loại hình cụ thể trước "đất" | `build_listing_core.py` |
| 1 | "chính chủ CC", "nhà phân lô" được xếp thành căn hộ, đất | Title fallback coi viết tắt đa nghĩa là bằng chứng | Chia tín hiệu mạnh/yếu: tín hiệu yếu chỉ dùng khi không có tín hiệu mạnh và mọi tín hiệu yếu cùng chỉ một loại hình | `src/silver/feature_rules.py` |
| 1 | Nhãn nguồn sai vẫn được giữ, không ai biết | Không phân biệt nguồn gốc nhãn | Thêm `category_evidence` (SOURCE_STRUCTURED / SOURCE_LABEL / ENDPOINT_CONTEXT / NONE). Thêm `category_title_conflict` khi title có tín hiệu mạnh thuộc **họ** loại hình khác (căn hộ / đất / nhà): chỉ gắn cờ, không ghi đè. Nhãn chỉ dựa vào endpoint (guland) nhường cho tín hiệu mạnh của title | `build_listing_core.py`, `build_listing_core_spark.py`, `feature_rules.py`, `build_listing_feature.py` |
| 2 | "Bán đất MT 5m … trong hẻm" có `frontage = TRUE` | Không phân biệt chiều ngang mặt tiền với "nằm mặt tiền đường" | `title_has_frontage` chỉ tính mặt tiền đường; bỏ qua "MT/mặt tiền + số đo" và "mặt tiền hẻm/ngõ". Thêm `title_negated_features`. Tài liệu ghi rõ: FALSE gộp "không nêu" và "nói không có"; `car_access` gộp hẻm/ngõ ô tô, ô tô vào, đỗ cửa; completeness không đo độ chính xác | `feature_rules.py`, `docs/data/canonical_listing_schema.md` |
| 3 | "Đường Điện Biên Phủ, Q. Bình Thạnh" được map thành tỉnh Điện Biên, cách tâm khoảng 1.241 km | Quét tên tỉnh trên text ghép chung, đã bỏ dấu ("Vĩnh" → "Vinh" → Nghệ An; "Hòa Bình" là tên đường → Phú Thọ) | Chọn tỉnh theo thứ tự bằng chứng: thành phần cuối của địa chỉ → quận TP.HCM → tên có tiền tố hành chính → tên đứng trần (so có dấu, bỏ qua khi đứng sau từ chỉ đường/phường, phải khớp tọa độ). Tâm gần nhất chỉ là suy luận (LQ04) | `src/silver/build_location.py`, `config/vietnam_province_centers.csv` |
| 3 | Bỏ mất tỉnh có cấu trúc của NhaDatVui | Core không dùng `province_name`; 92% bản ghi có address trống | Core ghép `ward_name` và `province_name` vào `address` | `build_listing_core.py` |
| 3 | Vị trí xung đột vẫn đi vào KPI (926/982 dòng LQ05 nằm trong fact) | Gold không mang theo trạng thái DQ vị trí | LQ05 → `distance_to_center_km` = NULL ở Silver; trong Gold `location_key = -1`, thêm `is_location_conflict`, `is_province_inferred_from_coordinates`, `location_dq_status` | `build_location.py`, `src/gold/build_fact_listing.py` |
| 4 | BQ3 nói "ngân sách thấp hơn" nhưng 216 cặp có median tổng giá không thấp hơn | Chỉ so median giá/m² | So chi phí trên **cùng diện tích mục tiêu** (median diện tích của nhóm gốc). `typical_budget_saving` đổi thành `median_total_price_diff` (chỉ để đọc) và `estimated_saving_at_target_area` | `src/gold/build_area_substitution.py`, `gold_rules.py` |
| 4 | Nhóm 100% mặt tiền và nhóm 0% mặt tiền vẫn đạt độ tương đồng 0,8 | Chỉ dùng trung bình chênh lệch | Thêm trần: không cờ nào lệch quá 0,3; ghi rõ similarity so tỷ lệ title nêu đặc điểm | như trên |
| 5 | Pareto coi số phòng không rõ là 0 | Quy ước đơn giản hóa | Số phòng không rõ không so được với số phòng đã biết. Giữ Pareto trong từng quận (có chủ đích), khoảng cách không là tiêu chí. Sửa diễn giải | `gold_rules.py`, `build_budget_tradeoff.py` |
| 6 | Nhãn `hop_ly` hàm ý giá hợp lý | Cách đặt tên | Đổi thành `duoi_p25 / p25_p75 / tren_p75`; ghi rõ BQ2 là benchmark mô tả, `feature_count_vs_peer` chỉ là chênh lệch số cờ | `build_price_benchmark.py` |
| 7 | Nhóm nghi trùng nối bắc cầu tới 30 tin; 180 nhóm chứa nhiều tin cùng nguồn | Union-find trên mọi cặp | Chỉ giữ cặp tốt nhất hai chiều; nhóm có tin cùng nguồn đánh dấu `AMBIGUOUS` và không gộp; thêm phân tích độ nhạy theo ngưỡng; xuất mẫu 40 cặp và kết quả rà soát thủ công | `build_fact_listing.py`, `gold_rules.py` |
| 8 | Rebuild chỉ so số dòng; một số check không tính lại kết quả | Phạm vi kiểm chứng hẹp | Fingerprint nội dung theo khóa (`table_fingerprints.py`). verify tính lại: tập khóa hai chiều, band theo giá trị, vị trí giá, nhãn Pareto (so từng cặp), điều kiện BQ3, FK `date_key` của bảng đổi giá | `src/common/*`, `scripts/check_deterministic_rebuild.ps1`, `verify_silver.py`, `verify_gold.py` |
| 9 | Báo cáo khẳng định khóa dimension không đổi | Diễn đạt quá mức | Ghi rõ khóa chỉ ổn định khi rebuild đồng bộ dimension và fact | `build_dimensions.py`, báo cáo |
| 11 | Các diễn giải quá mức trong báo cáo (5.1–5.3, 6.4, current, dedup, "Gold chỉ đọc Silver") | — | Viết lại báo cáo (`src/report/report_body.py`); thống nhất BQ với `docs/business` | `src/report/*`, `docs/business/*`, README |

## 2. Regression test và kiểm chứng đã chạy

- `python -m pytest -q`: **170/170 PASS** (trước đợt sửa: 122). Test mới bám vào từng lỗi: các ví dụ phân loại trong nhận xét, CC/lô, tín hiệu yếu không thống nhất, nhãn endpoint với title, cùng họ loại hình không tính là mâu thuẫn, "MT 5m"/"mặt tiền hẻm", tên đường/phường/người trùng tên tỉnh, Vĩnh/Vinh, tọa độ mâu thuẫn, LQ05 → khoảng cách NULL, taxonomy batdongsan/nhadatvui, phản ví dụ ngân sách 5,10 tỷ / 7,11 tỷ, nhóm 100%/0% mặt tiền, nối bắc cầu dedup, số phòng không rõ trong Pareto.
- `verify_silver.py`: **22/22 PASS**. `verify_gold.py`: **81/81 PASS**, trong đó tính lại được: 0 dòng sai band, 0 nhãn vị trí giá sai, 0 nhãn Pareto sai, 0 khóa fact thiếu hoặc thừa.
- Rebuild toàn bộ Silver + Gold 2 lần trên cùng Bronze: **26/26 bảng** khớp số dòng, số khóa grain và hash nội dung (bỏ `feature_built_at`). `listing_dq_quarantine` không có khóa duy nhất do chứa các dòng CSV vỡ giống hệt nhau.
- Báo cáo Word được sinh lại, xuất PDF và kiểm tra từng trang (39 trang).

## 3. Số liệu trước/sau

| Chỉ số | Trước | Sau | Nguyên nhân |
|---|---:|---:|---|
| Căn hộ (tin current) | 36.731 | 26.959 | Bỏ mặc định căn hộ của batdongsan |
| Nhà phố | 65.029 | 72.582 | Như trên |
| Biệt thự/liền kề/shophouse | 7.428 | 10.041 | `category_id` 325, 575 |
| Không rõ loại hình | 332 | 363 | Viết tắt đa nghĩa không còn đủ để phân loại |
| Nhãn mâu thuẫn với title | – | 4.621 | Cờ mới (chỉ tính khi khác họ loại hình) |
| LQ05 (Silver) | 982 | 16 | Tên đường/phường không còn bị đọc thành tỉnh |
| LQ05 trong fact | 926 | 16 | Nay nhận `location_key = -1` |
| LQ04 (tỉnh suy từ tâm gần nhất) | 27.772 | 0 | Tỉnh có cấu trúc nhadatvui, quận TP.HCM |
| LQ02 (không rõ tỉnh) | 12.818 | 163 | Như trên |
| Nhóm nghi trùng / nhóm lớn nhất | 606 / 30 | 850 / 5 | Cặp tốt nhất hai chiều; nhóm loại hình mới |
| Tin đại diện | 107.635 | 107.733 | Nhóm mơ hồ (3) không gộp |
| Dưới P25 / P25–P75 / trên P75 | 26.053 / 51.099 / 27.071 | 26.441 / 51.586 / 26.611 | Nhóm tương đồng theo loại hình mới |
| Tin không bị trội (Pareto) | 19.670 | 19.709 | Phòng không rõ không còn tính là 0 |
| Cặp BQ3 | 4.666 | 3.485 | Trần chênh lệch từng cờ, nhóm mới |

Độ nhạy của luật nghi trùng (chặt / mặc định / lỏng): 408 / 878 / 2.694 tin bị bớt khỏi bảng tổng hợp. Median giá/m² theo loại hình giữa "mọi tin" và "tin đại diện" lệch dưới 1%.

## 4. Phần chưa kiểm chứng được và còn thiếu

- **Độ chính xác của phân loại và cờ đặc điểm** chưa được đo: cần một mẫu gán nhãn thủ công (khoảng vài trăm tin, phân tầng theo nguồn). Hiện chỉ có các ca test chọn lọc.
- **4.621 tin mâu thuẫn** mới được gắn cờ, chưa được nhóm rà xem nhãn nguồn hay title đúng.
- **Nhãn nguồn sai mà title chỉ có viết tắt** vẫn giữ nhãn sai (ví dụ `homedy_3207890` "Bán CH Moonlight Park View" mang nhãn Nhà phố). Guland có 26.746 tin chỉ dựa vào endpoint.
- **Mẫu 40 cặp nghi trùng** được đánh giá sơ bộ qua title/giá/diện tích (15 trùng rõ, 15 có khả năng, 10 không chắc – đều là căn trong cùng dự án). Chưa đối chiếu ảnh hoặc trang tin, và chưa có tập nhãn để đo precision/recall.
- **Địa giới**: chỉ kiểm tra bằng khoảng cách 350 km tới tâm tỉnh; chưa có ranh giới GIS. 16 tin LQ05 còn lại chưa được rà nguyên nhân.
- **Dữ liệu và môi trường**: dữ liệu historical (`data/incoming/historical/`) không nằm trong Git. Máy khác cần chép dữ liệu này và (nếu không pull được image MinIO) dùng `docker-compose.override.yml` cục bộ như hướng dẫn trong README.
- Các mục ngoài phạm vi đợt này (hedonic, K-Means, GIS chi tiết, incremental MERGE, Airflow, dashboard): giữ trạng thái **chưa làm**, có trong Chương 8 của báo cáo.
