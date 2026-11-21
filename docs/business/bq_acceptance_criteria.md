# Tiêu chí chấp nhận kết quả BQ1–BQ3

Tài liệu này định nghĩa khi nào kết quả của mỗi Business Question được coi là **chấp nhận được** để đưa lên dashboard và báo cáo. Mỗi tiêu chí có ngưỡng đo được và chỉ ra minh chứng: một check trong `verify_gold.py` hoặc một số liệu trong `docs/validation/`.

Số liệu hiện trạng lấy từ lần chạy dữ liệu tới batch **20261027** (pipeline chạy ngày 29/10, cùng bộ dữ liệu với tag `v1.0-bao-ve`). Riêng mục 3.2 đến 3.4 (mô hình hedonic và backtest BQ2) đã cập nhật theo dữ liệu tới batch **20261117**. Ngưỡng ở đây là đề xuất của vai B; nhóm thống nhất trong buổi họp, sửa trực tiếp file này nếu đổi ngưỡng.

Trạng thái:

- **Đạt**: đã đo và thỏa ngưỡng.
- **Đạt có điều kiện**: thỏa ngưỡng nhưng cần ghi chú giới hạn khi trình bày.
- **Chưa đạt**: đã đo, không thỏa ngưỡng; có hướng xử lý.
- **Chưa đo**: chưa có minh chứng.

## 0. Người dùng, quyết định và chỉ số đo của từng BQ

Người dùng lấy từ `problem_definition.md` (người mua) và `target_users.md` (nhân viên phân tích thị trường, môi giới).

| BQ | Người dùng | Quyết định được hỗ trợ | Chỉ số đo chính | Bảng Gold |
|---|---|---|---|---|
| BQ1 | Người mua có ngân sách cố định; môi giới tư vấn cho người mua | Với ngân sách này, chọn quận nào và chấp nhận đánh đổi gì (diện tích, số phòng, đặc điểm); tin nào nên xem trước (không bị tin khác trội hơn) | Trung vị diện tích, số phòng, giá/m², khoảng cách theo quận; tỷ lệ tin có từng đặc điểm; nhãn Pareto | `agg_budget_tradeoff`, `fact_budget_pareto` |
| BQ2 | Người mua đang xem một tin cụ thể; môi giới định giá chào bán; nhân viên phân tích | Tin này đắt, rẻ hay ngang nhóm tương đồng; có nên thương lượng hoặc kiểm tra kỹ hơn không; chênh lệch giá đến từ đặc điểm nào | Vị trí so với P25–P75 nhóm; `price_ratio`; phần dư mô hình hedonic; % chênh giá theo từng cờ đặc điểm | `fact_listing_price_assessment`, `fact_listing_price_model`, `agg_price_model_coefficient` |
| BQ3 | Người mua không đủ tiền ở khu vực mong muốn; môi giới gợi ý phương án thay thế | Nên cân nhắc quận nào khác cùng tỉnh để mua cùng diện tích với chi phí thấp hơn mà không quá xa | Chi phí ước tính ở cùng diện tích; % rẻ hơn; độ tương đồng đặc điểm; chênh lệch khoảng cách tới trung tâm | `agg_area_substitution` |

## 1. Tiêu chí chung cho cả ba BQ

| Mã | Tiêu chí | Ngưỡng | Minh chứng | Trạng thái |
|---|---|---|---|---|
| G1 | Gold kiểm định đầy đủ | `verify_gold` status PASS, không check nào FAIL | `gold_verification.json` | Đạt (108/108, dữ liệu tới batch 20261027) |
| G2 | Chỉ dùng tin đại diện, tin nghi trùng chỉ đếm một lần | Các check `dup_*`, `non_suspect_is_representative` PASS | `gold_verification.json` | Đạt |
| G3 | Kết quả tái lập được | Rebuild hai lần cho cùng nội dung bảng | `deterministic_rebuild.json` | Đạt |
| G4 | Độ chính xác của 5 cờ đặc điểm và phân loại BĐS được đo | Có precision/recall cho từng cờ trên mẫu gán nhãn ≥ 150 tin; precision mỗi cờ ≥ 0,80 | `label_accuracy.json` | Chưa đo (`labelled_rows = 0`, việc của vai C) |
| G5 | Số liệu trên dashboard khớp Gold | Mọi biểu đồ đối chiếu khớp truy vấn Trino | `superset_dashboard_check.json` | Đạt |

G4 ảnh hưởng tới cả ba BQ, vì BQ1 (Pareto), BQ2 (mô hình, `feature_count_vs_peer`) và BQ3 (độ tương đồng) đều dựa trên cờ đọc từ tiêu đề. Khi G4 chưa đo, mọi kết luận dựa trên cờ phải ghi là "theo nội dung tiêu đề".

## 2. BQ1 – Đánh đổi trong cùng ngân sách

Câu hỏi: cùng ngân sách, người mua đánh đổi vị trí, diện tích, số phòng và đặc điểm thế nào; tin nào không bị tin khác trội hơn?

| Mã | Tiêu chí | Ngưỡng | Minh chứng | Trạng thái |
|---|---|---|---|---|
| B1.1 | Tổng số tin của bảng đánh đổi khớp số tin đại diện có khung giá | Check `budget_tradeoff_sum_matches` | `gold_verification.json` | Đạt |
| B1.2 | Nhãn Pareto đúng theo định nghĩa | Check `pareto_labels_recomputed` (tính lại độc lập) | `gold_verification.json` | Đạt |
| B1.3 | Mỗi nhóm so sánh có ít nhất một tin trên frontier | Check `pareto_every_group_has_frontier` | `gold_verification.json` | Đạt |
| B1.4 | Nhóm hiển thị đủ lớn để so sánh | Dashboard chỉ hiển thị nhóm có `n_listings ≥ 5`, hoặc hiển thị `n_listings` cạnh mỗi nhóm | `gold_budget_tradeoff_summary.json`: 1.133/2.469 nhóm có ≥ 5 tin | Đạt có điều kiện: 54% nhóm có dưới 5 tin, dashboard cần lọc hoặc ghi n |
| B1.5 | Tỷ lệ frontier được trình bày tách theo việc biết số phòng | Báo cáo tỷ lệ frontier riêng cho tin biết / không biết số phòng | Truy vấn trên `fact_budget_pareto` + `fact_listing` | Đạt (đo ngày 17/11): trên toàn bộ nhóm, tin biết số phòng có 12.399/76.577 = 16,2% nằm trên frontier, tin không rõ số phòng có 8.241/43.538 = 18,9%. Chỉ tính nhóm có ≥ 5 tin: 15,4% (11.627/75.704) so với 16,7% (7.036/42.122). Tin không rõ số phòng nằm trên frontier nhiều hơn như dự đoán (do không thể bị trội về số phòng) nhưng chênh lệch nhỏ (+2,7 điểm phần trăm, +1,3 điểm khi chỉ tính nhóm ≥ 5 tin), nên frontier chưa bị thổi phồng đáng kể. Khi trình bày tỷ lệ frontier cần ghi kèm cả hai con số |

## 3. BQ2 – Vị trí giá/m² trong nhóm tương đồng

Câu hỏi: giá/m² của tin nằm ở đâu trong nhóm bất động sản tương đồng, và vì sao tin đắt hơn nhóm?

### 3.1. Benchmark mô tả (P25–P75)

| Mã | Tiêu chí | Ngưỡng | Minh chứng | Trạng thái |
|---|---|---|---|---|
| B2.1 | Nhóm tương đồng đủ lớn | Mọi nhóm công bố có ≥ 5 tin (check `benchmark_min_group_size`) | `gold_verification.json` | Đạt. Đã chạy độ nhạy với ngưỡng 10 (mục 3.1.1): kết luận không đổi |
| B2.2 | Độ phủ | Tỷ lệ tin `khong_du_du_lieu` ≤ 5% số tin đại diện | `gold_price_benchmark_summary.json`: 3.807/123.066 = 3,1% | Đạt |
| B2.3 | Vị trí giá tính đúng | Checks `assessment_position_recomputed`, `assessment_peer_values_match_benchmark` | `gold_verification.json` | Đạt |
| B2.4 | Cờ cảnh báo phải phân biệt được | `is_low_price_missing_legal` bật cho ≤ 50% tin dưới P25 | `gold_price_benchmark_summary.json`: 24.767/30.094 = 82% | Chưa đạt: cờ pháp lý chỉ đọc từ tiêu đề nên quá thưa. Không trình bày như tín hiệu rủi ro; ghi là "tiêu đề không nêu pháp lý" |

#### 3.1.1. Độ nhạy theo ngưỡng cỡ nhóm (B4)

Tính lại benchmark trong bộ nhớ với ngưỡng nhóm tối thiểu 5 (đang dùng) và 10, không ghi bảng Gold (`src/quality/benchmark_sensitivity.py`, kết quả `docs/validation/benchmark_sensitivity.json`, 123.066 tin đại diện). Kết quả với ngưỡng 5 khớp từng tin với `fact_listing_price_assessment` (0 tin lệch).

| | Ngưỡng 5 | Ngưỡng 10 |
|---|---:|---:|
| Nhóm `LOC_CAT_AREA_ROOM` | 1.348 | 896 |
| Nhóm `LOC_CAT_AREA` | 973 | 721 |
| Nhóm `LOC_CAT` | 364 | 286 |
| Tin không đủ dữ liệu (`khong_du_du_lieu`) | 3.807 (3,09%) | 4.345 (3,53%) |
| Dưới P25 | 30.094 (24,5%) | 29.964 (24,3%) |
| P25–P75 | 58.773 (47,8%) | 58.429 (47,5%) |
| Trên P75 | 30.392 (24,7%) | 30.328 (24,6%) |
| Tin xếp ở mức nhóm mịn nhất (`LOC_CAT_AREA_ROOM`) | 72.361 | 69.425 |
| Cỡ nhóm trung vị mà tin được so sánh | 223 | 228 |

Chuyển từ 5 lên 10:
- 1.981 tin (1,6%) đổi vị trí giá. Trong đó 538 tin mất nhóm tương đồng (thành `khong_du_du_lieu`) và 1.443 tin đổi vị trí trong số 118.721 tin có nhóm ở cả hai ngưỡng (1,2%).
- 4.724 tin (3,8%) bị đẩy sang mức nhóm thô hơn do nhóm mịn không đủ 10 tin.
- Độ phủ giảm nhẹ (tin thiếu dữ liệu tăng từ 3,1% lên 3,5%), vẫn đạt B2.2 (≤ 5%). Tỷ lệ tin ở từng vị trí giá gần như không đổi (chênh dưới 0,3 điểm phần trăm).

Kết luận: benchmark ổn định theo ngưỡng cỡ nhóm; giữ ngưỡng 5 vì độ phủ cao hơn và kết quả gần như không khác. Phép đo này chỉ cho thấy kết quả không nhạy với ngưỡng, không chứng minh vị trí giá phản ánh giá hợp lý (xem mục 5).

### 3.2. Mô hình hedonic (giải thích chênh lệch giá)

Mô hình: `src/gold/build_price_model.py`. Kết quả: `docs/validation/price_model_summary.json`, bảng `gold.fact_listing_price_model` và `gold.agg_price_model_coefficient`.

Phương pháp (nhóm thống nhất ngày 29/10): kế hoạch ban đầu dùng `pyspark.ml`, nhưng `pyspark.ml` cần numpy mà image Spark của nhóm (`apache/spark:3.5.9-scala2.12-java17-python3-r-ubuntu`) không có (`docker exec tlcn_spark_master python3 -c "import numpy"` → `ModuleNotFoundError`). Nhóm giữ cách cài đặt OLS bằng phương trình chuẩn và phân tích Cholesky (Spark cộng dồn X'X phân tán, giải hệ nhỏ trên driver) để không phải sửa hạ tầng chung sát ngày đóng băng code. Đây cũng là phương pháp `pyspark.ml` dùng mặc định khi số biến dưới 4.096. Hướng phát triển: thêm numpy vào image Spark và chuyển sang `pyspark.ml`.

| Mã | Tiêu chí | Ngưỡng | Minh chứng | Trạng thái |
|---|---|---|---|---|
| B2.5 | Đánh giá trên dữ liệu chưa dùng để huấn luyện | Chia train/test 80/20 cố định theo hash của `source_id`; check `price_model_split_recomputed` | `price_model_summary.json` (`settings.split`), `gold_verification.json` | Đạt: 99.044 tin huấn luyện, 24.819 tin test; tỷ lệ test 20,0% |
| B2.6 | Mô hình tốt hơn baseline trung vị nhóm | Trên tập test: MAE của mô hình < MAE baseline, cho toàn bộ và cho ít nhất 3/4 loại BĐS | `price_model_summary.json` (`overall_test`, `by_category.*.model_beats_baseline_mae`), `price_model_selection.json` (`blend_selection`) | Đạt: toàn bộ thắng (MAE 41,8 so với 45,1 triệu/m², −7,4%, khoảng tin cậy 95% của chênh lệch 3,06 đến 3,69 triệu, không chứa 0); theo loại MAE thấp hơn ở 4/4 và cả 4 loại khác biệt có ý nghĩa thống kê. Nhà phố và căn hộ cải thiện rõ (−8,5% và −7,9%); biệt thự chỉ nhỉnh hơn (−1,3%, khoảng tin cậy 0,11 đến 1,46 triệu, sát 0) và đất −4,8% (0,48 đến 1,52 triệu). Tin cấp tỉnh và cấp quận đều thắng baseline (43,4 so với 45,5 và 40,4 so với 44,8) |
| B2.7 | Mô hình giải thích được phần lớn biến thiên giá | R² trên thang log, tập test, ≥ 0,50 cho nhà phố và căn hộ | `price_model_summary.json` (`by_category.*.test_model.r2_log`) | Đạt: nhà phố 0,68; căn hộ 0,51 (sát ngưỡng). Toàn bộ 0,72 so với baseline 0,66 |
| B2.8 | Hệ số từng cờ được báo cáo kèm độ bất định | Mỗi cờ có `premium_pct` và khoảng tin cậy 95%; chỉ kết luận với cờ có `significant_at_5pct = true` | `price_model_summary.json` (`by_category.*.flags`), `agg_price_model_coefficient` | Đạt: 16/20 hệ số cờ có ý nghĩa ở mức 5%. 4 hệ số không có ý nghĩa: nhà phố – nội thất (khoảng tin cậy −1,8% đến +1,9%); biệt thự – ô tô tiếp cận (364 tin); đất – thang máy (3 tin) và đất – nội thất (4 tin). Ba hệ số sau có rất ít tin mang cờ hoặc khoảng tin cậy chứa 0 |
| B2.9 | Không có tin nào thiếu dự đoán | Check `price_model_values_valid`, `price_model_covers_scope` | `gold_verification.json` | Đạt |
| B2.11 | Mô hình phủ phần lớn tin đại diện | `listings_in_scope` ≥ 80% số tin đại diện | `price_model_summary.json` (`scope`) | Đạt: 126.394/133.480 = 94,7%. 57.361 tin chỉ có tỉnh được đưa vào với mức vị trí cấp tỉnh `prov:`, tách khỏi hệ số quận `d:`; 180 tin bị loại vì không rõ vị trí. Đánh đổi: tin cấp tỉnh kém chính xác hơn (R² log 0,67 so với 0,76 ở cấp quận) |

Kết quả trên dữ liệu tới batch 20261117, tập test 24.819 tin (cùng tập train/test và cùng baseline ở mọi bước so sánh). Giá dự đoán cuối cùng là **mô hình kết hợp**: trung bình trong không gian log của mô hình hedonic và baseline, `log(giá dự đoán) = w·log(mô hình) + (1−w)·log(baseline)`, w riêng cho từng loại BĐS. Mô hình hedonic gồm quận, nhóm diện tích, nhóm số phòng, 5 cờ, cộng ba nhóm biến mở rộng: khoảng cách tới trung tâm tỉnh/thành, tương tác quận × nhóm diện tích (<50, 50–120, ≥120 m²) và phường trong quận (thay thế cho dự án/tòa nhà, vốn không quan sát được).

Cách chọn tham số (`src/quality/price_model_selection.py`, kết quả `docs/validation/price_model_selection.json`): w (0 đến 1), mức ridge và việc tin vị trí gộp cấp tỉnh có dùng riêng baseline hay không được chọn bằng kiểm định chéo 5 phần **chỉ trên tập huấn luyện** (tiêu chí MAE ngoài mẫu; giữ mô hình thuần trừ khi lựa chọn khác tốt hơn ít nhất 0,1%). Tập test chỉ dùng một lần để đánh giá. Giá trị đã chọn nằm trong `config/price_model_blend.json` (nhà phố w = 0,7; căn hộ w = 0,8; biệt thự w = 0,4; đất w = 0,4; không loại nào dùng riêng baseline cho tin gộp cấp tỉnh) và được kiểm tra khớp với kết quả chọn bởi `verify_price_model.py` và test đơn vị. Script chọn cũng tự kiểm tra rằng huấn luyện lại mô hình thuần tái tạo đúng dự đoán trong Gold. Cấu hình được chọn lại khi dữ liệu mở rộng từ batch 27/10 sang 17/11 (trước đó căn hộ w = 0,9 có dùng baseline cho tin cấp tỉnh, biệt thự w = 0,5, đất w = 0,6); giá trị trong file cấu hình luôn lấy nguyên từ kết quả chọn, không chỉnh tay.

| | Mô hình cơ sở | Mô hình mở rộng | **Mô hình kết hợp** | Baseline trung vị nhóm |
|---|---:|---:|---:|---:|
| MAE (triệu đồng/m²) | 45,0 | 42,5 | **41,8** | 45,1 |
| MAPE | 46,1% | 42,2% | **42,1%** | 48,1% |
| Trung vị sai số % | 28,4% | 26,1% | **25,0%** | 26,2% |
| R² (thang log) | 0,68 | 0,72 | **0,72** | 0,66 |

Theo loại bất động sản (MAE triệu đồng/m²; R² thang log; chênh lệch sai số tuyệt đối so với baseline, khoảng tin cậy 95% bootstrap ghép cặp 2.000 lần):

| Loại | Số tin test | MAE kết hợp | MAE baseline | R² kết hợp | R² baseline | Baseline − kết hợp (triệu/m²), KTC 95% | Kết luận |
|---|---:|---:|---:|---:|---:|---|---|
| Nhà phố | 15.639 | 48,3 | 52,8 | 0,68 | 0,61 | 4,46 (3,99 đến 4,93) | Tốt hơn, có ý nghĩa |
| Căn hộ | 5.617 | 20,8 | 22,6 | 0,51 | 0,34 | 1,78 (1,49 đến 2,07) | Tốt hơn, có ý nghĩa |
| Biệt thự | 2.235 | 61,5 | 62,3 | 0,59 | 0,54 | 0,79 (0,11 đến 1,46) | Nhỉnh hơn, sát ngưỡng |
| Đất | 1.328 | 20,0 | 21,0 | 0,68 | 0,62 | 1,00 (0,48 đến 1,52) | Tốt hơn, có ý nghĩa |
| Toàn bộ | 24.819 | 41,8 | 45,1 | 0,72 | 0,66 | 3,34 (3,06 đến 3,69) | Tốt hơn, có ý nghĩa |

Đóng góp từng nhóm biến (so trên cùng tập test, MAE toàn bộ triệu đồng/m²): mô hình cơ sở 45,0; thêm riêng khoảng cách 44,1; thêm riêng quận × diện tích 44,1; thêm riêng phường 44,1; thêm cả ba 42,5. Khoảng cách giúp căn hộ nhiều nhất (MAE 22,8 → 21,6), phường và quận × diện tích giúp nhà phố nhiều hơn. Kết hợp với baseline giảm thêm 42,5 → 41,8, chủ yếu ở nhà phố (49,2 → 48,3), biệt thự (63,0 → 61,5) và đất (21,4 → 20,0); ở căn hộ mô hình thuần đã ngang bản kết hợp (20,82 so với 20,77). Giới hạn: chỉ 39% tin có khoảng cách (cần tọa độ) và 65% có phường, phần còn lại rơi vào mức "không rõ".

Điểm cần nói rõ khi báo cáo: (1) với biệt thự, mô hình kết hợp chỉ nhỉnh hơn baseline (MAE thấp hơn 1,3%, khoảng tin cậy của chênh lệch sát 0; trung vị sai số % 30,0% so với 29,8% là ngang nhau), nên với loại này benchmark mô tả vẫn là kết quả chính. Với đất, MAE thấp hơn 4,8% và có ý nghĩa thống kê nhưng chỉ có 1.328 tin test, trung vị sai số % (28,4% so với 28,7%) gần như không đổi. (2) Trung vị sai số % của nhà phố (24,6% so với 25,5%) và căn hộ (24,4% so với 26,3%) thấp hơn baseline. (3) Các nhóm biến và loại hình kết hợp được cân nhắc trên cùng tập test nên con số trên có thể hơi lạc quan; phép kiểm chứng độc lập là đánh giá trên các tin ở batch mới hơn khi Gold được dựng lại.

Hệ số cờ ở nhà phố (phần mô hình hedonic; mức chênh giá/m², khoảng tin cậy 95%): mặt tiền +53,7% (+52,1% đến +55,3%), thang máy +31,2%, ô tô tiếp cận +8,1%, nội thất 0,0% (−1,8% đến +1,9%, không có ý nghĩa), pháp lý −15,3%. Hệ số pháp lý âm ở cả 4 loại và nội thất âm ở căn hộ (−9,4%) là dấu hiệu **gây nhiễu**: tin rẻ, ở khu vực xa hoặc căn hộ bình dân hay nhấn mạnh "sổ hồng", "full nội thất" trong tiêu đề. Không diễn giải các hệ số này là "có sổ làm giảm giá".

Nếu một loại bất động sản không tốt hơn baseline có ý nghĩa thống kê, vẫn báo cáo trung thực theo phương án trong bảng rủi ro của kế hoạch: giữ benchmark mô tả làm kết quả chính, phân tích nguyên nhân (thiếu biến dự án, độ rộng hẻm, pháp lý thực tế; cờ chưa kiểm chứng độ chính xác).

### 3.3. Backtest (tuần 2)

| Mã | Tiêu chí | Ngưỡng | Minh chứng | Trạng thái |
|---|---|---|---|---|
| B2.10 | Vị trí giá có ý nghĩa với hành vi thị trường | So sánh tỷ lệ giảm giá và số ngày quan sát giữa nhóm `tren_p75` (hoặc `residual_pct` lớn) và phần còn lại; kiểm định khác biệt p < 0,05 | `bq2_backtest.json` | Đạt một phần: H1 (vị trí so với P75) đạt cả khi kiểm soát nguồn (p = 0,0025); H2 (phần dư mô hình) đạt trên toàn bộ tin nhưng chưa đạt trên riêng tập test (p = 0,40); H3 không. Xem mục 3.4 |

### 3.4. Kết quả backtest

Nguồn: `docs/validation/bq2_backtest.json`, script `src/quality/bq2_backtest.py`. Đối tượng: 64.381 tin đại diện của 3 nguồn crawl, quan sát ít nhất 2 batch, từ 22/09 đến 17/11/2026 (62.472 tin có vị trí giá lúc mới đăng, 61.521 tin có phần dư mô hình). Vị trí giá tính theo **giá lúc mới đăng** (phiên bản đầu trong `listing_history`), vì giá hiện tại đã bao gồm lần giảm giá. Kết quả vẫn mang tính quan sát: tin không được gán ngẫu nhiên vào các mức giá.

| Vị trí lúc mới đăng | Số tin | Tỷ lệ có giảm giá |
|---|---:|---:|
| Dưới P25 | 16.634 | 2,22% |
| P25–P75 | 31.010 | 3,52% |
| Trên P75 | 14.828 | 4,48% |

- **H1 (trên P75 giảm giá nhiều hơn): đạt.** Nhóm trên P75 giảm giá 4,48% so với 3,07% của các tin còn lại (gấp 1,46 lần, khoảng tin cậy 95% của chênh lệch 1,05 đến 1,79 điểm phần trăm, p < 0,001). Kiểm soát theo nguồn bằng CMH: tỷ số odds 1,16, p = 0,0025, vẫn đạt nếu hiệu chỉnh Bonferroni cho 3 kiểm định chính (ngưỡng 0,017). Mức chênh tuyệt đối nhỏ, khoảng 1,4 điểm phần trăm.
- **H2 (phần dư mô hình cao giảm giá nhiều hơn): đạt một phần.** Trên toàn bộ tin, tỷ lệ tăng từ 2,63% (Q1) lên 4,46% (Q5); kiểm soát nguồn bằng CMH cho p = 0,017, sát ngưỡng Bonferroni 0,017 nên chỉ đạt ở mức 5% khi không hiệu chỉnh. Phần dư của tin huấn luyện nằm trong mẫu của mô hình, nên kiểm định đúng hơn là riêng tập test (12.354 tin): tỷ lệ 4,53% so với 3,14%, nhưng sau khi kiểm soát nguồn p = 0,40 (chưa đạt). Báo cáo là "chưa khẳng định".
- **H3 (tin đắt được quan sát lâu hơn): không.** Trung vị mọi nhóm là 3 ngày; khác biệt có ý nghĩa thống kê nhưng gần bằng 0 (xác suất trội 0,49) và ngược chiều.
- **Kết quả nhạy với lượng dữ liệu:** với dữ liệu tới 23/10, kiểm định CMH của H1 cho p = 0,08 (chưa đạt); thêm batch 27/10 thì p = 0,006; với dữ liệu tới 17/11 thì p = 0,0025. Kết quả H1 ổn định từ 27/10 trở đi, nhưng cần tiếp tục theo dõi khi có thêm batch.
- **Độ lệch khi dùng giá hiện tại được xác nhận:** xếp theo giá hiện tại thì nhóm trên P75 chỉ còn 3,45% giảm giá, thấp hơn nhóm P25–P75 (3,62%), thay vì 4,48%.
- **Chỉ số "biến mất" chưa dùng được:** khoảng 87% số tin vắng ở 2 batch gần nhất, quá cao để là tin đã bán; nhiều khả năng do crawler chỉ lấy một số trang đầu.

Giới hạn: khoảng 5 tuần dữ liệu, 1.376 tin từng giảm giá; kết quả là tương quan, không phải quan hệ nhân quả.

## 4. BQ3 – Khu vực thay thế cùng tỉnh

Câu hỏi: khu vực nào cùng tỉnh có nhóm bất động sản tương tự với chi phí thấp hơn cho cùng diện tích?

| Mã | Tiêu chí | Ngưỡng | Minh chứng | Trạng thái |
|---|---|---|---|---|
| B3.1 | Rẻ hơn ở cùng diện tích mục tiêu | Check `substitution_cheaper_at_target_area`, `substitution_target_area_is_origin_median` | `gold_verification.json` | Đạt |
| B3.2 | Cùng tỉnh | Check `substitution_same_province` | `gold_verification.json` | Đạt |
| B3.3 | Hồ sơ đặc điểm tương tự | Độ tương đồng ≥ 0,8 và mỗi cờ lệch ≤ 0,3 (check `substitution_similarity_and_flag_cap`) | `gold_verification.json` | Đạt có điều kiện: tương đồng về *tần suất tiêu đề nêu đặc điểm*, phụ thuộc G4 |
| B3.4 | Hai phía đủ dữ liệu | Cả nhóm gốc và nhóm thay thế có ≥ 5 tin | Dựng từ nhóm công bố của `agg_peer_group_benchmark` | Đạt. Đề xuất hiển thị n hai phía trên dashboard |
| B3.5 | Gợi ý thay thế phải gần | Gợi ý xếp hạng 1 có `distance_diff_km` ≤ 10 km, hoặc xếp hạng có tính khoảng cách | `agg_area_substitution`, check `substitution_distance_cap` | Đạt: từ 26/10 phương án thay thế phải không xa trung tâm tỉnh hơn quá 10 km; gợi ý hạng 1 xa hơn tối đa 9,995 km. Đổi lại còn 2.588/3.384 cặp (loại 532 cặp quá xa, 264 cặp không biết khoảng cách); 443 nhóm gốc có phương án |
| B3.6 | Chênh lệch tổng giá được giải thích | Ghi rõ số cặp mà tổng giá trung vị phía thay thế không thấp hơn | `gold_area_substitution_summary.json`: 130/2.588 cặp | Đạt có điều kiện: so sánh dựa trên chi phí ở cùng diện tích, không dựa trên tổng giá trung vị |

## 5. Giới hạn diễn giải áp dụng cho mọi BQ

Các giới hạn sau phải xuất hiện trong báo cáo và dashboard, không được trình bày như kết luận:

- Giá là **giá chào bán**, không phải giá giao dịch.
- "Current" là bản ghi mới nhất **đã quan sát**, không xác nhận tin còn hiển thị.
- Cờ đặc điểm chỉ đọc từ tiêu đề; FALSE nghĩa là tiêu đề không nêu.
- Vị trí P25–P75 không có nghĩa là giá hợp lý; hệ số mô hình hedonic là tương quan, không phải tác động nhân quả.
- Tin trùng giữa các nguồn chỉ là nghi trùng theo luật.
- Dữ liệu historical và snapshot có thời điểm đăng khác nhau; chưa kiểm soát yếu tố thời gian trong nhóm tương đồng.
