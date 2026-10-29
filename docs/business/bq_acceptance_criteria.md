# Tiêu chí chấp nhận kết quả BQ1–BQ3

Tài liệu này định nghĩa khi nào kết quả của mỗi Business Question được coi là **chấp nhận được** để đưa lên dashboard và báo cáo. Mỗi tiêu chí có ngưỡng đo được và chỉ ra minh chứng: một check trong `verify_gold.py` hoặc một số liệu trong `docs/validation/`.

Số liệu hiện trạng lấy từ lần chạy dữ liệu tới batch **20261027** (pipeline chạy ngày 29/10, cùng bộ dữ liệu với tag `v1.0-bao-ve`). Ngưỡng ở đây là đề xuất của vai B; nhóm thống nhất trong buổi họp, sửa trực tiếp file này nếu đổi ngưỡng.

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
| B1.5 | Tỷ lệ frontier được trình bày tách theo việc biết số phòng | Báo cáo tỷ lệ frontier riêng cho tin biết / không biết số phòng | Truy vấn trên `fact_budget_pareto` + `fact_listing` | Chưa đo. Tin không rõ số phòng không thể bị trội về số phòng nên dễ nằm trên frontier |

## 3. BQ2 – Vị trí giá/m² trong nhóm tương đồng

Câu hỏi: giá/m² của tin nằm ở đâu trong nhóm bất động sản tương đồng, và vì sao tin đắt hơn nhóm?

### 3.1. Benchmark mô tả (P25–P75)

| Mã | Tiêu chí | Ngưỡng | Minh chứng | Trạng thái |
|---|---|---|---|---|
| B2.1 | Nhóm tương đồng đủ lớn | Mọi nhóm công bố có ≥ 5 tin (check `benchmark_min_group_size`) | `gold_verification.json` | Đạt. Đề xuất chạy thêm độ nhạy với ngưỡng 10 |
| B2.2 | Độ phủ | Tỷ lệ tin `khong_du_du_lieu` ≤ 5% số tin đại diện | `gold_price_benchmark_summary.json`: 3.807/123.066 = 3,1% | Đạt |
| B2.3 | Vị trí giá tính đúng | Checks `assessment_position_recomputed`, `assessment_peer_values_match_benchmark` | `gold_verification.json` | Đạt |
| B2.4 | Cờ cảnh báo phải phân biệt được | `is_low_price_missing_legal` bật cho ≤ 50% tin dưới P25 | `gold_price_benchmark_summary.json`: 24.767/30.094 = 82% | Chưa đạt: cờ pháp lý chỉ đọc từ tiêu đề nên quá thưa. Không trình bày như tín hiệu rủi ro; ghi là "tiêu đề không nêu pháp lý" |

### 3.2. Mô hình hedonic (giải thích chênh lệch giá)

Mô hình: `src/gold/build_price_model.py`. Kết quả: `docs/validation/price_model_summary.json`, bảng `gold.fact_listing_price_model` và `gold.agg_price_model_coefficient`.

Phương pháp (nhóm thống nhất ngày 29/10): kế hoạch ban đầu dùng `pyspark.ml`, nhưng `pyspark.ml` cần numpy mà image Spark của nhóm (`apache/spark:3.5.9-scala2.12-java17-python3-r-ubuntu`) không có (`docker exec tlcn_spark_master python3 -c "import numpy"` → `ModuleNotFoundError`). Nhóm giữ cách cài đặt OLS bằng phương trình chuẩn và phân tích Cholesky (Spark cộng dồn X'X phân tán, giải hệ nhỏ trên driver) để không phải sửa hạ tầng chung sát ngày đóng băng code. Đây cũng là phương pháp `pyspark.ml` dùng mặc định khi số biến dưới 4.096. Hướng phát triển: thêm numpy vào image Spark và chuyển sang `pyspark.ml`.

| Mã | Tiêu chí | Ngưỡng | Minh chứng | Trạng thái |
|---|---|---|---|---|
| B2.5 | Đánh giá trên dữ liệu chưa dùng để huấn luyện | Chia train/test 80/20 cố định theo hash của `source_id`; check `price_model_split_recomputed` | `price_model_summary.json` (`settings.split`), `gold_verification.json` | Đạt: 54.013 tin huấn luyện, 13.598 tin test; tỷ lệ test 20,1% |
| B2.6 | Mô hình tốt hơn baseline trung vị nhóm | Trên tập test: MAE của mô hình < MAE baseline, cho toàn bộ và cho ít nhất 3/4 loại BĐS | `price_model_summary.json` (`overall_test`, `by_category.*.model_beats_baseline_mae`) | Chưa đạt: toàn bộ thắng (MAE 43,4 so với 44,8 triệu/m², −3,0%); theo loại chỉ thắng 2/4 (nhà phố, căn hộ), thua ở biệt thự và đất |
| B2.7 | Mô hình giải thích được phần lớn biến thiên giá | R² trên thang log, tập test, ≥ 0,50 cho nhà phố và căn hộ | `price_model_summary.json` (`by_category.*.test_model.r2_log`) | Đạt một phần: nhà phố 0,69; căn hộ 0,42 (dưới ngưỡng). Toàn bộ 0,73 so với baseline 0,68 |
| B2.8 | Hệ số từng cờ được báo cáo kèm độ bất định | Mỗi cờ có `premium_pct` và khoảng tin cậy 95%; chỉ kết luận với cờ có `significant_at_5pct = true` | `price_model_summary.json` (`by_category.*.flags`), `agg_price_model_coefficient` | Đạt: 15/20 hệ số cờ có ý nghĩa ở mức 5%. 5 hệ số không có ý nghĩa: biệt thự – ô tô tiếp cận (131 tin), nội thất (79 tin); đất – ô tô tiếp cận (377 tin), thang máy (11 tin), nội thất (3 tin) |
| B2.9 | Không có tin nào thiếu dự đoán | Check `price_model_values_valid`, `price_model_covers_scope` | `gold_verification.json` | Đạt |
| B2.11 | Mô hình phủ phần lớn tin đại diện | `listings_in_scope` ≥ 80% số tin đại diện | `price_model_summary.json` (`scope`) | Chưa đạt: 68.999/123.066 = 56,1%. 52.833 tin bị loại vì chỉ có tỉnh, không có quận. Hướng xử lý: đưa tin chỉ có tỉnh vào mô hình với mức vị trí cấp tỉnh |

Kết quả trên dữ liệu tới batch 20261027 (`hedonic_ols_v1`), tập test 13.598 tin:

| | Mô hình | Baseline trung vị nhóm |
|---|---:|---:|
| MAE (triệu đồng/m²) | 43,4 | 44,8 |
| MAPE | 38,5% | 42,5% |
| Trung vị sai số % | 24,8% | 23,6% |
| R² (thang log) | 0,73 | 0,68 |

Mô hình giảm các sai số lớn (MAE, MAPE thấp hơn) nhưng với tin điển hình thì baseline vẫn sát hơn một chút (trung vị sai số % thấp hơn). Lý do: baseline dùng nhóm quận × diện tích × số phòng nên bắt được tương tác giữa các yếu tố, còn mô hình cộng dồn từng yếu tố riêng lẻ.

Hệ số cờ ở nhà phố (mức chênh giá/m², khoảng tin cậy 95%): mặt tiền +48,6% (+46,9% đến +50,2%), thang máy +31,4%, nội thất +4,8%, ô tô tiếp cận +3,1%, pháp lý −27,3%. Hệ số pháp lý âm ở cả 4 loại và nội thất âm ở căn hộ (−19,7%) là dấu hiệu **gây nhiễu**: tin rẻ, ở khu vực xa hoặc căn hộ bình dân hay nhấn mạnh "sổ hồng", "full nội thất" trong tiêu đề. Không diễn giải các hệ số này là "có sổ làm giảm giá".

Nếu B2.6 không đạt (mô hình không tốt hơn baseline), vẫn báo cáo trung thực theo phương án trong bảng rủi ro của kế hoạch: giữ benchmark mô tả làm kết quả chính, phân tích nguyên nhân (thiếu biến dự án, độ rộng hẻm, pháp lý thực tế; cờ chưa kiểm chứng độ chính xác).

### 3.3. Backtest (tuần 2)

| Mã | Tiêu chí | Ngưỡng | Minh chứng | Trạng thái |
|---|---|---|---|---|
| B2.10 | Vị trí giá có ý nghĩa với hành vi thị trường | So sánh tỷ lệ giảm giá và số ngày quan sát giữa nhóm `tren_p75` (hoặc `residual_pct` lớn) và phần còn lại; kiểm định khác biệt p < 0,05 | `bq2_backtest.json` | Đạt một phần (sơ bộ): H1 (vị trí so với P75) đạt cả khi kiểm soát nguồn; H2 (phần dư mô hình) chưa đạt. Xem mục 3.4 |

### 3.4. Kết quả backtest (sơ bộ)

Nguồn: `docs/validation/bq2_backtest.json`, script `src/quality/bq2_backtest.py`. Đối tượng: 57.939 tin đại diện của 3 nguồn crawl, quan sát ít nhất 2 batch, từ 22/09 đến 27/10/2026. Vị trí giá tính theo **giá lúc mới đăng** (phiên bản đầu trong `listing_history`), vì giá hiện tại đã bao gồm lần giảm giá.

| Vị trí lúc mới đăng | Số tin | Tỷ lệ có giảm giá |
|---|---:|---:|
| Dưới P25 | 15.089 | 1,52% |
| P25–P75 | 28.146 | 2,49% |
| Trên P75 | 13.235 | 3,26% |

- **H1 (trên P75 giảm giá nhiều hơn): đạt, ở mức sơ bộ.** Nhóm trên P75 giảm giá 3,26% so với 2,15% của các tin còn lại (gấp 1,52 lần, p < 0,001). Kiểm soát theo nguồn bằng CMH: tỷ số odds 1,18, p = 0,006. Kết quả vẫn đạt nếu hiệu chỉnh Bonferroni cho 3 kiểm định chính (ngưỡng 0,017). Mức chênh tuyệt đối nhỏ, khoảng 1,1 điểm phần trăm.
- **H2 (phần dư mô hình cao giảm giá nhiều hơn): chưa khẳng định.** Tỷ lệ tăng từ 0,88% (Q1) lên 1,68% (Q5), nhưng kiểm soát nguồn thì p = 0,38; xét riêng tập test cũng không có ý nghĩa (p = 0,32).
- **H3 (tin đắt được quan sát lâu hơn): không.** Trung vị mọi nhóm là 3 ngày; khác biệt có ý nghĩa thống kê nhưng gần bằng 0 (xác suất trội 0,49) và ngược chiều.
- **Kết quả còn nhạy với lượng dữ liệu:** với dữ liệu tới 23/10, kiểm định CMH của H1 cho p = 0,08 (chưa đạt); thêm batch 27/10 thì p = 0,006. Cần thêm batch để khẳng định.
- **Độ lệch khi dùng giá hiện tại được xác nhận:** xếp theo giá hiện tại thì nhóm trên P75 chỉ còn 2,41% giảm giá, thay vì 3,26%.
- **Chỉ số "biến mất" chưa dùng được:** khoảng 84% số tin vắng ở 2 batch gần nhất, quá cao để là tin đã bán; nhiều khả năng do crawler chỉ lấy một số trang đầu.

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
