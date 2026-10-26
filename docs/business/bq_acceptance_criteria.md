# Tiêu chí chấp nhận kết quả BQ1–BQ3

Tài liệu này định nghĩa khi nào kết quả của mỗi Business Question được coi là **chấp nhận được** để đưa lên dashboard và báo cáo. Mỗi tiêu chí có ngưỡng đo được và chỉ ra minh chứng: một check trong `verify_gold.py` hoặc một số liệu trong `docs/validation/`.

Số liệu hiện trạng lấy từ lần chạy dữ liệu tới batch **20261023**. Ngưỡng ở đây là đề xuất của vai B; nhóm thống nhất trong buổi họp, sửa trực tiếp file này nếu đổi ngưỡng.

Trạng thái:

- **Đạt**: đã đo và thỏa ngưỡng.
- **Đạt có điều kiện**: thỏa ngưỡng nhưng cần ghi chú giới hạn khi trình bày.
- **Chưa đạt**: đã đo, không thỏa ngưỡng; có hướng xử lý.
- **Chưa đo**: chưa có minh chứng.

## 1. Tiêu chí chung cho cả ba BQ

| Mã | Tiêu chí | Ngưỡng | Minh chứng | Trạng thái |
|---|---|---|---|---|
| G1 | Gold kiểm định đầy đủ | `verify_gold` status PASS, không check nào FAIL | `gold_verification.json` | Đạt (107/107 sau khi thêm mô hình, chạy 26/10) |
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
| B1.4 | Nhóm hiển thị đủ lớn để so sánh | Dashboard chỉ hiển thị nhóm có `n_listings ≥ 5`, hoặc hiển thị `n_listings` cạnh mỗi nhóm | `gold_budget_tradeoff_summary.json`: 1.131/2.464 nhóm có ≥ 5 tin | Đạt có điều kiện: 54% nhóm có dưới 5 tin, dashboard cần lọc hoặc ghi n |
| B1.5 | Tỷ lệ frontier được trình bày tách theo việc biết số phòng | Báo cáo tỷ lệ frontier riêng cho tin biết / không biết số phòng | Truy vấn trên `fact_budget_pareto` + `fact_listing` | Chưa đo. Tin không rõ số phòng không thể bị trội về số phòng nên dễ nằm trên frontier |

## 3. BQ2 – Vị trí giá/m² trong nhóm tương đồng

Câu hỏi: giá/m² của tin nằm ở đâu trong nhóm bất động sản tương đồng, và vì sao tin đắt hơn nhóm?

### 3.1. Benchmark mô tả (P25–P75)

| Mã | Tiêu chí | Ngưỡng | Minh chứng | Trạng thái |
|---|---|---|---|---|
| B2.1 | Nhóm tương đồng đủ lớn | Mọi nhóm công bố có ≥ 5 tin (check `benchmark_min_group_size`) | `gold_verification.json` | Đạt. Đề xuất chạy thêm độ nhạy với ngưỡng 10 |
| B2.2 | Độ phủ | Tỷ lệ tin `khong_du_du_lieu` ≤ 5% số tin đại diện | `gold_price_benchmark_summary.json`: 3.716/121.338 = 3,1% | Đạt |
| B2.3 | Vị trí giá tính đúng | Checks `assessment_position_recomputed`, `assessment_peer_values_match_benchmark` | `gold_verification.json` | Đạt |
| B2.4 | Cờ cảnh báo phải phân biệt được | `is_low_price_missing_legal` bật cho ≤ 50% tin dưới P25 | `gold_price_benchmark_summary.json`: 24.441/29.679 = 82% | Chưa đạt: cờ pháp lý chỉ đọc từ tiêu đề nên quá thưa. Không trình bày như tín hiệu rủi ro; ghi là "tiêu đề không nêu pháp lý" |

### 3.2. Mô hình hedonic (giải thích chênh lệch giá)

Mô hình: `src/gold/build_price_model.py`. Kết quả: `docs/validation/price_model_summary.json`, bảng `gold.fact_listing_price_model` và `gold.agg_price_model_coefficient`.

| Mã | Tiêu chí | Ngưỡng | Minh chứng | Trạng thái |
|---|---|---|---|---|
| B2.5 | Đánh giá trên dữ liệu chưa dùng để huấn luyện | Chia train/test 80/20 cố định theo hash của `source_id`; check `price_model_split_recomputed` | `price_model_summary.json` (`settings.split`), `gold_verification.json` | Đạt: 53.765 tin huấn luyện, 13.527 tin test; tỷ lệ test 20,1% |
| B2.6 | Mô hình tốt hơn baseline trung vị nhóm | Trên tập test: MAE của mô hình < MAE baseline, cho toàn bộ và cho ít nhất 3/4 loại BĐS | `price_model_summary.json` (`overall_test`, `by_category.*.model_beats_baseline_mae`) | Chưa đạt: toàn bộ thắng (MAE 43,5 so với 44,9 triệu/m², −3,0%); theo loại chỉ thắng 2/4 (nhà phố, căn hộ), thua ở biệt thự và đất |
| B2.7 | Mô hình giải thích được phần lớn biến thiên giá | R² trên thang log, tập test, ≥ 0,50 cho nhà phố và căn hộ | `price_model_summary.json` (`by_category.*.test_model.r2_log`) | Đạt một phần: nhà phố 0,69; căn hộ 0,42 (dưới ngưỡng). Toàn bộ 0,73 so với baseline 0,68 |
| B2.8 | Hệ số từng cờ được báo cáo kèm độ bất định | Mỗi cờ có `premium_pct` và khoảng tin cậy 95%; chỉ kết luận với cờ có `significant_at_5pct = true` | `price_model_summary.json` (`by_category.*.flags`), `agg_price_model_coefficient` | Đạt: 16/20 hệ số cờ có ý nghĩa ở mức 5%. 4 hệ số không có ý nghĩa: biệt thự – nội thất (80 tin); đất – ô tô tiếp cận, thang máy (11 tin), nội thất (3 tin) |
| B2.9 | Không có tin nào thiếu dự đoán | Check `price_model_values_valid`, `price_model_covers_scope` | `gold_verification.json` | Đạt |
| B2.11 | Mô hình phủ phần lớn tin đại diện | `listings_in_scope` ≥ 80% số tin đại diện | `price_model_summary.json` (`scope`) | Chưa đạt: 68.666/121.338 = 56,6%. 51.456 tin bị loại vì chỉ có tỉnh, không có quận. Hướng xử lý: đưa tin chỉ có tỉnh vào mô hình với mức vị trí cấp tỉnh |

Kết quả lần chạy 26/10 (`hedonic_ols_v1`), tập test 13.527 tin:

| | Mô hình | Baseline trung vị nhóm |
|---|---:|---:|
| MAE (triệu đồng/m²) | 43,5 | 44,9 |
| MAPE | 38,5% | 42,5% |
| Trung vị sai số % | 24,8% | 23,6% |
| R² (thang log) | 0,73 | 0,68 |

Mô hình giảm các sai số lớn (MAE, MAPE thấp hơn) nhưng với tin điển hình thì baseline vẫn sát hơn một chút (trung vị sai số % thấp hơn). Lý do: baseline dùng nhóm quận × diện tích × số phòng nên bắt được tương tác giữa các yếu tố, còn mô hình cộng dồn từng yếu tố riêng lẻ.

Hệ số cờ ở nhà phố (mức chênh giá/m², khoảng tin cậy 95%): mặt tiền +48,8% (+47,1% đến +50,4%), thang máy +31,3%, nội thất +4,9%, ô tô tiếp cận +3,2%, pháp lý −27,4%. Hệ số pháp lý âm ở cả 4 loại và nội thất âm ở căn hộ (−19,4%) là dấu hiệu **gây nhiễu**: tin rẻ, ở khu vực xa hoặc căn hộ bình dân hay nhấn mạnh "sổ hồng", "full nội thất" trong tiêu đề. Không diễn giải các hệ số này là "có sổ làm giảm giá".

Nếu B2.6 không đạt (mô hình không tốt hơn baseline), vẫn báo cáo trung thực theo phương án trong bảng rủi ro của kế hoạch: giữ benchmark mô tả làm kết quả chính, phân tích nguyên nhân (thiếu biến dự án, độ rộng hẻm, pháp lý thực tế; cờ chưa kiểm chứng độ chính xác).

### 3.3. Backtest (tuần 2)

| Mã | Tiêu chí | Ngưỡng | Minh chứng | Trạng thái |
|---|---|---|---|---|
| B2.10 | Vị trí giá có ý nghĩa với hành vi thị trường | So sánh tỷ lệ giảm giá và số ngày quan sát giữa nhóm `tren_p75` (hoặc `residual_pct` lớn) và phần còn lại; kiểm định khác biệt p < 0,05 | `bq2_backtest.json` | Chưa đo. Số batch còn ít nên ghi là kết quả sơ bộ |

## 4. BQ3 – Khu vực thay thế cùng tỉnh

Câu hỏi: khu vực nào cùng tỉnh có nhóm bất động sản tương tự với chi phí thấp hơn cho cùng diện tích?

| Mã | Tiêu chí | Ngưỡng | Minh chứng | Trạng thái |
|---|---|---|---|---|
| B3.1 | Rẻ hơn ở cùng diện tích mục tiêu | Check `substitution_cheaper_at_target_area`, `substitution_target_area_is_origin_median` | `gold_verification.json` | Đạt |
| B3.2 | Cùng tỉnh | Check `substitution_same_province` | `gold_verification.json` | Đạt |
| B3.3 | Hồ sơ đặc điểm tương tự | Độ tương đồng ≥ 0,8 và mỗi cờ lệch ≤ 0,3 (check `substitution_similarity_and_flag_cap`) | `gold_verification.json` | Đạt có điều kiện: tương đồng về *tần suất tiêu đề nêu đặc điểm*, phụ thuộc G4 |
| B3.4 | Hai phía đủ dữ liệu | Cả nhóm gốc và nhóm thay thế có ≥ 5 tin | Dựng từ nhóm công bố của `agg_peer_group_benchmark` | Đạt. Đề xuất hiển thị n hai phía trên dashboard |
| B3.5 | Gợi ý thay thế phải gần | Gợi ý xếp hạng 1 có `distance_diff_km` ≤ 10 km, hoặc xếp hạng có tính khoảng cách | `agg_area_substitution` | Chưa đạt: xếp hạng hiện chỉ theo mức rẻ hơn, ví dụ trong dữ liệu báo cáo ngày 09/10: Bình Tân → Củ Chi rẻ hơn 77,6% nhưng xa trung tâm hơn khoảng 15 km. Việc của vai B tuần 2 |
| B3.6 | Chênh lệch tổng giá được giải thích | Ghi rõ số cặp mà tổng giá trung vị phía thay thế không thấp hơn | `gold_area_substitution_summary.json`: 159/3.387 cặp | Đạt có điều kiện: so sánh dựa trên chi phí ở cùng diện tích, không dựa trên tổng giá trung vị |

## 5. Giới hạn diễn giải áp dụng cho mọi BQ

Các giới hạn sau phải xuất hiện trong báo cáo và dashboard, không được trình bày như kết luận:

- Giá là **giá chào bán**, không phải giá giao dịch.
- "Current" là bản ghi mới nhất **đã quan sát**, không xác nhận tin còn hiển thị.
- Cờ đặc điểm chỉ đọc từ tiêu đề; FALSE nghĩa là tiêu đề không nêu.
- Vị trí P25–P75 không có nghĩa là giá hợp lý; hệ số mô hình hedonic là tương quan, không phải tác động nhân quả.
- Tin trùng giữa các nguồn chỉ là nghi trùng theo luật.
- Dữ liệu historical và snapshot có thời điểm đăng khác nhau; chưa kiểm soát yếu tố thời gian trong nhóm tương đồng.
