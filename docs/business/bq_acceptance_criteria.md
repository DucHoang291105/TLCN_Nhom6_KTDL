# Tiêu chí chấp nhận kết quả BQ1–BQ3

Tài liệu này định nghĩa khi nào kết quả của mỗi Business Question được coi là **chấp nhận được** để đưa lên dashboard và báo cáo. Mỗi tiêu chí có ngưỡng đo được và chỉ ra minh chứng: một check trong `verify_gold.py` hoặc một số liệu trong `docs/validation/`.

Số liệu hiện trạng lấy từ lần chạy dữ liệu tới batch **20261117** (Gold 138/138 check đạt). Một số bảng đếm ở các mục BQ1 và BQ3 có thể còn ghi số của lần chạy tới batch 20261027; khi trích vào báo cáo cần đối chiếu lại với file JSON tương ứng. Ngưỡng ở đây là đề xuất của vai B; nhóm thống nhất trong buổi họp, sửa trực tiếp file này nếu đổi ngưỡng.

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
| G1 | Gold kiểm định đầy đủ | `verify_gold` status PASS, không check nào FAIL | `gold_verification.json` | Đạt (138/138, dữ liệu tới batch 20261117) |
| G2 | Chỉ dùng tin đại diện, tin nghi trùng chỉ đếm một lần | Các check `dup_*`, `non_suspect_is_representative` PASS | `gold_verification.json` | Đạt |
| G3 | Kết quả tái lập được | Rebuild hai lần cho cùng nội dung bảng | `deterministic_rebuild.json` | Đạt |
| G4 | Độ chính xác của 5 cờ đặc điểm và phân loại BĐS được đo | Có precision/recall cho từng cờ trên mẫu gán nhãn ≥ 150 tin; precision mỗi cờ ≥ 0,80 | `label_accuracy.json`, `label_accuracy_rules_compare.json` | Đạt sơ bộ (`status = PARTIAL`): mẫu 198 tin (22 tin × 9 nguồn). Trên dự đoán trước bộ quy tắc v5, precision cả 5 cờ từ 0,95 đến 1,00; recall từ 0,67 (pháp lý) đến 1,00; phân loại BĐS đúng 84,9%. Sau khi sửa quy tắc (v5) trên phần dev (122 tin), phần test giữ riêng 76 tin: precision 1,00 cả 5 cờ, recall pháp lý 1,00, mặt tiền 1,00, ô tô 1,00, nội thất 0,86, phân loại đúng 89,5%. Chưa chốt: nhãn gán sơ bộ bằng AI, chỉ có **một lượt rà soát** (hai thành viên chia nhau 198 tin, mỗi tin một người), sửa 6/1.188 ô (0,5%), còn 4 tin tranh cãi. Lượt rà soát thứ hai và đo độ nhất quán (kappa) không thực hiện do hết thời gian. Số ca dương tính trên test ít (thang máy 2, nội thất 7, ô tô 11, pháp lý 15, mặt tiền 21), nên các giá trị 1,00 chỉ có nghĩa trên số ca này |
| G5 | Số liệu trên dashboard khớp Gold | Mọi biểu đồ đối chiếu khớp truy vấn Trino | `superset_dashboard_check.json` | Đạt |

G4 ảnh hưởng tới cả ba BQ, vì BQ1 (Pareto), BQ2 (mô hình, `feature_count_vs_peer`) và BQ3 (độ tương đồng) đều dựa trên cờ đọc từ tiêu đề. Cách đọc kết quả đo:

- **Khi cờ bật thì gần như luôn đúng** (precision 0,95 đến 1,00 ở cả 5 cờ). Cờ bỏ sót nhiều hơn: trước v5, pháp lý bỏ sót 12/36 tin thật (recall 0,67), mặt tiền 6/47, ô tô 4/26. Điều này khớp với cách diễn giải "FALSE = tiêu đề không nêu", không phải "không có".
- **Số đo sau v5 chỉ tin được ở phần test giữ riêng (76 tin).** Quy tắc v5 được sửa sau khi xem lỗi trên phần dev, nên số trên cả 198 tin sau v5 (ví dụ recall pháp lý, mặt tiền, ô tô đều 1,00) là lạc quan và không dùng làm kết luận. 76 tin cũng là mẫu nhỏ: một lỗi đổi recall khoảng 1 đến 3 điểm phần trăm.
- **Thang máy chỉ có 3 tin thật** nên 1,00 chưa đủ để kết luận. Loại hình `khong_ro` (5 tin thật) chưa nhận ra được tin nào, `biet_thu` (9 tin thật) recall 0,78.
- **Quy trình nhãn là giới hạn:** nhãn gán sơ bộ bằng AI không xem dự đoán hệ thống, hai thành viên rà soát và sửa 6/1.188 ô (0,5%, đều ở cột loại hình). Tỷ lệ sửa thấp có thể do người rà soát bị bản sơ bộ dẫn dắt, nên chưa thể coi là chuẩn tuyệt đối. Cần chốt 4 tin tranh cãi (L101, L108, L130, L172) và một lượt rà soát độc lập thật sự để `status` thành `COMPLETE`.

Khi G4 chưa `COMPLETE`, mọi kết luận dựa trên cờ ghi là "theo nội dung tiêu đề, độ chính xác đo sơ bộ trên 198 tin".

## 2. BQ1 – Đánh đổi trong cùng ngân sách

Câu hỏi: cùng ngân sách, người mua đánh đổi vị trí, diện tích, số phòng và đặc điểm thế nào; tin nào không bị tin khác trội hơn?

| Mã | Tiêu chí | Ngưỡng | Minh chứng | Trạng thái |
|---|---|---|---|---|
| B1.1 | Tổng số tin của bảng đánh đổi khớp số tin đại diện có khung giá | Check `budget_tradeoff_sum_matches` | `gold_verification.json` | Đạt |
| B1.2 | Nhãn Pareto đúng theo định nghĩa | Check `pareto_labels_recomputed` (tính lại độc lập) | `gold_verification.json` | Đạt |
| B1.3 | Mỗi nhóm so sánh có ít nhất một tin trên frontier | Check `pareto_every_group_has_frontier` | `gold_verification.json` | Đạt |
| B1.4 | Nhóm hiển thị đủ lớn để so sánh | Dashboard chỉ hiển thị nhóm có `n_listings ≥ 5`, hoặc hiển thị `n_listings` cạnh mỗi nhóm | `gold_budget_tradeoff_summary.json`: 1.212/2.652 nhóm có ≥ 5 tin | Đạt có điều kiện: 54,3% nhóm có dưới 5 tin, dashboard cần lọc hoặc ghi n |
| B1.5 | Tỷ lệ frontier được trình bày tách theo việc biết số phòng | Báo cáo tỷ lệ frontier riêng cho tin biết / không biết số phòng | Truy vấn trên `fact_budget_pareto` + `fact_listing`; `gold_budget_tradeoff_summary.json` (`frontier_share_by_rooms_known`) | Đạt (dữ liệu tới 17/11): trên toàn bộ nhóm, tin biết số phòng có 13.752/83.749 = 16,4% nằm trên frontier, tin không rõ số phòng có 9.401/46.250 = 20,3%. Chỉ tính nhóm có ≥ 5 tin: 15,6% (12.926/82.817) so với 18,0% (8.021/44.658). Tin không rõ số phòng nằm trên frontier nhiều hơn như dự đoán (do không thể bị trội về số phòng) nhưng chênh lệch nhỏ (+3,9 điểm phần trăm, +2,4 điểm khi chỉ tính nhóm ≥ 5 tin), nên frontier chưa bị thổi phồng đáng kể. Khi trình bày tỷ lệ frontier cần ghi kèm cả hai con số |

## 3. BQ2 – Vị trí giá/m² trong nhóm tương đồng

Câu hỏi: giá/m² của tin nằm ở đâu trong nhóm bất động sản tương đồng, và vì sao tin đắt hơn nhóm?

### 3.1. Benchmark mô tả (P25–P75)

| Mã | Tiêu chí | Ngưỡng | Minh chứng | Trạng thái |
|---|---|---|---|---|
| B2.1 | Nhóm tương đồng đủ lớn | Mọi nhóm công bố có ≥ 5 tin (check `benchmark_min_group_size`) | `gold_verification.json` | Đạt. Đã chạy độ nhạy với ngưỡng 10 (mục 3.1.1): kết luận không đổi |
| B2.2 | Độ phủ | Tỷ lệ tin `khong_du_du_lieu` ≤ 5% số tin đại diện | `gold_price_benchmark_summary.json`: 4.365/133.480 = 3,3% | Đạt |
| B2.3 | Vị trí giá tính đúng | Checks `assessment_position_recomputed`, `assessment_peer_values_match_benchmark` | `gold_verification.json` | Đạt |
| B2.4 | Cờ cảnh báo phải phân biệt được | `is_low_price_missing_legal` bật cho ≤ 50% tin dưới P25 | `gold_price_benchmark_summary.json` (khối `legal_not_stated_by_position`, dữ liệu tới 17/11): tiêu đề không nêu pháp lý ở nhóm dưới P25 là 24.993/32.556 = 76,8%; nhóm P25–P75 84,1%; nhóm trên P75 88,7%; hai nhóm còn lại gộp 85,7% (chênh −8,9 điểm phần trăm) | Chưa đạt, và không đạt được bằng cách sửa quy tắc. Cờ không phân biệt được tin giá thấp, thậm chí ngược chiều: tin dưới P25 ít bị "không nêu pháp lý" hơn phần còn lại, và tỷ lệ tăng dần theo mức giá. Cờ chỉ phản ánh việc người đăng không ghi pháp lý ở tiêu đề (khoảng 16% tin nêu, 21.906/133.480; mẫu gán nhãn cho 18%, 36/198), không phải rủi ro pháp lý. Đã kiểm tra theo loại bất động sản (khối `legal_not_stated_by_category`): chiều ngược còn trong cả 5 loại, từ −1,5 điểm phần trăm (biệt thự) đến −14,9 (căn hộ; nhà phố −8,5, đất −4,7, phòng trọ và thương mại −2,5), và chênh lệch chuẩn hóa theo cơ cấu loại là −8,9 điểm, gần như bằng mức chung, nên cơ cấu loại không giải thích được chiều ngược. Chưa kiểm tra theo nguồn hoặc khu vực, nên chưa biết nguyên nhân. Quy tắc v5 đã tăng recall pháp lý (0,67 → 1,00 trên phần test giữ riêng) nhưng không đổi kết luận. Không trình bày như tín hiệu rủi ro; ghi là "tiêu đề không nêu pháp lý". Đề xuất thay tiêu chí bằng so sánh nhóm dưới P25 với nhóm còn lại, chờ nhóm xác nhận khi họp |

#### 3.1.1. Độ nhạy theo ngưỡng cỡ nhóm (B4)

Tính lại benchmark trong bộ nhớ với ngưỡng nhóm tối thiểu 5 (đang dùng) và 10, không ghi bảng Gold (`src/quality/benchmark_sensitivity.py`, kết quả `docs/validation/benchmark_sensitivity.json`, 133.480 tin đại diện, dữ liệu tới 17/11). Kết quả với ngưỡng 5 khớp từng tin với `fact_listing_price_assessment` (0 tin lệch).

| | Ngưỡng 5 | Ngưỡng 10 |
|---|---:|---:|
| Nhóm `LOC_CAT_AREA_ROOM` | 1.437 | 979 |
| Nhóm `LOC_CAT_AREA` | 1.050 | 788 |
| Nhóm `LOC_CAT` | 389 | 308 |
| Tin không đủ dữ liệu (`khong_du_du_lieu`) | 4.365 (3,27%) | 4.915 (3,68%) |
| Dưới P25 | 32.556 (24,4%) | 32.445 (24,3%) |
| P25–P75 | 63.658 (47,7%) | 63.253 (47,4%) |
| Trên P75 | 32.901 (24,6%) | 32.867 (24,6%) |
| Tin xếp ở mức nhóm mịn nhất (`LOC_CAT_AREA_ROOM`) | 79.364 | 76.365 |
| Cỡ nhóm trung vị mà tin được so sánh | 225 | 233 |

Chuyển từ 5 lên 10:
- 2.082 tin (1,6%) đổi vị trí giá. Trong đó 550 tin mất nhóm tương đồng (thành `khong_du_du_lieu`) và 1.532 tin đổi vị trí trong số 128.565 tin có nhóm ở cả hai ngưỡng (1,2%).
- 4.943 tin (3,7%) bị đẩy sang mức nhóm thô hơn do nhóm mịn không đủ 10 tin.
- Độ phủ giảm nhẹ (tin thiếu dữ liệu tăng từ 3,3% lên 3,7%), vẫn đạt B2.2 (≤ 5%). Tỷ lệ tin ở từng vị trí giá gần như không đổi (chênh không quá 0,3 điểm phần trăm).

Kết luận: benchmark ổn định theo ngưỡng cỡ nhóm; giữ ngưỡng 5 vì độ phủ cao hơn và kết quả gần như không khác. Phép đo này chỉ cho thấy kết quả không nhạy với ngưỡng, không chứng minh vị trí giá phản ánh giá hợp lý (xem mục 5).

### 3.2. Mô hình hedonic (giải thích chênh lệch giá)

Mô hình: `src/gold/build_price_model.py`. Kết quả: `docs/validation/price_model_summary.json`, bảng `gold.fact_listing_price_model` và `gold.agg_price_model_coefficient`.

Phương pháp (nhóm thống nhất ngày 29/10): kế hoạch ban đầu dùng `pyspark.ml`, nhưng `pyspark.ml` cần numpy mà image Spark của nhóm (`apache/spark:3.5.9-scala2.12-java17-python3-r-ubuntu`) không có (`docker exec tlcn_spark_master python3 -c "import numpy"` → `ModuleNotFoundError`). Nhóm giữ cách cài đặt OLS bằng phương trình chuẩn và phân tích Cholesky (Spark cộng dồn X'X phân tán, giải hệ nhỏ trên driver) để không phải sửa hạ tầng chung sát ngày đóng băng code. Đây cũng là phương pháp `pyspark.ml` dùng mặc định khi số biến dưới 4.096. Hướng phát triển: thêm numpy vào image Spark và chuyển sang `pyspark.ml`.

| Mã | Tiêu chí | Ngưỡng | Minh chứng | Trạng thái |
|---|---|---|---|---|
| B2.5 | Đánh giá trên dữ liệu chưa dùng để huấn luyện | Chia train/test 80/20 cố định theo hash của `source_id`; check `price_model_split_recomputed` | `price_model_summary.json` (`settings.split`), `gold_verification.json` | Đạt: 99.044 tin huấn luyện, 24.819 tin test; tỷ lệ test 20,0% |
| B2.6 | Mô hình tốt hơn baseline trung vị nhóm | Trên tập test: MAE của mô hình < MAE baseline, cho toàn bộ và cho ít nhất 3/4 loại BĐS | `price_model_summary.json` (`overall_test`, `by_category.*.model_beats_baseline_mae`), `price_model_selection.json` (`blend_selection`) | Đạt: toàn bộ thắng (MAE 41,8 so với 45,1 triệu/m², −7,4%, tính trên 24.819 tin test nằm trong khoảng cắt ngoại lai; khi tính cả 512 tin ngoài khoảng, trên 25.331 tin, MAE là 47,2 so với 50,7 triệu/m², −6,8%, vẫn thắng baseline (`overall_test_including_outside_trim`), khoảng tin cậy 95% của chênh lệch 3,06 đến 3,69 triệu, không chứa 0); theo loại MAE thấp hơn ở 4/4 và cả 4 loại khác biệt có ý nghĩa thống kê. Nhà phố và căn hộ cải thiện rõ (−8,5% và −7,9%); biệt thự chỉ nhỉnh hơn (−1,3%, khoảng tin cậy 0,11 đến 1,46 triệu, sát 0) và đất −4,8% (0,48 đến 1,52 triệu). Tin cấp tỉnh và cấp quận đều thắng baseline (43,4 so với 45,5 và 40,4 so với 44,8) |
| B2.7 | Mô hình giải thích được phần lớn biến thiên giá | R² trên thang log, tập test, ≥ 0,50 cho nhà phố và căn hộ | `price_model_summary.json` (`by_category.*.test_model.r2_log`) | Đạt: nhà phố 0,68; căn hộ 0,51 (sát ngưỡng). Toàn bộ 0,72 so với baseline 0,66 |
| B2.8 | Hệ số từng cờ được báo cáo kèm độ bất định | Mỗi cờ có `premium_pct` và khoảng tin cậy 95%; chỉ kết luận với cờ có `significant_at_5pct = true` | `price_model_summary.json` (`by_category.*.flags`), `agg_price_model_coefficient` | Đạt: 16/20 hệ số cờ có ý nghĩa ở mức 5%. Sai số chuẩn tính từ phần dư của mô hình thuần (không phải dự đoán đã trộn với baseline) bằng công thức OLS có dùng ma trận ridge; đây là xấp xỉ, chưa kiểm chứng riêng cho ridge. 4 hệ số không có ý nghĩa: nhà phố – nội thất (khoảng tin cậy −1,8% đến +1,9%); biệt thự – ô tô tiếp cận (364 tin); đất – thang máy (3 tin) và đất – nội thất (4 tin). Ba hệ số sau có rất ít tin mang cờ hoặc khoảng tin cậy chứa 0 |
| B2.9 | Không có tin nào thiếu dự đoán | Check `price_model_values_valid`, `price_model_covers_scope` | `gold_verification.json` | Đạt |
| B2.11 | Mô hình phủ phần lớn tin đại diện | `listings_in_scope` ≥ 80% số tin đại diện | `price_model_summary.json` (`scope`) | Đạt: 126.394/133.480 = 94,7%. 57.361 tin chỉ có tỉnh được đưa vào với mức vị trí cấp tỉnh `prov:`, tách khỏi hệ số quận `d:`; 180 tin bị loại vì không rõ vị trí. Đánh đổi: tin cấp tỉnh kém chính xác hơn (R² log 0,67 so với 0,76 ở cấp quận) |

Kết quả trên dữ liệu tới batch 20261117, tập test 24.819 tin (cùng tập train/test và cùng baseline ở mọi bước so sánh). Giá dự đoán cuối cùng là **mô hình kết hợp**: trung bình trong không gian log của mô hình hedonic và baseline, `log(giá dự đoán) = w·log(mô hình) + (1−w)·log(baseline)`, w riêng cho từng loại BĐS. Mô hình hedonic gồm quận, nhóm diện tích, nhóm số phòng, 5 cờ, cộng ba nhóm biến mở rộng: khoảng cách tới trung tâm tỉnh/thành, tương tác quận × nhóm diện tích (<50, 50–120, ≥120 m²) và phường trong quận (thay thế cho dự án/tòa nhà, vốn không quan sát được).

Cách chọn tham số (`src/quality/price_model_selection.py`, kết quả `docs/validation/price_model_selection.json`): w (0 đến 1), mức ridge và việc tin vị trí gộp cấp tỉnh có dùng riêng baseline hay không được chọn bằng kiểm định chéo 5 phần **chỉ trên tập huấn luyện** (tiêu chí MAE ngoài mẫu; giữ mô hình thuần trừ khi lựa chọn khác tốt hơn ít nhất 0,1%). Tập test chỉ dùng một lần để đánh giá. Giá trị đã chọn nằm trong `config/price_model_blend.json` (nhà phố w = 0,7; căn hộ w = 0,8; biệt thự w = 0,4; đất w = 0,4; không loại nào dùng riêng baseline cho tin gộp cấp tỉnh) và được kiểm tra khớp với kết quả chọn bởi `verify_price_model.py` và test đơn vị. Script chọn cũng tự kiểm tra rằng huấn luyện lại mô hình thuần tái tạo đúng dự đoán trong Gold, so từng tin chứ không chỉ R²: trên 24.819 tin test, 100% lệch dưới 1% (lệch tối đa 0,0 trong không gian log; ngưỡng đạt là ≥ 99,9%). Giới hạn đã biết: trong kiểm định chéo, vị trí và khoảng cắt ngoại lai dùng chung cho cả tập huấn luyện, chỉ baseline được dựng lại trong từng fold; tập test không bao giờ được dùng cho việc này. Cấu hình được chọn lại khi dữ liệu mở rộng từ batch 27/10 sang 17/11 (trước đó căn hộ w = 0,9 có dùng baseline cho tin cấp tỉnh, biệt thự w = 0,5, đất w = 0,6); giá trị trong file cấu hình luôn lấy nguyên từ kết quả chọn, không chỉnh tay.

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

Khoảng sai số của giá dự đoán (tập test 24.819 tin, mô hình kết hợp; `error_ratio` trong `price_model_selection.json`). Tỷ lệ giá chào thực tế chia giá dự đoán có trung vị 1,01, tức không lệch hệ thống về một phía, nhưng phân tán rộng: một nửa số tin nằm trong khoảng 0,78 đến 1,30 lần giá dự đoán và 80% số tin nằm trong khoảng 0,57 đến 1,72 lần. Giá chào thực tế lệch không quá 25% so với dự đoán ở 49,7% số tin và không quá 50% ở 77,2%; nghĩa là cứ hai tin thì khoảng một tin lệch hơn 25%, và cứ bốn đến năm tin thì một tin lệch hơn 50%. Theo loại: căn hộ 52,9% và 82,1% (chính xác nhất), nhà phố 49,9% và 77,2%, đất 46,5% và 69,4%, biệt thự 41,8% và 68,9% (kém nhất; 80% số tin biệt thự nằm trong khoảng 0,45 đến 1,88 lần). So với baseline, mô hình giảm sai số lớn rõ hơn sai số điển hình (MAPE 42,1% so với 48,1%, trung vị sai số 25,0% so với 26,2%). Vì vậy kết quả BQ2 dùng để so sánh tương đối (tin nằm cao hay thấp so với các tin tương đồng) và xếp hạng, không dùng làm định giá cho từng tin. Các con số là thực nghiệm trên khoảng 8 tuần dữ liệu giá chào, không phải đảm bảo cho dữ liệu sau này; phần sai số còn lại một phần đến từ đặc điểm không có trong dữ liệu (hướng, tình trạng nhà, mức thương lượng).

Kiểm tra rò rỉ do tin đăng lại (`src/quality/repost_leakage_check.py`, kết quả `docs/validation/repost_leakage_check.json`). Việc loại trùng chỉ áp dụng giữa các nguồn, còn train/test chia theo hash của `source_id`, nên một tin đăng lại trong cùng nguồn (mã mới) có thể rơi vào hai phía. Với quy tắc loại trùng mặc định (cùng vị trí và loại, diện tích lệch tối đa 2%, giá lệch tối đa 3%, tiêu đề giống từ 0,35) nhưng không giới hạn nguồn, 5.124/24.819 tin test (20,6%) có ít nhất một bản gần giống ở train, hầu hết (5.065) cùng nguồn; quy tắc chặt cho 12,9% và quy tắc lỏng cho 38,1%. Bản gần giống có thể là tin đăng lại hoặc căn tương tự cùng dự án, chưa phân biệt được. Khi bỏ các tin đó khỏi tập test, trung vị sai số % tăng từ 25,0% lên 25,5%, MAPE từ 42,1% lên 43,2% và tỷ lệ tin lệch không quá 25% giảm từ 49,7% xuống 48,5%; với quy tắc chặt gần như không đổi (trung vị 25,0%, tỷ lệ trong ±25% 49,5%). Quy tắc lỏng cho thay đổi lớn hơn (trung vị 27,6%, MAPE 47,1%, tỷ lệ trong ±25% 45,5%), nhưng phần còn lại cũng là những tin ít giống tin nào khác nên khó dự đoán hơn, vì vậy đây là cận trên của mức lạc quan chứ không phải mức rò rỉ thực. Kết luận: tin đăng lại không giải thích được kết quả của mô hình, nhưng sai số công bố có thể lạc quan khoảng 0,5 đến 1,2 điểm phần trăm tùy chỉ số (tối đa khoảng 2,6 đến 4,9 điểm phần trăm theo quy tắc lỏng). Chưa kiểm tra lại phép so sánh với baseline và khoảng tin cậy trên tập đã loại, và chưa chia lại train/test theo nhóm tin gần giống.

Hệ số cờ ở nhà phố (phần mô hình hedonic; mức chênh giá/m², khoảng tin cậy 95%): mặt tiền +53,7% (+52,1% đến +55,3%), thang máy +31,2%, ô tô tiếp cận +8,1%, nội thất 0,0% (−1,8% đến +1,9%, không có ý nghĩa), pháp lý −15,3%. Hệ số pháp lý âm ở cả 4 loại và nội thất âm ở căn hộ (−9,4%) là dấu hiệu **gây nhiễu**: tin rẻ, ở khu vực xa hoặc căn hộ bình dân hay nhấn mạnh "sổ hồng", "full nội thất" trong tiêu đề. Không diễn giải các hệ số này là "có sổ làm giảm giá".

Nếu một loại bất động sản không tốt hơn baseline có ý nghĩa thống kê, vẫn báo cáo trung thực theo phương án trong bảng rủi ro của kế hoạch: giữ benchmark mô tả làm kết quả chính, phân tích nguyên nhân (thiếu biến dự án, độ rộng hẻm, pháp lý thực tế; cờ chưa kiểm chứng độ chính xác).

### 3.3. Backtest (tuần 2)

| Mã | Tiêu chí | Ngưỡng | Minh chứng | Trạng thái |
|---|---|---|---|---|
| B2.10 | Vị trí giá có ý nghĩa với hành vi thị trường | So sánh tỷ lệ giảm giá và số ngày quan sát giữa nhóm `tren_p75` (hoặc `residual_pct` lớn) và phần còn lại; kiểm định khác biệt p < 0,05 | `bq2_backtest.json` | Đạt một phần: H1 (vị trí so với P75) đạt ở mức yếu, p = 0,047 khi kiểm soát nguồn (`H1_supported_at_5pct = true` nhưng `H1_supported_after_multiple_testing = false`, ngưỡng Bonferroni 0,05/3 ≈ 0,0167) và chỉ rõ ở TP.HCM; H2 (phần dư mô hình) chưa đạt khi kiểm soát nguồn (p = 0,25 toàn bộ, p = 0,59 tập test); H3 không. Xem mục 3.4 |

### 3.4. Kết quả backtest

Nguồn: `docs/validation/bq2_backtest.json`, script `src/quality/bq2_backtest.py`. Đối tượng: 64.381 tin đại diện của 3 nguồn crawl, quan sát ít nhất 2 batch, từ 22/09 đến 17/11/2026 (62.472 tin có vị trí giá lúc mới đăng, 61.521 tin có phần dư mô hình). "Giảm giá" tính theo cùng định nghĩa với dashboard BQ2: lần giảm từ 1% đến 30% so với giá trước đó (`GENUINE_CUT` trong `build_bq_evidence.py`), để loại các lần đổi giá quá lớn, nhiều khả năng là sửa lỗi nhập liệu. Vị trí giá tính theo **giá lúc mới đăng** (phiên bản đầu trong `listing_history`), vì giá hiện tại đã bao gồm lần giảm giá. Kết quả vẫn mang tính quan sát: tin không được gán ngẫu nhiên vào các mức giá.

| Vị trí lúc mới đăng | Số tin | Tỷ lệ có giảm giá |
|---|---:|---:|
| Dưới P25 | 16.634 | 2,16% |
| P25–P75 | 31.010 | 3,32% |
| Trên P75 | 14.828 | 4,09% |

- **H1 (trên P75 giảm giá nhiều hơn): đạt ở mức yếu.** Nhóm trên P75 giảm giá 4,09% so với 2,92% của các tin còn lại (gấp 1,40 lần, chênh 1,17 điểm phần trăm, khoảng tin cậy 95% từ 0,81 đến 1,52). Khi so sánh thô p < 0,001, nhưng sau khi kiểm soát nguồn bằng CMH, tỷ số odds chỉ còn 1,11 với p = 0,047, tức sát ngưỡng 0,05 và không vượt ngưỡng Bonferroni 0,05/3 (khoảng 0,0167) cho 3 kiểm định chính. Kiểm soát đồng thời tỉnh và nguồn cho tỷ số odds 1,12, p = 0,030. Vì vậy kết luận được diễn đạt là "có xu hướng", không phải khẳng định chắc. Trong `bq2_backtest.json`: `H1_supported_at_5pct = true`, `H1_supported_after_multiple_testing = false`; H2 sai ở cả hai cờ. Giá ban đầu lấy từ phiên bản đầu của `listing_history`, còn `gold.ev_reprice` lấy giá trước lần đổi đầu tiên nên hai nhóm có thể lệch ở một số ít tin.
- **H1 theo tỉnh:** hiệu ứng tập trung ở TP.HCM (3,73% so với 2,23%, gấp 1,67 lần; CMH theo nguồn p = 0,013). Hà Nội gần như không có khác biệt (6,09% so với 5,93%, p = 0,63), Đà Nẵng và nhóm tỉnh khác chưa có ý nghĩa sau khi kiểm soát nguồn (p = 0,75 và 0,10). Mặt bằng giảm giá ở Hà Nội cao hơn hẳn các nơi khác nên nhận định "tin đắt giảm giá nhiều hơn" không nên áp dụng chung cho mọi tỉnh. Các so sánh theo tỉnh và theo định nghĩa giảm giá là phân tích thăm dò, không hiệu chỉnh riêng nên không dùng để khẳng định.
- **Độ nhạy theo định nghĩa giảm giá:** nếu tính mọi lần giảm kể cả trên 30%, tỷ lệ là 4,48% so với 3,07% và CMH cho p = 0,0025. Chênh lệch giữa hai định nghĩa cho thấy các lần giảm rất lớn đóng góp đáng kể vào kết quả mạnh hơn, nên báo cáo dùng định nghĩa chặt (1–30%) làm kết quả chính.
- **Kiểm tra theo độ sâu crawl** (`H1_by_crawl_depth`, thăm dò, không hiệu chỉnh): số trang crawl thay đổi theo thời gian (Guland từ 45.664 dòng xuống 9.000 rồi 1.350 dòng mỗi batch; Batdongsan và Nhadatvui giảm xuống mức cố định từ 29/09), và một tin chỉ có thể lộ ra lần đổi giá nếu được crawl lại. Vì vậy H1 được chạy riêng cho nhóm tin xuất hiện lần đầu sau khi độ sâu của nguồn đã ổn định (Batdongsan và Nhadatvui từ 29/09, Guland từ 23/10) và nhóm xuất hiện sớm hơn. Nhóm ổn định (3.085 tin trên P75 so với 8.610 tin còn lại): giảm giá 6,65% so với 4,90% (gấp 1,36 lần, chênh 1,74 điểm phần trăm, khoảng tin cậy 0,75 đến 2,73), CMH theo nguồn tỷ số odds 1,22, p = 0,024: có ý nghĩa ở mức 5% nhưng không vượt 0,05/3. Nhóm xuất hiện sớm (11.743 so với 39.034 tin): 3,41% so với 2,48%, nhưng sau khi kiểm soát nguồn tỷ số odds chỉ 1,04, p = 0,57, tức chênh lệch thô trong nhóm này chủ yếu do khác biệt giữa các nguồn. Kết luận: xu hướng không biến mất khi cố định độ sâu crawl và không bị độ sâu crawl tạo ra, nhưng cũng không mạnh hơn: hiệu ứng trong nguồn chỉ hiện ở nhóm tin mới. Tỷ lệ giảm giá tuyệt đối của hai nhóm (6,65% so với 3,41% cho nhóm trên P75) không so sánh trực tiếp được vì phụ thuộc độ sâu crawl và thời gian theo dõi, nhóm ổn định cũng có thời gian theo dõi ngắn hơn.
- **Chỉ số "biến mất" phụ thuộc độ sâu crawl:** khoảng 87% số tin vắng ở hai batch gần nhất, quá cao để là tin đã bán; nhóm xuất hiện sớm có 93,7% tin vắng ở hai batch gần nhất, nhóm ổn định chỉ 61,7% (trên P75) và 56,3% (còn lại). Chênh lệch này là do độ sâu crawl, không phải do tin đã bán, nên chỉ số này không dùng để kết luận.
- **H2 (phần dư mô hình cao giảm giá nhiều hơn): chưa khẳng định.** Trên toàn bộ tin, tỷ lệ tăng từ 2,45% (Q1) lên 4,03% (Q5) và so sánh thô có ý nghĩa, nhưng kiểm soát nguồn bằng CMH cho p = 0,25. Phần dư của tin huấn luyện nằm trong mẫu của mô hình, nên kiểm định đúng hơn là riêng tập test (12.354 tin): 4,25% so với 3,02%, sau khi kiểm soát nguồn p = 0,59. Phần lớn chênh lệch thô đến từ khác biệt giữa các nguồn.
- **H3 (tin đắt được quan sát lâu hơn): không.** Trung vị mọi nhóm là 3 ngày; khác biệt có ý nghĩa thống kê nhưng gần bằng 0 (xác suất trội 0,49) và ngược chiều.
- **Kết quả nhạy với lượng dữ liệu:** các giá trị p của H1 ở phần trên và ở mục độ nhạy dao động theo lượng dữ liệu (với định nghĩa tính mọi lần giảm: p = 0,08 tới 23/10, 0,006 tới 27/10, 0,0025 tới 17/11; định nghĩa chặt chưa chạy trên các mốc cũ). Cần chạy lại khi có thêm batch.
- **Độ lệch khi dùng giá hiện tại được xác nhận:** xếp theo giá hiện tại thì nhóm trên P75 chỉ còn 3,21% giảm giá, thấp hơn nhóm P25–P75 (3,38%), thay vì 4,09% khi xếp theo giá lúc mới đăng.

Giới hạn: độ sâu crawl thay đổi theo thời gian (xem mục kiểm tra theo độ sâu crawl), khoảng 8 tuần dữ liệu (22/09 đến 17/11), 1.997 tin từng giảm giá theo định nghĩa 1–30% (606 trong nhóm trên P75, 1.391 ở nhóm còn lại); kết quả là tương quan, không phải quan hệ nhân quả.

## 4. BQ3 – Khu vực thay thế cùng tỉnh

Câu hỏi: khu vực nào cùng tỉnh có nhóm bất động sản tương tự với chi phí thấp hơn cho cùng diện tích?

| Mã | Tiêu chí | Ngưỡng | Minh chứng | Trạng thái |
|---|---|---|---|---|
| B3.1 | Rẻ hơn ở cùng diện tích mục tiêu | Check `substitution_cheaper_at_target_area`, `substitution_target_area_is_origin_median` | `gold_verification.json` | Đạt |
| B3.2 | Cùng tỉnh | Check `substitution_same_province` | `gold_verification.json` | Đạt |
| B3.3 | Hồ sơ đặc điểm tương tự | Độ tương đồng ≥ 0,8 và mỗi cờ lệch ≤ 0,3 (check `substitution_similarity_and_flag_cap`) | `gold_verification.json` | Đạt có điều kiện: tương đồng về *tần suất tiêu đề nêu đặc điểm*, phụ thuộc G4 |
| B3.4 | Hai phía đủ dữ liệu | Cả nhóm gốc và nhóm thay thế có ≥ 5 tin | Dựng từ nhóm công bố của `agg_peer_group_benchmark` | Đạt. Đề xuất hiển thị n hai phía trên dashboard |
| B3.5 | Gợi ý thay thế phải gần | Gợi ý xếp hạng 1 có `distance_diff_km` ≤ 10 km, hoặc xếp hạng có tính khoảng cách | `agg_area_substitution`, check `substitution_distance_cap` | Đạt: từ 26/10 phương án thay thế phải không xa trung tâm tỉnh hơn quá 10 km; gợi ý hạng 1 xa hơn tối đa 9,996 km. Đổi lại còn 2.740/3.296 cặp (loại 386 cặp quá xa, 170 cặp không biết khoảng cách); 498 nhóm gốc có phương án (dữ liệu tới 17/11, `gold_area_substitution_summary.json`) |
| B3.6 | Chênh lệch tổng giá được giải thích | Ghi rõ số cặp mà tổng giá trung vị phía thay thế không thấp hơn | `gold_area_substitution_summary.json`: 175/2.740 cặp (6,4%) | Đạt có điều kiện: so sánh dựa trên chi phí ở cùng diện tích, không dựa trên tổng giá trung vị |

## 5. Giới hạn diễn giải áp dụng cho mọi BQ

Các giới hạn sau phải xuất hiện trong báo cáo và dashboard, không được trình bày như kết luận:

- Giá là **giá chào bán**, không phải giá giao dịch.
- "Current" là bản ghi mới nhất **đã quan sát**, không xác nhận tin còn hiển thị.
- Cờ đặc điểm chỉ đọc từ tiêu đề; FALSE nghĩa là tiêu đề không nêu.
- Vị trí P25–P75 không có nghĩa là giá hợp lý; hệ số mô hình hedonic là tương quan, không phải tác động nhân quả.
- Tin trùng giữa các nguồn chỉ là nghi trùng theo luật.
- Dữ liệu historical và snapshot có thời điểm đăng khác nhau; chưa kiểm soát yếu tố thời gian trong nhóm tương đồng.
- Giá dự đoán của mô hình BQ2 có sai số lớn (một nửa số tin lệch hơn 25% so với giá chào thực tế trên tập test); chỉ dùng để so sánh tương đối, không công bố như giá định giá.
- Tin gần giống trong cùng một nguồn không bị loại trùng (loại trùng chỉ áp dụng giữa các nguồn): số tin trong nhóm tương đồng có thể gồm cả tin đăng lại, và sai số của mô hình BQ2 có thể lạc quan khoảng 0,5 đến 1,2 điểm phần trăm vì tin đăng lại nằm cả ở train lẫn test.
