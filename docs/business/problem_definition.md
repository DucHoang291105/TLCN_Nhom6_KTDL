# Problem Definition

## Bài toán chính

Hệ thống hỗ trợ **người mua** phân tích dữ liệu tin đăng bất động sản. Ba Business Question (BQ) của giai đoạn Gold:

| Mã | Câu hỏi | Bảng Gold |
|---|---|---|
| BQ1 | Trong cùng một mức ngân sách, người mua đánh đổi những gì về vị trí, diện tích, số phòng và đặc điểm? Tin nào không bị tin khác trội hơn? | `agg_budget_tradeoff`, `fact_budget_pareto` |
| BQ2 | Tin đăng nào có giá/m² lệch so với nhóm bất động sản tương đồng (dưới P25, trong P25–P75, trên P75)? | `agg_peer_group_benchmark`, `fact_listing_price_assessment` |
| BQ3 | Nếu không mua được ở khu vực mong muốn, khu vực nào trong cùng tỉnh có nhóm bất động sản tương tự với chi phí thấp hơn cho cùng diện tích? | `agg_area_substitution` |

Bảng hỗ trợ: tổng quan thị trường (`agg_market_overview`), chất lượng dữ liệu (`agg_dq_kpi`), lịch sử đổi giá (`fact_listing_price_change`).

## Giới hạn diễn giải

- Giá là giá đăng/giá chào bán, không phải giá giao dịch thực tế.
- "Current" là bản ghi mới nhất **đã quan sát** của mỗi tin, không xác nhận tin còn đang hiển thị.
- Đặc điểm (pháp lý, nội thất, mặt tiền đường, thang máy, ô tô tiếp cận) chỉ đọc từ title; FALSE nghĩa là title không nêu.
- BQ2 là benchmark mô tả; vị trí P25–P75 không chứng minh giá hợp lý và chưa giải thích định lượng chênh lệch giá (chưa có mô hình hedonic).
- Tin trùng giữa các nguồn chỉ là **nghi trùng** theo luật; bảng tổng hợp dùng tin đại diện theo luật đó.
