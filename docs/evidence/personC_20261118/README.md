# Minh chứng dashboard v3 – vai C (18/11/2026)

Dữ liệu: batch tới 20261117, run Airflow `dashboard_v2_20261118` (25/25 task SUCCESS, có thêm job `commute_choice`;
bảng `agg_commute_district` được ghi lại sau đó để thêm tọa độ nơi làm/học, Gold kiểm định lại 122/122).
Kiểm chứng: Silver 30/30, Gold 122/122, 36/36 biểu đồ khớp Trino (`docs/validation/superset_dashboard_check.json`),
`verify_pipeline_dashboard` PASS, unit test 497/497.

| Ảnh | Dashboard | Biểu đồ chính |
|---|---|---|
| `01_tlcn-tong-quan.png` | Tổng quan | Bản đồ giá/m² nhà phố TP.HCM và Hà Nội; giá theo khoảng cách tới trung tâm; giá theo tỉnh |
| `02_tlcn-bq1.png` | BQ1 | Bản đồ diện tích nhận được theo quận với cùng ngân sách; diện tích theo khoảng cách; danh sách tin nên xem |
| `03_tlcn-bq2.png` | BQ2 | Bản đồ tin rao cao/thấp so với giá ước tính; quận có nhiều tin rao cao; đặc điểm đi kèm chênh giá (KTC 95%); tra cứu tin |
| `04_tlcn-bq3.png` | BQ3 | Bản đồ các quận quanh nơi làm/học; quận thay thế rẻ hơn; chi phí theo quãng đường; tin để đi xem |
| `05_tlcn-chat-luong.png` | Chất lượng | PASS/WARN/REJECT theo nguồn; độ phủ trường; từ crawl tới tin đại diện; độ mới nguồn |

Cách đọc:

- Tiêu đề mỗi biểu đồ là kết luận, tính từ Gold ở lựa chọn mặc định của dashboard (không tự đổi theo bộ lọc; trang có ghi rõ).
- Màu mang nghĩa: đỏ = đắt / rao cao / diện tích nhỏ; xanh = rẻ / rao thấp / rộng hoặc đáng cân nhắc; xám = bình thường; đen = nơi làm/học.
- Bản đồ dùng nền OpenStreetMap; mỗi chấm là một tin có tọa độ (36% tin), hoặc tâm quận (median tọa độ tin trong quận).
  Bản đồ BQ1–BQ3 chỉ vẽ trong bán kính 30 km quanh trung tâm hoặc nơi làm/học để nội thành không bị thu quá nhỏ.
- Nhóm dưới 5 tin không được so sánh; nhãn trục ghi số tin; hệ số đặc điểm có khoảng tin cậy 95%.
- Không đưa lên: tỷ lệ "tin biến mất" (chưa dùng được) và so sánh giảm giá theo vị trí giá hiện tại (thiên lệch).
