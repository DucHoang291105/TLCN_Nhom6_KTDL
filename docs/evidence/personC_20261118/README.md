# Minh chứng dashboard v4 – vai C (20/11/2026, cập nhật bộ lọc theo tỉnh)

Mỗi BQ đọc theo thứ tự: **kết luận (KPI) → biểu đồ chứng minh → ví dụ tin thật kèm link → kiểm chứng**.
Mỗi trang có **thẻ "Kết luận cho lựa chọn hiện tại"** (Handlebars): câu chữ, số liệu và kiểm định được tính bằng SQL
cho đúng tỉnh / loại hình / ngân sách / nơi làm đang chọn. Biểu đồ đổi theo bộ lọc có tiêu đề mô tả trung tính;
biểu đồ ghi **(Cố định)** hoặc **(Tất cả…)** là so sánh cố định, không theo bộ lọc.

Dữ liệu: batch tới 20261117. Bảng bằng chứng `ev_*` do `src/gold/build_bq_evidence.py` tính trong pipeline;
Gold 126/126 PASS (gồm 4 check cho bảng bằng chứng), 49/49 biểu đồ khớp Trino, unit test 500/500.
Đã thử đổi bộ lọc sang Hà Nội, Đà Nẵng cho mọi biểu đồ: không biểu đồ nào lỗi hay trống; chỉ biểu đồ (Cố định)/(Tất cả…) không đổi.

| Ảnh | Phát hiện chính | Bằng chứng |
|---|---|---|
| `01_tlcn-tong-quan.png` | Cùng cách trung tâm 5–10 km, nhà phố Hà Nội 256 tr/m², TP.HCM 119 tr/m² (gấp 2,1 lần); Hà Nội đắt hơn ở mọi vành đai | Bản đồ giá từng tin; median theo vành đai khoảng cách (mỗi điểm ≥ 30 tin) |
| `02_tlcn-bq1.png` | Từ 3–5 lên 10–20 tỷ: quận trung tâm chỉ thêm ~22 m², quận xa > 10 km thêm ~65 m²; 90% tin có tin cùng quận tốt hơn | Bản đồ diện tích theo quận; đường diện tích theo ngân sách × vùng; mỗi quận một cặp tin thật (B rẻ hơn ≥ 10%, rộng hơn ≥ 20%, đủ phòng) |
| `03_tlcn-bq2.png` | Tin rao trên P75 lúc đăng hạ giá nhiều hơn các tin còn lại – rõ ở TP.HCM (nhà phố 2,2% vs 1,7%, p = 0,003), không đúng ở Hà Nội; toàn bộ chênh thô 4,1% vs 2,9% (62.595 tin) nhưng sau khi kiểm soát nguồn chỉ còn p = 0,047 | Tỷ lệ hạ giá theo vị trí lúc đăng; 147 tin thật đã hạ giá; mô hình kiểm chứng trên tập test (8/10 nhóm lệch ≤ 5%); dữ liệu thô xác nhận hệ số ở 4/5 đặc điểm (ô tô ngược chiều – ghi rõ) |
| `04_tlcn-bq3.png` | Làm ở Khu công nghệ cao: Thủ Đức 3,7 km, rẻ hơn 54% so với Phú Nhuận 14 km; ở cả 16 nơi làm/học đều có quận rẻ hơn 6–69% mà đường đi không xa hơn | Bản đồ quận quanh nơi làm; xếp hạng đổi quận tốt nhất mỗi nơi; kiểm chứng tọa độ 12/16 điểm đến đúng quận (4 điểm còn lại ở quận giáp ranh) |
| `05_tlcn-chat-luong.png` | Sửa luật trên nhãn sơ bộ + 1 lượt người rà soát: loại hình 84,2% → 89,5%, recall pháp lý/mặt tiền/ô tô lên 100% trên 76 tin test, nhưng mỗi cờ có ít ca dương tính (pháp lý 15, mặt tiền 21, ô tô 11) | G4 trước/sau; PASS/WARN/REJECT theo nguồn; độ phủ trường; phễu dữ liệu |

Giới hạn ghi trên dashboard: giá chào bán (không phải giá giao dịch); hệ số mô hình là tương quan; khoảng cách là đường chim bay;
chỉ 36% tin có tọa độ; nhãn G4 mới có 1 lượt review; bản đồ cần Internet để tải nền OpenStreetMap.

## Ảnh đổi bộ lọc (cho phần hỏi đáp)

| Ảnh | Lựa chọn | Thẻ kết luận hiển thị |
|---|---|---|
| `06_loc_tong-quan_ha-noi.png` | Tổng quan · Hà Nội | 12.770 tin, median 274 tr/m²; trong 5 km 298 tr/m², từ 15 km 93 tr/m² (3,2 lần) |
| `07_loc_bq1_da-nang.png` | BQ1 · Đà Nẵng · 3–5 tỷ | Hải Châu 48 m², Hòa Vang 104 m²; mỗi 10 km thêm ~83 m² |
| `08_loc_bq2_ha-noi.png` | BQ2 · Hà Nội · nhà phố (loại hình mặc định) | H1 **ngược chiều ở nhà phố**: tin trên P75 hạ giá 2,0% vs 3,0% (z = −2,2; p = 0,031); căn hộ Hà Nội vẫn đúng chiều (17,5% vs 13,7%) |
| `09_loc_bq3_ho-guom_cau-giay.png` | BQ3 · làm ở Hồ Gươm, nhắm Cầu Giấy | 7 quận rẻ hơn; Hai Bà Trưng 2,9 km, rẻ hơn 36%; Hoàng Mai rẻ hơn 41% |

Lưu ý khi trình bày H1 (rao cao lúc đăng → phải hạ giá):

- **Đúng ở TP.HCM**: nhà phố p = 0,003; mọi loại hình sau khi kiểm soát nguồn tỷ số odds 1,19, p = 0,013.
- **Chưa đủ bằng chứng ở Đà Nẵng** (nhà phố p = 0,34).
- **Hà Nội không có chênh lệch chung** (mọi loại hình sau khi kiểm soát nguồn: tỷ số odds 0,96, p = 0,63).
  Tách theo loại hình thì nhà phố **ngược chiều** (2,0% vs 3,0%, p = 0,031), còn căn hộ vẫn đúng chiều (17,5% vs 13,7%).
- **Toàn bộ dữ liệu**: chênh lệch thô 4,1% vs 2,9% (p < 0,001), nhưng phần lớn do khác biệt giữa các nguồn tin.
  Kiểm soát theo nguồn (CMH, cùng tập 62.472 tin của `bq2_backtest.py`) chỉ còn tỷ số odds 1,11, p = 0,047:
  đạt ở mức 5%, không qua ngưỡng Bonferroni 0,017.

"Hạ giá" ở đây là giảm thật 1–30% (`GENUINE_CUT` trong `src/gold/build_bq_evidence.py`); lần giảm > 30% thường là sửa giá gõ nhầm nên không tính.
Thẻ kết luận tự nêu đúng chiều cho từng tỉnh; không trình bày H1 như quy luật chung cho mọi thành phố.
