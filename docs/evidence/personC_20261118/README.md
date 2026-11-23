# Minh chứng dashboard v4 – vai C (chụp lại 22/11/2026 sau các sửa R07, R08, R13)

Mỗi BQ đọc theo thứ tự: **kết luận (KPI) → biểu đồ chứng minh → ví dụ tin thật kèm link → kiểm chứng**.
Mỗi trang có **thẻ "Kết luận cho lựa chọn hiện tại"** (Handlebars): câu chữ, số liệu và kiểm định được tính bằng SQL
cho đúng tỉnh / loại hình / ngân sách / nơi làm đang chọn. Biểu đồ đổi theo bộ lọc có tiêu đề mô tả trung tính;
biểu đồ ghi **(Cố định)** hoặc **(Tất cả…)** là so sánh cố định, không theo bộ lọc.

Dữ liệu: batch tới 20261117. Bảng bằng chứng `ev_*` do `src/gold/build_bq_evidence.py` tính trong pipeline;
Gold 139/139 PASS (gồm 5 check cho bảng bằng chứng), 49/49 biểu đồ khớp Trino, unit test 566/566 (nhánh c/fix-g4-r07-r13). Bảng ev_reprice dùng cùng tập tin và cùng giá đầu với bq2_backtest.py nên số đếm hai bên trùng nhau.
Đã thử đổi bộ lọc sang Hà Nội, Đà Nẵng cho mọi biểu đồ: không biểu đồ nào lỗi hay trống; chỉ biểu đồ (Cố định)/(Tất cả…) không đổi.

| Ảnh | Phát hiện chính | Bằng chứng |
|---|---|---|
| `01_tlcn-tong-quan.png` | Cùng cách trung tâm 5–10 km, nhà phố Hà Nội 256 tr/m², TP.HCM 119 tr/m² (gấp 2,1 lần); Hà Nội đắt hơn ở mọi vành đai | Bản đồ giá từng tin; median theo vành đai khoảng cách (mỗi điểm ≥ 30 tin) |
| `02_tlcn-bq1.png` | Từ 3–5 lên 10–20 tỷ: quận trung tâm chỉ thêm ~22 m², quận xa > 10 km thêm ~65 m²; 90% tin có tin cùng quận tốt hơn | Bản đồ diện tích theo quận; đường diện tích theo ngân sách × vùng; mỗi quận một cặp tin thật (B rẻ hơn ≥ 10%, rộng hơn ≥ 20%, đủ phòng) |
| `03_tlcn-bq2.png` | Tin rao trên P75 lúc đăng hạ giá nhiều hơn các tin còn lại – rõ ở TP.HCM (nhà phố 2,2% vs 1,7%, p = 0,003), không đúng ở Hà Nội; toàn bộ chênh thô 4,09% vs 2,92% (62.472 tin, trùng backtest) nhưng sau khi kiểm soát nguồn chỉ còn p = 0,047 | Tỷ lệ hạ giá theo vị trí lúc đăng; 147 tin thật đã hạ giá; mô hình kiểm chứng trên tập test (8/10 nhóm lệch ≤ 5%); dữ liệu thô xác nhận hệ số ở 4/5 đặc điểm (ô tô ngược chiều – ghi rõ) |
| `04_tlcn-bq3.png` | Làm ở Khu công nghệ cao, nhà phố 50–80 m², 3–5 tỷ: tin vừa ngân sách gần nhất 1,4 km đường bộ (~2 phút), 188/1.740 tin trong 10 km; ra vành 10–15 km rẻ hơn ~20% so với dưới 5 km nhưng vành 15–30 km (trung tâm TP.HCM) lại đắt hơn; đường biên: Thủ Đức 8,2 km – 5,7 tỷ, Thuận An 20 km – 2,9 tỷ, Củ Chi 41 km – 1,3 tỷ (ảnh chụp lại 23/11/2026, dữ liệu tới 20/11, BQ3 theo đường bộ) | Bản đồ tin tô theo vành đai đường bộ; giá/m² theo vành đai; bong bóng đánh đổi km – chi phí; 12 nơi làm/học: vành 10–15 km từ rẻ hơn 40% (Bách khoa TP.HCM) tới đắt hơn 26% (Phú Mỹ Hưng); đường bộ dài hơn chim bay 1,21–1,45 lần |
| `05_tlcn-chat-luong.png` | Sửa luật trên nhãn sơ bộ + 1 lượt người rà soát: loại hình 84,2% → 89,5%, recall pháp lý/mặt tiền/ô tô lên 100% trên 76 tin test, nhưng mỗi cờ có ít ca dương tính (pháp lý 15, mặt tiền 21, ô tô 11) | G4 trước/sau; PASS/WARN/REJECT theo nguồn; độ phủ trường; phễu dữ liệu |

Giới hạn ghi trên dashboard: giá chào bán (không phải giá giao dịch); hệ số mô hình là tương quan; BQ3 dùng quãng đường đường bộ OSRM theo ô lưới ~1 km, thời gian khi đường thông thoáng;
chỉ 36% tin có tọa độ; nhãn G4 mới có 1 lượt review; bản đồ cần Internet để tải nền OpenStreetMap.

## Ảnh đổi bộ lọc (cho phần hỏi đáp)

| Ảnh | Lựa chọn | Thẻ kết luận hiển thị |
|---|---|---|
| `06_loc_tong-quan_ha-noi.png` | Tổng quan · Hà Nội | 12.770 tin, median 274 tr/m²; trong 5 km 298 tr/m², từ 15 km 93 tr/m² (3,2 lần) |
| `07_loc_bq1_da-nang.png` | BQ1 · Đà Nẵng · 3–5 tỷ | Hải Châu 48 m², Hòa Vang 104 m²; mỗi 10 km thêm ~83 m² |
| `08_loc_bq2_ha-noi.png` | BQ2 · Hà Nội · nhà phố (loại hình mặc định) | H1 **ngược chiều ở nhà phố**: tin trên P75 hạ giá 2,0% vs 3,0% (z = −2,25; p = 0,024); căn hộ Hà Nội vẫn đúng chiều (17,5% vs 13,6%) |
| `09_loc_bq3_ho-hoan-kiem.png` | BQ3 · Hồ Hoàn Kiếm · nhà phố 50–80 m² · 10–20 tỷ | Tin gần nhất ở Hai Bà Trưng 2,3 km (~3 phút), 757/1.004 tin trong 10 km; vành 10–15 km **đắt hơn** 7% so với dưới 5 km (thẻ ghi rõ chiều ngược); đường biên Hoàn Kiếm 2,3 km – 33,4 tỷ → Hai Bà Trưng 4,4 km – 16,0 tỷ → Đông Anh 17,8 km – 5,2 tỷ |

Lưu ý khi trình bày H1 (rao cao lúc đăng → phải hạ giá):

- **Đúng ở TP.HCM**: nhà phố p = 0,003; mọi loại hình sau khi kiểm soát nguồn tỷ số odds 1,19, p = 0,013.
- **Chưa đủ bằng chứng ở Đà Nẵng** (nhà phố p = 0,34).
- **Hà Nội không có chênh lệch chung** (mọi loại hình sau khi kiểm soát nguồn: tỷ số odds 0,96, p = 0,63).
  Tách theo loại hình thì nhà phố **ngược chiều** (2,0% vs 3,0%, p = 0,024), còn căn hộ vẫn đúng chiều (17,5% vs 13,6%).
- **Toàn bộ dữ liệu**: chênh lệch thô 4,09% vs 2,92% (p < 0,001), nhưng phần lớn do khác biệt giữa các nguồn tin.
  Kiểm soát theo nguồn (CMH, cùng tập 62.472 tin của `bq2_backtest.py`) chỉ còn tỷ số odds 1,11, p = 0,047:
  đạt ở mức 5%, không qua ngưỡng Bonferroni 0,017.

"Hạ giá" ở đây là giảm thật 1–30% (`GENUINE_CUT` trong `src/gold/build_bq_evidence.py`); lần giảm > 30% thường là sửa giá gõ nhầm nên không tính.
Thẻ kết luận tự nêu đúng chiều cho từng tỉnh; không trình bày H1 như quy luật chung cho mọi thành phố.
