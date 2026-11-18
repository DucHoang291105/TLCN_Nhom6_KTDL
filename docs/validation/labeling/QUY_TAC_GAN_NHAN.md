# Quy tắc gán nhãn – dành cho người review

Tài liệu này dùng cho hai lượt review file `labeling_sheet.csv` (198 tin, 22 tin × 9 nguồn). Nhãn đã được gán sơ bộ; việc của bạn là **kiểm tra từng dòng và sửa khi thấy sai**. Kết quả của bạn là "đáp án" để chấm độ chính xác của hệ thống (tiêu chí G4), nên cần làm cẩn thận và theo **cùng một quy tắc** như dưới đây.

Thời gian ước tính: 60–90 phút cho 198 dòng.

---

## 1. Ba điều bắt buộc

1. **Không mở `labeling_predictions.csv`** cho tới khi cả hai lượt review xong. Đó là dự đoán của hệ thống; xem trước sẽ làm nhãn bị lệch theo hệ thống.
2. **Không sửa `labeling_sheet_prelabel.csv`.** File này giữ bản sơ bộ để đo bạn đã sửa bao nhiêu.
3. **Không đồng ý thì sửa.** Đừng giữ nhãn sơ bộ chỉ vì nó đã có sẵn. Mỗi lần sửa, ghi lý do ngắn vào cột `note`.

---

## 2. Các cột cần kiểm tra

| Cột | Giá trị hợp lệ | Ghi chú |
|---|---|---|
| `true_category` | `nha_pho`, `can_ho`, `biet_thu`, `dat`, `phong_tro_khac`, `khong_ro` | Loại hình **thực tế** của bất động sản |
| `true_legal` | `1` / `0` / `?` | Title có **nêu** pháp lý không |
| `true_furnished` | `1` / `0` / `?` | Title có **nêu** nội thất không |
| `true_frontage` | `1` / `0` / `?` | Title có **nêu** BĐS nằm mặt tiền đường/phố không |
| `true_elevator` | `1` / `0` / `?` | Title có **nêu** thang máy không |
| `true_car_access` | `1` / `0` / `?` | Title có **nêu** ô tô tiếp cận được không |
| `note` | tự do | Lý do sửa, hoặc điểm còn phân vân |
| `review_1` / `review_2` | MSSV | Chỉ điền **sau khi đã xem hết 198 dòng** |

Không sửa các cột `sample_id`, `source`, `title`, `price`, `area`, `rooms`, `address`, `url`, `labeler`, `labeled_at`.

---

## 3. Hai loại nhãn khác nhau – đọc kỹ

- **Loại hình (`true_category`) là sự thật về bất động sản.** Được dùng mọi nguồn: title, giá, diện tích, địa chỉ, và **mở link** khi title không đủ.
- **5 cờ đặc điểm chỉ xét TITLE có ghi hay không**, không xét BĐS thật có hay không. Hệ thống chỉ đọc title, nên đáp án cũng chỉ dựa vào title.
  - Ví dụ: tin có sổ hồng nhưng title không ghi → `true_legal = 0`.
  - Ví dụ: trang tin có ảnh thang máy nhưng title không ghi → `true_elevator = 0`.
  - **Không mở link để quyết định cờ.**

---

## 4. Loại hình (`true_category`)

### 4.1. Định nghĩa

| Nhãn | Gồm | Ví dụ title trong mẫu |
|---|---|---|
| `nha_pho` | Nhà phố, nhà riêng, nhà hẻm, nhà mặt tiền, nhà cấp 4, nhà nát, tòa nhà cho thuê không nói rõ là CHDV/văn phòng | L015 "Nhà Ưu Long, Ngang 4,2m, 3 tầng BTCT…", L021 "Bán nhà MT Trương Phước Phan… cấp 4" |
| `can_ho` | Căn hộ chung cư, officetel, condotel, penthouse, duplex, studio, tập thể. Title viết tắt "CH" cũng là căn hộ | L091 "Bán Chung Cư Wilton 67m2…", L032 "penthouse… duplex 3 tầng" |
| `biet_thu` | Biệt thự, villa, nhà vườn, **liền kề (LK)**, **shophouse** | L027 "Biệt thự liền kề…", L028 "Nhà phố LK 126m2" |
| `dat` | Đất nền, đất thổ cư, lô đất, đất nông nghiệp, đất "tặng nhà" | L090 "Đất FO…", L062 "Đất làm nhà vườn…" |
| `phong_tro_khac` | Phòng trọ, dãy trọ, **căn hộ dịch vụ (CHDV) cả tòa**, chung cư mini (CCMN) cả tòa, kho xưởng, văn phòng, mặt bằng, khách sạn, trang trại | L109 "Dãy trọ 5 phòng…", L007 "KHO XƯỞNG…", L072 "Bán CHDV… hầm+5 tầng" |
| `khong_ro` | Đã đọc title và mở link mà **vẫn** không xác định được | L085 "Vị trí trung tâm, khu dân cư sầm uất…" |

### 4.2. Cách quyết định khi phân vân

Đi theo thứ tự, dừng ở bước đầu tiên cho được câu trả lời:

1. Title có từ khóa loại hình rõ ràng (bảng 4.1)? → dùng nhãn đó.
2. Title có **nhiều** từ khóa → chọn theo thứ **đang được bán**:
   - "Bán **đất** tặng nhà" → `dat` (người bán định giá theo đất).
   - "Nhà phố **LK**" → `biet_thu` (liền kề thắng "nhà phố").
   - "Bán **nhà** gần chung cư C3" → `nha_pho`; "chung cư" ở đây chỉ là địa danh.
   - "Tòa CHDV", "tòa CCMN" → `phong_tro_khac`, kể cả khi có chữ "nhà".
3. Title không có từ khóa (L067, L070, L133…) → **mở `url`** và đọc mục loại hình hoặc mô tả trên trang.
4. Link chết hoặc trang không ghi → suy từ giá và diện tích chỉ khi rất rõ (ví dụ 2 tỷ cho 70 m² có "2PN 2WC" là căn hộ). Nếu không rõ → `khong_ro`.

### 4.3. Ca hay gặp trong mẫu

| Tình huống | Nhãn | Ví dụ |
|---|---|---|
| "Tòa nhà MT… 8 tầng, HĐ thuê 160tr" (không nói CHDV/văn phòng) | `nha_pho`; nếu mở link thấy là văn phòng → `phong_tro_khac` | L013, L018 |
| "Nhà cấp 4 đang làm kho" | `nha_pho` (bán nhà, kho là cách dùng hiện tại) | L131 |
| "Shophouse tầng 1 tòa chung cư" | `biet_thu` theo quy ước shophouse | L185 |
| "Dòng tiền cho thuê xưởng" | Mở link: là xưởng → `phong_tro_khac`; là nhà ở → `nha_pho` | L044 |
| "Góc 3MT… XD 3 hầm 15 tầng" | Mở link: đất có phép xây → `dat`; tòa nhà có sẵn → `phong_tro_khac` | L016 |
| Title chỉ có "Mặt tiền …", "Lô góc …" | Mở link; không rõ thì `khong_ro` | L020, L089, L138 |

---

## 5. Cờ đặc điểm (chỉ xét title)

Quy tắc chung:
- `1` = title ghi rõ.
- `0` = title không ghi, hoặc ghi điều ngược lại.
- `?` = title có ghi nhưng **không thể** quyết định là có hay không. Dùng `?` ít nhất có thể, vì dòng `?` bị bỏ khỏi phép chấm.

### 5.1. Pháp lý – `true_legal`

| `1` | `0` |
|---|---|
| sổ hồng, sổ đỏ, SHR, sổ riêng, sổ chung | không nhắc gì tới giấy tờ |
| "đã có sổ", "sổ sẵn", "sẵn sổ", "có sổ" | "giấy tay", "vi bằng" (là nêu giấy tờ nhưng **không** phải pháp lý đầy đủ) → `0`, ghi `note` |
| "công chứng", "bao công chứng sang tên" | "cấp phép XD" (giấy phép xây dựng, không phải sổ) |
| "bao pháp lý", "pháp lý chuẩn/rõ ràng" | "chính chủ" (nói về người bán, không phải giấy tờ) |
| **Quy ước nhóm:** "thổ cư", "full thổ cư", "hoàn công" → `1` | |

### 5.2. Nội thất – `true_furnished`

| `1` | `0` |
|---|---|
| "full nội thất", "nội thất đầy đủ", "full NT", "NTCC", "nội thất đẹp/siêu đẹp/cao cấp" | không nhắc tới nội thất |
| "tặng nội thất", "để lại nội thất" | "nhà thô", "bàn giao thô", "không nội thất" |
| | "nhà mới", "dọn vào ở ngay" (không nói nội thất) |

### 5.3. Mặt tiền – `true_frontage`

Câu hỏi: BĐS có nằm **mặt tiền đường/phố** không?

| `1` | `0` |
|---|---|
| "MT …", "mặt tiền đường …", "nhà mặt phố", "MTKD" | "mặt tiền hẻm", "góc 2 mặt hẻm" (L132, L140) |
| "góc 2MT", "góc 3MT" (khi là mặt tiền đường) | "**sát** mặt tiền", "**cách** MT 50m", "gần mặt tiền" |
| | "MT 5m", "mặt tiền 4,2m" khi rõ là **chiều ngang** của nhà trong hẻm |
| | "mặt tiền bờ sông", "view sông" (không phải đường) |
| | "đường ô tô thông mặt phố" (đường dẫn ra phố, nhà không nằm mặt phố) |

Đặt `?` khi title mâu thuẫn, ví dụ "nhà **hẻm** 532 … **mặt tiền** đường 20m" (L121), hoặc "mặt tiền thụt" (L014). Ghi lý do vào `note`.

### 5.4. Thang máy – `true_elevator`

| `1` | `0` |
|---|---|
| "thang máy", "TM", "có thang máy 450kg" | "thang bộ", không nhắc |
| | Căn hộ chung cư **không** ghi thang máy → vẫn `0`, dù chung cư nào cũng có |

### 5.5. Ô tô tiếp cận – `true_car_access`

Câu hỏi: title có nói **ô tô vào tới nơi** không?

| `1` | `0` |
|---|---|
| "HXH" (hẻm xe hơi), "hẻm ô tô", "ngõ ô tô", "đường ô tô" | "**gần** ô tô", "cách ô tô vài bước", "ngõ nông gần ôtô" (L193) |
| "ô tô đỗ cửa", "xe hơi đậu trước cửa", "ô tô ngủ trong nhà", "gara ô tô" | "hẻm 3m", "hẻm 4m", "hẻm nhựa 8m" (chỉ có bề rộng, không nói ô tô) |
| "hẻm xe tải" (xe tải vào được thì ô tô cũng vào được) | Nhà mặt tiền nhưng title không nói ô tô → `0` |
| "xe hơi tới nền", "ô tô tránh" | |

Đặt `?` khi title nói ô tô nhưng không rõ có vào tới BĐS không, ví dụ "**sát** mặt tiền xe tải" (L046), "hẻm xe hơi tránh **cách** 90m" (L055).

---

## 6. Cách làm từng dòng

1. Đọc `title`, nhìn `price`, `area`, `address`.
2. Kiểm tra `true_category` theo mục 4. Nếu dòng có `note` hoặc nhãn là `khong_ro` → mở `url`.
3. Kiểm tra 5 cờ theo mục 5, **chỉ nhìn title**.
4. Sửa ô sai. Ghi vào `note` theo mẫu `R1: frontage 1→0, mặt tiền hẻm` (R2 cho lượt 2). Không xóa ghi chú cũ, chỉ viết thêm vào sau.
5. Xem hết 198 dòng rồi mới điền MSSV vào `review_1` (hoặc `review_2`) **cho tất cả các dòng**.

Mẹo khi mở bằng Excel/Google Sheets:
- Lọc cột `note` khác rỗng để làm các dòng khó trước.
- Bật **Wrap text** cho cột `title`.
- Khi lưu, giữ đúng định dạng **CSV UTF-8**. Trong Excel chọn "CSV UTF-8 (Comma delimited)", nếu không tiếng Việt sẽ bị lỗi font.
- Không thêm hoặc xóa cột, không đổi thứ tự dòng.

---

## 7. Phân công hai lượt

| Lượt | Người | Việc | Hạn |
|---|---|---|---|
| Review 1 | _(điền tên)_ | Xem 198 dòng, sửa, điền `review_1` | _(điền ngày)_ |
| Review 2 | _(điền tên)_ | Xem lại toàn bộ **sau** lượt 1 (cả những ô lượt 1 đã sửa), điền `review_2` | _(điền ngày)_ |

Nếu hai lượt bất đồng ở một ô (lượt 2 muốn sửa lại ô lượt 1 vừa sửa), lượt 2 **không tự quyết**. Hãy ghi `R2: không đồng ý R1, …` vào `note`, rồi cả nhóm chốt trong 5 phút họp.

Đổi một quy ước ở mục 4–5 (ví dụ không coi "thổ cư" là pháp lý nữa) là việc của **cả nhóm**. Khi đổi, phải sửa đồng loạt mọi dòng liên quan và cập nhật tài liệu này.

---

## 8. Sau khi xong

Người cuối cùng chạy:

```powershell
python -m src.quality.evaluate_labels
```

Kiểm tra trong `docs/validation/label_accuracy.json`:
- `status` = `COMPLETE`.
- `reviewers` có đủ hai MSSV.
- `review_changes` cho biết số dòng/ô đã sửa so với bản sơ bộ. Số này phải được đưa vào báo cáo cùng với precision/recall.

## 9. Checklist nhanh

- [ ] Chưa mở `labeling_predictions.csv`
- [ ] Cờ chỉ xét title; loại hình được mở link khi cần
- [ ] "mặt tiền hẻm", "sát mặt tiền" → frontage `0`
- [ ] "gần ô tô" → car `0`; "hẻm xe tải" → car `1`
- [ ] "thổ cư", "hoàn công" → legal `1`; "giấy tay", "vi bằng" → legal `0`
- [ ] "LK", "shophouse" → `biet_thu`; "tòa CHDV" → `phong_tro_khac`
- [ ] Mọi lần sửa có ghi `note`
- [ ] Đã điền MSSV vào cột review cho cả 198 dòng
- [ ] Lưu đúng CSV UTF-8
