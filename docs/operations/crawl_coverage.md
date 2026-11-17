# Độ phủ crawl và giới hạn của vòng đời quan sát

Đo ngày 17/11/2026 bằng `src.quality.crawl_coverage`, trên CSV local; kết quả
chi tiết nằm ở [crawl_coverage.json](../validation/crawl_coverage.json).
Đây là đầu ra điều tra A2, phục vụ kiểm tra đầu vào A1 trước khi nhận đủ dữ liệu.

## Cách đo

```powershell
.\.venv\Scripts\python.exe -m src.quality.crawl_coverage --as-of 20261117
# Dùng trước khi nạp dữ liệu; exit 2 nếu còn điều kiện đầu vào chưa đạt:
.\.venv\Scripts\python.exe -m src.quality.crawl_coverage --as-of 20261117 --strict
```

Công cụ đọc từng CSV, SHA256 trước và sau đọc, kiểm tra cấu trúc hàng/cột,
LFS pointer, listing_id, số dòng, ID phân biệt, trang phân biệt và trang lớn nhất.
ID trùng trong batch được gộp để đo độ phủ, giữ số trang nhỏ nhất từng ID.
Các hash có trong manifest pipeline được đối chiếu; CSV cũ mất hoặc đổi nội dung
làm kiểm tra thất bại. JSON xuất riêng, không ghi đè manifest của lần chạy thật.

Phép đo vắng hai batch lấy tập ID từng xuất hiện trừ hợp của hai batch gần nhất
**của chính nguồn đó**. Mẫu số là tổng ID từng quan sát trong nguồn, không phải
số tin đang đăng toàn website. So sánh các cặp batch ghi cả số ID giữ lại và tỷ lệ
giữ lại trên batch trước. Mốc baseline 27/10 giúp so với dữ liệu Gold đã kiểm định.
CSV thô chưa qua DQ Silver; số đếm raw không mặc nhiên bằng Gold.

## Số liệu baseline tới 27/10

| Nguồn | Trang đầu kỳ → cuối kỳ | Dòng đầu kỳ → cuối kỳ | ID từng thấy | Vắng hai batch | Tỷ lệ |
|---|---:|---:|---:|---:|---:|
| Batdongsan | 1.000 → 200 | 20.000 → 4.000 | 38.100 | 32.527 | 85,37% |
| Guland | 1.000 → 30 | 45.664 → 1.350 | 52.544 | 50.240 | 95,62% |
| Nhadatvui | 937 → 200 | 16.860 → 3.600 | 18.516 | 14.751 | 79,67% |

Gộp ba nguồn: 97.518 / 109.160 = **89,34%**. Đây cũng là số vắng trong
[Gold verification](../validation/gold_verification.json), trường
`metrics.lifecycle_disappeared_rows`; mẫu số crawl ở
[Silver verification](../validation/silver_final_verification.json),
`table_counts.crawl_current_27`. Con số 84% trong kế hoạch không phải số đo chính
xác của bộ dữ liệu này.

Đối chiếu ID của batch đầu 22/09 theo độ sâu của batch cuối:

| Nguồn | ID thuộc trang vượt độ sâu cuối kỳ | Vắng hai batch trong nhóm này | Tỷ lệ | Tỷ lệ của nhóm trong độ sâu |
|---|---:|---:|---:|---:|
| Batdongsan | 13.526 | 13.215 | 97,70% | 85,33% |
| Guland | 43.532 | 42.405 | 97,41% | 85,58% |
| Nhadatvui | 13.260 | 13.260 | 100% | 34,28% |

Các trang đầu cũng thay đổi: tỷ lệ giữ ID từ 23/10 sang 27/10 lần lượt là
46,51%, 25,63%, 95,30%. Việc thu hẹp phạm vi và thay đổi thứ tự/tập tin quan sát
là yếu tố phải tính đến. Các số liệu cho thấy quan hệ giữa phạm vi crawl và
vắng quan sát; chưa tách được ảnh hưởng của tin thực sự hết hạn, bị gỡ hay đổi URL.
Chưa có kiểm tra URL trực tiếp và chưa có thí nghiệm tăng độ sâu rồi crawl lại.

## Hiện trạng file local ngày 17/11

- Sau bổ sung: 48 CSV đúng tên/thư mục, 316.241 dòng; checksum các snapshot
  cũ đã pin vẫn khớp. Batch chung đầy đủ mới nhất là 17/11.
- Batdongsan có 17 batch, Guland 15, Nhadatvui 16 (có cả batch 03/10 ngoài
  lịch thứ ba/thứ sáu). Bốn CSV Nhadatvui mới được chuẩn hóa tên, giữ nguyên bytes.
- Nhadatvui 03/10 được khôi phục nguyên file cũ từ Git LFS để bảo toàn đầu vào
  đã nghiệm thu. Không xóa batch đó khỏi lịch sử để làm đẹp tỷ lệ vắng.
- Chủ dữ liệu đã chấp nhận thiếu Guland 20/10, 30/10 và Nhadatvui 30/10,
  10/11; các khoảng trống được ghi rõ trong `config/source_gaps.json`.
- Audit hiện tại PASS cả cấu trúc/bytes, không có CSV bị bỏ qua, khoảng trống
  lịch đã khai báo và batch 17/11 đủ ba nguồn.
- CSV Guland batch 10/11 ban đầu ghi timestamp 12/11; chủ dữ liệu đã sửa về
  10/11 và yêu cầu dùng mốc đó trước khi ingest. `input_date_review.json`
  lưu checksum trước/sau cùng quyết định này. Chưa xác minh độc lập ngày thu
  thập, nên cần công bố giới hạn khi dùng dữ liệu trong backtest theo thời gian.

## Đo lại sau bổ sung, cutoff 17/11

| Nguồn | ID từng thấy | Vắng hai batch | Tỷ lệ |
|---|---:|---:|---:|
| Batdongsan | 46.222 | 40.699 | 88,05% |
| Guland | 54.130 | 52.077 | 96,21% |
| Nhadatvui | 19.348 | 15.639 | 80,83% |

Tổng: **108.415 / 119.700 = 90,57%**, so với 89,34% ở cutoff 27/10.
Độ sâu cuối kỳ vẫn khoảng 200/30/200 trang; tỷ lệ cao không được xem là
bằng chứng bán nhanh hoặc nhu cầu thị trường. Mẫu tích lũy thêm ID trong khi
batch mới chỉ thu thập phạm vi giới hạn. Đây là so sánh hai cutoff thực tế,
chưa phải thí nghiệm tăng độ sâu hoặc xác minh tình trạng từng URL.

## Chính sách nhận batch tiếp theo

1. Nhận file gốc đúng ngày, đúng nguồn; giữ nguyên các CSV đã có manifest.
2. Kiểm tra tên chuẩn, summary crawler và JSON điều tra ở trên.
3. Chỉ bổ sung `config/source_gaps.json` cho ngày đã xác nhận không thu thập
   được, với lý do cụ thể; không khai báo trước các batch 20/11 và 24/11.
4. Chốt độ sâu trước giờ crawl. `.env` hiện hỗ trợ `GULAND_CRAWL_PAGES` và
   `NHADATVUI_CRAWL_PAGES` (mặc định 200); CSV Guland mới chỉ có 30 trang.
   Tăng cấu hình chỉ ảnh hưởng lượt crawl mới khi chưa có CSV, không tăng độ phủ
   của batch đã xuất. Chọn thử mức 200 cho Guland, so với 30 trang hiện có, rồi
   đánh giá thời gian, lỗi trang và ID thu được trước khi tăng thêm.
5. Batdongsan tiếp tục xuất extension, cố định bộ lọc và ghi số trang đã xuất.
6. Đóng băng input rồi chạy manifest và Airflow. Dùng bộ dữ liệu mới chạy lại
   điều tra; so sánh theo cùng khoảng thời gian và phạm vi, không lấy tỷ lệ
   giảm do thay mẫu làm bằng chứng tin đã bán.

Trong báo cáo và dashboard phải ghi: **“Vắng trong hai batch được thu thập gần
nhất của nguồn; phạm vi crawl có giới hạn và thay đổi theo thời gian. Không xác
nhận tin đã bán, bị gỡ hoặc không còn tồn tại trên website.”** Phần dashboard
do C phụ trách; tài liệu này cung cấp chú thích để tích hợp, chưa sửa dashboard.
