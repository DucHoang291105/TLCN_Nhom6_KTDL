# Nhận dữ liệu và tái lập môi trường

Phạm vi A1/A4. Quy trình này không thay thế nghiệm thu A3. Thực hiện từ thư
mục gốc repository bằng PowerShell; Docker Desktop ở chế độ Linux containers.

## Tiếp nhận CSV thật

File nhận có thể nằm ngoài project. Chọn **ngày thu thập thực tế**, không lấy
ngày nhận file hay tự suy từ tên thư mục. Trước tiên kiểm tra không ghi:

```powershell
.\.venv\Scripts\python.exe -m src.orchestration.receive_snapshot --source batdongsan --batch-id 20261117 --file 'D:\Du_lieu\batdongsan_raw.csv'
```

Thay đường dẫn ví dụ bằng file thật. Nếu đúng nguồn, ngày, số dòng và checksum,
thêm `--apply`. Công cụ copy nguyên bytes, không đổi/xóa file gốc, không ghi đè
snapshot khác nội dung, không xuất file đích dở dang. Chạy lại cùng bytes trả
`REUSE_IDENTICAL`. Ngày tương lai và CSV không hợp lệ bị chặn. Nguồn và ngày
do người cung cấp xác nhận; công cụ không chứng minh nguồn gốc nội dung CSV.

Sau khi nhận các nguồn:

```powershell
.\.venv\Scripts\python.exe -m src.quality.crawl_coverage --as-of 20261117 --strict
```

Xem `missing_scheduled_batches`, `extra_csv_not_consumed_by_pipeline`, `errors`
và `matches_pipeline_manifest` trong JSON. Các CSV cũ đã pin phải giữ nguyên.
Các thư mục Batdongsan mới đã được cung cấp theo tên chuẩn; bốn CSV Nhadatvui
tháng 11 đã đổi tên thành `nhadatvui_raw.csv` và giữ nguyên bytes. Batch
Nhadatvui 03/10 được khôi phục nguyên file đã pin từ Git LFS để giữ lịch sử.
`source_gaps.json` chỉ ghi ngày đã được xác nhận không có dữ liệu và lý do.
Tạm thiếu file local ở một lượt khác vẫn là chờ nhận, không tự khai báo gap.

Ngày thiếu đã chấp nhận cho bộ dữ liệu hiện tại: Guland 20/10 và 30/10;
Nhadatvui 30/10 và 10/11. Không tạo batch thay thế. CSV Guland batch 10/11
ban đầu ghi `_scraped_at` ngày 12/11; chủ dữ liệu đã sửa timestamp và yêu cầu
dùng ngày 10/11 trước ingest. Ghi nhận checksum trước/sau và quyết định này
trong `docs/validation/input_date_review.json`; ngày thu thập không được xác
minh độc lập. Các phân tích theo thời gian cần công bố giới hạn đó.

Lịch còn lại: 17/11, 20/11, 24/11, 22:00 Việt Nam. Chuẩn bị Batdongsan bằng
extension trước giờ chạy; hai crawler Python dùng `.env` với
`GULAND_CRAWL_PAGES` / `NHADATVUI_CRAWL_PAGES`. Thay `.env` cần khởi tạo lại
container Airflow để nhận cấu hình mới; không thực hiện khi có run đang chạy.
Batch đã có CSV sẽ được tái sử dụng, không crawl lại chỉ vì đổi số trang.
Không bật lịch DAG trước khi đủ đầu vào; A3 chịu trách nhiệm chạy pipeline.

Manifest `incoming_snapshot_manifest.json` là **preflight** của tập CSV local,
không xác nhận các file mới đã được ingest. Manifest pipeline của lần chạy
đã nghiệm thu vẫn là `snapshot_manifest.json`. Không đổi hai file cho nhau.

## Thực hiện trên máy Kiệt

Sau khi nhánh đã được review và có trên GitHub, clone vào thư mục trống.
Không copy `.env`, volume hoặc `.venv` từ máy Hoàng để giả lập máy sạch.

```powershell
git clone https://github.com/DucHoang291105/TLCN_Nhom6_KTDL.git
Set-Location TLCN_Nhom6_KTDL
# Nếu thử trước khi merge, checkout nhánh A đã push thay cho main.
.\scripts\reproduce_environment.ps1 -Phase Prepare
.\scripts\reproduce_environment.ps1 -Phase Check
```

`Prepare` tải LFS, tạo venv, cài dependencies và tạo `.env` nếu chưa có.
`Check` kiểm tra Python/Git/LFS/Docker daemon, Compose, secret/Fernet và các
CSV LFS pointer. Các phase trả lỗi ngay khi bước con thất bại, có thời điểm
bắt đầu/kết thúc và số giây thực tế tại `outputs/reproduction/<Phase>.json`.
Preflight PASS chỉ xác nhận điều kiện ban đầu.

Tiếp theo dùng README để mở write mode, ingest historical lần đầu và chạy
DAG trên batch đủ ba nguồn; đây là bước chạy thực tế A3, không được thay
bằng JSON mẫu. Đợi run SUCCESS rồi:

```powershell
.\scripts\reproduce_environment.ps1 -Phase Dashboard
```

Phase Dashboard kiểm tra không còn writer, mở read mode, provision dashboard
và đối chiếu bảng/biểu đồ/filter. Nó thay đổi dashboard do script quản lý;
nếu tái sử dụng môi trường có chỉnh sửa tay thì sao lưu trước. JSON probe
riêng ở `outputs/reproduction/dashboard_check.json`, không thay bằng chứng
nghiệm thu lịch sử. Bất kỳ bảng nào lệch số dòng so với JSON của pipeline vừa
chạy đều làm phase FAIL, không tự điều chỉnh expected về dữ liệu cũ.

Dùng `snapshot_manifest.json` và `pipeline_dashboard_integration.json` của lượt nghiệm thu hiện hành để xác nhận dữ liệu và trạng thái đồng bộ. Kết quả preflight chỉ kiểm tra điều kiện máy, không thay thế kiểm chứng pipeline đầy đủ.

## Nhật ký bắt buộc khi người khác thực hiện

Ghi commit, người thao tác, hệ điều hành, tài nguyên Docker, thời điểm bắt đầu
clone và thời điểm dashboard/filter hoạt động. Đính kèm JSON các phase, run ID
và lỗi đã sửa; tổng thời gian là hiệu hai thời điểm của toàn lượt, không chỉ
cộng các bước nhanh hoặc lấy thời gian preflight làm thời gian tái lập.
Chỉ sau khi người không viết code hoàn thành được quy trình mới chốt A4.
Hiện chưa có kết quả thực tế từ máy Kiệt.
