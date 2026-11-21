# Công bố dữ liệu và phục hồi môi trường

## Trước khi chạy

Đóng băng CSV trong suốt lượt chạy. Dùng nguồn thật, lịch thật; không đổi ngày để lấp batch thiếu. Cắm nguồn laptop, giữ Docker Desktop hoạt động và không đóng nắp máy. Script nghiệm thu dùng `SetThreadExecutionState` để chống ngủ do không thao tác; không thay đổi power plan, không ngăn được tắt máy hoặc mất điện và khôi phục chính sách của thread khi kết thúc. Không kéo dài JWT để che lỗi máy ngủ.

```powershell
cd D:\Code\TLCN_BDS_Lakehouse
.\.venv\Scripts\python.exe -m src.orchestration.environment write
.\scripts\validate_defense.ps1 -BatchId 20261120 -RunId official_operations_20261121
```

Chọn một RunId mới mỗi lần nghiệm thu. Ví dụ ngày trong lệnh phải thay bằng batch thật mới nhất có đủ ba nguồn. Lịch thu thập là 22:00 thứ ba/thứ sáu theo giờ Việt Nam. Kiểm định cuối so sánh cả mốc phải có dữ liệu theo lịch; một bộ input cũ đầy đủ vẫn chưa đủ để chứng nhận cập nhật tới hôm nay.

## Một cửa sổ ghi, một cửa sổ đọc

Trước DAG, suite chạy `python -m src.quality.check_runtime_identity` để bảo đảm host Windows, Spark và Airflow cùng nhìn thấy cây source/input đầy đủ và cùng SHA256. Airflow mount repository chỉ đọc, với các đường dẫn dữ liệu tiếp nhận, output và validation có mount ghi riêng. Tên đường dẫn được sort theo chuỗi tương đối chuẩn, không dùng thứ tự Path phụ thuộc hệ điều hành. `start_platform.ps1` ở profile query/dashboard cũng đi qua hàng rào publication; không dùng Docker/Compose trực tiếp để bỏ qua quy trình này.

Hash toàn bộ byte chạy bốn luồng I/O, kết quả ghép theo thứ tự đường dẫn cố định; không dùng mtime/size thay cho checksum. `identity_benchmark.json` đối chiếu một/bốn worker trên cùng source/input và ghi thời gian thực tế. Spark tiếp tục dùng một writer. Nếu code/input đổi giữa hai stage, build dừng trước khi khởi chạy stage tiếp theo.

Fingerprint phát hành mô tả các file thực tế đang tồn tại. Xóa file sau lúc capture làm fingerprint thay đổi; stage/commit một thao tác xóa hoặc đổi tên đã có từ trước không làm fingerprint đổi giả. Điều này cho phép commit nguyên source/evidence đã nghiệm thu rồi kiểm tra lại trên chính commit đó.

SQLite REST catalog dùng cửa sổ ghi Spark riêng với cửa sổ đọc Trino/Superset. Chuyển `write` sẽ kiểm tra Spark/Airflow đang rảnh, dừng readers rồi mở lại kết nối REST. Chỉ mở `read` sau khi toàn bộ pipeline đã chạy và kiểm định thành công:

```powershell
.\.venv\Scripts\python.exe -m src.orchestration.environment read
.\.venv\Scripts\python.exe -m src.orchestration.status
```

Airflow và `scripts/run_spark.ps1` dùng chung khóa `outputs/operation/catalog.lock`. Trạng thái công bố ở `outputs/operation/publication.json`: `UNVERIFIED`, `BUILDING`, `FAILED`, `PUBLISHED`. Mỗi job ghi làm mất hiệu lực các bước phụ thuộc phía sau. Chỉ khi đủ mọi bước Bronze/Silver/Gold, cùng dấu vân tay code/config/input và các báo cáo PASS thì trạng thái mới là `PUBLISHED`. Chuyển read đặt hàng rào đọc dưới cùng khóa trước khi khởi động Trino. Sửa CSV/code hoặc thay báo cáo kiểm định làm cửa sổ đọc bị từ chối. Đây là hàng rào vận hành, không phải transaction nhiều bảng hoặc chống người có quyền chạy trực tiếp `spark-submit`/Docker ngoài runner.

Sau lỗi giữa Gold, giữ readers đóng, xem task/log đầu tiên, sửa nguyên nhân rồi chạy lại toàn pipeline. Không dùng lại báo cáo PASS của lượt trước. Job đã commit theo từng bảng/batch được giữ để phục hồi; không tự xóa dữ liệu hoặc rollback giả.

Nếu một chuỗi lệnh chạy tay dừng giữa chừng và muốn bỏ lượt đó, sau khi xác nhận không có Spark app/DAG đang chạy:

```powershell
.\.venv\Scripts\python.exe -m src.orchestration.environment recover
```

Lệnh chỉ đánh dấu lượt chưa xong là thất bại; vẫn cần chạy lại pipeline. Nếu tiến trình bị kill khiến thư mục khóa còn sót, lệnh sẽ dừng an toàn. Kiểm tra owner trong khóa, Docker/Spark/Airflow và tiến trình trên host trước khi quản trị viên gỡ khóa; không có cơ chế tự hết hạn khóa rồi cho phép writer thứ hai.

## Bronze historical và location Iceberg

Hợp đồng sáu nguồn historical nằm tại `src/bronze/historical_contract.py`. Batch đã tồn tại không bị overwrite. `historical_input_manifest.json` lưu SHA256, byte size, schema, số dòng và hash multiset dữ liệu raw. Khi bootstrap batch cũ, đối chiếu `EXCEPT ALL` hai chiều giữa toàn bộ cột CSV và Parquet trước khi pin checksum, giữ đúng cả số lần xuất hiện dòng trùng. Mọi lượt Bronze audit kiểm tra lại nội dung. Đổi CSV cùng số dòng vẫn bị chặn; sửa dữ liệu phải có batch version và kế hoạch migration rõ ràng.

Thay cấu hình location Iceberg làm job báo lỗi kèm location cũ/mới, không `DROP ... PURGE`. Muốn di chuyển: đóng readers/writers, backup nhất quán, sao chép sang đích riêng, đối chiếu schema/grain/hash, cập nhật catalog theo kế hoạch được kiểm tra rồi mới mở đọc. Không dùng rebuild bình thường như một thao tác migration.

## Backup và kiểm tra phục hồi

Volume tồn tại qua restart không thay thế backup. Dừng mọi container sử dụng năm volume nghiệp vụ rồi chạy từ thư mục repo:

```powershell
.\scripts\stop_platform.ps1 -All
.\.venv\Scripts\python.exe -m src.orchestration.maintenance backup --directory outputs/backups/ten_moc_moi
.\.venv\Scripts\python.exe -m src.orchestration.maintenance restore-check --directory outputs/backups/ten_moc_moi
```

Backup gồm MinIO, SQLite catalog, PostgreSQL, Superset và Airflow cùng thời điểm offline; manifest lưu SHA256 từng archive. Công cụ từ chối nếu bất kỳ container đang chạy nào mount volume nguồn. Restore-check chỉ giải nén vào volume mới với tên ngẫu nhiên, dùng `tar --compare` kiểm tra bytes, mode và ownership; không ghi đè volume sản xuất. Archive chứa metadata riêng tư và được giữ trong `outputs/` ngoài Git. Trước phục hồi thật phải dừng hệ thống, kiểm tra hash, gắn bộ volume đã khôi phục dưới project riêng và chạy kiểm định ứng dụng. Kiểm tra bytes cục bộ chưa chứng nhận một máy vật lý thứ hai hoặc khôi phục sau mọi loại sự cố.

## Tái lập image và cấu hình crawl

Compose ghim image ngoài bằng digest; `config/runtime_images.json` ghi tham chiếu đã dùng. MinIO dùng image Chainguard đã kiểm tra pull và ghi/đọc bốn bucket trên volume mới, kể cả sau restart. Tài liệu nhà cung cấp: [MinIO image](https://images.chainguard.dev/directory/image/minio/overview). Server và init đều dùng image này; init dừng ngay khi tạo bucket lỗi. Kiểm tra lại bằng `python -m src.quality.fresh_runtime_check`.

`requirements-verified.txt` ghi chính xác bộ runtime/test trên Windows/Python 3.12; nền tảng khác dùng `requirements-dev.txt` rồi chạy lại test. Image build Airflow/Superset có base digest riêng. Hadoop/YARN và FastAPI/web-ui nằm trong `docker-compose.experimental.yml`; chỉ bật có chủ đích bằng hai cờ `-f`, không thuộc pipeline chính.

Toàn bộ `CRAWL_REQUEST_TIMEOUT`, `CRAWL_MAX_RETRIES`, `CRAWL_MAX_CONSECUTIVE_FAILURES`, `CRAWL_RUN_TIMEOUT_SECONDS`, `CRAWL_DELAY_SECONDS` và số trang mỗi nguồn được truyền từ Compose vào Airflow. Đổi `.env` cần recreate container Airflow. Delay phải hữu hạn trong 0–60 giây, các ngưỡng số nguyên có giới hạn; không bypass 403/429. Độ sâu từng nguồn/batch được đo trong `crawl_coverage.json`. “Vắng hai batch” luôn là vắng trong mẫu quan sát, không xác nhận đã bán.

## Nghiệm thu kỹ thuật và nghiệp vụ

`defense_verification.json` là nguồn số liệu của lượt kỹ thuật hiện hành: run, batch, code/input hash, toàn bộ checks và số dòng. `src.orchestration.status` từ chối coi báo cáo cũ là hiện hành nếu code/input đã đổi. Suite chạy lại backtest, kiểm tra repost và kiểm tra tái tạo mô hình của B sau khi rebuild Gold. Đề xuất CV mới ở `price_model_selection_candidate.json`; cấu hình production và `price_model_selection.json` gốc được giữ để truy nguyên quyết định chọn trọng số. A không tự chọn lại trọng số của B.

Manifest nghiệm thu đóng hash tiêu chí nghiệp vụ, cấu hình mô hình và hồ sơ nhãn, nhưng không tự chứng nhận nhãn mới được con người duyệt. G4 PARTIAL, kết luận BQ và nghiệm thu trên máy khác vẫn có trạng thái riêng. CI cũ không chứng nhận source chưa push. Công cụ tag chỉ chấp nhận commit sạch, proof đúng bytes và CI xanh của chính commit đó; tag đã có không được di chuyển. Chọn version mới khi phát hành, không dùng lại tag cũ.
