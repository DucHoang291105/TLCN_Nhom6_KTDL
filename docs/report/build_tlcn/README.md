# Dựng báo cáo TLCN

Tạo `docs/report/Bao_cao_TLCN_Nhom6.docx` và `.pdf` theo mẫu trình bày của Khoa: khổ A4, Times New Roman, dãn dòng 1.5, lề 2/2/3/2 cm, chương 14 pt, đề mục 13 pt, tên bảng ở trên bảng, tên hình ở dưới hình, số trang ở giữa cuối trang, mục lục, danh mục hình và danh mục bảng tự động.

## Nội dung nằm ở đâu

| Tệp | Nội dung |
|---|---|
| `content_front.js` | Bìa chính, bìa phụ, phiếu nhận xét GVHD/GVPB, lời cảm ơn, trang đề cương |
| `content_main1.js` | Tóm tắt, Mở đầu, Chương 1 (cơ sở lý thuyết) |
| `content_main2.js` | Chương 2 (yêu cầu, thiết kế), Chương 3 (hiện thực, ví dụ theo dấu một tin) |
| `content_main3.js` | Chương 4 (kết quả), Kết luận, tài liệu tham khảo, phụ lục, chữ viết tắt |
| `lib.js` | Định dạng theo mẫu: đề mục, bảng, hình, đánh số |
| `figs.py` | Vẽ sơ đồ (kiến trúc, DAG, hình sao, use case, hoạt động, luồng bảng) và cắt ảnh dashboard từ `docs/evidence/personC_20261118/` |

Số liệu được ghi trong các tệp `content_*.js` và lấy từ `docs/validation/` của bản phát hành ghi trong báo cáo; riêng bảng thời gian 26 task ở Phụ lục C đọc trực tiếp `docs/validation/integration_airflow_run.json`. Khi có lượt nghiệm thu mới, sửa số trong `content_*.js` rồi dựng lại.

Chỗ tô vàng trong báo cáo (tên bộ môn, logo, tên GVHD, trang đề cương có chữ ký) cần điền tay trong Word.

## Cách dựng

Chạy trong thư mục này (PowerShell):

```powershell
npm install
..\..\..\.venv\Scripts\python.exe figs.py ..\..\..
node build.js ..\..\.. ..\Bao_cao_TLCN_Nhom6_raw.docx
powershell -ExecutionPolicy Bypass -File word_finalize.ps1 -In "$PWD\..\Bao_cao_TLCN_Nhom6_raw.docx" -OutDocx "$PWD\..\Bao_cao_TLCN_Nhom6.docx" -OutPdf "$PWD\..\Bao_cao_TLCN_Nhom6.pdf"
```

`word_finalize.ps1` cần Microsoft Word: script mở tệp, cập nhật mục lục và hai danh mục (điền số trang), lưu `.docx` và xuất `.pdf`. Nếu không có Word, mở tệp `_raw.docx`, bấm chuột phải vào mục lục, chọn **Update Field**, rồi lưu.

Biến môi trường `NO_UPDATE_FIELDS=1` tắt cờ "cập nhật field khi mở", dùng khi xử lý tự động bằng Word để tránh hộp thoại.
