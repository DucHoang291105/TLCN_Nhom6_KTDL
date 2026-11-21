const { d, FONT, TEXT_WIDTH, runs, P, blank, center, pageBreak } = require("./lib");
const { Paragraph, TextRun, AlignmentType, Table, TableRow, TableCell, WidthType, BorderStyle, TabStopType } = d;

const TITLE = "XÂY DỰNG DATA LAKEHOUSE PHÂN TÍCH THỊ TRƯỜNG BẤT ĐỘNG SẢN VIỆT NAM";
const STUDENTS = [["VÕ ĐỨC HOÀNG", "23133024"], ["VƯƠNG ĐỨC HUY", "23133029"], ["NGUYỄN LÊ HOÀNG KIỆT", "23133040"]];
const fill = (text) => new TextRun({ text, font: FONT, shading: { type: "clear", color: "auto", fill: "FFFF00" } });

function cover() {
  const c = (children, size = 28, after = 60) => new Paragraph({ alignment: AlignmentType.CENTER, spacing: { line: 300, after }, children });
  const t = (text, size = 28, extra = {}) => new TextRun({ text, font: FONT, size, ...extra });
  return [
    c([t("TRƯỜNG ĐẠI HỌC SƯ PHẠM KỸ THUẬT TP.HCM", 28, { bold: true })]),
    c([t("KHOA CÔNG NGHỆ THÔNG TIN", 28, { bold: true })]),
    c([t("BỘ MÔN ", 28, { bold: true }), new TextRun({ text: "<TÊN BỘ MÔN>", font: FONT, size: 28, bold: true, shading: { type: "clear", color: "auto", fill: "FFFF00" } })]),
    ...blank(1),
    c([new TextRun({ text: "<logo khoa CNTT>", font: FONT, size: 24, shading: { type: "clear", color: "auto", fill: "FFFF00" } })], 24, 240),
    ...STUDENTS.map(([n, id]) => c([t(`${n} – ${id}`, 26, { bold: true })], 26, 20)),
    ...blank(2),
    new Paragraph({ spacing: { after: 120 }, children: [t("Đề tài:", 26)] }),
    c([t(TITLE, 34, { bold: true })], 34, 360),
    ...blank(1),
    c([t("TIỂU LUẬN CHUYÊN NGÀNH", 28, { bold: true })], 28, 0),
    c([t("NGÀNH CÔNG NGHỆ THÔNG TIN", 26, { bold: true })], 26, 360),
    ...blank(1),
    c([t("GIÁO VIÊN HƯỚNG DẪN", 26, { bold: true })]),
    c([new TextRun({ text: "<TÊN GV HƯỚNG DẪN>", font: FONT, size: 26, bold: true, shading: { type: "clear", color: "auto", fill: "FFFF00" } })], 26, 720),
    ...blank(3),
    c([t("KHÓA 2023 – 2027", 26, { bold: true })]),
  ];
}

const dotted = (label = "") => new Paragraph({
  tabStops: [{ type: TabStopType.RIGHT, position: TEXT_WIDTH, leader: "dot" }],
  spacing: { line: 300, after: 0 },
  children: [...runs(label), new TextRun({ text: "\t", font: FONT })],
});

function noBorderTable(left, right) {
  const nb = { style: BorderStyle.NONE, size: 0, color: "FFFFFF" };
  const b = { top: nb, bottom: nb, left: nb, right: nb, insideHorizontal: nb, insideVertical: nb };
  const half = Math.round(TEXT_WIDTH / 2);
  const col = (lines, w) => new TableCell({ borders: b, width: { size: w, type: WidthType.DXA },
    children: lines.map(([text, bold]) => new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 0 },
      children: [new TextRun({ text, font: FONT, size: 24, bold })] })) });
  return new Table({ width: { size: TEXT_WIDTH, type: WidthType.DXA }, columnWidths: [half, TEXT_WIDTH - half], borders: b,
    rows: [new TableRow({ children: [col(left, half), col(right, TEXT_WIDTH - half)] })] });
}

function reviewForm(role) {
  const out = [
    noBorderTable([["ĐH SƯ PHẠM KỸ THUẬT TP.HCM", true], ["KHOA CNTT", true], ["*******", false]],
      [["CỘNG HÒA XÃ HỘI CHỦ NGHĨA VIỆT NAM", true], ["Độc lập – Tự do – Hạnh phúc", true], ["*******", false]]),

    new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 240 },
      children: [new TextRun({ text: `PHIẾU NHẬN XÉT CỦA GIÁO VIÊN ${role}`, font: FONT, size: 32, bold: true })] }),
  ];
  STUDENTS.forEach(([n, id], i) => out.push(new Paragraph({ spacing: { line: 360, after: 0 },
    children: runs(`Họ và tên Sinh viên ${i + 1}: ${n.split(" ").map((w) => w[0] + w.slice(1).toLowerCase()).join(" ")}        MSSV ${i + 1}: ${id}`) })));
  out.push(P("Ngành: Công nghệ Thông tin", { noIndent: true, spacing: { after: 0 } }));
  out.push(new Paragraph({ spacing: { line: 360, after: 0 }, children: runs("Tên đề tài: Xây dựng Data Lakehouse phân tích thị trường bất động sản Việt Nam") }));
  out.push(new Paragraph({ spacing: { line: 360, after: 120 }, children: [...runs(`Họ và tên Giáo viên ${role.toLowerCase()}: `), fill("……………………………………")] }));
  out.push(P("**NHẬN XÉT**", { noIndent: true, spacing: { after: 0 } }));
  for (const item of ["1. Về nội dung đề tài và khối lượng thực hiện:", "2. Ưu điểm:", "3. Khuyết điểm:"]) {
    out.push(P(item, { noIndent: true, spacing: { after: 0 } }));
    for (let k = 0; k < 3; k++) out.push(dotted());
  }
  out.push(P("4. Đề nghị cho bảo vệ hay không?", { noIndent: true, spacing: { after: 0 } }));
  out.push(P("5. Đánh giá loại:", { noIndent: true, spacing: { after: 0 } }));
  out.push(P("6. Điểm:", { noIndent: true, spacing: { after: 120 } }));
  out.push(new Paragraph({ alignment: AlignmentType.RIGHT, spacing: { after: 0 }, children: runs("*Tp. Hồ Chí Minh, ngày …… tháng …… năm 2026*") }));
  out.push(new Paragraph({ alignment: AlignmentType.RIGHT, indent: { right: 900 }, spacing: { after: 0 }, children: runs(`Giáo viên ${role.toLowerCase()}`) }));
  out.push(new Paragraph({ alignment: AlignmentType.RIGHT, indent: { right: 700 }, children: runs("*(Ký và ghi rõ họ tên)*") }));
  return out;
}

const unnumberedTitle = (text) => new Paragraph({ style: "TieuDeKhongSo", pageBreakBefore: true, children: [new TextRun({ text, font: FONT })] });

function thanks() {
  return [
    unnumberedTitle("LỜI CẢM ƠN"),
    P("Nhóm thực hiện xin gửi lời cảm ơn chân thành đến quý Thầy Cô Khoa Công nghệ Thông tin, Trường Đại học Sư phạm Kỹ thuật Thành phố Hồ Chí Minh đã trang bị cho nhóm nền tảng kiến thức về cơ sở dữ liệu, kỹ thuật dữ liệu và phân tích dữ liệu, là cơ sở để nhóm thực hiện tiểu luận chuyên ngành này."),
    new Paragraph({ alignment: AlignmentType.JUSTIFIED, spacing: { line: 360, after: 120 }, indent: { firstLine: 567 },
      children: [...runs("Đặc biệt, nhóm xin cảm ơn "), fill("<học hàm, học vị, họ tên GVHD>"), ...runs(" đã tận tình hướng dẫn, góp ý về hướng tiếp cận, cách đặt câu hỏi nghiệp vụ và cách kiểm chứng kết quả trong suốt quá trình thực hiện đề tài.")] }),
    P("Nhóm cũng cảm ơn các bạn trong lớp đã trao đổi, góp ý khi nhóm trình bày các phiên bản trung gian của hệ thống và dashboard."),
    P("Do thời gian và kinh nghiệm còn hạn chế, báo cáo không tránh khỏi thiếu sót. Nhóm rất mong nhận được ý kiến đóng góp của quý Thầy Cô để hoàn thiện đề tài."),
    new Paragraph({ alignment: AlignmentType.RIGHT, spacing: { before: 240, after: 0 }, children: runs("*Tp. Hồ Chí Minh, tháng 11 năm 2026*") }),
    new Paragraph({ alignment: AlignmentType.RIGHT, indent: { right: 600 }, children: runs("Nhóm sinh viên thực hiện") }),
  ];
}

function outline() {
  return [
    unnumberedTitle("ĐỀ CƯƠNG CHI TIẾT"),
    new Paragraph({ alignment: AlignmentType.CENTER, spacing: { line: 360, before: 480 }, children: [
      new TextRun({ text: "(Đóng tại đây bản Đề cương chi tiết của tiểu luận theo mẫu của Khoa, có chữ ký của giáo viên hướng dẫn và sinh viên thực hiện; chấp nhận bản photo.)", font: FONT, size: 26, italics: true, shading: { type: "clear", color: "auto", fill: "FFFF00" } })] }),
  ];
}

module.exports = { cover, reviewForm, thanks, outline, unnumberedTitle, TITLE };
