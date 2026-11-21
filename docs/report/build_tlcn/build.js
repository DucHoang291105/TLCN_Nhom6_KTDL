const fs = require("fs");
const path = require("path");
const { d, FONT, TEXT_WIDTH, CM, runs } = require("./lib");
const { Document, Packer, Paragraph, TextRun, AlignmentType, Footer, PageNumber, NumberFormat, LevelFormat, TableOfContents,
  StyleLevel, Table, TableRow, TableCell, WidthType, BorderStyle, ShadingType, SectionType, PageBorderDisplay, PageBorderOffsetFrom } = d;
const front = require("./content_front");
const m1 = require("./content_main1");
const m2 = require("./content_main2");
const m3 = require("./content_main3");

const REPO = process.argv[2];
const OUT = process.argv[3];
const airflow = JSON.parse(fs.readFileSync(path.join(REPO, "docs/validation/integration_airflow_run.json"), "utf8")).tasks;

const margins = { top: Math.round(2 * CM), bottom: Math.round(2 * CM), left: Math.round(3 * CM), right: Math.round(2 * CM), footer: Math.round(1 * CM) };
const pageNumberFooter = () => ({ default: new Footer({ children: [new Paragraph({ alignment: AlignmentType.CENTER,
  children: [new TextRun({ children: [PageNumber.CURRENT], font: FONT, size: 26 })] })] }) });
const double = { style: BorderStyle.DOUBLE, size: 12, color: "000000", space: 24 };

function abbrTable() {
  const b = { style: BorderStyle.SINGLE, size: 4, color: "808080" };
  const borders = { top: b, bottom: b, left: b, right: b };
  const w = [Math.round(TEXT_WIDTH * 0.22), TEXT_WIDTH - Math.round(TEXT_WIDTH * 0.22)];
  const cell = (text, width, head) => new TableCell({ borders, width: { size: width, type: WidthType.DXA },
    shading: head ? { fill: "E7ECEC", type: ShadingType.CLEAR, color: "auto" } : undefined, margins: { top: 50, bottom: 50, left: 90, right: 90 },
    children: [new Paragraph({ alignment: head ? AlignmentType.CENTER : AlignmentType.LEFT, spacing: { line: 276, after: 0 },
      children: runs(text, { size: 24, bold: head || undefined }) })] });
  return new Table({ width: { size: TEXT_WIDTH, type: WidthType.DXA }, columnWidths: w, rows: [
    new TableRow({ tableHeader: true, children: [cell("Chữ viết tắt", w[0], true), cell("Ý nghĩa", w[1], true)] }),
    ...m3.ABBR.map(([a, b2]) => new TableRow({ children: [cell(a, w[0]), cell(b2, w[1])] })),
  ] });
}

const tocTitle = (t) => front.unnumberedTitle(t);

// Build content in document order so figure/table numbers follow chapters.
const summary = m1.summary().flat(Infinity);
const body = [...m1.moDau(), ...m1.chapter1(), ...m2.chapter2(), ...m2.chapter3(), ...m3.chapter4(), ...m3.ketLuan(), ...m3.references(), ...m3.appendices(airflow)].flat(Infinity);

const doc = new Document({
  creator: "Nhóm 6 – TLCN", title: front.TITLE, description: "Báo cáo tiểu luận chuyên ngành",
  features: { updateFields: process.env.NO_UPDATE_FIELDS ? false : true },
  styles: {
    default: { document: { run: { font: FONT, size: 26 }, paragraph: { spacing: { line: 360 } } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: FONT, size: 28, bold: true, color: "000000" }, paragraph: { alignment: AlignmentType.CENTER, spacing: { before: 0, after: 360, line: 360 }, keepNext: true, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: FONT, size: 26, bold: true, color: "000000" }, paragraph: { alignment: AlignmentType.LEFT, spacing: { before: 240, after: 120, line: 360 }, keepNext: true, outlineLevel: 1 } },
      { id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: FONT, size: 26, bold: true, color: "000000" }, paragraph: { alignment: AlignmentType.LEFT, spacing: { before: 180, after: 120, line: 360 }, keepNext: true, outlineLevel: 2 } },
      { id: "Heading4", name: "Heading 4", basedOn: "Normal", next: "Normal", quickFormat: true,
        run: { font: FONT, size: 26, italics: true, color: "000000" }, paragraph: { alignment: AlignmentType.LEFT, spacing: { before: 120, after: 120, line: 360 }, keepNext: true, outlineLevel: 3 } },
      { id: "TenBang", name: "TenBang", basedOn: "Normal", next: "Normal",
        run: { font: FONT, size: 22, bold: true }, paragraph: { alignment: AlignmentType.CENTER, spacing: { before: 120, after: 60, line: 276 }, keepNext: true } },
      { id: "TenHinh", name: "TenHinh", basedOn: "Normal", next: "Normal",
        run: { font: FONT, size: 22, bold: true }, paragraph: { alignment: AlignmentType.CENTER, spacing: { before: 0, after: 240, line: 276 } } },
      { id: "ChuThichBang", name: "ChuThichBang", basedOn: "Normal", next: "Normal",
        run: { font: FONT, size: 20, italics: true }, paragraph: { alignment: AlignmentType.CENTER, spacing: { before: 60, after: 200, line: 276 } } },
      { id: "TieuDeKhongSo", name: "TieuDeKhongSo", basedOn: "Normal", next: "Normal",
        run: { font: FONT, size: 28, bold: true }, paragraph: { alignment: AlignmentType.CENTER, spacing: { before: 0, after: 360, line: 360 } } },
    ],
  },
  numbering: { config: [{ reference: "bullet", levels: [{ level: 0, format: LevelFormat.BULLET, text: "–", alignment: AlignmentType.LEFT,
    style: { paragraph: { indent: { left: 720, hanging: 360 } }, run: { font: FONT } } }] }] },
  sections: [
    { properties: { page: { margin: margins, borders: { pageBorderTop: double, pageBorderBottom: double, pageBorderLeft: double, pageBorderRight: double,
        pageBorders: { display: PageBorderDisplay.ALL_PAGES, offsetFrom: PageBorderOffsetFrom.TEXT } } } },
      children: front.cover() },
    { properties: { type: SectionType.NEXT_PAGE, page: { margin: margins } },
      children: [...front.cover(), new Paragraph({ pageBreakBefore: true, children: [] }), ...front.reviewForm("HƯỚNG DẪN"),
        new Paragraph({ pageBreakBefore: true, children: [] }), ...front.reviewForm("PHẢN BIỆN")] },
    { properties: { type: SectionType.NEXT_PAGE, page: { margin: margins, pageNumbers: { start: 1, formatType: NumberFormat.LOWER_ROMAN } } },
      footers: pageNumberFooter(),
      children: [
        ...front.thanks(), ...front.outline(),
        tocTitle("MỤC LỤC"), new TableOfContents("Mục lục", { hyperlink: true, headingStyleRange: "1-3" }),
        tocTitle("DANH MỤC HÌNH"), new TableOfContents("Danh mục hình", { hyperlink: true, stylesWithLevels: [new StyleLevel("TenHinh", 1)] }),
        tocTitle("DANH MỤC BẢNG"), new TableOfContents("Danh mục bảng", { hyperlink: true, stylesWithLevels: [new StyleLevel("TenBang", 1)] }),
        tocTitle("DANH MỤC CHỮ VIẾT TẮT"), abbrTable(),
        ...summary,
      ] },
    { properties: { type: SectionType.NEXT_PAGE, page: { margin: margins, pageNumbers: { start: 1, formatType: NumberFormat.DECIMAL } } },
      footers: pageNumberFooter(), children: body },
  ],
});

Packer.toBuffer(doc).then((buf) => { fs.writeFileSync(OUT, buf); console.log("written", OUT, buf.length); });
