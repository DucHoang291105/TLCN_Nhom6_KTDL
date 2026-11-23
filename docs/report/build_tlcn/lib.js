// Formatting helpers that follow the faculty template (Mau-5-TrinhBay-KLTN):
// A4, margins 2/2/3/2 cm, Times New Roman, 1.5 lines, body 13 pt justified,
// chapter 14 pt bold upper centre, level-1 13 bold upper, level-2 13 bold,
// level-3 13 italic, table text 12, table name 11 bold above, figure name 11
// bold below, table note 10 italic, references 11.
const fs = require("fs");
const path = require("path");
const d = require("docx");
const { Paragraph, TextRun, HeadingLevel, AlignmentType, Table, TableRow, TableCell, WidthType, BorderStyle,
  ShadingType, ImageRun, PageBreak, VerticalAlign, TableLayoutType } = d;

const FONT = "Times New Roman";
const CM = 567; // twips per cm
const TEXT_WIDTH = Math.round((21 - 3 - 2) * CM); // 9072 twips
const FIG_DIR = path.join(__dirname, "figs");
const SPACING = { line: 360, before: 0, after: 120 };

// **bold**, *italic*, `code` inline markup
function runs(text, base = {}) {
  const out = [];
  const re = /(\*\*[^*]+\*\*|\*[^*]+\*|`[^`]+`)/g;
  let last = 0, m;
  while ((m = re.exec(text)) !== null) {
    if (m.index > last) out.push(new TextRun({ text: text.slice(last, m.index), font: FONT, ...base }));
    const tok = m[0];
    if (tok.startsWith("**")) out.push(new TextRun({ text: tok.slice(2, -2), font: FONT, bold: true, ...base }));
    else if (tok.startsWith("`")) out.push(new TextRun({ text: tok.slice(1, -1), font: "Consolas", size: base.size ? base.size - 4 : 22, ...base, ...{ font: "Consolas" } }));
    else out.push(new TextRun({ text: tok.slice(1, -1), font: FONT, italics: true, ...base }));
    last = m.index + tok.length;
  }
  if (last < text.length) out.push(new TextRun({ text: text.slice(last), font: FONT, ...base }));
  return out;
}

const P = (text, opts = {}) => new Paragraph({
  children: runs(text, opts.run || {}),
  alignment: opts.align || AlignmentType.JUSTIFIED,
  spacing: { ...SPACING, ...(opts.spacing || {}) },
  indent: opts.noIndent ? undefined : { firstLine: Math.round(1 * CM) },
  keepNext: opts.keepNext,
  pageBreakBefore: opts.pageBreakBefore,
});

// Chapter / unnumbered part title (MỞ ĐẦU, CHƯƠNG n, KẾT LUẬN …) – Heading 1, new page
const H1 = (text) => new Paragraph({ heading: HeadingLevel.HEADING_1, pageBreakBefore: true, children: [new TextRun({ text: text.toUpperCase(), font: FONT })] });
const H2 = (text) => new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun({ text: text.toUpperCase(), font: FONT })] });
const H3 = (text) => new Paragraph({ heading: HeadingLevel.HEADING_3, children: [new TextRun({ text, font: FONT })] });
const H4 = (text) => new Paragraph({ heading: HeadingLevel.HEADING_4, children: [new TextRun({ text, font: FONT })] });

const bullets = (items, ref = "bullet") => items.map((t) => new Paragraph({
  children: runs(t), numbering: { reference: ref, level: 0 }, alignment: AlignmentType.JUSTIFIED, spacing: { ...SPACING, after: 60 },
}));
const numbered = (items, ref) => items.map((t) => new Paragraph({
  children: runs(t), numbering: { reference: ref, level: 0 }, alignment: AlignmentType.JUSTIFIED, spacing: { ...SPACING, after: 60 },
}));

const border = { style: BorderStyle.SINGLE, size: 4, color: "808080" };
const borders = { top: border, bottom: border, left: border, right: border };

function cell(text, width, head, align) {
  return new TableCell({
    borders, width: { size: width, type: WidthType.DXA }, verticalAlign: VerticalAlign.CENTER,
    shading: head ? { fill: "E7ECEC", type: ShadingType.CLEAR, color: "auto" } : undefined,
    margins: { top: 50, bottom: 50, left: 90, right: 90 },
    children: String(text).split("\n").map((line) => new Paragraph({
      children: runs(line, { size: 24, bold: head || undefined }),
      alignment: head ? AlignmentType.CENTER : (align || AlignmentType.LEFT),
      spacing: { line: 276, before: 0, after: 0 },
    })),
  });
}

let tableNo = {}, figNo = {};
let chapter = "0";
const setChapter = (c) => { chapter = c; };
const nextNo = (store) => { store[chapter] = (store[chapter] || 0) + 1; return chapter === "0" ? `${store[chapter]}` : `${chapter}.${store[chapter]}`; };

// T(caption, header[], rows[][], widthsInCm[], {note, numAlign: [colIdx]})
function T(caption, header, rows, widthsCm, opts = {}) {
  const total = widthsCm.reduce((a, b) => a + b, 0);
  const widths = widthsCm.map((w) => Math.round((w / total) * TEXT_WIDTH));
  widths[widths.length - 1] = TEXT_WIDTH - widths.slice(0, -1).reduce((a, b) => a + b, 0);
  const no = nextNo(tableNo);
  const right = new Set(opts.right || []);
  const out = [new Paragraph({ style: "TenBang", keepNext: true, children: [new TextRun({ text: `Bảng ${no}. ${caption}`, font: FONT })] })];
  out.push(new Table({
    width: { size: TEXT_WIDTH, type: WidthType.DXA }, columnWidths: widths, alignment: AlignmentType.CENTER,
    // Fixed layout: Word keeps the given column widths instead of widening a column for a long file name.
    layout: TableLayoutType.FIXED,
    rows: [
      new TableRow({ tableHeader: true, children: header.map((h, i) => cell(h, widths[i], true)) }),
      ...rows.map((r) => new TableRow({ cantSplit: true, children: r.map((v, i) => cell(v, widths[i], false, right.has(i) ? AlignmentType.RIGHT : undefined)) })),
    ],
  }));
  if (opts.note) out.push(new Paragraph({ style: "ChuThichBang", children: runs(opts.note, { size: 20, italics: true }) }));
  else out.push(new Paragraph({ spacing: { before: 0, after: 120 }, children: [] }));
  return out;
}

// F(caption, file, widthCm)
function F(caption, file, widthCm = 16) {
  const full = path.join(FIG_DIR, file);
  const buf = fs.readFileSync(full);
  const w = buf.readUInt32BE(16), h = buf.readUInt32BE(20); // PNG IHDR
  const px = Math.round(widthCm * 37.795); // 96 dpi
  const no = nextNo(figNo);
  return [
    new Paragraph({ alignment: AlignmentType.CENTER, keepNext: true, spacing: { before: 120, after: 60 },
      children: [new ImageRun({ type: "png", data: buf, transformation: { width: px, height: Math.round(px * h / w) },
        altText: { title: caption, description: caption, name: file } })] }),
    new Paragraph({ style: "TenHinh", children: [new TextRun({ text: `Hình ${no}. ${caption}`, font: FONT })] }),
  ];
}

const pageBreak = () => new Paragraph({ children: [new PageBreak()] });
const blank = (n = 1) => Array.from({ length: n }, () => new Paragraph({ children: [], spacing: { before: 0, after: 0 } }));
const center = (text, size = 26, extra = {}) => new Paragraph({
  alignment: AlignmentType.CENTER, spacing: { line: 360, before: 0, after: 60 }, children: runs(text, { size, ...extra }),
});

module.exports = { d, FONT, CM, TEXT_WIDTH, runs, P, H1, H2, H3, H4, bullets, numbered, T, F, pageBreak, blank, center, setChapter, figNo, tableNo };
