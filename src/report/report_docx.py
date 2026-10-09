"""Thin python-docx layer with the formatting rules of the university report.

A4, margins 3/2/2/2 cm, Times New Roman 13, line spacing 1.3, built-in
Heading 1-3 (so Word can build the table of contents), numbered captions
("Hình c.n." below figures, "Bảng c.n." above tables), shaded table headers,
right-aligned numeric columns, grey Consolas code boxes and page numbers.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Iterable, Sequence

from docx import Document
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_BREAK
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm, Pt, RGBColor

BODY_FONT = "Times New Roman"
CODE_FONT = "Consolas"
HEADER_FILL = "D9E2F3"
CODE_FILL = "F2F2F2"
TEXT_WIDTH_CM = 16.0  # A4 21 cm - 3 cm - 2 cm


def _set_font(target, name: str) -> None:
    """Set a font on a style or run, overriding theme fonts for every script."""

    target.font.name = name
    element = target.element if hasattr(target, "element") else target._element
    rpr = element.get_or_add_rPr()
    fonts = rpr.find(qn("w:rFonts"))
    if fonts is None:
        fonts = OxmlElement("w:rFonts")
        rpr.insert(0, fonts)
    for attr in ("w:asciiTheme", "w:hAnsiTheme", "w:eastAsiaTheme", "w:cstheme"):
        fonts.attrib.pop(qn(attr), None)
    for attr in ("w:ascii", "w:hAnsi", "w:eastAsia", "w:cs"):
        fonts.set(qn(attr), name)


def _shade(cell, fill: str) -> None:
    tc_pr = cell._element.get_or_add_tcPr()
    shading = OxmlElement("w:shd")
    shading.set(qn("w:val"), "clear")
    shading.set(qn("w:color"), "auto")
    shading.set(qn("w:fill"), fill)
    tc_pr.append(shading)


def _field(paragraph, instruction: str, placeholder: str = "") -> None:
    run = paragraph.add_run()
    begin = OxmlElement("w:fldChar")
    begin.set(qn("w:fldCharType"), "begin")
    run._element.append(begin)
    instr = OxmlElement("w:instrText")
    instr.set(qn("xml:space"), "preserve")
    instr.text = instruction
    run._element.append(instr)
    separate = OxmlElement("w:fldChar")
    separate.set(qn("w:fldCharType"), "separate")
    run._element.append(separate)
    paragraph.add_run(placeholder)
    end_run = paragraph.add_run()
    end = OxmlElement("w:fldChar")
    end.set(qn("w:fldCharType"), "end")
    end_run._element.append(end)


def _add_inline(paragraph, text: str, italic: bool = False) -> None:
    """Add runs for **bold** and `code` markup; `code` may sit inside **bold**."""

    for token in re.split(r"(\*\*[^*]+\*\*)", text):
        if not token:
            continue
        bold = token.startswith("**") and token.endswith("**")
        inner = token[2:-2] if bold else token
        for part in re.split(r"(`[^`]+`)", inner):
            if not part:
                continue
            if part.startswith("`"):
                run = paragraph.add_run(part[1:-1])
                _set_font(run, CODE_FONT)
                run.font.size = Pt(11)
            else:
                run = paragraph.add_run(part)
            run.bold = bold or None
            run.italic = italic or None


class ReportDocument:
    def __init__(self) -> None:
        self.doc = Document()
        self.chapter = 0
        self.figures = 0
        self.tables = 0
        self._setup_page()
        self._setup_styles()

    # ------------------------------------------------------------------ setup
    def _setup_page(self) -> None:
        section = self.doc.sections[0]
        section.page_width, section.page_height = Cm(21), Cm(29.7)
        section.left_margin, section.right_margin = Cm(3), Cm(2)
        section.top_margin, section.bottom_margin = Cm(2), Cm(2)
        section.different_first_page_header_footer = True  # no number on the cover
        footer = section.footer.paragraphs[0]
        footer.alignment = WD_ALIGN_PARAGRAPH.CENTER
        _field(footer, "PAGE", "1")
        settings = self.doc.settings.element
        update = OxmlElement("w:updateFields")
        update.set(qn("w:val"), "true")
        settings.append(update)

    def _setup_styles(self) -> None:
        styles = self.doc.styles
        normal = styles["Normal"]
        _set_font(normal, BODY_FONT)
        normal.font.size = Pt(13)
        normal.paragraph_format.line_spacing = 1.3
        normal.paragraph_format.space_after = Pt(6)
        for level, size in ((1, 16), (2, 14), (3, 13)):
            style = styles[f"Heading {level}"]
            _set_font(style, BODY_FONT)
            style.font.size = Pt(size)
            style.font.bold = True
            style.font.italic = False
            style.font.color.rgb = RGBColor(0, 0, 0)
            style.paragraph_format.space_before = Pt(12 if level > 1 else 0)
            style.paragraph_format.space_after = Pt(6)
            style.paragraph_format.keep_with_next = True
            style.paragraph_format.line_spacing = 1.3
        caption = styles["Caption"]
        _set_font(caption, BODY_FONT)
        caption.font.size = Pt(12)
        caption.font.italic = True
        caption.font.bold = False
        caption.font.color.rgb = RGBColor(0, 0, 0)
        caption.paragraph_format.alignment = WD_ALIGN_PARAGRAPH.CENTER
        caption.paragraph_format.space_after = Pt(8)
        for name in ("List Bullet",):
            _set_font(styles[name], BODY_FONT)
            styles[name].font.size = Pt(13)

    # ----------------------------------------------------------------- blocks
    def cover(self, lines: Sequence[tuple[str, int, bool]]) -> None:
        for text, size, bold in lines:
            paragraph = self.doc.add_paragraph()
            paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
            run = paragraph.add_run(text)
            run.font.size = Pt(size)
            run.bold = bold
        self.page_break()

    def page_break(self) -> None:
        self.doc.add_paragraph().add_run().add_break(WD_BREAK.PAGE)

    def toc(self) -> None:
        title = self.doc.add_paragraph()
        title.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run = title.add_run("MỤC LỤC")
        run.bold = True
        run.font.size = Pt(16)
        _field(self.doc.add_paragraph(), 'TOC \\o "1-3" \\h \\z \\u', "Nhấn F9 (hoặc chuột phải → Update Field) để cập nhật mục lục.")
        note = self.doc.add_paragraph()
        note_run = note.add_run("Ghi chú: mục lục là trường tự động; nhấn F9 để cập nhật sau khi mở tài liệu.")
        note_run.italic = True
        note_run.font.size = Pt(11)

    def h1(self, text: str, numbered: bool = True) -> None:
        if numbered:
            self.chapter += 1
            self.figures = self.tables = 0
            text = f"CHƯƠNG {self.chapter}. {text.upper()}"
        paragraph = self.doc.add_heading(text, level=1)
        paragraph.paragraph_format.page_break_before = True

    def h2(self, text: str) -> None:
        self.doc.add_heading(text, level=2)

    def h3(self, text: str) -> None:
        self.doc.add_heading(text, level=3)

    def para(self, text: str, italic: bool = False, align_justify: bool = True) -> None:
        """Paragraph with **bold** and `code` inline markup."""

        paragraph = self.doc.add_paragraph()
        if align_justify:
            paragraph.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
        _add_inline(paragraph, text, italic)

    def bullets(self, items: Iterable[str]) -> None:
        for item in items:
            paragraph = self.doc.add_paragraph(style="List Bullet")
            paragraph.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
            _add_inline(paragraph, item)

    def steps(self, items: Sequence[tuple[str, str]]) -> None:
        """Numbered steps 'Bước n. <title>: <detail>' (manual numbering, never restarts wrongly)."""

        for index, (title, detail) in enumerate(items, start=1):
            self.para(f"**Bước {index}. {title}.** {detail}")

    def code(self, text: str) -> None:
        table = self.doc.add_table(rows=1, cols=1)
        table.alignment = WD_TABLE_ALIGNMENT.CENTER
        cell = table.rows[0].cells[0]
        cell.width = Cm(TEXT_WIDTH_CM)
        _shade(cell, CODE_FILL)
        lines = text.rstrip("\n").split("\n")
        cell.paragraphs[0].text = ""
        for index, line in enumerate(lines):
            paragraph = cell.paragraphs[0] if index == 0 else cell.add_paragraph()
            paragraph.paragraph_format.line_spacing = 1.0
            paragraph.paragraph_format.space_after = Pt(0)
            run = paragraph.add_run(line if line else " ")
            _set_font(run, CODE_FONT)
            run.font.size = Pt(10)
        self.doc.add_paragraph().paragraph_format.space_after = Pt(0)

    def table(self, caption: str, headers: Sequence[str], rows: Sequence[Sequence[object]],
              numeric: Iterable[int] = (), widths_cm: Sequence[float] | None = None, font_size: int = 11,
              keep_together: bool = False) -> None:
        self.tables += 1
        self.doc.add_paragraph(f"Bảng {self.chapter}.{self.tables}. {caption}", style="Caption").paragraph_format.keep_with_next = True
        numeric = set(numeric)
        table = self.doc.add_table(rows=1, cols=len(headers))
        table.style = "Table Grid"
        table.alignment = WD_TABLE_ALIGNMENT.CENTER
        table.autofit = False
        widths = widths_cm or [TEXT_WIDTH_CM / len(headers)] * len(headers)
        header_row = table.rows[0]
        tr_pr = header_row._tr.get_or_add_trPr()
        repeat = OxmlElement("w:tblHeader")
        repeat.set(qn("w:val"), "true")
        tr_pr.append(repeat)
        for index, header in enumerate(headers):
            cell = header_row.cells[index]
            _shade(cell, HEADER_FILL)
            self._cell_text(cell, str(header), font_size, bold=True,
                            align=WD_ALIGN_PARAGRAPH.CENTER)
        for values in rows:
            cells = table.add_row().cells
            for index, value in enumerate(values):
                self._cell_text(cells[index], "" if value is None else str(value), font_size,
                                align=WD_ALIGN_PARAGRAPH.RIGHT if index in numeric else WD_ALIGN_PARAGRAPH.LEFT)
        for row_index, row in enumerate(table.rows):
            tr_pr = row._tr.get_or_add_trPr()
            cant_split = OxmlElement("w:cantSplit")
            tr_pr.append(cant_split)
            for index, cell in enumerate(row.cells):
                cell.width = Cm(widths[index])
                # Small tables stay on one page: chain every row to the next.
                if keep_together and row_index < len(table.rows) - 1:
                    for paragraph in cell.paragraphs:
                        paragraph.paragraph_format.keep_with_next = True
        self.doc.add_paragraph().paragraph_format.space_after = Pt(0)

    @staticmethod
    def _cell_text(cell, text: str, size: int, bold: bool = False, align=WD_ALIGN_PARAGRAPH.LEFT) -> None:
        paragraph = cell.paragraphs[0]
        paragraph.alignment = align
        paragraph.paragraph_format.line_spacing = 1.0
        paragraph.paragraph_format.space_after = Pt(0)
        run = paragraph.add_run(text)
        run.font.size = Pt(size)
        run.bold = bold

    def figure(self, path: Path, caption: str, width_cm: float = 15.5) -> None:
        self.figures += 1
        paragraph = self.doc.add_paragraph()
        paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
        paragraph.paragraph_format.keep_with_next = True
        paragraph.add_run().add_picture(str(path), width=Cm(width_cm))
        self.doc.add_paragraph(f"Hình {self.chapter}.{self.figures}. {caption}", style="Caption")

    def appendix(self, text: str) -> None:
        paragraph = self.doc.add_heading(text, level=1)
        paragraph.paragraph_format.page_break_before = True
        self.chapter = text.split(".")[0].replace("PHỤ LỤC", "").strip() or self.chapter
        self.figures = self.tables = 0

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.doc.save(str(path))
