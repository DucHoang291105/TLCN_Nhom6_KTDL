"""Diagrams and dashboard crops for the TLCN report (numbers from docs/validation at batch 20261117)."""
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, Ellipse
from PIL import Image

REPO = Path(sys.argv[1])
OUT = Path(__file__).resolve().parent / "figs"
OUT.mkdir(exist_ok=True)
plt.rcParams.update({"font.family": "Times New Roman", "font.size": 11})

INK, MUTED, LINE = "#1b2a2a", "#4f5f5f", "#7a8a8a"
FILL = {"src": "#eef2f2", "bronze": "#f3e6d8", "silver": "#e6ecf2", "gold": "#f6efd2", "serve": "#e4f0e8", "ops": "#ece6f3"}


def box(ax, x, y, w, h, title, lines=(), fill="#f2f2f2", title_size=12, line_size=10, edge=LINE, bold=True):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.08", fc=fill, ec=edge, lw=1.2))
    ax.text(x + w / 2, y + h - 0.22, title, ha="center", va="top", fontsize=title_size, fontweight="bold" if bold else "normal", color=INK)
    for i, line in enumerate(lines):
        ax.text(x + w / 2, y + h - 0.62 - i * 0.34, line, ha="center", va="top", fontsize=line_size, color=MUTED)


def arrow(ax, x1, y1, x2, y2, text=None, color=INK):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1), arrowprops=dict(arrowstyle="-|>", lw=1.3, color=color))
    if text:
        ax.text((x1 + x2) / 2, (y1 + y2) / 2 + 0.12, text, ha="center", va="bottom", fontsize=9, color=MUTED)


def save(fig, name):
    fig.savefig(OUT / name, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def architecture():
    fig, ax = plt.subplots(figsize=(13, 7.2))
    ax.set_xlim(0, 13); ax.set_ylim(0, 7.2); ax.axis("off")
    box(ax, 0.1, 3.0, 2.2, 3.2, "Nguồn dữ liệu", ["Batdongsan (CSV xuất tay)", "Guland, Nhadatvui", "(crawler Python)", "6 nguồn historical", "54 CSV · Git LFS"], FILL["src"])
    box(ax, 2.8, 3.0, 2.2, 3.2, "Bronze", ["MinIO · Parquet", "lakehouse-bronze", "theo nguồn / batch", "manifest SHA256", "346.558 dòng"], FILL["bronze"])
    box(ax, 5.5, 3.0, 2.4, 3.2, "Silver", ["Spark 3.5 + Iceberg", "observation 334.820", "current 150.017", "history (SCD2) 167.794", "location · feature"], FILL["silver"])
    box(ax, 8.4, 3.0, 2.2, 3.2, "Gold", ["Iceberg · 30 bảng", "9 dim + fact_listing", "bảng BQ1–BQ3", "mô hình giá", "bảng bằng chứng ev_*"], FILL["gold"])
    box(ax, 11.0, 4.7, 1.9, 1.5, "Trino 483", ["SQL trên Iceberg"], FILL["serve"])
    box(ax, 11.0, 3.0, 1.9, 1.5, "Superset 6", ["5 dashboard", "50 biểu đồ"], FILL["serve"])
    for x1, x2 in ((2.3, 2.8), (5.0, 5.5), (7.9, 8.4), (10.6, 11.0)):
        arrow(ax, x1, 4.6, x2, 4.6)
    arrow(ax, 11.95, 4.7, 11.95, 4.5)
    box(ax, 0.1, 0.4, 6.0, 1.9, "Apache Airflow 3 — DAG lakehouse_pipeline (26 task)", ["Lịch: 22:00 Thứ Ba và Thứ Sáu", "thu thập → Bronze → Silver → kiểm định → Gold → kiểm định", "task lỗi chặn mọi bước sau · lưu bằng chứng (JSON)"], FILL["ops"], line_size=10)
    box(ax, 6.5, 0.4, 3.1, 1.9, "Iceberg REST Catalog", ["catalog SQLite (1 writer)", "tách cửa sổ ghi / đọc"], FILL["ops"])
    box(ax, 10.0, 0.4, 2.9, 1.9, "PostgreSQL 16", ["metadata Airflow", "và Superset"], FILL["ops"])
    for x in (1.2, 3.9, 6.7, 9.5):
        ax.annotate("", xy=(x, 3.0), xytext=(min(x, 5.5), 2.3), arrowprops=dict(arrowstyle="-|>", lw=1, color=LINE, ls="--"))
    ax.text(6.5, 6.75, "Toàn bộ dịch vụ chạy bằng Docker Compose; dữ liệu nghiệp vụ nằm trên MinIO, không nằm trong PostgreSQL", ha="center", fontsize=10, color=MUTED, style="italic")
    save(fig, "hinh_kien_truc.png")


def dag():
    groups = [
        ("acquisition", ["prepare_guland", "prepare_nhadatvui", "wait_batdongsan_csv", "validate_snapshot_manifest"], FILL["src"]),
        ("bronze", ["ingest_snapshots", "audit_bronze"], FILL["bronze"]),
        ("silver", ["iceberg_smoke", "listing_core", "listing_history", "location", "feature", "verify_silver"], FILL["silver"]),
        ("gold (1)", ["dimensions", "fact_listing", "price_benchmark", "price_model", "budget_tradeoff", "area_substitution", "commute_choice"], FILL["gold"]),
        ("gold (2)", ["data_quality", "repricing", "market_overview", "listing_lifecycle", "bq_evidence", "verify_gold"], FILL["gold"]),
        ("kết thúc", ["publish_evidence"], FILL["ops"]),
    ]
    fig, ax = plt.subplots(figsize=(13, 6.4))
    ax.set_xlim(0, 13); ax.set_ylim(0, 6.4); ax.axis("off")
    x, w, gap = 0.1, 1.95, 0.22
    for i, (name, tasks, fill) in enumerate(groups):
        h = 0.62 + len(tasks) * 0.66
        y0 = 6.1 - h
        ax.add_patch(FancyBboxPatch((x, y0), w, h, boxstyle="round,pad=0.02,rounding_size=0.08", fc=fill, ec=LINE, lw=1.2))
        ax.text(x + w / 2, 6.1 - 0.18, name, ha="center", va="top", fontsize=12, fontweight="bold", color=INK)
        for j, t in enumerate(tasks):
            ty = 6.1 - 0.75 - j * 0.66
            ax.add_patch(FancyBboxPatch((x + 0.1, ty - 0.24), w - 0.2, 0.48, boxstyle="round,pad=0.01,rounding_size=0.05", fc="white", ec=LINE, lw=0.8))
            ax.text(x + w / 2, ty, t, ha="center", va="center", fontsize=9.2 if len(t) < 22 else 7.6, color=INK)
            if j < len(tasks) - 1 and name not in ("acquisition",):
                ax.annotate("", xy=(x + w / 2, ty - 0.42), xytext=(x + w / 2, ty - 0.24), arrowprops=dict(arrowstyle="-|>", lw=0.8, color=LINE))
        if i < len(groups) - 1:
            arrow(ax, x + w, 5.5, x + w + gap, 5.5)
        x += w + gap
    ax.text(0.1, 0.15, "acquisition: hai crawler chạy độc lập → chờ CSV Batdongsan → kiểm tra manifest; mỗi nhóm khác chạy tuần tự (max_active_tasks = 1). "
            "Lượt chạy 21/11/2026: 26/26 SUCCESS, khoảng 15 phút.", fontsize=9.5, color=MUTED)
    save(fig, "hinh_dag.png")


def star():
    fig, ax = plt.subplots(figsize=(11, 7.4))
    ax.set_xlim(0, 11); ax.set_ylim(0, 7.4); ax.axis("off")
    cx, cy = 5.5, 3.7
    dims = [("dim_source", "9 nguồn"), ("dim_date", "ngày quan sát / ngày đăng"), ("dim_location", "tỉnh – quận – phường"),
            ("dim_property_category", "5 loại hình + không rõ"), ("dim_price_band", "nhóm tổng giá"), ("dim_area_band", "nhóm diện tích"),
            ("dim_unit_price_band", "nhóm giá/m²"), ("dim_room_band", "nhóm số phòng"), ("dim_dq_status", "PASS / WARN")]
    import math
    for k, (name, desc) in enumerate(dims):
        a = math.pi / 2 - k * 2 * math.pi / len(dims)
        x, y = cx + 4.3 * math.cos(a), cy + 2.95 * math.sin(a)
        ax.plot([cx + 1.35 * math.cos(a), x - 0.0], [cy + 0.75 * math.sin(a), y], color=LINE, lw=1, zorder=1)
        ax.add_patch(FancyBboxPatch((x - 1.15, y - 0.36), 2.3, 0.72, boxstyle="round,pad=0.02,rounding_size=0.06", fc="#f2f4f4", ec=LINE, lw=1, zorder=2))
        ax.text(x, y + 0.1, name, ha="center", va="center", fontsize=10.5, fontweight="bold", color=INK, zorder=3)
        ax.text(x, y - 0.17, desc, ha="center", va="center", fontsize=9, color=MUTED, zorder=3)
    ax.add_patch(FancyBboxPatch((cx - 1.6, cy - 0.95), 3.2, 1.9, boxstyle="round,pad=0.02,rounding_size=0.08", fc=FILL["gold"], ec="#b08a1e", lw=1.8, zorder=2))
    for i, (t, s, b) in enumerate((("fact_listing", 13, True), ("1 dòng / tin bán (134.660)", 9.5, False), ("giá, diện tích, giá/m², số phòng", 9.5, False),
                                   ("5 cờ đặc điểm, cờ nghi trùng", 9.5, False), ("is_dup_representative", 9.5, False))):
        ax.text(cx, cy + 0.65 - i * 0.33, t, ha="center", va="center", fontsize=s, fontweight="bold" if b else "normal", color=INK, zorder=3)
    save(fig, "hinh_star_schema.png")


def usecase():
    fig, ax = plt.subplots(figsize=(12, 7.6))
    ax.set_xlim(0, 12); ax.set_ylim(0, 7.6); ax.axis("off")
    ax.add_patch(FancyBboxPatch((2.4, 0.3), 7.2, 7.0, boxstyle="round,pad=0.02,rounding_size=0.1", fc="white", ec=INK, lw=1.2))
    ax.text(6.0, 7.05, "Hệ thống Data Lakehouse BĐS", ha="center", fontsize=12, fontweight="bold")

    def actor(x, y, name):
        ax.add_patch(plt.Circle((x, y + 0.55), 0.16, fc="white", ec=INK, lw=1.2))
        ax.plot([x, x], [y + 0.39, y - 0.05], color=INK); ax.plot([x - 0.25, x + 0.25], [y + 0.25, y + 0.25], color=INK)
        ax.plot([x, x - 0.2], [y - 0.05, y - 0.4], color=INK); ax.plot([x, x + 0.2], [y - 0.05, y - 0.4], color=INK)
        ax.text(x, y - 0.6, name, ha="center", va="top", fontsize=10.5)

    left = {"Người mua": 5.6, "Môi giới": 3.6, "Nhân viên\nphân tích": 1.5}
    for n, y in left.items():
        actor(1.1, y, n)
    actor(10.9, 4.0, "Kỹ sư dữ liệu\n(vận hành)")
    ucs_left = [("Xem tổng quan thị trường", 6.3), ("BQ1: chọn theo ngân sách", 5.45), ("BQ2: tin đắt hay rẻ so với nhóm", 4.6),
                ("BQ3: khu vực thay thế theo nơi làm/học", 3.75), ("Lọc theo tỉnh, loại hình, ngân sách", 2.9), ("Xem chất lượng dữ liệu", 2.05)]
    ucs_right = [("Nhận / thu thập batch CSV", 6.0), ("Chạy pipeline Airflow", 4.9), ("Kiểm định Silver / Gold", 3.8), ("Dựng lại dashboard", 2.7), ("Nghiệm thu, gắn tag", 1.6)]
    for t, y in ucs_left:
        ax.add_patch(Ellipse((4.55, y), 3.6, 0.62, fc="#eef2f2", ec=INK, lw=1)); ax.text(4.55, y, t, ha="center", va="center", fontsize=9.6)
    for t, y in ucs_right:
        ax.add_patch(Ellipse((7.75, y), 3.0, 0.62, fc="#ece6f3", ec=INK, lw=1)); ax.text(7.75, y, t, ha="center", va="center", fontsize=9.6)
    links = {"Người mua": [6.3, 5.45, 4.6, 3.75, 2.9], "Môi giới": [5.45, 4.6, 3.75, 2.9], "Nhân viên\nphân tích": [6.3, 4.6, 2.9, 2.05]}
    for n, ys in links.items():
        for y in ys:
            ax.plot([1.38, 2.75], [left[n] + 0.25, y], color=LINE, lw=0.7)
    for _, y in ucs_right:
        ax.plot([9.25, 10.62], [y, 4.25], color=LINE, lw=0.7)
    save(fig, "hinh_use_case.png")


def lineage():
    fig, ax = plt.subplots(figsize=(13, 7.0))
    ax.set_xlim(0, 13); ax.set_ylim(0, 7.0); ax.axis("off")
    col = {
        0: [("CSV snapshot", "48 file · 316.241 dòng"), ("CSV historical", "6 file · 30.317 dòng")],
        1: [("Bronze Parquet", "346.558 dòng"), ("snapshot_manifest.json", "SHA256 54 file")],
        2: [("listing_observation", "334.820 · grain source_id×batch"), ("listing_dq_quarantine", "664 bản ghi REJECT"),
            ("silver_listings_current_27", "150.017 · mới nhất (từ observation)"), ("listing_history", "167.794 phiên bản SCD2 (từ observation)")],
        3: [("listing_location", "tỉnh/quận/phường, tọa độ"), ("listing_feature", "loại hình + 5 cờ (luật v5)")],
        4: [("dim_* (9) + fact_listing", "134.660 tin bán"), ("bảng BQ1–BQ3", "benchmark, mô hình, thay thế"),
            ("lifecycle, price_change", "150.017 · 3.021 sự kiện"), ("ev_* bằng chứng", "6 bảng cho dashboard")],
    }
    fills = [FILL["src"], FILL["bronze"], FILL["silver"], FILL["silver"], FILL["gold"]]
    heads = ["Đầu vào", "Bronze", "Silver (lõi)", "Silver (làm giàu)", "Gold"]
    xs = [0.1, 2.65, 5.2, 7.75, 10.3]
    centers = {}
    for c, items in col.items():
        ax.text(xs[c] + 1.2, 6.75, heads[c], ha="center", fontsize=12, fontweight="bold", color=INK)
        for i, (t, s) in enumerate(items):
            y = 5.7 - i * 1.45
            ax.add_patch(FancyBboxPatch((xs[c], y), 2.4, 0.95, boxstyle="round,pad=0.02,rounding_size=0.06", fc=fills[c], ec=LINE, lw=1))
            ax.text(xs[c] + 1.2, y + 0.62, t, ha="center", va="center", fontsize=10, fontweight="bold", color=INK)
            ax.text(xs[c] + 1.2, y + 0.3, s, ha="center", va="center", fontsize=8.8, color=MUTED)
            centers[(c, i)] = (xs[c], xs[c] + 2.4, y + 0.47)
    edges = [((0, 0), (1, 0)), ((0, 1), (1, 0)), ((0, 0), (1, 1)), ((1, 0), (2, 0)), ((1, 0), (2, 1)),
             ((2, 2), (3, 0)), ((2, 2), (3, 1)), ((3, 0), (4, 0)), ((3, 1), (4, 0)), ((3, 1), (4, 1)), ((2, 3), (4, 2))]
    for (a, b) in edges:
        (ax1, bx1, ay), (ax2, _bx2, by) = centers[a], centers[b]
        if a[0] == b[0]:
            ax.annotate("", xy=(ax1 + 0.1, by + 0.48), xytext=(ax1 + 0.1, ay - 0.48), arrowprops=dict(arrowstyle="-|>", lw=0.8, color=LINE, connectionstyle="arc3,rad=0.35"))
        else:
            ax.annotate("", xy=(ax2, by), xytext=(bx1, ay), arrowprops=dict(arrowstyle="-|>", lw=0.9, color=LINE))
    save(fig, "hinh_luong_bang.png")


def crops():
    plan = {"01_tlcn-tong-quan": [(0, 1150), (1150, 2160)], "02_tlcn-bq1": [(0, 1400), (1400, 2150)],
            "03_tlcn-bq2": [(0, 1250), (1250, 2300)], "04_tlcn-bq3": [(0, 1575), (1575, 3090)], "05_tlcn-chat-luong": [(0, 1650)],
            "08_loc_bq2_ha-noi": [(0, 700)]}
    for name, parts in plan.items():
        im = Image.open(REPO / "docs/evidence/personC_20261118" / f"{name}.png").convert("RGB")
        for k, (y1, y2) in enumerate(parts, 1):
            part = im.crop((240, y1, 1600, min(y2, im.size[1])))
            part.save(OUT / f"dash_{name}_{k}.png", optimize=True)


if __name__ == "__main__":
    architecture(); dag(); star(); usecase(); lineage(); crops()
    print(sorted(p.name for p in OUT.iterdir()))


def activity():
    """UML activity diagram of one pipeline run."""
    from matplotlib.patches import Polygon
    fig, ax = plt.subplots(figsize=(10, 12.5))
    ax.set_xlim(0, 10); ax.set_ylim(0, 12.5); ax.axis("off")
    cx = 3.6
    ax.add_patch(plt.Circle((cx, 12.1), 0.16, color=INK))
    steps = [
        (11.3, "Airflow kích hoạt theo lịch (22:00 thứ Ba, thứ Sáu) hoặc chạy tay với batch_id"),
        (10.3, "prepare_guland, prepare_nhadatvui: dùng lại batch hoặc crawl"),
        (9.3, "wait_batdongsan_csv: chờ tệp CSV xuất thủ công (tối đa 2 giờ)"),
        (8.3, "validate_snapshot_manifest: SHA256 54 tệp, kiểm schema và số dòng"),
        (6.9, "Bronze: nạp batch mới thành Parquet, kiểm tra trạng thái"),
        (5.9, "Silver: chuẩn hóa + DQ, lịch sử SCD2, vị trí, đặc điểm"),
        (4.5, "Gold: dimension, fact, BQ1–BQ3, mô hình giá, lifecycle, bằng chứng"),
        (3.1, "publish_evidence: xuất trạng thái 26 task ra JSON"),
    ]
    for y, text in steps:
        ax.add_patch(FancyBboxPatch((cx - 2.9, y - 0.3), 5.8, 0.6, boxstyle="round,pad=0.02,rounding_size=0.25", fc="#eef2f2", ec=INK, lw=1))
        ax.text(cx, y, text, ha="center", va="center", fontsize=9.6)
    decisions = [(7.6, "manifest hợp lệ?"), (5.2, "verify_silver đạt?"), (3.8, "verify_gold đạt?")]
    for y, text in decisions:
        ax.add_patch(Polygon([[cx, y + 0.32], [cx + 1.3, y], [cx, y - 0.32], [cx - 1.3, y]], closed=True, fc="white", ec=INK, lw=1))
        ax.text(cx, y, text, ha="center", va="center", fontsize=9)
        ax.annotate("", xy=(7.4, y), xytext=(cx + 1.3, y), arrowprops=dict(arrowstyle="-|>", lw=1, color="#a33"))
        ax.text(cx + 1.5, y + 0.1, "không", fontsize=9, color="#a33")
        ax.text(cx + 0.12, y - 0.48, "có", fontsize=9, color=INK)
    ax.add_patch(FancyBboxPatch((7.4, 3.5), 2.4, 4.4, boxstyle="round,pad=0.02,rounding_size=0.2", fc="#fbecec", ec="#a33", lw=1))
    ax.text(8.6, 5.7, "Task lỗi\n\ncác task sau:\nupstream_failed\n\nkhông công bố\nkết quả mới", ha="center", va="center", fontsize=9.6, color="#7a1f1f")
    ys = [12.1, 11.3, 10.3, 9.3, 8.3, 7.6, 6.9, 5.9, 5.2, 4.5, 3.8, 3.1]
    for a, b in zip(ys, ys[1:]):
        top = a - (0.16 if a == 12.1 else 0.32 if a in (7.6, 5.2, 3.8) else 0.3)
        bot = b + (0.32 if b in (7.6, 5.2, 3.8) else 0.3)
        ax.annotate("", xy=(cx, bot), xytext=(cx, top), arrowprops=dict(arrowstyle="-|>", lw=1, color=INK))
    ax.annotate("", xy=(cx, 2.45), xytext=(cx, 2.8), arrowprops=dict(arrowstyle="-|>", lw=1, color=INK))
    ax.add_patch(plt.Circle((cx, 2.25), 0.2, fc="white", ec=INK, lw=1.2)); ax.add_patch(plt.Circle((cx, 2.25), 0.12, color=INK))
    ax.text(cx, 1.6, "Chuyển cửa sổ đọc → provision Superset → dashboard", ha="center", fontsize=9.6, color=MUTED, style="italic")
    save(fig, "hinh_hoat_dong.png")


if __name__ == "__main__":
    activity()
