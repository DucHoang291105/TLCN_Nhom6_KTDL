"""Static PNG charts for the Word report (matplotlib, light/print palette).

Palette: validated categorical slots blue/orange/aqua (aqua < 3:1 contrast, so
charts using it carry direct value labels), blue<->red diverging pair with a
grey midpoint for the low/fair/high price position, single-hue blue ramp for
heatmaps. One y-axis per chart, thin bars with a 2 px gap, recessive grid.
"""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.colors import LinearSegmentedColormap  # noqa: E402
from matplotlib.patches import FancyBboxPatch  # noqa: E402

BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
RED, NEUTRAL, MUTED = "#e34948", "#cfcdc6", "#8a8984"
INK, INK_2, GRID = "#0b0b0b", "#52514e", "#e6e5e1"
NEW_FILL, NEW_EDGE, OLD_FILL = "#e8f1fc", BLUE, "#f4f4f2"
SEQUENTIAL = LinearSegmentedColormap.from_list("blue_ramp", ["#f4f8fd", "#9ec5f4", "#3987e5", "#1c5cab", "#0d366b"])

plt.rcParams.update({
    "font.family": "Times New Roman",
    "font.size": 11,
    "axes.edgecolor": GRID,
    "axes.labelcolor": INK_2,
    "axes.titlesize": 12,
    "axes.titlecolor": INK,
    "xtick.color": INK_2,
    "ytick.color": INK_2,
    "axes.grid": True,
    "grid.color": GRID,
    "grid.linewidth": 0.8,
    "axes.axisbelow": True,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "savefig.dpi": 200,
    "savefig.bbox": "tight",
    "figure.facecolor": "white",
})


def vn(value: float, decimals: int = 0) -> str:
    text = f"{value:,.{decimals}f}"
    return text.replace(",", "X").replace(".", ",").replace("X", ".")


def _save(fig, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path)
    plt.close(fig)
    return path


def _box(ax, x, y, w, h, title, lines, new=False):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.02,rounding_size=0.08",
                                facecolor=NEW_FILL if new else OLD_FILL, edgecolor=NEW_EDGE if new else MUTED,
                                linewidth=1.6 if new else 1.0))
    ax.text(x + w / 2, y + h - 0.22, title, ha="center", va="top", fontsize=11, fontweight="bold", color=INK)
    for index, line in enumerate(lines):
        ax.text(x + w / 2, y + h - 0.62 - index * 0.34, line, ha="center", va="top", fontsize=9.5,
                color=BLUE if line.startswith("+") else INK_2)


def architecture(path: Path) -> Path:
    fig, ax = plt.subplots(figsize=(11, 4.6))
    ax.set_xlim(0, 15.6)
    ax.set_ylim(0, 6)
    ax.axis("off")
    _box(ax, 0.1, 1.2, 2.6, 3.6, "Nguồn", ["3 nguồn crawl", "6 nguồn historical", "CSV theo batch"])
    _box(ax, 3.2, 1.2, 3.0, 3.6, "Bronze", ["MinIO + Parquet", "lakehouse-bronze", "195.141 dòng"])
    _box(ax, 6.7, 0.4, 4.0, 5.2, "Silver (Iceberg)", ["listing_observation", "listings_current_27", "listing_history",
                                                       "listing_location", "+ listing_feature", "lakehouse-silver"])
    _box(ax, 11.2, 0.4, 4.3, 5.2, "Gold (Iceberg)", ["+ 9 dimension", "+ fact_listing (dedup)", "+ BQ1: budget trade-off",
                                                     "+ BQ2: price benchmark", "+ BQ3: area substitution",
                                                     "+ DQ KPI, repricing,", "+ market overview"], new=True)
    for x0, x1 in ((2.7, 3.2), (6.2, 6.7), (10.7, 11.2)):
        ax.annotate("", xy=(x1, 3.0), xytext=(x0, 3.0), arrowprops=dict(arrowstyle="-|>", color=INK_2, lw=1.4))
    ax.text(15.5, 0.05, "Viền xanh / dấu + : phần xây dựng trong giai đoạn này", ha="right", fontsize=9, color=BLUE)
    return _save(fig, path)


def star_schema(path: Path, dims: Sequence[tuple[str, str]]) -> Path:
    fig, ax = plt.subplots(figsize=(10, 6.4))
    ax.set_xlim(-6, 6)
    ax.set_ylim(-4.3, 4.3)
    ax.axis("off")
    ax.add_patch(FancyBboxPatch((-1.7, -1.1), 3.4, 2.2, boxstyle="round,pad=0.03,rounding_size=0.1",
                                facecolor=NEW_FILL, edgecolor=BLUE, linewidth=1.8))
    ax.text(0, 0.6, "fact_listing", ha="center", fontsize=13, fontweight="bold", color=INK)
    ax.text(0, 0.05, "1 dòng / tin bán current", ha="center", fontsize=9.5, color=INK_2)
    ax.text(0, -0.4, "giá, diện tích, giá/m², phòng,", ha="center", fontsize=9.5, color=INK_2)
    ax.text(0, -0.78, "cờ đặc điểm, cờ trùng nguồn", ha="center", fontsize=9.5, color=INK_2)
    import math
    for index, (name, grain) in enumerate(dims):
        angle = math.pi / 2 - index * 2 * math.pi / len(dims)
        x, y = 4.4 * math.cos(angle), 3.3 * math.sin(angle)
        ax.plot([0.0 + 1.0 * math.cos(angle), x * 0.82], [0.0 + 0.8 * math.sin(angle), y * 0.82], color=MUTED, lw=1)
        ax.add_patch(FancyBboxPatch((x - 1.35, y - 0.42), 2.7, 0.84, boxstyle="round,pad=0.02,rounding_size=0.08",
                                    facecolor=OLD_FILL, edgecolor=MUTED, linewidth=1))
        ax.text(x, y + 0.08, name, ha="center", fontsize=10.5, fontweight="bold", color=INK)
        ax.text(x, y - 0.25, grain, ha="center", fontsize=8.5, color=INK_2)
    return _save(fig, path)


def feature_heatmap(path: Path, rows: Sequence[dict], labels: dict[str, str]) -> Path:
    flags = ["legal", "furnished", "frontage", "elevator", "car_access"]
    flag_labels = ["Pháp lý", "Nội thất", "Mặt tiền", "Thang máy", "Ô tô vào"]
    rows = [r for r in rows if r["model_category"] != "khong_ro"]
    matrix = [[r[f] * 100 for f in flags] for r in rows]
    fig, ax = plt.subplots(figsize=(8.5, 3.6))
    ax.grid(False)
    image = ax.imshow(matrix, cmap=SEQUENTIAL, aspect="auto", vmin=0, vmax=max(max(row) for row in matrix))
    ax.set_xticks(range(len(flags)), flag_labels)
    ax.set_yticks(range(len(rows)), [f"{labels.get(r['model_category'], r['model_category'])} ({vn(r['n'])})" for r in rows])
    for i, row in enumerate(matrix):
        for j, value in enumerate(row):
            ax.text(j, i, f"{vn(value, 1)}%", ha="center", va="center", fontsize=10,
                    color="white" if value > 0.55 * image.get_clim()[1] else INK)
    ax.tick_params(length=0)
    for spine in ax.spines.values():
        spine.set_visible(False)
    cbar = fig.colorbar(image, ax=ax, fraction=0.03, pad=0.02)
    cbar.outline.set_visible(False)
    cbar.set_label("% title nhắc đặc điểm", color=INK_2)
    return _save(fig, path)


def horizontal_bars(path: Path, labels: Sequence[str], values: Sequence[float], xlabel: str,
                    value_fmt=lambda v: vn(v), color: str = BLUE, annotations: Sequence[str] | None = None,
                    height: float | None = None) -> Path:
    fig, ax = plt.subplots(figsize=(9, height or max(2.6, 0.42 * len(labels) + 0.8)))
    positions = list(range(len(labels)))[::-1]
    ax.barh(positions, values, height=0.62, color=color, edgecolor="white", linewidth=2)
    ax.set_yticks(positions, labels)
    ax.set_xlabel(xlabel)
    ax.grid(axis="y", visible=False)
    limit = max(values) if values else 1
    for pos, value, index in zip(positions, values, range(len(values))):
        text = value_fmt(value) + (f"  {annotations[index]}" if annotations else "")
        ax.text(value + limit * 0.01, pos, text, va="center", fontsize=9.5, color=INK)
    ax.set_xlim(0, limit * 1.28)
    ax.tick_params(axis="y", length=0)
    return _save(fig, path)


def position_stacked(path: Path, categories: Sequence[str], shares: dict[str, Sequence[float]]) -> Path:
    order = [("duoi_p25", "Dưới P25", BLUE), ("p25_p75", "Trong P25–P75", NEUTRAL),
             ("tren_p75", "Trên P75", RED), ("khong_du_du_lieu", "Không đủ dữ liệu", "#f0efec")]
    fig, ax = plt.subplots(figsize=(9, 0.55 * len(categories) + 1.4))
    positions = list(range(len(categories)))[::-1]
    left = [0.0] * len(categories)
    for key, label, color in order:
        values = shares[key]
        ax.barh(positions, values, left=left, height=0.6, color=color, edgecolor="white", linewidth=2, label=label)
        for pos, start, value in zip(positions, left, values):
            if value >= 6:
                ax.text(start + value / 2, pos, f"{vn(value, 0)}%", ha="center", va="center", fontsize=9,
                        color="white" if color in (BLUE, RED) else INK)
        left = [a + b for a, b in zip(left, values)]
    ax.set_yticks(positions, categories)
    ax.set_xlim(0, 100)
    ax.set_xlabel("% tin đại diện")
    ax.grid(axis="y", visible=False)
    ax.tick_params(axis="y", length=0)
    ax.legend(ncol=4, loc="upper center", bbox_to_anchor=(0.5, -0.22), frameon=False, fontsize=9.5)
    return _save(fig, path)


def pareto_scatter(path: Path, points: Sequence[dict], title: str) -> Path:
    fig, ax = plt.subplots(figsize=(9, 5))
    dominated = [p for p in points if not p["is_pareto_efficient"]]
    efficient = sorted((p for p in points if p["is_pareto_efficient"]), key=lambda p: p["price"])
    ax.scatter([p["area"] for p in dominated], [p["price"] / 1e9 for p in dominated], s=26, color=MUTED,
               alpha=0.55, edgecolor="white", linewidth=0.6, label=f"Bị trội ({len(dominated)})")
    ax.scatter([p["area"] for p in efficient], [p["price"] / 1e9 for p in efficient], s=46, color=BLUE,
               edgecolor="white", linewidth=1.2, label=f"Pareto – không bị trội ({len(efficient)})")
    ax.set_xlabel("Diện tích (m²)")
    ax.set_ylabel("Giá chào bán (tỷ đồng)")
    ax.set_title(title, loc="left")
    areas = sorted(p["area"] for p in points)
    if areas:
        ax.set_xlim(0, areas[int(len(areas) * 0.97) - 1] * 1.15)
    ax.legend(frameon=False, loc="lower right")
    return _save(fig, path)


def grouped_bars(path: Path, groups: Sequence[str], series: Sequence[tuple[str, Sequence[float | None]]], ylabel: str,
                 value_fmt=lambda v: vn(v)) -> Path:
    colors = [BLUE, ORANGE, AQUA]
    fig, ax = plt.subplots(figsize=(9.5, 4.6))
    width = 0.8 / len(series)
    for index, (label, values) in enumerate(series):
        xs = [g + (index - (len(series) - 1) / 2) * width for g in range(len(groups))]
        heights = [v or 0 for v in values]
        ax.bar(xs, heights, width=width * 0.92, color=colors[index], edgecolor="white", linewidth=2, label=label)
        for x, value in zip(xs, values):
            if value:
                ax.text(x, value, value_fmt(value), ha="center", va="bottom", fontsize=8.5, color=INK)
    ax.set_xticks(range(len(groups)), groups)
    ax.set_ylabel(ylabel)
    ax.grid(axis="x", visible=False)
    ax.legend(frameon=False, ncol=len(series), loc="upper right")
    return _save(fig, path)
