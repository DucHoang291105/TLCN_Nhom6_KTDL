"""Chapters 1-8 of the progress report (content), built from pipeline evidence.

Separated from build_report.py (setup, cover, appendices) to keep each file
readable. Every number comes from docs/validation/*.json or report_data.json.
"""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from src.gold.gold_rules import (
    DUP_AREA_TOLERANCE, DUP_MIN_TITLE_JACCARD, DUP_PRICE_TOLERANCE, DUP_SENSITIVITY, MAX_FLAG_SHARE_GAP,
    MIN_PEER_GROUP_SIZE, MIN_SUBSTITUTION_SIMILARITY,
)
from src.report import report_charts as charts
from src.report.report_docx import ReportDocument

POSITION_LABELS = {
    "duoi_p25": "Dưới P25", "p25_p75": "Trong P25–P75", "tren_p75": "Trên P75", "khong_du_du_lieu": "Không đủ dữ liệu",
}
POSITION_ORDER = ("duoi_p25", "p25_p75", "tren_p75", "khong_du_du_lieu")
METHOD_LABELS = {
    "CATEGORY_NAME": "Nhãn nguồn", "ENDPOINT_CONTEXT": "Mặc định endpoint", "TITLE_OVER_ENDPOINT": "Title thay mặc định endpoint",
    "TITLE_FALLBACK": "Đọc title (nhãn thiếu/chung)", "UNMAPPED": "Không đủ bằng chứng",
}
EVIDENCE_LABELS = {
    "SOURCE_STRUCTURED": "Taxonomy của site", "SOURCE_LABEL": "Nhãn/tag của site", "ENDPOINT_CONTEXT": "Chỉ endpoint crawl", "NONE": "Không có",
}


def n(value) -> str:
    return charts.vn(float(value or 0), 0)


def dec(value, digits: int = 1) -> str:
    return "–" if value is None else charts.vn(float(value), digits)


def pct(value, digits: int = 1) -> str:
    return "–" if value is None else charts.vn(float(value) * 100, digits) + "%"


def ty(value) -> str:
    return "–" if value is None else charts.vn(float(value) / 1e9, 2) + " tỷ"


def trm2(value) -> str:
    return "–" if value is None else charts.vn(float(value) / 1e6, 1)


def yes(flag) -> str:
    return "Có" if flag else ""


def dedup_review(sample: Path) -> tuple[Counter, int]:
    """Verdicts from dedup_pair_review.csv for the pairs in the current sample."""

    review_path = sample.with_name("dedup_pair_review.csv")
    if not sample.exists():
        return Counter(), 0
    pairs = [(r["left_source_id"], r["right_source_id"]) for r in csv.DictReader(sample.open(encoding="utf-8-sig"))]
    reviewed = {}
    if review_path.exists():
        reviewed = {(r["left_source_id"], r["right_source_id"]): r["verdict"] for r in csv.DictReader(review_path.open(encoding="utf-8-sig"))}
    return Counter(reviewed.get(pair, "chưa đánh giá") for pair in pairs), len(pairs)


def write_body(r: ReportDocument, ev: dict[str, Any], data: dict[str, Any], tests: dict[str, Any], figures: Path,
               snippet, category_labels: dict[str, str], bands) -> None:
    feature, sv, dims_summary, fact = ev["feature"], ev["silver_verify"], ev["dims"], ev["fact"]
    gv, bench, budget, sub = ev["gold_verify"], ev["benchmark"], ev["budget"], ev["substitution"]
    dq, repricing, overview, det, loc, before = ev["dq"], ev["repricing"], ev["overview"], ev["determinism"], ev["location"], ev["before"]
    content_ok = det.get("status") == "PASS" and "content" in det.get("method", "")

    # ================================================================ CHƯƠNG 1
    r.h1("Tổng quan")
    r.h2("1.1. Mục tiêu giai đoạn")
    r.para(
        "Ở tuần 5, nhóm đã hoàn thành Silver Data Foundation: 9 nguồn tin đăng (3 nguồn crawl, 6 nguồn historical) "
        "được chuẩn hóa về schema 27 cột, kiểm tra chất lượng, khử trùng theo batch và lưu dưới dạng bảng Apache Iceberg. "
        "Giai đoạn này bổ sung các biến phân tích (loại hình được gom nhóm, đặc điểm nêu trong title), tổ chức Gold theo "
        "star schema và xây các bảng trả lời ba câu hỏi nghiệp vụ."
    )
    r.bullets([
        "**Bước 1 – `listing_feature` (Silver):** nhóm loại hình kèm nguồn gốc bằng chứng, cờ đặc điểm đọc từ title.",
        "**Bước 2 – Gold nền:** 9 dimension và `fact_listing`, có gắn cờ **nghi trùng** giữa các nguồn và cờ xung đột vị trí.",
        "**Bước 3 – Gold theo Business Question:** bảng cho BQ1–BQ3 và các bảng hỗ trợ (chất lượng dữ liệu, đổi giá, tổng quan).",
        "**Đợt rà soát (Chương 7):** sửa các lỗi phân loại, vị trí, BQ3, dedup và nâng mức kiểm chứng theo nhận xét ngày 09/10/2026.",
    ])
    r.para(
        "Hai quy ước dùng xuyên suốt: giá là **giá chào bán**, không phải giá giao dịch; bảng \"current\" là **bản ghi mới nhất đã "
        "quan sát** của mỗi tin, không xác nhận tin còn đang hiển thị trên site."
    )
    r.h2("1.2. Business Questions")
    r.table("Ba Business Question (thống nhất với docs/business/problem_definition.md)", ["Mã", "Câu hỏi", "Bảng Gold"], [
        ["BQ1", "Trong cùng một mức ngân sách, người mua đánh đổi những gì về vị trí, diện tích, số phòng và đặc điểm? Trong khu vực đã chọn, tin nào không bị tin khác trội hơn?",
         "agg_budget_tradeoff, fact_budget_pareto"],
        ["BQ2", "Tin đăng nào có giá/m² dưới P25, trong P25–P75 hay trên P75 của nhóm bất động sản tương đồng?",
         "agg_peer_group_benchmark, fact_listing_price_assessment"],
        ["BQ3", "Nếu không mua được ở khu vực mong muốn, quận nào cùng tỉnh có nhóm bất động sản tương tự với chi phí thấp hơn cho cùng diện tích?",
         "agg_area_substitution"],
    ], widths_cm=[1.4, 9.4, 5.2])
    r.h2("1.3. Kiến trúc và phần được bổ sung")
    r.para(
        "Mỗi tầng Medallion có một bucket MinIO riêng: `lakehouse-bronze` (Parquet), `lakehouse-silver` và `lakehouse-gold` "
        "(bảng Iceberg). Spark 3.5 thực hiện biến đổi; Trino đọc các bảng Gold qua Iceberg REST Catalog."
    )
    r.figure(charts.architecture(figures / "architecture.png"), "Kiến trúc Bronze → Silver → Gold; phần viền xanh là phần xây dựng trong giai đoạn này")
    r.h2("1.4. Tóm tắt các hạng mục")
    done = "Hoàn thành"
    r.table("Trạng thái các hạng mục", ["Hạng mục", "File chính", "Bảng output", "Trạng thái"], [
        ["Silver feature", "build_listing_feature.py", "listing_feature", done],
        ["9 dimension", "build_dimensions.py", "dim_*", done],
        ["Fact, nghi trùng giữa nguồn", "build_fact_listing.py", "fact_listing", "Hoàn thành (gắn cờ theo luật)"],
        ["3a – BQ2 benchmark mô tả", "build_price_benchmark.py", "agg_peer_group_benchmark, fact_listing_price_assessment", done],
        ["3b – BQ1 trade-off, Pareto", "build_budget_tradeoff.py", "agg_budget_tradeoff, fact_budget_pareto", done],
        ["3c – BQ3 khu vực thay thế", "build_area_substitution.py", "agg_area_substitution", done],
        ["3d – Data Quality KPI", "build_data_quality.py", "agg_dq_kpi", done],
        ["3e – Lịch sử đổi giá", "build_repricing.py", "fact_listing_price_change", done],
        ["3f – Tổng quan thị trường", "build_market_overview.py", "agg_market_overview", done],
        ["Rebuild 2 lần, so nội dung", "check_deterministic_rebuild.ps1", "deterministic_rebuild.json",
         done if content_ok else "Một phần"],
        ["Mô hình hedonic (giải thích chênh lệch giá BQ2)", "–", "–", "Chưa làm"],
        ["K-Means, GIS point-in-polygon, MERGE INTO, Airflow, dashboard", "–", "–", "Chưa làm"],
    ], widths_cm=[4.4, 4.6, 4.6, 2.4], keep_together=True)

    # ================================================================ CHƯƠNG 2
    r.h1("Bước 1: Silver listing_feature")
    r.h2("2.1. Mục tiêu")
    r.para(
        "Ba BQ đều cần so sánh tin \"cùng loại\" và \"cùng đặc điểm\". Bảng current chỉ có `category_name` do từng nguồn đặt, "
        "và không có cột nào cho biết tin có pháp lý, mặt tiền đường hay ô tô tiếp cận. `listing_feature` có grain **một dòng "
        "cho mỗi `source_id`** của bảng current."
    )
    r.h2("2.2. Cách làm")
    r.steps([
        ("Lấy loại hình có cấu trúc ở Silver Core", "Batdongsan dùng `category_id` của site (dự phòng: tiền tố URL tin); NhaDatVui dùng `product_slug`; Guland dùng tag loại hình. Khi không có bằng chứng, `category_name` để NULL (DQ07 WARN), không gán mặc định."),
        ("Ghi nguồn gốc nhãn", "Cột `category_evidence` trên `listing_observation`: `SOURCE_STRUCTURED`, `SOURCE_LABEL`, `ENDPOINT_CONTEXT` (Guland không có tag, chỉ dựa vào endpoint nhà phố) hoặc `NONE`."),
        ("Đọc loại hình từ title theo tín hiệu mạnh/yếu", "Tín hiệu mạnh gọi tên loại hình (\"căn hộ\", \"nhà 3 tầng\", \"bán đất\"); tín hiệu yếu là viết tắt hoặc từ đa nghĩa (\"CH\", \"CC\", \"lô\", \"2PN\") và chỉ dùng khi không có tín hiệu mạnh và cùng chỉ một loại hình. Không đủ bằng chứng thì `khong_ro`."),
        ("Phát hiện mâu thuẫn, không ghi đè hàng loạt", "Nhãn có bằng chứng từ nguồn được giữ; nếu title có tín hiệu mạnh chỉ loại hình khác thì `category_title_conflict = TRUE`. Chỉ nhãn dựa trên endpoint mới nhường cho tín hiệu mạnh của title (`TITLE_OVER_ENDPOINT`)."),
        ("Cờ đặc điểm", "Năm cờ `title_has_*` xử lý phủ định, viết tắt và vị trí gần (\"cách mặt tiền 20m\"). `title_has_frontage` chỉ tính BĐS nằm mặt tiền đường; \"MT 5m\" (chiều ngang) và \"mặt tiền hẻm\" không tính. Đặc điểm bị phủ định được liệt kê trong `title_negated_features`."),
        ("Unit test và kiểm chứng", f"{tests['per_file'].get('test_feature_rules', 0)} ca test cho quy tắc feature, gồm các ca hồi quy từ nhận xét; `verify_silver.py` so khớp tập `source_id` hai chiều và kiểm tra `category_evidence`."),
    ])
    r.h2("2.3. Đoạn code chính")
    r.para("Phân loại title theo tín hiệu mạnh/yếu và quyết định giữa nhãn nguồn với title:")
    r.code(snippet("src/silver/feature_rules.py", "def category_from_title(", "def _positive_number", 52))
    r.h2("2.4. Kết quả")
    mc, total = feature["model_category_counts"], feature["output_rows"]
    old_mc = before.get("model_category_counts", {})
    r.table("Phân bố model_category trước và sau đợt sửa", ["model_category", "Ý nghĩa", "Trước", "Sau", "Tỷ lệ sau"], [
        [code, category_labels.get(code, code), n(old_mc.get(code, 0)), n(count), pct(count / total)]
        for code, count in sorted(mc.items(), key=lambda kv: -kv[1])
    ], numeric=[2, 3, 4], widths_cm=[3.2, 5.8, 2.4, 2.4, 2.2], keep_together=True)
    r.para(
        f"Căn hộ giảm từ {n(old_mc.get('can_ho'))} xuống {n(mc.get('can_ho'))} tin. Nguyên nhân chính: trước đây Core gán mặc định "
        f"\"căn hộ\" cho {n(before.get('batdongsan_default_to_apartment_rows_bronze'))}/{n(before.get('batdongsan_bronze_rows'))} dòng Bronze của batdongsan "
        "vì cho rằng endpoint /ban-can-ho-chung-cu chỉ chứa căn hộ, trong khi `category_id` và URL của chính các tin đó cho thấy nhiều "
        "loại hình (nhà riêng, nhà mặt phố, biệt thự liền kề, shophouse…)."
    )
    r.table("Nguồn gốc nhãn và cách xác định model_category", ["Nguồn gốc nhãn", "Cách xác định", "Số tin"], [
        [EVIDENCE_LABELS.get(row["category_evidence"], row["category_evidence"]), METHOD_LABELS.get(row["model_category_method"], row["model_category_method"]), n(row["count"])]
        for row in data["category_evidence_method"]
    ], numeric=[2], widths_cm=[5.0, 7.0, 4.0], keep_together=True)
    r.table("Mâu thuẫn giữa nhãn và title theo nguồn", ["Nguồn", "Số tin", "Mâu thuẫn", "Tỷ lệ", "Title thay mặc định endpoint"], [
        [row["source"], n(row["n"]), n(row["conflicts"]), pct(row["conflicts"] / row["n"]), n(row["title_over_endpoint"])]
        for row in data["category_conflict_by_source"]
    ], numeric=[1, 2, 3, 4], widths_cm=[3.4, 2.8, 2.8, 2.4, 4.6], keep_together=True)
    r.para(
        f"{n(feature.get('category_title_conflict_rows'))} tin có nhãn nguồn mâu thuẫn với tín hiệu mạnh trong title. Các tin này giữ nhãn nguồn "
        "và được gắn cờ để kiểm tra; báo cáo không tự kết luận bên nào đúng. Một số ví dụ:"
    )
    r.table("Ví dụ mâu thuẫn giữa nhãn nguồn và title", ["Nguồn", "Nhãn nguồn", "model_category", "Title gợi ý", "Title"], [
        [row["source"], row["category_name"], row["model_category"], row["title_model_category"], (row["title"] or "")[:80]]
        for row in data["category_conflict_examples"]
    ], widths_cm=[2.0, 2.8, 2.2, 2.0, 7.0], font_size=9, keep_together=True)
    trace = [row for row in data.get("moonlight_trace", []) if row["source"] == "homedy"] or data.get("moonlight_trace", [])
    if trace:
        row = trace[0]
        r.para(
            f"**Truy vết tin \"Moonlight Park View\" trong nhóm nhà phố (Bảng 5.2).** Tin `{row['source_id']}` của {row['source']} có nhãn nguồn "
            f"\"{row['category_name']}\" ({EVIDENCE_LABELS.get(row['category_evidence'], row['category_evidence'])}). Title \"{(row['title'] or '')[:60]}…\" "
            "chỉ có tín hiệu yếu cho căn hộ (\"CH\", \"1PN\"), nên quy tắc giữ nhãn nguồn và không gắn cờ mâu thuẫn. Nguyên nhân là **nhãn nguồn sai**, "
            "không phải lỗi gán mặc định. Cho tín hiệu yếu ghi đè nhãn nguồn sẽ gây sai ở chỗ khác (\"CC\" = chính chủ), nên trường hợp này được ghi nhận "
            "là giới hạn còn lại; các tin batdongsan cùng dự án nay đều được xếp căn hộ nhờ `category_id`."
        )
    rates = feature["true_rates"]
    r.figure(charts.feature_heatmap(figures / "feature_heatmap.png", data["feature_by_category"], category_labels),
             "Tỷ lệ title nêu từng đặc điểm theo model_category (toàn bộ tin current)")
    r.para(
        f"Title nêu pháp lý ở {pct(rates['title_has_legal'])} tin, nội thất {pct(rates['title_has_furnished'])}, mặt tiền đường "
        f"{pct(rates['title_has_frontage'])}, thang máy {pct(rates['title_has_elevator'])} và ô tô tiếp cận {pct(rates['title_has_car_access'])}. "
        "Các tỷ lệ này là **cận dưới**: `title_has_* = FALSE` gộp \"title không nêu\" và \"title nói không có\", nên không được hiểu là "
        "BĐS chắc chắn không có đặc điểm đó. `feature_completeness_score` đo mức đầy đủ thông tin, không đo độ chính xác."
    )
    r.table("Độ đầy đủ thông tin theo nguồn", ["Nguồn", "Số tin", "rooms_known", "legal_known", "Completeness TB"], [
        [row["source"], n(row["n"]), pct(row["rooms_known"]), pct(row["legal_known"]), dec(row["avg_completeness"], 3)]
        for row in data["feature_by_source"]
    ], numeric=[1, 2, 3, 4], widths_cm=[3.6, 2.8, 3.0, 3.0, 3.6], keep_together=True)
    if data.get("feature_examples"):
        r.table("Ví dụ tin nêu cả pháp lý, ô tô tiếp cận và thang máy (chỉ lấy tin có nhãn từ taxonomy của site, không mâu thuẫn với title)",
                ["Nguồn", "model_category", "Title"], [
                    [row["source"], row["model_category"], row["title"][:110]] for row in data["feature_examples"]
                ], widths_cm=[2.4, 2.8, 10.8], font_size=10)
    r.h2("2.5. Kiểm chứng")
    feature_checks = {k: v for k, v in sv.get("checks", {}).items() if k.startswith(("feature", "observation_category", "location_source"))}
    r.table("Các check liên quan listing_feature trong verify_silver.py", ["Check", "Kết quả"], [
        [name, "PASS" if passed else "FAIL"] for name, passed in feature_checks.items()
    ], widths_cm=[11.0, 5.0], keep_together=True)
    r.h2("2.6. Hạn chế")
    r.bullets([
        "Cờ đặc điểm chỉ đọc từ title; Silver Core không giữ phần mô tả chi tiết.",
        f"`legal_known` chỉ đạt {pct(rates['legal_known'])}; kết luận liên quan pháp lý chỉ mang tính tham khảo.",
        "Guland không có tag loại hình cho khoảng một nửa số tin; các tin này dựa vào endpoint, chỉ được sửa khi title có tín hiệu mạnh.",
        "Quy tắc title là heuristic; các ca khó đã có test, nhưng chưa có mẫu gán nhãn thủ công đủ lớn để đo độ chính xác.",
    ])

    # ================================================================ CHƯƠNG 3
    r.h1("Bước 2: Gold nền – dimension và fact")
    r.h2("3.1. Mục tiêu")
    r.para(
        "Tổ chức dữ liệu theo star schema: `fact_listing` ở grain một tin bán và các dimension cho vị trí, loại hình, band giá, "
        "diện tích, số phòng. Khóa ngoại không bao giờ NULL (giá trị không xác định trỏ về `-1`), band không chồng lấn, và tin "
        "nghi trùng giữa các nguồn chỉ được tính một lần trong bảng tổng hợp."
    )
    r.h2("3.2. Cách làm")
    r.steps([
        ("Band trong cấu hình", "`config/gold_bands.csv` khai báo band theo khoảng nửa mở [lower, upper); `validate_bands` từ chối lỗ hổng và chồng lấn."),
        ("9 dimension", "Mỗi dimension có khóa INT và một dòng `-1 = Không rõ`. Khóa đánh từ khóa tự nhiên đã sắp xếp nên **cùng đầu vào** cho cùng khóa; khi đầu vào đổi (thêm địa điểm mới), khóa cũ có thể dịch. Điều này an toàn vì mọi lần chạy rebuild đồng bộ dimension và fact; không hệ thống nào bên ngoài được lưu surrogate key của Gold."),
        ("fact_listing", "Lọc tin bán, loại REJECT, gán band, nối dimension. Tin có tỉnh theo text mâu thuẫn với tọa độ (LQ05) nhận `location_key = -1` và khoảng cách NULL, kèm cờ `is_location_conflict`."),
        ("Cặp nghi trùng giữa các nguồn", f"Khác nguồn, cùng location và loại hình, diện tích lệch ≤ {pct(DUP_AREA_TOLERANCE, 0)}, giá lệch ≤ {pct(DUP_PRICE_TOLERANCE, 0)}, Jaccard token title ≥ {dec(DUP_MIN_TITLE_JACCARD, 2)}. Chỉ giữ cặp **tốt nhất hai chiều**: mỗi tin chỉ ghép với tin giống nó nhất ở mỗi nguồn khác, để một tin chung chung không nối bắc cầu nhiều tin không liên quan."),
        ("Nhóm và tin đại diện", "Gom cặp bằng union-find. Nhóm có hai tin cùng một nguồn bị đánh dấu `AMBIGUOUS` và **không gộp** (mọi tin vẫn được tính); nhóm `RESOLVED` giữ một tin đại diện (đầy đủ thông tin nhất)."),
        ("Độ nhạy và mẫu kiểm tra", "Chạy lại luật với ngưỡng chặt và lỏng để xem số liệu thay đổi bao nhiêu; xuất 40 cặp mẫu cố định ra `docs/validation/dedup_pair_sample.csv` để kiểm tra thủ công."),
    ])
    r.h2("3.3. Đoạn code chính")
    r.code(snippet("src/gold/gold_rules.py", "def mutual_best_pairs(", "def resolve_dup_groups", 32))
    r.h2("3.4. Kết quả")
    dim_grain = {
        "dim_source": "1 nguồn", "dim_date": "1 ngày", "dim_location": "tỉnh – quận/huyện", "dim_property_category": "1 model_category",
        "dim_price_band": "khoảng tổng giá", "dim_area_band": "khoảng diện tích", "dim_unit_price_band": "khoảng giá/m²",
        "dim_room_band": "nhóm số phòng", "dim_dq_status": "PASS / WARN",
    }
    r.figure(charts.star_schema(figures / "star_schema.png", list(dim_grain.items())), "Star schema của Gold nền: fact_listing và 9 dimension")
    r.table("Các dimension", ["Dimension", "Grain", "Số dòng (gồm -1)"], [
        [name, grain, n(dims_summary["table_counts"].get(name))] for name, grain in dim_grain.items()
    ], numeric=[2], widths_cm=[5.0, 7.0, 4.0], keep_together=True)
    r.table("Các band khai báo trong config/gold_bands.csv", ["Band", "Các khoảng"], [
        [label, "; ".join(b.band_label for b in bands[key])]
        for key, label in (("price", "Tổng giá"), ("area", "Diện tích"), ("unit_price", "Giá/m²"), ("room", "Số phòng"))
    ], widths_cm=[3.0, 13.0], font_size=10, keep_together=True)
    fc = fact["filter_counts"]
    r.table("Từ bảng current tới fact_listing", ["Bước", "Số dòng"], [
        ["Tin current (Silver, bản ghi mới nhất đã quan sát)", n(fc["current_rows"])],
        ["Loại: tin cho thuê", n(fc["excluded_is_rent_true"])],
        ["Loại: không xác định bán/thuê", n(fc["excluded_is_rent_null"])],
        ["Loại: REJECT", n(fc["excluded_dq_reject"])],
        ["fact_listing", n(fact["output_rows"])],
        ["Trong đó: tin đại diện theo luật nghi trùng", n(fact["dedup"]["representative_rows"])],
    ], numeric=[1], widths_cm=[11.0, 5.0], keep_together=True)
    unknown = fact["unknown_fk_counts"]
    loc_fact = fact.get("location", {})
    r.table("Số dòng fact có khóa ngoại trỏ về -1 (Không rõ)", ["Khóa ngoại", "Số dòng", "Tỷ lệ", "Nguyên nhân chính"], [
        ["room_band_key", n(unknown["room_band_key"]), pct(unknown["room_band_key"] / fact["output_rows"]), "Nguồn không ghi số phòng"],
        ["posted_date_key", n(unknown["posted_date_key"]), pct(unknown["posted_date_key"] / fact["output_rows"]), "Không có ngày đăng tin cậy"],
        ["price_band_key", n(unknown["price_band_key"]), pct(unknown["price_band_key"] / fact["output_rows"]), "Giá thỏa thuận"],
        ["location_key", n(unknown["location_key"]), pct(unknown["location_key"] / fact["output_rows"]),
         f"Không xác định được tỉnh, hoặc xung đột tỉnh–tọa độ ({n(loc_fact.get('conflict_rows_set_to_unknown_location'))} dòng)"],
        ["property_category_key", n(unknown["property_category_key"]), pct(unknown["property_category_key"] / fact["output_rows"]), "model_category = khong_ro"],
    ], numeric=[1, 2], widths_cm=[3.8, 2.4, 2.0, 7.8], keep_together=True)
    dd = fact["dedup"]
    r.para(
        f"Luật nghi trùng tìm được {n(dd['candidate_pairs'])} cặp ứng viên, còn {n(dd['mutual_best_pairs'])} cặp sau bước tốt nhất hai chiều, "
        f"gom thành {n(dd['dup_groups'])} nhóm ({n(dd['ambiguous_groups'])} nhóm mơ hồ, không gộp) với {n(dd['suspect_rows'])} tin nghi trùng "
        f"({pct(dd['suspect_rate'], 2)} fact). Các bảng tổng hợp dùng {n(dd['representative_rows'])} **tin đại diện theo luật**; "
        "điều này không bảo đảm mỗi bất động sản thực tế chỉ có một dòng."
    )
    sens = fact.get("dedup_sensitivity", {})
    r.table("Độ nhạy của luật nghi trùng theo ngưỡng", ["Thiết lập", "Ngưỡng (DT / giá / Jaccard)", "Cặp ứng viên", "Cặp giữ", "Nhóm", "Nhóm mơ hồ", "Tin bị bớt khỏi tổng hợp", "Nhóm lớn nhất"], [
        [name, f"{pct(DUP_SENSITIVITY[name][0], 0)} / {pct(DUP_SENSITIVITY[name][1], 0)} / {dec(DUP_SENSITIVITY[name][2], 2)}",
         n(s["candidate_pairs"]), n(s["mutual_best_pairs"]), n(s["dup_groups"]), n(s["ambiguous_groups"]), n(s["rows_removed_from_aggregates"]), n(s["max_group_size"])]
        for name, s in sens.items()
    ], numeric=[2, 3, 4, 5, 6, 7], widths_cm=[1.6, 3.2, 1.8, 1.6, 1.5, 1.7, 2.6, 2.0], font_size=9, keep_together=True)
    effect = fact.get("median_price_per_m2_by_category", {})
    r.table("Median giá/m² (triệu) theo loại hình: mọi tin bán so với tin đại diện", ["model_category", "Mọi tin", "Tin đại diện", "Chênh lệch"], [
        [cat, trm2(v["all_rows"]), trm2(v["representatives"]), pct((v["representatives"] or 0) / v["all_rows"] - 1, 2) if v["all_rows"] else "–"]
        for cat, v in sorted(effect.items())
    ], numeric=[1, 2, 3], widths_cm=[5.0, 3.6, 3.6, 3.8], keep_together=True)
    verdicts, sample_n = dedup_review(ev["dedup_sample"])
    if sample_n:
        r.para(
            f"**Kiểm tra mẫu thủ công.** Trong {sample_n} cặp mẫu (chọn cố định theo hash): "
            + ", ".join(f"{k}: {v}" for k, v in verdicts.most_common())
            + ". Không cặp nào rõ ràng là ghép nhầm; các cặp \"không chắc\" đều là căn trong cùng một dự án (nhiều căn cùng diện tích, cùng giá), "
            "nơi luật dễ ghép hai căn khác nhau. Đánh giá dựa trên title, giá và diện tích (không mở ảnh/URL), nên chỉ là kiểm tra sơ bộ; "
            "kết quả ở `docs/validation/dedup_pair_review.csv` để nhóm rà lại."
        )
    fact_by_source = data["fact_by_source"]
    r.figure(charts.horizontal_bars(
        figures / "fact_by_source.png", [row["source"] for row in fact_by_source], [row["fact_rows"] for row in fact_by_source],
        "Số tin bán trong fact_listing",
        annotations=[f"({pct((row['dup_suspect'] or 0) / row['fact_rows'])} nghi trùng)" for row in fact_by_source]),
        "Số tin bán theo nguồn trong fact_listing và tỷ lệ nghi trùng với nguồn khác")
    r.h2("3.5. Kiểm chứng")
    base_checks = {k: v for k, v in gv.get("checks", {}).items() if k.startswith(("dim_", "fact_", "dup_", "non_suspect", "location_conflict"))}
    r.table("Các check của Gold nền trong verify_gold.py", ["Check", "Kết quả"], [
        [name, "PASS" if passed else "FAIL"] for name, passed in base_checks.items()
    ], widths_cm=[11.0, 5.0], font_size=9)
    r.para(
        "Ngoài các check hình thức (một dòng -1, khóa duy nhất, band liên tục), verify tính lại: tập `source_id` của fact khớp tập tin bán "
        "ở Silver theo cả hai chiều, mỗi dòng fact nằm đúng band theo giá trị của nó, và mọi dòng xung đột vị trí có `location_key = -1`."
    )
    r.h2("3.6. Hạn chế")
    r.bullets([
        "Luật nghi trùng là heuristic: có thể bỏ sót tin cùng BĐS mà title viết khác hẳn, và có thể ghép nhầm tin giống nhau về giá, diện tích, câu chữ quảng cáo.",
        f"{pct(unknown['room_band_key'] / fact['output_rows'])} tin không có số phòng, làm nhóm tương đồng theo số phòng kém chi tiết.",
        "`dim_location` dừng ở cấp quận/huyện; chưa có mã hành chính chuẩn và ranh giới GIS.",
        "Khóa dimension chỉ ổn định khi rebuild đồng bộ; nạp tăng dần cần bảng ánh xạ khóa lưu bền vững.",
    ])

    # ================================================================ CHƯƠNG 4
    r.h1("Bước 3: Gold theo Business Question")
    r.para(
        "Các job nghiệp vụ (3a, 3b, 3c, 3f) chỉ đọc `fact_listing` và dimension, và chỉ dùng tin đại diện theo luật nghi trùng. "
        "Hai ngoại lệ có chủ đích: 3d (Data Quality KPI) đọc cả Bronze và Silver để dựng phễu dữ liệu; 3e đọc `listing_history` của Silver."
    )
    r.table("Các job của Bước 3", ["Mục", "File", "Bảng output", "Đọc từ"], [
        ["3a", "build_price_benchmark.py", "agg_peer_group_benchmark, fact_listing_price_assessment", "fact_listing"],
        ["3b", "build_budget_tradeoff.py", "agg_budget_tradeoff, fact_budget_pareto", "fact_listing"],
        ["3c", "build_area_substitution.py", "agg_area_substitution", "agg_peer_group_benchmark"],
        ["3d", "build_data_quality.py", "agg_dq_kpi", "Bronze, Silver, Gold"],
        ["3e", "build_repricing.py", "fact_listing_price_change", "listing_history, fact"],
        ["3f", "build_market_overview.py", "agg_market_overview", "fact_listing"],
    ], widths_cm=[1.2, 4.6, 6.6, 3.6], keep_together=True)

    r.h2("4.1. 3a – Benchmark giá theo nhóm tương đồng (BQ2)")
    levels = bench["benchmark_groups_by_level"]
    r.steps([
        ("Ba cấp nhóm tương đồng", "(quận, loại hình, band diện tích, band phòng) → (quận, loại hình, band diện tích) → (quận, loại hình)."),
        ("Chỉ công bố nhóm đủ lớn", f"Ít nhất {MIN_PEER_GROUP_SIZE} tin đại diện: {n(levels.get('LOC_CAT_AREA_ROOM'))} nhóm cấp 1, {n(levels.get('LOC_CAT_AREA'))} cấp 2, {n(levels.get('LOC_CAT'))} cấp 3."),
        ("Thống kê chính xác", "P25, median, P75 giá/m² bằng percentile chính xác (kết quả ổn định giữa các lần chạy)."),
        ("Vị trí của từng tin", "`duoi_p25`, `p25_p75`, `tren_p75` hoặc `khong_du_du_lieu`. Đây là vị trí trong phân phối giá chào của nhóm, **không** phải đánh giá giá hợp lý."),
        ("Biến tham khảo", "`feature_count_vs_peer`: số cờ đặc điểm của tin trừ mức kỳ vọng của nhóm. Đây là chênh lệch số cờ, không phải phần chênh lệch giá được giải thích bởi đặc điểm."),
    ])
    pos = bench["price_position_counts"]
    r.table("Vị trí giá/m² của tin đại diện so với nhóm tương đồng", ["price_position", "Số tin", "Tỷ lệ"], [
        [POSITION_LABELS[k], n(pos.get(k, 0)), pct(pos.get(k, 0) / bench["assessment_rows"])] for k in POSITION_ORDER
    ], numeric=[1, 2], widths_cm=[7.0, 4.5, 4.5], keep_together=True)

    r.h2("4.2. 3b – Trade-off theo ngân sách và tập Pareto (BQ1)")
    r.steps([
        ("Trade-off", "Nhóm theo (band tổng giá, quận, loại hình): số tin, median diện tích, phòng, giá/m², khoảng cách tới tâm, tỷ lệ cờ đặc điểm. Đây là nơi so sánh **giữa** các quận."),
        ("Pareto trong một quận", "Trong cùng (ngân sách, quận, loại hình), tin A bị trội nếu có tin B rẻ hơn hoặc bằng, rộng hơn hoặc bằng, nhiều phòng hơn hoặc bằng, có mọi cờ A có, và tốt hơn hẳn ở ít nhất một tiêu chí."),
        ("Thông tin chưa biết", "Số phòng không rõ không so được với số phòng đã biết (trước đây tính là 0). Cờ FALSE gồm cả \"title không nêu\". Khoảng cách không là tiêu chí vì phần lớn tin không có tọa độ; khoảng cách chỉ hiển thị ở bảng trade-off."),
    ])
    r.code(snippet("src/gold/gold_rules.py", "def _rooms_at_least(", "def pareto_efficient_ids", 24))
    r.para(
        f"`agg_budget_tradeoff` có {n(budget['tradeoff_groups'])} nhóm. Trong {n(budget['pareto_rows'])} tin được xét, {n(budget['pareto_efficient_rows'])} "
        f"tin ({pct(budget['pareto_efficient_rows'] / budget['pareto_rows'])}) không bị trội theo các tiêu chí đã chọn và thông tin trích xuất được "
        f"(trước đợt sửa: {n(before.get('pareto_efficient_rows'))}). Nhãn Pareto được `verify_gold.py` tính lại bằng phép so từng cặp."
    )
    r.table("Tỷ lệ tin không bị trội theo ngân sách", ["Ngân sách", "Số tin", "Không bị trội", "Tỷ lệ"], [
        [row["price_band"], n(row["n"]), n(row["efficient"]), pct(row["efficient"] / row["n"])] for row in data["bq1_pareto_overall"]
    ], numeric=[1, 2, 3], widths_cm=[5.2, 3.6, 3.6, 3.6], keep_together=True)

    r.h2("4.3. 3c – Khu vực thay thế (BQ3)")
    r.steps([
        ("Nhóm hai phía", f"Nhóm (quận, loại hình, band diện tích) đã công bố ở 3a, nên mỗi phía có ít nhất {MIN_PEER_GROUP_SIZE} tin; chỉ ghép quận cùng tỉnh."),
        ("Ngân sách so trên cùng diện tích", "Diện tích mục tiêu = diện tích median của nhóm gốc. Chi phí ước tính mỗi phía = median giá/m² × diện tích mục tiêu; khu vực thay thế phải rẻ hơn theo cách tính này."),
        ("Vì sao không so median tổng giá", "Trong cùng band 50–80 m², nhóm gốc có thể có tin điển hình 51 m² × 100 triệu = 5,10 tỷ, nhóm thay thế 79 m² × 90 triệu = 7,11 tỷ: rẻ hơn 10% theo giá/m² nhưng cần thêm 2,01 tỷ. Cột `median_total_price_diff` vẫn được giữ để đọc, không dùng làm điều kiện."),
        ("Tương đồng đặc điểm", f"`feature_similarity` ≥ {dec(MIN_SUBSTITUTION_SIMILARITY, 1)} và không cờ nào lệch quá {dec(MAX_FLAG_SHARE_GAP, 1)}. Không có trần này, nhóm 100% mặt tiền và nhóm 0% mặt tiền vẫn đạt 0,8. Chỉ số này so tỷ lệ title nêu đặc điểm, không xác nhận hai nhóm BĐS tương đương; số phòng và khoảng cách không là điều kiện."),
    ])
    r.code(snippet("src/gold/build_area_substitution.py", '.withColumn("target_area"', '    rank = Window', 12))
    r.para(
        f"Bảng có {n(sub['substitution_pairs'])} cặp (trước đợt sửa: {n(before.get('substitution_pairs'))}), cho {n(sub['origins_with_alternative'])} nhóm gốc. "
        f"Trong số này có {n(sub.get('pairs_where_median_total_price_not_lower'))} cặp mà median tổng giá của khu vực thay thế **không** thấp hơn, "
        "dù chi phí cho cùng diện tích thấp hơn; người dùng cần đọc cả hai cột."
    )
    r.table("Cặp thay thế theo loại hình", ["Loại hình", "Số cặp", "Rẻ hơn (trung vị)", "Chênh km (trung vị)", "Median tổng giá không thấp hơn"], [
        [row["category_label"], n(row["pairs"]), pct(row["median_gap"]), dec(row["median_distance_diff"], 1), n(row.get("total_not_lower"))] for row in data["bq3_by_category"]
    ], numeric=[1, 2, 3, 4], widths_cm=[4.6, 2.0, 2.8, 3.0, 3.6], keep_together=True)

    r.h2("4.4. 3d – Data Quality KPI")
    r.para(
        "`agg_dq_kpi` theo dõi mỗi nguồn qua phễu Bronze → Silver → Gold (job này đọc Bronze để đếm đầu phễu). Dòng quarantine có cột "
        "`source` hỏng do CSV bị vỡ được quy về nguồn qua lineage `bronze_path`."
    )
    r.table("Phễu dữ liệu theo nguồn (agg_dq_kpi)", ["Nguồn", "Bronze", "Reject", "Trùng batch", "Current", "Thuê", "Fact", "WARN chính"], [
        [row["source"], n(row["bronze_rows"]), n(row["reject_rows"]), n(row["duplicate_rows_dropped"]), n(row["current_rows"]),
         n(row["rent_rows"]), n(row["fact_rows"]), ", ".join((row.get("top_warn_reasons") or [])[:2])]
        for row in data["dq_kpi"]
    ], numeric=[1, 2, 3, 4, 5, 6], widths_cm=[2.3, 1.8, 1.4, 1.7, 1.8, 1.6, 1.7, 3.7], font_size=9, keep_together=True)

    r.h2("4.5. 3e – Lịch sử đổi giá")
    direction, medians = repricing["direction_counts"], repricing["median_price_change_pct"]
    r.para(
        f"{n(repricing['price_change_events'])} lần đổi giá trên {n(repricing['listings_with_change'])} tin bán: {n(direction.get('giam'))} lần giảm "
        f"(trung vị {pct(medians.get('giam'))}) và {n(direction.get('tang'))} lần tăng (trung vị {pct(medians.get('tang'))}). Số lần quan sát còn ít "
        "(vài snapshot), nên bảng này chưa đủ để kiểm chứng ngược BQ2."
    )

    r.h2("4.6. 3f – Tổng quan thị trường")
    r.para(f"`agg_market_overview`: {n(overview['rows'])} dòng (tỉnh × loại hình), {n(overview['listings'])} tin đại diện.")
    mo, provinces = data["market_overview"], data["top_provinces"][:5]
    names = [category_labels["nha_pho"], category_labels["can_ho"], category_labels["dat"]]
    values = {(row["province_name"], row["category_label"]): row for row in mo}
    r.figure(charts.grouped_bars(
        figures / "market_overview.png", provinces,
        [(label, [values[(p, label)]["median_price_per_m2"] / 1e6 if (p, label) in values else None for p in provinces]) for label in names],
        "Median giá/m² (triệu đồng)", value_fmt=lambda v: charts.vn(v, 0)),
        "Median giá chào bán/m² theo tỉnh/thành và loại hình (5 tỉnh nhiều tin nhất)")

    # ================================================================ CHƯƠNG 5
    r.h1("Trả lời Business Questions")
    r.para(
        f"Ví dụ lấy từ {data['focus_province']} (nhiều tin nhất) và được chọn bằng quy tắc cố định trong `export_report_data.py`. "
        "Mọi kết luận dưới đây dựa trên giá chào bán và thông tin trích xuất từ title."
    )
    r.h2("5.1. BQ1 – Đánh đổi trong cùng ngân sách")
    r.para("**Bảng Gold:** `agg_budget_tradeoff` (so sánh giữa các quận), `fact_budget_pareto` (lọc tin trong một quận).")
    house = data["bq1_tradeoff_nha_pho"]
    if house:
        top = house[:12]
        r.figure(charts.horizontal_bars(
            figures / "bq1_area_by_district.png", [row["district_name"] for row in top], [row["median_area"] for row in top],
            "Median diện tích (m²)", value_fmt=lambda v: charts.vn(v, 0) + " m²",
            annotations=[f"– cách tâm {dec(row['median_distance_km'], 1)} km" if row["median_distance_km"] is not None else "" for row in top]),
            f"Nhà phố ngân sách {data['bq1_budget_label']} tại {data['focus_province']}: median diện tích theo quận (nhóm ≥ 20 tin)")
        r.table(f"Nhà phố {data['bq1_budget_label']}: người mua nhận được gì ở từng quận", [
            "Quận/huyện", "Số tin", "Diện tích", "Phòng", "Giá/m² (tr)", "Cách tâm (km)", "Pháp lý", "Mặt tiền", "Ô tô"], [
            [row["district_name"], n(row["n_listings"]), dec(row["median_area"], 0), dec(row["median_rooms"], 0), trm2(row["median_price_per_m2"]),
             dec(row["median_distance_km"], 1), pct(row["share_legal"], 0), pct(row["share_frontage"], 0), pct(row["share_car_access"], 0)]
            for row in house
        ], numeric=[1, 2, 3, 4, 5, 6, 7, 8], widths_cm=[3.2, 1.4, 1.6, 1.3, 1.9, 1.9, 1.6, 1.6, 1.5], font_size=9)
        largest, smallest = house[0], house[-1]
        r.para(
            f"**Diễn giải.** Cùng {data['bq1_budget_label']}, nhà phố ở {largest['district_name']} có diện tích trung vị {dec(largest['median_area'], 0)} m², "
            f"còn ở {smallest['district_name']} là {dec(smallest['median_area'], 0)} m²; khu vực cho diện tích lớn thường xa trung tâm hơn. "
            "Tỷ lệ title nêu mặt tiền đường hay ô tô tiếp cận cũng khác nhau giữa các quận. Đây là mô tả các tin đang chào bán, "
            "không phải giá giao dịch; người mua dùng bảng để thấy mình phải hy sinh tiêu chí nào khi chọn khu vực."
        )
    pg = data["bq1_pareto_group"]
    r.figure(charts.pareto_scatter(figures / "bq1_pareto.png", data["bq1_pareto_points"], f"Nhà phố {data['bq1_budget_label']} – {pg['district_name']}"),
             f"Tin không bị trội theo tiêu chí đã chọn trong nhóm nhà phố {data['bq1_budget_label']} tại {pg['district_name']}")
    r.table(f"Một số tin không bị trội trong nhóm {pg['district_name']}", ["Nguồn", "Title", "Giá", "m²", "Phòng", "Pháp lý", "Ô tô", "MT", "TM"], [
        [row["source"], row["title"][:70], ty(row["price"]), dec(row["area"], 0), "" if row["rooms"] is None else str(row["rooms"]),
         yes(row["title_has_legal"]), yes(row["title_has_car_access"]), yes(row["title_has_frontage"]), yes(row["title_has_elevator"])]
        for row in data["bq1_pareto_examples"]
    ], numeric=[2, 3, 4], widths_cm=[1.8, 6.6, 1.6, 1.1, 1.1, 1.0, 1.0, 0.9, 0.9], font_size=9)
    r.para(
        f"**Diễn giải.** Nhóm có {n(pg['group_size'])} tin, trong đó {n(pg['frontier_size'])} tin không bị trội theo các tiêu chí đã chọn (giá, diện tích, "
        "số phòng, năm cờ title) và thông tin trích xuất được, trong cùng nhóm so sánh. Một tin bị trội không có nghĩa là kém hơn ở mọi mặt thực tế "
        "(vị trí chính xác, hướng, chất lượng xây dựng không có trong dữ liệu), và tin Pareto không chắc chắn \"đáng tiền\". Danh sách này giúp "
        "người mua rút gọn số tin cần xem trước trong khu vực đã chọn."
    )

    r.h2("5.2. BQ2 – Giá chào so với nhóm tương đồng")
    r.para("**Bảng Gold:** `agg_peer_group_benchmark`, `fact_listing_price_assessment`.")
    pos_rows: dict[str, dict[str, int]] = defaultdict(dict)
    for row in data["position_by_category"]:
        pos_rows[row["category_label"]][row["price_position"]] = row["count"]
    order = [label for label in category_labels.values() if label in pos_rows and label != "Không rõ"]
    shares = {key: [pos_rows[label].get(key, 0) / sum(pos_rows[label].values()) * 100 for label in order] for key in POSITION_ORDER}
    r.figure(charts.position_stacked(figures / "bq2_positions.png", order, shares),
             "Vị trí giá/m² của tin đại diện trong phân phối của nhóm tương đồng, theo loại hình")
    pg2 = data["bq2_peer_group"]
    r.para(
        f"Ví dụ nhóm căn hộ lớn nhất: **{pg2['district_name']}, {pg2['area_band']}, {pg2['room_band']}**, {n(pg2['n_listings'])} tin; "
        f"giá/m² P25 – median – P75 = {trm2(pg2['p25_price_per_m2'])} – {trm2(pg2['median_price_per_m2'])} – {trm2(pg2['p75_price_per_m2'])} triệu."
    )
    r.table("Ví dụ vị trí giá trong một nhóm tương đồng", ["Title", "Giá", "m²", "Giá/m² (tr)", "÷ median", "Vị trí", "Số cờ (so nhóm)"], [
        [row["title"][:70], ty(row["price"]), dec(row["area"], 0), trm2(row["price_per_m2"]), dec(row["price_ratio"], 2),
         POSITION_LABELS[row["price_position"]], f"{row['feature_count']} ({'+' if (row['feature_count_vs_peer'] or 0) >= 0 else ''}{dec(row['feature_count_vs_peer'], 1)})"]
        for row in data["bq2_examples"]
    ], numeric=[1, 2, 3, 4, 6], widths_cm=[6.4, 1.6, 1.0, 1.7, 1.5, 1.9, 1.9], font_size=9)
    gap = {row["price_position"]: row for row in data["position_feature_gap"]}
    r.table("Số cờ đặc điểm trung bình theo vị trí giá", ["Vị trí giá", "Số tin", "Số cờ TB", "So với kỳ vọng nhóm", "Dưới P25 + không nêu pháp lý"], [
        [POSITION_LABELS[k], n(gap[k]["n"]), dec(gap[k]["avg_feature_count"], 2), dec(gap[k]["avg_feature_count_vs_peer"], 2),
         pct(gap[k]["share_low_missing_legal"]) if k == "duoi_p25" else "–"]
        for k in ("duoi_p25", "p25_p75", "tren_p75") if k in gap
    ], numeric=[1, 2, 3, 4], widths_cm=[3.4, 2.6, 2.6, 3.6, 3.8], keep_together=True)
    hi, lo = gap.get("tren_p75", {}), gap.get("duoi_p25", {})
    r.para(
        f"**Diễn giải.** BQ2 hiện là **benchmark mô tả**: nó cho biết một tin nằm ở đâu trong phân phối giá chào của các tin cùng quận, loại hình, "
        f"diện tích và số phòng. Tin trên P75 trung bình có {dec(hi.get('avg_feature_count'), 2)} cờ đặc điểm, tin dưới P25 có {dec(lo.get('avg_feature_count'), 2)}; "
        "chênh lệch số cờ này nhỏ và **không** phải phần chênh lệch giá được giải thích bởi đặc điểm. Nhóm tương đồng vẫn có thể khác nhau về dự án, "
        "phân khúc, chất lượng và pháp lý, nên \"trong P25–P75\" không có nghĩa là giá hợp lý và tỷ lệ giá cao nhiều lần median chưa đủ để kết luận là lỗi nhập. "
        f"Tin dưới P25 mà title không nêu pháp lý ({pct(lo.get('share_low_missing_legal'))} tin dưới P25) là tin nên kiểm tra giấy tờ trước. "
        "Giải thích định lượng chênh lệch giá cần mô hình hedonic, chưa làm trong đợt này."
    )

    r.h2("5.3. BQ3 – Khu vực thay thế")
    r.para("**Bảng Gold:** `agg_area_substitution`, nối `dim_location` cho tên quận.")
    origin, examples = data["bq3_origin"], data["bq3_examples"]
    if examples:
        r.figure(charts.horizontal_bars(
            figures / "bq3_alternatives.png", [row["alternative_district"] for row in examples], [row["price_gap_pct"] * 100 for row in examples],
            "Chi phí thấp hơn cho cùng diện tích (%)", value_fmt=lambda v: charts.vn(v, 0) + "%",
            annotations=[f"– tương đồng {dec(row['feature_similarity'], 2)}, {'+' if (row['distance_diff_km'] or 0) >= 0 else ''}{dec(row['distance_diff_km'], 1)} km" for row in examples]),
            f"Khu vực thay thế cho nhà phố {origin['area_band']} tại {origin['origin_district']}")
        r.table(f"Khu vực thay thế cho nhà phố {origin['area_band']} tại {origin['origin_district']}", [
            "Hạng", "Quận thay thế", "Giá/m² gốc (tr)", "Giá/m² thay thế (tr)", "Thấp hơn", "Chênh chi phí cùng DT", "Chênh median tổng giá", "Chênh km", "Tương đồng"], [
            [row["substitution_rank"], row["alternative_district"], trm2(row["origin_median_price_per_m2"]), trm2(row["alternative_median_price_per_m2"]),
             pct(row["price_gap_pct"], 0), ty(row["estimated_saving_at_target_area"]), ty(row["median_total_price_diff"]), dec(row["distance_diff_km"], 1),
             dec(row["feature_similarity"], 2)]
            for row in examples
        ], numeric=[0, 2, 3, 4, 5, 6, 7, 8], widths_cm=[1.0, 2.8, 1.7, 1.9, 1.4, 2.0, 2.0, 1.4, 1.8], font_size=9)
        first = examples[0]
        r.para(
            f"**Diễn giải.** Diện tích mục tiêu là {dec(first['target_area'], 0)} m² (median của nhóm gốc). Với diện tích này, chi phí ước tính ở "
            f"{first['alternative_district']} thấp hơn khoảng {pct(first['price_gap_pct'], 0)} so với {origin['origin_district']}. "
            "Cột \"chênh chi phí cùng diện tích\" là ước tính từ median giá/m², **không** phải khoản tiết kiệm bảo đảm cho một bất động sản tương đương; "
            "cột \"chênh median tổng giá\" có thể nhỏ hơn hoặc âm vì tin điển hình ở hai khu vực có diện tích khác nhau. Độ tương đồng chỉ so tỷ lệ title "
            "nêu đặc điểm; khoảng cách và số phòng không phải điều kiện nên người dùng cần đọc thêm cột chênh km."
        )

    # ================================================================ CHƯƠNG 6
    r.h1("Tổng hợp kiểm chứng")
    r.h2("6.1. Unit test")
    r.table("Kết quả pytest theo file", ["File test", "Số ca", "Kết quả"], [
        [f"tests/unit/{name}.py", n(count), "PASS" if not tests["failed"].get(name) else f"FAIL ({tests['failed'][name]})"]
        for name, count in sorted(tests["per_file"].items())
    ] + [["Tổng", n(tests["total"]), "PASS" if tests["failures"] == 0 else f"FAIL ({tests['failures']})"]],
        numeric=[1], widths_cm=[9.0, 3.0, 4.0], keep_together=True)
    r.para("Các test mới bám vào lỗi đã gặp: ví dụ phân loại sai trong nhận xét, tên đường trùng tên tỉnh, \"MT 5m\", phản ví dụ ngân sách BQ3, nhóm 100%/0% mặt tiền, nối bắc cầu dedup, số phòng không rõ trong Pareto.")
    r.h2("6.2. verify_silver.py")
    r.table("Các check của verify_silver.py", ["Check", "Kết quả"], [
        [name, "PASS" if passed else "FAIL"] for name, passed in sv.get("checks", {}).items()
    ], widths_cm=[11.0, 5.0], font_size=10)
    r.h2("6.3. verify_gold.py")
    gc = gv.get("checks", {})
    r.para(f"`verify_gold.py` chạy {len(gc)} check, kết quả {sum(gc.values())}/{len(gc)} PASS. Các check tính lại kết quả (không chỉ kiểm tra hình thức):")
    r.bullets([
        "`fact_source_id_set_matches_silver`: tập khóa fact khớp tập tin bán ở Silver theo hai chiều.",
        "`fact_*_band_key_matches_value`: mỗi dòng nằm đúng band theo giá trị.",
        "`assessment_position_recomputed`, `assessment_peer_values_match_benchmark`: nhãn vị trí giá tính lại từ P25/P75 của nhóm.",
        "`pareto_labels_recomputed`: nhãn Pareto tính lại bằng so từng cặp trong mỗi nhóm.",
        "`substitution_cheaper_at_target_area`, `substitution_similarity_and_flag_cap`, `substitution_target_area_is_origin_median`: điều kiện BQ3.",
        "`location_conflict_has_unknown_location`, `dup_resolved_*`, `dup_ambiguous_*`: xử lý xung đột vị trí và nhóm nghi trùng.",
    ])
    r.table("Các check của verify_gold.py", ["Check", "Kết quả"], [
        [name, "PASS" if passed else "FAIL"] for name, passed in gc.items()
    ], widths_cm=[11.0, 5.0], font_size=9)
    r.h2("6.4. Rebuild hai lần trên cùng đầu vào")
    if det:
        if content_ok or "content" in det.get("method", ""):
            r.para(
                "`scripts/check_deterministic_rebuild.ps1` chạy toàn bộ Silver và Gold hai lần trên cùng dữ liệu Bronze. Sau mỗi lần, "
                "`table_fingerprints.py` tính cho từng bảng: số dòng, số khóa grain khác nhau và một hash SHA-256 không phụ thuộc thứ tự của toàn bộ nội dung "
                f"(bỏ các cột thời điểm build `*_built_at`). Kết quả trên {det['tables_compared']} bảng: **{det['status']}**"
                + ("." if det["status"] == "PASS" else f"; bảng lệch: {', '.join(det['mismatches'])}.")
                + " Phép kiểm tra này chứng minh cùng đầu vào cho cùng nội dung; nó không nói gì về độ ổn định khi đầu vào thay đổi."
                + " Riêng `listing_dq_quarantine` không có khóa duy nhất: đó là các dòng CSV vỡ giống hệt nhau (không có `source_id`), được giữ nguyên để truy vết."
            )
            r.table("So sánh hai lần rebuild", ["Bảng", "Số dòng", "Khóa duy nhất", "Số dòng khớp", "Nội dung khớp"], [
                [row["table"], n(row["rows_run1"]), "Có" if row.get("grain_unique") else "Không", "Có" if row["rows_equal"] else "Không", "Có" if row["content_equal"] else "Không"]
                for row in det["tables"]
            ], numeric=[1], widths_cm=[6.6, 2.4, 2.4, 2.2, 2.4], font_size=9)
        else:
            r.para("Bằng chứng hiện có chỉ so số dòng giữa hai lần rebuild, nên chỉ kết luận được **số dòng ổn định**, chưa chứng minh nội dung giống nhau.")

    # ================================================================ CHƯƠNG 7
    r.h1("Đợt rà soát và sửa lỗi")
    r.para("Chương này ghi lại các lỗi được chỉ ra trong nhận xét ngày 09/10/2026, cách sửa và số liệu trước/sau. Số liệu \"trước\" lấy từ `docs/validation/review_before_fix.json`.")
    r.table("Lỗi đã sửa", ["#", "Lỗi", "Nguyên nhân", "Cách sửa"], [
        ["1", "Nhà riêng xếp thành căn hộ; \"Căn hộ Đất Xanh\" thành đất", "Core mặc định căn hộ cho batdongsan; khớp chuỗi con trên nhiều trường ghép chung",
         "Dùng `category_id`/URL (batdongsan), `product_slug` (nhadatvui); khớp theo từng trường, theo ranh giới từ; ghi `category_evidence`"],
        ["1", "\"CC\" (chính chủ), \"lô\" (phân lô) đọc thành căn hộ, đất", "Viết tắt đa nghĩa được coi là bằng chứng", "Tín hiệu mạnh/yếu; mâu thuẫn được gắn cờ, không ghi đè"],
        ["2", "\"MT 5m … trong hẻm\" thành mặt tiền", "Không phân biệt chiều ngang với mặt tiền đường", "Bỏ qua \"MT + số đo\" và \"mặt tiền hẻm\"; liệt kê đặc điểm bị phủ định"],
        ["3", "\"Đường Điện Biên Phủ, Q. Bình Thạnh\" thành tỉnh Điện Biên", "Quét tên tỉnh trên text ghép chung, bỏ dấu (Vĩnh → Vinh)",
         "Thứ tự bằng chứng: thành phần địa chỉ, quận TP.HCM, tiền tố hành chính, tên trần có dấu + kiểm tra tọa độ; giữ tỉnh có cấu trúc của nhadatvui"],
        ["3", "Vị trí xung đột vẫn vào KPI", "Gold không mang trạng thái DQ vị trí", "LQ05 → khoảng cách NULL, `location_key = -1`, cờ `is_location_conflict`"],
        ["4", "BQ3 \"rẻ hơn\" nhưng tổng ngân sách cao hơn", "Chỉ so median giá/m²", "So chi phí trên cùng diện tích mục tiêu; thêm trần chênh lệch từng cờ"],
        ["5", "Pareto coi phòng không rõ là 0", "Quy ước đơn giản hóa", "Phòng không rõ không so được với phòng đã biết; sửa diễn giải"],
        ["7", "Nhóm trùng nối bắc cầu (tới 30 tin)", "Union-find trên mọi cặp", "Cặp tốt nhất hai chiều; nhóm có tin cùng nguồn không gộp"],
        ["8", "Rebuild chỉ so số dòng; check chưa tính lại", "Phạm vi kiểm chứng hẹp", "Fingerprint nội dung; verify tính lại band, vị trí giá, Pareto, BQ3"],
    ], widths_cm=[0.8, 4.2, 4.6, 6.4], font_size=9)
    loc_reasons, old_reasons = loc.get("dq_reason_counts", {}), before.get("location_reason_counts", {})
    sens_def = fact.get("dedup_sensitivity", {}).get("default", {})
    after_pos = bench["price_position_counts"]
    old_pos = before.get("price_position_counts", {})
    r.table("Số liệu trước và sau đợt sửa", ["Chỉ số", "Trước", "Sau", "Nguyên nhân thay đổi"], [
        ["Tin current model_category = căn hộ", n(old_mc.get("can_ho")), n(mc.get("can_ho")), "Bỏ mặc định căn hộ, dùng taxonomy batdongsan"],
        ["Tin current model_category = nhà phố", n(old_mc.get("nha_pho")), n(mc.get("nha_pho")), "Như trên"],
        ["Tin không rõ loại hình", n(old_mc.get("khong_ro")), n(mc.get("khong_ro")), "Viết tắt đa nghĩa không còn đủ để phân loại"],
        ["Dòng LQ05 (Silver)", n(old_reasons.get("LQ05_PROVINCE_COORDINATE_CONFLICT")), n(loc_reasons.get("LQ05_PROVINCE_COORDINATE_CONFLICT", 0)), "Tên đường/phường không còn bị đọc thành tỉnh"],
        ["Dòng LQ05 trong fact", n(before.get("lq05_rows_in_fact")), n(loc_fact.get("conflict_rows_set_to_unknown_location")), "Nay nhận location_key = -1"],
        ["Tỉnh suy từ tâm gần nhất (LQ04)", n(old_reasons.get("LQ04_PROVINCE_INFERRED_BY_NEAREST_CENTER")), n(loc_reasons.get("LQ04_PROVINCE_INFERRED_BY_NEAREST_CENTER", 0)), "Dùng tỉnh có cấu trúc nhadatvui, quận TP.HCM"],
        ["Không xác định tỉnh (LQ02)", n(old_reasons.get("LQ02_PROVINCE_UNKNOWN")), n(loc_reasons.get("LQ02_PROVINCE_UNKNOWN", 0)), "Tên trần không dấu/không khớp tọa độ không còn được nhận"],
        ["Nhóm nghi trùng", n(before.get("dedup", {}).get("dup_groups")), n(fact["dedup"]["dup_groups"]), "Cặp tốt nhất hai chiều, loại hình mới"],
        ["Nhóm lớn nhất", n(before.get("dedup", {}).get("max_group_size")), n(sens_def.get("max_group_size")), "Hết nối bắc cầu"],
        ["Tin đại diện", n(before.get("representative_rows")), n(fact["dedup"]["representative_rows"]), "Nhóm mơ hồ không gộp"],
        ["Tin dưới P25 / trên P75", f"{n(old_pos.get('thap'))} / {n(old_pos.get('cao'))}", f"{n(after_pos.get('duoi_p25'))} / {n(after_pos.get('tren_p75'))}", "Nhóm tương đồng thay đổi theo loại hình mới"],
        ["Tin không bị trội (Pareto)", n(before.get("pareto_efficient_rows")), n(budget["pareto_efficient_rows"]), "Phòng không rõ không còn tính là 0"],
        ["Cặp BQ3", n(before.get("substitution_pairs")), n(sub["substitution_pairs"]), "Trần chênh lệch từng cờ, nhóm mới"],
        ["Cặp BQ3 có median tổng giá không thấp hơn", n(before.get("substitution_pairs_total_price_not_lower")), n(sub.get("pairs_where_median_total_price_not_lower")), "Vẫn tồn tại; nay được công bố và giải thích"],
        ["verify_silver / verify_gold", f"{before.get('verify_silver')} / {before.get('verify_gold')}", f"{sum(sv['checks'].values())}/{len(sv['checks'])} / {sum(gc.values())}/{len(gc)}", "Thêm check tính lại"],
        ["Unit test", before.get("pytest"), f"{tests['total'] - tests['failures']}/{tests['total']}", "Regression test cho từng lỗi"],
    ], numeric=[1, 2], widths_cm=[5.0, 2.4, 2.4, 6.2], font_size=9)
    r.h2("7.1. Phần chưa kiểm chứng được")
    r.bullets([
        "Chưa có mẫu gán nhãn thủ công đủ lớn để đo độ chính xác của phân loại loại hình và cờ đặc điểm; mới có các ca test chọn lọc.",
        "Mẫu 40 cặp nghi trùng mới được đánh giá sơ bộ qua title/giá/diện tích, chưa đối chiếu ảnh hoặc trang tin.",
        "Địa giới chỉ kiểm tra bằng khoảng cách tới tâm tỉnh (350 km); chưa có ranh giới GIS nên tỉnh suy từ tọa độ (LQ04) vẫn là suy luận.",
        "Nhãn nguồn sai mà title chỉ có viết tắt (ví dụ tin homedy \"Bán CH Moonlight Park View\" mang nhãn Nhà phố) vẫn bị xếp theo nhãn nguồn; "
        f"{n(feature['model_category_method_counts'].get('ENDPOINT_CONTEXT'))} tin Guland chỉ dựa vào endpoint.",
    ])

    # ================================================================ CHƯƠNG 8
    r.h1("Hạn chế và hướng phát triển")
    r.h2("8.1. Hạn chế")
    r.bullets([
        "**BQ2 mới là benchmark mô tả**: chưa có mô hình hedonic nên chưa tách được phần chênh lệch giá do từng đặc điểm.",
        "**Giá là giá chào bán**; \"current\" là bản ghi mới nhất đã quan sát, không xác nhận tin còn hiển thị.",
        "**Đặc điểm chỉ đọc từ title**, FALSE gộp \"không nêu\" và \"nói không có\".",
        "**Nghi trùng theo luật**: tổng hợp dùng tin đại diện theo heuristic, không bảo đảm một dòng cho mỗi BĐS thực.",
        "**Vị trí** dừng ở quận/huyện; chưa có GIS point-in-polygon và mã hành chính chuẩn.",
        "**Pipeline rebuild toàn bộ**; chưa có incremental `MERGE INTO`, khóa dimension chỉ ổn định khi rebuild đồng bộ.",
    ])
    r.h2("8.2. Giả định cần nhóm xác nhận")
    r.bullets([
        "Cách gom 5 `model_category` (liền kề, shophouse xếp cùng biệt thự; condotel, officetel xếp cùng căn hộ; trang trại, kho xưởng vào nhóm khác).",
        "Nhãn từ taxonomy của site được giữ khi mâu thuẫn với title (chỉ gắn cờ); nhãn chỉ dựa vào endpoint thì nhường cho tín hiệu mạnh của title.",
        f"Ngưỡng band, nhóm tương đồng tối thiểu {MIN_PEER_GROUP_SIZE} tin, tương đồng ≥ {dec(MIN_SUBSTITUTION_SIMILARITY, 1)} với trần {dec(MAX_FLAG_SHARE_GAP, 1)} mỗi cờ.",
        "BQ3 so chi phí tại diện tích median của nhóm gốc.",
        f"Luật nghi trùng (diện tích ±{pct(DUP_AREA_TOLERANCE, 0)}, giá ±{pct(DUP_PRICE_TOLERANCE, 0)}, Jaccard ≥ {dec(DUP_MIN_TITLE_JACCARD, 2)}, cặp tốt nhất hai chiều).",
        "Pareto tính trong từng quận; khoảng cách không là tiêu chí.",
    ])
    r.h2("8.3. Hướng phát triển (ngoài phạm vi đợt sửa này)")
    r.bullets([
        "Mô hình hedonic cho BQ2 bằng `pyspark.ml`, lưu `model_version` và sai số.",
        "K-Means phân cụm quận theo hồ sơ giá và đặc điểm để mở rộng BQ3.",
        "Airflow DAG Bronze → Silver → Gold → verify; dashboard Superset qua Trino.",
        "Ranh giới GIS, bảng ánh xạ khóa dimension bền vững, incremental `MERGE INTO`, dọn snapshot Iceberg.",
    ])
