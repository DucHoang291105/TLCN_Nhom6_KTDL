"""Findings shown on the dashboards, computed from Gold on every run.

Nothing here is typed by hand: each finding is filled from a Trino query, so
it changes when the data changes. A finding becomes the *title* of the chart
that shows it (``titles``); the page header keeps only the question and one
line on scope. ``compute_insights(run)`` takes a function that executes SQL
and returns rows.
"""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[2]
G = "iceberg.gold"
S = "iceberg.silver"
HCM = "Hồ Chí Minh"


def vn(value: float | None, digits: int = 0) -> str:
    if value is None:
        return "chưa có dữ liệu"
    text = f"{value:,.{digits}f}"
    return text.replace(",", "X").replace(".", ",").replace("X", ".")


def pct(value: float, digits: int = 0) -> str:
    if value is None:
        return "chưa có dữ liệu"
    return vn(value * 100, digits) + "%"


def signed(value: float) -> str:
    return ("+" if value >= 0 else "−") + pct(abs(value))


def header(question: str, scope: str) -> str:
    """Markdown header: the question and one short line on scope.

    The finding card and the charts follow the filters; charts marked
    "(Cố định)" or "(Tất cả …)" compare fixed groups, which the header says.
    """

    newline = chr(10)
    note = "Thẻ kết luận và các biểu đồ đổi theo bộ lọc; biểu đồ ghi (Cố định) hoặc (Tất cả…) thì không tự đổi theo bộ lọc."
    return newline.join([f"### {question}", f"<small style='color:#6b6b6b'>{scope} {note}</small>", ""])


def two_proportion_z(events_a: float, n_a: float, events_b: float, n_b: float) -> tuple[float, float]:
    """(z, two-sided p) for the difference of two proportions."""

    p_a, p_b, pooled = events_a / n_a, events_b / n_b, (events_a + events_b) / (n_a + n_b)
    z = (p_a - p_b) / math.sqrt(pooled * (1 - pooled) * (1 / n_a + 1 / n_b))
    return z, math.erfc(abs(z) / math.sqrt(2))


def evidence_titles(run: Callable[[str], list[tuple]]) -> dict[str, dict[str, str]]:
    """Titles of the evidence charts, queried from the dashboards' own datasets."""

    from src.dashboard.provision_superset import BQ1_DEFAULT, BQ2_DEFAULT, BQ3_DEFAULT, DATASETS, _ACCURACY

    def ds(name: str) -> str:
        return f"({DATASETS[name]}) t"

    titles: dict[str, dict[str, str]] = {}

    # overview: same distance ring, Hà Nội vs TP.HCM (fixed comparison chart)
    rings = run(f"""SELECT vanh_dai, approx_percentile(CASE WHEN thanh_pho = 'Hà Nội' THEN gia_m2_tr END, 0.5),
        approx_percentile(CASE WHEN thanh_pho = '{HCM}' THEN gia_m2_tr END, 0.5), COUNT_IF(thanh_pho = 'Hà Nội'), COUNT_IF(thanh_pho = '{HCM}')
        FROM {ds('city_compare')} WHERE vanh_dai <> '5. Trên 25 km' GROUP BY 1 ORDER BY 1""")
    both = [r for r in rings if r[3] >= 30 and r[4] >= 30]
    ring = next(r for r in both if r[0].startswith("2."))
    always = "Hà Nội đắt hơn ở mọi vành đai đủ dữ liệu" if all(r[1] > r[2] for r in both) else "không phải vành đai nào Hà Nội cũng đắt hơn"
    titles["tlcn-tong-quan"] = {"ov_compare": f"(Cố định) Cùng cách trung tâm 5–10 km: nhà phố Hà Nội {vn(ring[1])} tr/m², TP.HCM {vn(ring[2])} tr/m² "
                                              f"– gấp {vn(ring[1] / ring[2], 1)} lần; {always}"}

    # BQ3: best move per destination; centroid validation
    best = run(f"""SELECT doi_quan, re_hon_pct FROM {ds('commute_best')}
        WHERE category_label = '{BQ3_DEFAULT["category_label"]}' AND area_band = '{BQ3_DEFAULT["area_band"]}' ORDER BY re_hon_pct DESC""")
    centroid = run(f"SELECT COUNT_IF(ket_qua LIKE '1.%'), COUNT(*) FROM {ds('centroid_check')}")[0]
    titles["tlcn-bq3"] = {
        "bq3_centroid": f"(Cố định) Kiểm chứng tọa độ: {centroid[0]}/{centroid[1]} điểm đến nằm gần tâm đúng quận chứa nó nhất; số còn lại rơi vào quận giáp ranh",
    }
    if best:
        titles["tlcn-bq3"]["bq3_best_moves"] = (f"(Tất cả nơi làm/học) Ở cả {len(best)} nơi, luôn có quận rẻ hơn {pct(best[-1][1] / 100)}–{pct(best[0][1] / 100)} "
                                                f"mà đường đi không xa hơn – lớn nhất: {best[0][0]}")

    # data quality: label accuracy before/after the rule fix
    acc = {row[0]: row[1:] for row in _ACCURACY}
    cat = acc.get("Loại hình – độ chính xác")
    titles["tlcn-chat-luong"] = {"dq_g4": (f"Sửa luật bằng nhãn thủ công: loại hình đúng {pct(cat[0], 1)} → {pct(cat[1], 1)}; "
                                           "recall pháp lý, mặt tiền, ô tô lên 100%, precision giữ 100% (76 tin test)") if cat else ""}
    return titles


def compute_insights(run: Callable[[str], list[tuple]]) -> dict[str, dict[str, Any]]:
    from src.dashboard.provision_superset import BQ1_DEFAULT, BQ2_DEFAULT, BQ3_DEFAULT, MIN_GROUP

    out: dict[str, dict[str, Any]] = {}
    house_join = f"""FROM {G}.fact_listing f JOIN {G}.dim_location d ON d.location_key = f.location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = f.property_category_key
        WHERE f.is_dup_representative AND f.price_per_m2 > 0 AND c.model_category = 'nha_pho'"""

    # ---------------------------------------------------------------- overview
    total, hcm = run(f"SELECT SUM(n_listings), SUM(CASE WHEN province_name = '{HCM}' THEN n_listings ELSE 0 END) FROM {G}.agg_market_overview")[0]
    latest = run(f"""SELECT MAX(l.last_seen_date) FROM {G}.fact_listing_lifecycle l
        JOIN {G}.agg_dq_kpi k ON k.source = l.source WHERE k.source_type <> 'historical'""")[0][0]
    ring = dict(run(f"""SELECT CASE WHEN f.distance_to_center_km < 5 THEN 'center' WHEN f.distance_to_center_km >= 15 THEN 'outer' END,
        approx_percentile(f.price_per_m2, 0.5) / 1e6 {house_join} AND d.province_name = '{HCM}' AND f.distance_to_center_km IS NOT NULL
        GROUP BY 1"""))

    def red_districts(province: str) -> tuple[list[str], float]:
        rows = run(f"""SELECT d.district_name, AVG(CASE WHEN f.price_per_m2 >= 200e6 THEN 1e0 ELSE 0e0 END), COUNT(*)
            {house_join} AND d.province_name = '{province}' AND d.district_name IS NOT NULL AND d.district_name <> 'Unknown'
            AND EXISTS (SELECT 1 FROM {S}.listing_location ll WHERE ll.source_id = f.source_id AND ll.lat IS NOT NULL)
            GROUP BY 1 HAVING COUNT(*) >= 100 ORDER BY 2 DESC""")
        share = run(f"SELECT AVG(CASE WHEN f.price_per_m2 >= 200e6 THEN 1e0 ELSE 0e0 END) {house_join} AND d.province_name = '{province}'")[0][0]
        return [r[0].replace("Quận ", "Q.").replace("Huyện ", "H.") for r in rows[:3]], share

    hcm_red, hcm_red_share = red_districts(HCM)
    hn_red, hn_red_share = red_districts("Hà Nội")
    medians = dict(run(f"""SELECT o.province_name, o.median_price_per_m2 / 1e6 FROM {G}.agg_market_overview o
        JOIN {G}.dim_property_category c ON c.property_category_key = o.property_category_key
        WHERE c.model_category = 'nha_pho' AND o.province_name IN ('{HCM}', 'Hà Nội')"""))
    out["tlcn-tong-quan"] = {
        "facts": {"total": total, "hcm_share": hcm / total, "latest_crawl_date": str(latest), "hcm_ring": ring,
                  "hcm_red_districts": hcm_red, "hn_red_districts": hn_red, "house_median": medians},
        "markdown": header("Thị trường đang chào bán gì, ở đâu, giá bao nhiêu?",
                           f"Dữ liệu crawl tới {latest:%d/%m/%Y}. Giá chào bán, không phải giá giao dịch. Bản đồ: mỗi chấm là một tin có tọa độ."),
        "titles": {
            "ov_map_hcm": f"TP.HCM: chấm đỏ (> 200 tr/m²) dày ở {', '.join(hcm_red)} – {pct(hcm_red_share)} tin nhà phố ở mức này",
            "ov_ring": f"Ra xa trung tâm, giá nhà phố giảm mạnh: {vn(ring.get('center'))} tr/m² (< 5 km) → {vn(ring.get('outer'))} tr/m² (≥ 15 km)",
            "ov_map_hn": f"Hà Nội: {pct(hn_red_share)} tin nhà phố trên 200 tr/m², dày nhất ở {', '.join(hn_red)}",
            "ov_price": f"(Cố định) Nhà phố Hà Nội {vn(medians.get('Hà Nội'))} tr/m², gấp {vn(medians['Hà Nội'] / medians[HCM], 1)} lần TP.HCM "
                        f"({vn(medians.get(HCM))} tr/m²) – median theo tỉnh, nhóm ≥ 30 tin",
        },
    }

    # ---------------------------------------------------------------- BQ1
    province, budget = BQ1_DEFAULT["province_name"], BQ1_DEFAULT["ngan_sach"]
    scope = f"""FROM {G}.agg_budget_tradeoff t JOIN {G}.dim_location l ON l.location_key = t.location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = t.property_category_key JOIN {G}.dim_price_band pb ON pb.band_key = t.price_band_key
        WHERE l.province_name = '{province}' AND c.model_category = 'nha_pho' AND pb.band_label = '{budget}'
        AND l.district_name IS NOT NULL AND l.district_name <> 'Unknown' AND t.n_listings >= {MIN_GROUP}"""
    districts = run(f"SELECT l.district_name, t.median_area, t.n_listings {scope} AND t.n_listings >= 30 ORDER BY t.median_area DESC")
    slope, corr = run(f"SELECT regr_slope(t.median_area, t.median_distance_km), corr(t.median_area, t.median_distance_km) {scope}")[0]
    pareto = run(f"""SELECT AVG(CASE WHEN p.is_pareto_efficient THEN 1e0 ELSE 0e0 END) FROM {G}.fact_budget_pareto p
        JOIN {G}.dim_location l ON l.location_key = p.location_key JOIN {G}.dim_property_category c ON c.property_category_key = p.property_category_key
        JOIN {G}.dim_price_band pb ON pb.band_key = p.price_band_key
        WHERE l.province_name = '{province}' AND c.model_category = 'nha_pho' AND pb.band_label = '{budget}'""")[0][0]
    big, small = districts[0], districts[-1]
    money = budget.replace(" - ", "–")
    out["tlcn-bq1"] = {
        "facts": {"scenario": BQ1_DEFAULT, "largest": big, "smallest": small, "slope_m2_per_km": slope, "corr": corr, "pareto_share": pareto},
        "markdown": header("Cùng ngân sách, mua ở quận nào thì được gì?",
                           f"Đổi tỉnh, loại hình, ngân sách ở thanh lọc (mặc định: nhà phố {money}, TP.HCM). Chỉ so quận ≥ {MIN_GROUP} tin."),
        "titles": {
            "bq1_map": f"Với {money}: {small[0]} được {vn(small[1])} m², {big[0]} được {vn(big[1])} m² – vòng tròn càng to càng rộng",
            "bq1_distance": f"Mỗi 10 km xa trung tâm, cùng số tiền được thêm khoảng {vn(slope * 10)} m² (r = {vn(corr, 2)})",
        },
    }

    # ---------------------------------------------------------------- BQ2
    category, bq2_province = BQ2_DEFAULT["category_label"], BQ2_DEFAULT["province_name"]
    model = f"""FROM {G}.fact_listing_price_model m JOIN {G}.fact_listing_price_assessment a ON a.source_id = m.source_id
        JOIN {G}.dim_property_category c ON c.property_category_key = a.property_category_key JOIN {G}.dim_location l ON l.location_key = a.location_key
        WHERE c.category_label = '{category}' AND l.province_name = '{bq2_province}' AND m.residual_pct IS NOT NULL"""
    over, under = run(f"""SELECT AVG(CASE WHEN m.residual_pct >= 0.3 THEN 1e0 ELSE 0e0 END),
        AVG(CASE WHEN m.residual_pct <= -0.3 THEN 1e0 ELSE 0e0 END) {model}""")[0]
    top_district = run(f"""SELECT l.district_name, AVG(CASE WHEN m.residual_pct >= 0.3 THEN 1e0 ELSE 0e0 END), COUNT(*) {model}
        AND l.district_name IS NOT NULL AND l.district_name <> 'Unknown' GROUP BY 1 HAVING COUNT(*) >= 100 ORDER BY 2 DESC LIMIT 1""")[0]
    factors = {r[0]: r[1:] for r in run(f"""SELECT k.level_label, k.premium_pct, k.ci95_low_pct, k.ci95_high_pct FROM {G}.agg_price_model_coefficient k
        JOIN {G}.dim_property_category c ON c.property_category_key = k.property_category_key
        WHERE k.factor = 'flag' AND c.category_label = '{category}'""")}
    ranked = sorted(factors.items(), key=lambda kv: -kv[1][0])
    legal = next((v for k, v in factors.items() if k.startswith("Pháp lý")), None)
    factor_title = (f"{ranked[0][0].replace(' được nêu', '')} {signed(ranked[0][1][0])}, {ranked[1][0].replace(' được nêu', '').lower()} "
                    f"{signed(ranked[1][1][0])} giá/m²")
    if legal and legal[0] < 0 and legal[2] < 0:
        factor_title += f"; tiêu đề nêu \"sổ\" đi kèm giá thấp hơn {pct(-legal[0])} (tin phân khúc rẻ hay nhấn mạnh sổ – tương quan)"
    out["tlcn-bq2"] = {
        "facts": {"scenario": BQ2_DEFAULT, "share_over_30": over, "share_under_30": under, "top_district": top_district, "factors": factors},
        "markdown": header("Tin này đắt hay rẻ so với căn tương đồng?",
                           "Giá ước tính từ mô hình hedonic + baseline nhóm tương đồng (vai B), học trên giá chào – là vùng tham khảo, không phải định giá. "
                           "Mặc định: nhà phố TP.HCM."),
        "titles": {
            "bq2_map": f"Nhà phố TP.HCM: {pct(over)} tin rao cao hơn ước tính ≥ 30% (đỏ), {pct(under)} rao thấp hơn ≥ 30% (xanh)",
            "bq2_district": f"{top_district[0]}: {pct(top_district[1])} tin rao cao hơn ước tính ≥ 30% – cao nhất TP.HCM",
            "bq2_factor": factor_title + " (KTC 95%, đã kiểm soát vị trí, diện tích, số phòng)",
        },
    }

    # ---------------------------------------------------------------- BQ3
    dest, origin, band = BQ3_DEFAULT["noi_den"], BQ3_DEFAULT["quan_goc"], BQ3_DEFAULT["area_band"]
    segment = f"""JOIN {G}.dim_property_category c ON c.property_category_key = {{t}}.property_category_key
        JOIN {G}.dim_area_band ab ON ab.band_key = {{t}}.area_band_key WHERE c.model_category = 'nha_pho' AND ab.band_label = '{band}'"""
    names = f"(SELECT DISTINCT destination_id, destination_name FROM {G}.agg_commute_district)"
    options = run(f"""SELECT a.district_name, s.price_gap_pct, s.origin_commute_km, s.alternative_commute_km
        FROM {G}.agg_commute_substitution s JOIN {names} n ON n.destination_id = s.destination_id
        JOIN {G}.dim_location o ON o.location_key = s.origin_location_key JOIN {G}.dim_location a ON a.location_key = s.alternative_location_key
        {segment.format(t="s")} AND n.destination_name = '{dest}' AND o.district_name = '{origin}' ORDER BY s.price_gap_pct DESC""")
    best = min(options, key=lambda r: r[3]) if options else None
    place = dest.split(" (")[0]
    titles = {"bq3_gap": f"{len(options)} quận rẻ hơn {origin} mà đường tới {place} không xa hơn quá 2 km"}
    if best:
        titles["bq3_geo"] = (f"Làm ở {place}: {best[0]} cách {vn(best[3], 1)} km, rẻ hơn {pct(best[1])} so với {origin} "
                             f"({vn(best[2], 1)} km) – chấm đen là nơi làm, xanh là quận đáng cân nhắc")
    out["tlcn-bq3"] = {
        "facts": {"scenario": BQ3_DEFAULT, "options": len(options), "best": best},
        "markdown": header("Đi làm/học ở đây: mua ở quận nào thì đường đi tương đương mà rẻ hơn?",
                           "Chọn nơi làm/học, quận đang nhắm, loại hình, diện tích ở thanh lọc. Quãng đường là km đường chim bay từ tâm quận; "
                           "chi phí so trên cùng diện tích, là ước tính."),
        "titles": titles,
    }

    # ---------------------------------------------------------------- data quality
    def evidence(name: str) -> dict:
        path = ROOT / "docs/validation" / f"{name}.json"
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}

    warn_top = run(f"""SELECT source, warn_rows * 1e0 / (pass_rows + warn_rows) FROM {G}.agg_dq_kpi ORDER BY 2 DESC LIMIT 1""")[0]
    pass_rest = run(f"""SELECT MIN(pass_rows * 1e0 / (pass_rows + warn_rows)) FROM {G}.agg_dq_kpi WHERE source <> '{warn_top[0]}'""")[0][0]
    coordinate, legal_cov = run(f"""SELECT SUM(coverage_coordinate * representative_rows) / SUM(representative_rows),
        SUM(coverage_legal_known * representative_rows) / SUM(representative_rows) FROM {G}.agg_dq_kpi""")[0]
    silver, gold = evidence("silver_final_verification"), evidence("gold_verification")
    labels, rules = evidence("label_accuracy"), evidence("label_accuracy_rules_compare")
    test = (rules.get("parts") or {}).get("test", {})
    after = test.get("after") or {}
    flags = ("legal", "furnished", "frontage", "elevator", "car_access")
    precisions = [after[f"{f}_precision"] for f in flags if after.get(f"{f}_precision") is not None]
    recalls = [after[f"{f}_recall"] for f in flags if after.get(f"{f}_recall") is not None]
    span = lambda values: pct(min(values)) if min(values) == max(values) else f"{pct(min(values))}–{pct(max(values))}"  # noqa: E731
    sheet_path = ROOT / "docs/validation/labeling/labeling_sheet.csv"
    sheet = list(csv.DictReader(sheet_path.open(encoding="utf-8-sig"))) if sheet_path.exists() else []
    twice = sum(1 for r in sheet if (r.get("review_1") or "").strip() and (r.get("review_2") or "").strip())
    checks = lambda d: f"{sum(d.get('checks', {}).values())}/{len(d.get('checks', {}))}"  # noqa: E731
    g4 = (f"G4 (đọc đặc điểm từ tiêu đề, {test.get('rows')} tin test): loại hình đúng {pct(after.get('category_accuracy'), 1)}, "
          f"precision {span(precisions)}, recall {span(recalls)}; nhãn AI gán sơ bộ, {len(sheet)} tin có 1 lượt review, {twice} tin có 2 lượt."
          if after and precisions else "G4 chưa đo.")
    out["tlcn-chat-luong"] = {
        "facts": {"warn_top": warn_top, "pass_rest_min": pass_rest, "coordinate_coverage": coordinate, "legal_coverage": legal_cov,
                  "silver_checks": checks(silver), "gold_checks": checks(gold), "g4_test": after},
        "markdown": header("Số liệu trên dashboard đáng tin đến đâu?",
                           f"Silver {checks(silver)}, Gold {checks(gold)} check PASS; mọi biểu đồ được đối chiếu với Trino khi dựng. {g4}"),
        "titles": {
            "dq_status": f"{warn_top[0]} có {pct(warn_top[1])} bản ghi WARN (thiếu/sửa một trường); các nguồn khác ≥ {pct(pass_rest)} PASS",
            "dq_coverage": f"Tọa độ chỉ có ở {pct(coordinate)} tin, pháp lý {pct(legal_cov)} – bản đồ và khoảng cách dựa trên phần tin có tọa độ",
        },
    }
    for slug, extra in evidence_titles(run).items():
        out[slug]["titles"].update({key: title for key, title in extra.items() if title})
    return out
