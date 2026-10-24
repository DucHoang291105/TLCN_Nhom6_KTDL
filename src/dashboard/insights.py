"""Findings shown in each dashboard header, computed from Gold on every run.

Nothing here is typed by hand: each sentence is filled from a Trino query, so
the header changes when the data changes. ``compute_insights(run)`` takes a
function that executes SQL and returns rows.
"""

from __future__ import annotations

from typing import Any, Callable

G = "iceberg.gold"
HCM = "Hồ Chí Minh"


def vn(value: float, digits: int = 0) -> str:
    text = f"{value:,.{digits}f}"
    return text.replace(",", "X").replace(".", ",").replace("X", ".")


def pct(value: float, digits: int = 0) -> str:
    return vn(value * 100, digits) + "%"


def header(question: str, findings: list[str], note: str) -> str:
    """Markdown header: the question, three findings, one note on scope."""

    newline = chr(10)
    bullets = newline.join(f"- {f}" for f in findings)
    return newline.join([f"## {question}", "", bullets, "", f"<small style='color:#6b6b6b'>{note}</small>", ""])


def compute_insights(run: Callable[[str], list[tuple]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}

    # ---------------------------------------------------------------- overview
    total, hcm = run(f"SELECT SUM(n_listings), SUM(CASE WHEN province_name = '{HCM}' THEN n_listings ELSE 0 END) FROM {G}.agg_market_overview")[0]
    house = run(f"""SELECT o.province_name, o.median_price_per_m2 / 1e6 FROM {G}.agg_market_overview o
        JOIN {G}.dim_property_category c ON c.property_category_key = o.property_category_key
        WHERE c.model_category = 'nha_pho' AND o.n_listings >= 300 AND o.province_name <> 'Không rõ' ORDER BY 2 DESC""")
    medians = dict(run(f"""SELECT c.model_category, approx_percentile(f.price_per_m2, 0.5) / 1e6 FROM {G}.fact_listing f
        JOIN {G}.dim_property_category c ON c.property_category_key = f.property_category_key
        WHERE f.is_dup_representative AND f.price_per_m2 > 0 GROUP BY 1"""))
    bronze, reps = run(f"SELECT SUM(bronze_rows), SUM(representative_rows) FROM {G}.agg_dq_kpi")[0]
    facts = {
        "total": total, "hcm_share": hcm / total,
        "most_expensive_house": house[0], "cheapest_house": house[-1],
        "median_house": medians.get("nha_pho"), "median_apartment": medians.get("can_ho"),
        "bronze_rows": bronze, "representative_rows": reps,
    }
    out["tlcn-tong-quan"] = {"facts": facts, "markdown": header(
        "Thị trường đang chào bán gì, ở đâu, giá bao nhiêu?",
        [
            f"**{pct(facts['hcm_share'])}** trong {vn(total)} tin đại diện nằm ở **TP.HCM** – dữ liệu nghiêng mạnh về thị trường phía Nam.",
            f"Nhà phố đắt nhất ở **{house[0][0]}** (median {vn(house[0][1], 0)} triệu/m²), gấp khoảng {vn(house[0][1] / house[-1][1], 1)} lần "
            f"**{house[-1][0]}** ({vn(house[-1][1], 0)} triệu/m²) – chỉ xét tỉnh có ≥ 300 tin.",
            f"Căn hộ có median **{vn(medians.get('can_ho', 0), 0)} triệu/m²**, thấp hơn nhà phố ({vn(medians.get('nha_pho', 0), 0)} triệu/m²) "
            f"vì giá căn hộ không gồm đất.",
        ],
        f"Từ {vn(bronze)} dòng crawl còn {vn(reps)} tin đại diện sau kiểm tra chất lượng, lấy bản mới nhất mỗi tin, bỏ tin thuê và tin nghi trùng giữa nguồn. "
        "Giá là giá chào bán, không phải giá giao dịch.",
    )}

    # ---------------------------------------------------------------- BQ1
    scope = f"""FROM {G}.agg_budget_tradeoff t JOIN {G}.dim_location l ON l.location_key = t.location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = t.property_category_key
        WHERE l.province_name = '{HCM}' AND c.model_category = 'nha_pho' AND t.price_band_key = 4 AND t.n_listings >= 20 AND l.district_name IS NOT NULL"""
    districts = run(f"SELECT l.district_name, t.median_area, t.median_distance_km, t.median_rooms {scope} ORDER BY t.median_area DESC")
    corr = run(f"SELECT corr(t.median_area, t.median_distance_km) {scope}")[0][0]
    pareto = run(f"""SELECT AVG(CASE WHEN p.is_pareto_efficient THEN 1e0 ELSE 0e0 END), COUNT(*) FROM {G}.fact_budget_pareto p
        JOIN {G}.dim_location l ON l.location_key = p.location_key JOIN {G}.dim_property_category c ON c.property_category_key = p.property_category_key
        WHERE l.province_name = '{HCM}' AND c.model_category = 'nha_pho' AND p.price_band_key = 4""")[0]
    big, small = districts[0], districts[-1]
    out["tlcn-bq1"] = {"facts": {"largest": big, "smallest": small, "corr_area_distance": corr, "pareto_share": pareto[0], "pareto_n": pareto[1]},
                       "markdown": header(
        "Với 3–5 tỷ, mua nhà phố ở TP.HCM thì được gì và phải đánh đổi gì?",
        [
            f"Cùng ngân sách: **{big[0]}** cho diện tích median **{vn(big[1])} m²**, còn **{small[0]}** chỉ **{vn(small[1])} m²** "
            f"(gấp {vn(big[1] / small[1], 1)} lần).",
            f"Đánh đổi chính là khoảng cách: tương quan giữa diện tích và khoảng cách tới trung tâm là **r = {vn(corr, 2)}** "
            f"– xa hơn thì rộng hơn ({big[0]} cách trung tâm {vn(big[2] or 0, 1)} km, {small[0]} {vn(small[2] or 0, 1)} km).",
            f"Trong {vn(pareto[1])} tin của phân khúc, chỉ **{pct(pareto[0])}** không bị tin khác cùng quận trội hơn về giá, diện tích, số phòng và đặc điểm "
            "– đây là danh sách nên xem trước.",
        ],
        "Mỗi quận cần ≥ 20 tin. Khoảng cách chỉ có ở tin có tọa độ. Đặc điểm (mặt tiền, ô tô) đọc từ title nên là cận dưới. "
        "Giá/diện tích không thể có (lỗi parse) đã được Silver sửa hoặc để trống (DQ14–DQ16).",
    )}

    # ---------------------------------------------------------------- BQ2
    # P25/P75 split the group into quarters by construction, so the finding is
    # the size of the gap, not the share beyond the quartiles.
    over25, double, under20 = run(f"""SELECT AVG(CASE WHEN price_ratio >= 1.25 THEN 1e0 ELSE 0e0 END),
        AVG(CASE WHEN price_ratio >= 2 THEN 1e0 ELSE 0e0 END), AVG(CASE WHEN price_ratio <= 0.8 THEN 1e0 ELSE 0e0 END)
        FROM {G}.fact_listing_price_assessment WHERE price_ratio IS NOT NULL AND NOT (price_ratio > 10)""")[0]
    low_legal = run(f"""SELECT AVG(CASE WHEN is_low_price_missing_legal THEN 1e0 ELSE 0e0 END) FROM {G}.fact_listing_price_assessment
        WHERE price_position = 'duoi_p25'""")[0][0]
    district = run(f"""SELECT l.district_name, AVG(CASE WHEN a.price_position = 'tren_p75' THEN 1e0 ELSE 0e0 END), COUNT(*)
        FROM {G}.fact_listing_price_assessment a JOIN {G}.dim_location l ON l.location_key = a.location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = a.property_category_key
        WHERE l.province_name = '{HCM}' AND c.model_category = 'can_ho' AND l.district_name IS NOT NULL
        GROUP BY 1 HAVING COUNT(*) >= 100 ORDER BY 2 DESC LIMIT 1""")[0]
    suspect = run(f"SELECT SUM(CASE WHEN price_ratio > 10 THEN 1 ELSE 0 END) FROM {G}.fact_listing_price_assessment")[0][0]
    out["tlcn-bq2"] = {"facts": {"share_over_25pct": over25, "share_double": double, "share_under_20pct": under20, "low_missing_legal": low_legal, "top_district": district, "suspect": suspect},
                       "markdown": header(
        "Tin nào đang rao cao hoặc thấp bất thường so với các căn tương đồng?",
        [
            f"**{pct(over25)}** tin rao giá/m² cao hơn median các căn tương đồng (cùng quận, loại hình, diện tích, số phòng) từ 25% trở lên, "
            f"**{pct(double)}** rao **gấp đôi**; ở chiều ngược lại {pct(under20)} tin rẻ hơn median từ 20% trở lên.",
            f"Căn hộ ở **{district[0]}** có tỷ lệ tin rao cao nhất TP.HCM: **{pct(district[1])}** trong {vn(district[2])} tin vượt P75 của nhóm.",
            f"**{pct(low_legal)}** tin rao rẻ (dưới P25) không nêu pháp lý trong title – 'giá tốt' thường đi kèm thông tin còn thiếu, cần hỏi giấy tờ trước.",
        ],
        f"P25–P75 là vị trí trong phân phối giá chào của nhóm, không phải đánh giá giá hợp lý. {vn(suspect)} tin rao > 10× median nhóm "
        "là outlier (giữ lại, xem riêng); lỗi parse giá/diện tích đã được Silver xử lý (DQ14–DQ16).",
    )}

    # ---------------------------------------------------------------- BQ3
    base = f"""FROM {G}.agg_area_substitution s JOIN {G}.dim_location o ON o.location_key = s.origin_location_key
        JOIN {G}.dim_location a ON a.location_key = s.alternative_location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = s.property_category_key
        JOIN {G}.dim_area_band ab ON ab.band_key = s.area_band_key
        WHERE c.model_category = 'nha_pho' AND ab.band_label = '50 - 80 m²'"""
    alternatives = run(f"""SELECT a.district_name, s.price_gap_pct, s.estimated_saving_at_target_area / 1e9, s.distance_diff_km, s.target_area
        {base} AND o.district_name = 'Quận Bình Tân' ORDER BY s.price_gap_pct DESC""")
    near = [r for r in alternatives if r[3] is not None and r[3] <= 5]
    best_near = near[0] if near else None
    cheapest = alternatives[0]
    coverage = run(f"""SELECT COUNT(DISTINCT o.district_name) {base} AND o.province_name = '{HCM}' AND s.distance_diff_km <= 5""")[0][0]
    origins = run(f"""SELECT COUNT(*) FROM {G}.agg_peer_group_benchmark b JOIN {G}.dim_location l ON l.location_key = b.location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = b.property_category_key JOIN {G}.dim_area_band ab ON ab.band_key = b.area_band_key
        WHERE b.benchmark_level = 'LOC_CAT_AREA' AND l.province_name = '{HCM}' AND c.model_category = 'nha_pho' AND ab.band_label = '50 - 80 m²'""")[0][0]
    findings = [
        f"Muốn mua nhà phố 50–80 m² ở **Quận Bình Tân** (diện tích mục tiêu {vn(cheapest[4])} m²): có **{len(alternatives)}** quận cùng tỉnh "
        "có hồ sơ đặc điểm tương tự và chi phí thấp hơn.",
    ]
    if best_near:
        findings.append(
            f"Lựa chọn cân bằng nhất: **{best_near[0]}** – chi phí thấp hơn **{pct(best_near[1])}** (khoảng {vn(best_near[2], 1)} tỷ) "
            f"mà chỉ xa trung tâm thêm {vn(best_near[3], 1)} km.")
    findings.append(
        f"Rẻ nhất là **{cheapest[0]}** (thấp hơn {pct(cheapest[1])}) nhưng xa thêm {vn(cheapest[3] or 0, 1)} km – rẻ hơn nhiều thường đi kèm xa hơn nhiều.")
    out["tlcn-bq3"] = {"facts": {"alternatives": len(alternatives), "best_near": best_near, "cheapest": cheapest,
                                 "hcm_origins_with_near_option": coverage, "hcm_origins": origins},
                       "markdown": header(
        "Không mua được ở quận mong muốn thì nên xem quận nào?", findings,
        f"Ở TP.HCM, {coverage}/{origins} quận có nhà phố 50–80 m² đủ dữ liệu có ít nhất một lựa chọn rẻ hơn trong phạm vi +5 km. "
        "Chi phí so trên cùng diện tích mục tiêu (median giá/m² × diện tích), là ước tính, không phải khoản tiết kiệm bảo đảm.",
    )}
    return out
