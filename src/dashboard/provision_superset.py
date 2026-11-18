"""Provision the Superset dashboards for the Gold layer (idempotent).

Each dashboard answers one question: a header states the question and three
findings whose numbers are computed from Gold on every run (src/dashboard/
insights.py), a row of KPIs follows, then the charts that support the
findings, and detail tables last.

Every chart is saved with its query context, fetched back through the chart
data API and, where a check SQL exists, cross-checked against Trino; results go
to docs/validation/superset_dashboard_check.json.

Requires: docker compose --profile dashboard --profile query up -d
Run on the host: python -m src.dashboard.provision_superset
"""

from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE = os.getenv("SUPERSET_URL", "http://localhost:8088")
DB_NAME = "Lakehouse Gold (Trino)"
DB_URI = "trino://superset@trino:8080/iceberg"
PREFIX = "tlcn_"  # every dataset this script owns starts with this prefix
HCM = "Hồ Chí Minh"
HOUSE = "Nhà phố / nhà riêng"
APARTMENT = "Căn hộ chung cư"


def env(name: str, default: str | None = None) -> str:
    values = {}
    for line in (PROJECT_ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip()
    return os.getenv(name) or values.get(name) or default


# Impossible prices/areas are repaired or set to NULL in Silver (DQ14-DQ16).
# What remains above 10x the peer median is mostly genuine (prime frontage,
# beach land in a mixed peer group): kept, shown apart as extreme outliers and
# left out of the "typical gap" KPIs so a few cases do not drive them.
SUSPECT = "(price_ratio > 10)"
# B1.4: a district group needs at least this many listings to be shown.
MIN_GROUP = 5
OSM = "https://tile.openstreetmap.org/{z}/{x}/{y}.png"
VIEW_HCM = {"longitude": 106.68, "latitude": 10.80, "zoom": 10.4, "bearing": 0, "pitch": 0}
# Maps show the core metro only: the 2025 provinces reach 70+ km (Bà Rịa,
# Bình Dương), and fitting them all shrinks the city to a few pixels.
MAP_RADIUS_KM = 30
VIEW_HN = {"longitude": 105.83, "latitude": 21.01, "zoom": 10.8, "bearing": 0, "pitch": 0}

# ----------------------------------------------------------------- datasets
# SQL is sent as one line, so no "--" comments inside these strings.
# Map datasets name province/category "tinh"/"loai_hinh" when a map has a
# fixed scope, so the dashboard's province filter does not apply to it.
G = "iceberg.gold"
S = "iceberg.silver"
RING = """CASE WHEN f.distance_to_center_km < 5 THEN '1. Dưới 5 km' WHEN f.distance_to_center_km < 10 THEN '2. 5–10 km'
          WHEN f.distance_to_center_km < 15 THEN '3. 10–15 km' WHEN f.distance_to_center_km < 25 THEN '4. 15–25 km' ELSE '5. Trên 25 km' END"""
PRICE_CLASS = """CASE WHEN f.price_per_m2 < 60e6 THEN '1. Dưới 60 tr/m²' WHEN f.price_per_m2 < 100e6 THEN '2. 60–100 tr/m²'
                 WHEN f.price_per_m2 < 200e6 THEN '3. 100–200 tr/m²' ELSE '4. Trên 200 tr/m²' END"""
GAP_CLASS = """CASE WHEN m.residual_pct IS NULL THEN NULL WHEN m.residual_pct <= -0.3 THEN '1. Rao thấp hơn ước tính ≥ 30%'
               WHEN m.residual_pct < 0.3 THEN '2. Trong ±30% ước tính' ELSE '3. Rao cao hơn ước tính ≥ 30%' END"""
DATASETS: dict[str, str] = {
    "market_overview": f"""
        SELECT o.province_name, c.category_label, o.n_listings,
               o.median_price_per_m2 / 1e6 AS median_price_per_m2_tr, o.median_price / 1e9 AS median_price_ty, o.median_area
        FROM {G}.agg_market_overview o
        JOIN {G}.dim_property_category c ON c.property_category_key = o.property_category_key""",
    "map_price": f"""
        SELECT d.province_name AS tinh, c.model_category AS loai_hinh, ll.lat, ll.lon, f.price_per_m2 / 1e6 AS gia_m2_tr,
               {PRICE_CLASS} AS muc_gia, {RING} AS vanh_dai
        FROM {G}.fact_listing f
        JOIN {S}.listing_location ll ON ll.source_id = f.source_id
        JOIN {G}.dim_location d ON d.location_key = f.location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = f.property_category_key
        WHERE f.is_dup_representative AND ll.lat IS NOT NULL AND f.price_per_m2 > 0 AND f.distance_to_center_km IS NOT NULL""",
    "funnel": f"""
        SELECT stage, rows FROM (
            SELECT 1 AS ord, '1. Bronze (đã crawl)' AS stage, SUM(bronze_rows) AS rows FROM {G}.agg_dq_kpi
            UNION ALL SELECT 2, '2. Silver observation (hợp lệ, bỏ trùng batch)', SUM(observation_rows) FROM {G}.agg_dq_kpi
            UNION ALL SELECT 3, '3. Tin current (mới nhất mỗi tin)', SUM(current_rows) FROM {G}.agg_dq_kpi
            UNION ALL SELECT 4, '4. Tin bán (fact_listing)', SUM(fact_rows) FROM {G}.agg_dq_kpi
            UNION ALL SELECT 5, '5. Tin đại diện (bỏ nghi trùng nguồn)', SUM(representative_rows) FROM {G}.agg_dq_kpi
        ) t""",
    "price_check": f"""
        SELECT l.province_name, NULLIF(l.district_name, 'Unknown') AS district_name, c.category_label,
               COALESCE(NULLIF(l.district_name, 'Unknown'), l.province_name) AS khu_vuc,
               a.price_position, a.price_ratio, a.peer_n,
               a.price / 1e9 AS gia_ty, a.area, a.price_per_m2 / 1e6 AS gia_m2_tr,
               a.peer_p25_price_per_m2 / 1e6 AS p25_nhom_tr, a.peer_p75_price_per_m2 / 1e6 AS p75_nhom_tr,
               m.predicted_price_per_m2 / 1e6 AS uoc_tinh_tr, m.residual_pct * 100 AS chenh_uoc_tinh_pct,
               m.split AS tap_mo_hinh, COALESCE(m.is_outlier_trimmed, false) AS is_outlier_trimmed,
               {GAP_CLASS} AS muc_chenh, ll.lat, ll.lon, f.distance_to_center_km AS kc_trung_tam, f.title, f.ad_url
        FROM {G}.fact_listing_price_assessment a
        JOIN {G}.dim_location l ON l.location_key = a.location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = a.property_category_key
        JOIN {G}.fact_listing f ON f.source_id = a.source_id
        LEFT JOIN {G}.fact_listing_price_model m ON m.source_id = a.source_id
        LEFT JOIN {S}.listing_location ll ON ll.source_id = a.source_id""",
    "price_factor": f"""
        SELECT c.category_label, k.premium_pct * 100 AS chenh_gia_pct, k.train_rows,
               CASE WHEN k.ci95_low_pct > 0 OR k.ci95_high_pct < 0 THEN '1. Chắc chắn (KTC 95% không chứa 0)'
                    ELSE '2. Chưa đủ chắc chắn' END AS do_tin_cay,
               replace(k.level_label, ' được nêu', '') || ' (KTC ' || format('%.0f', k.ci95_low_pct * 100) || '…'
                   || format('%.0f', k.ci95_high_pct * 100) || '%, ' || CAST(k.train_rows AS varchar) || ' tin)' AS dac_diem_nhan
        FROM {G}.agg_price_model_coefficient k
        JOIN {G}.dim_property_category c ON c.property_category_key = k.property_category_key
        WHERE k.factor = 'flag'""",
    "budget_tradeoff": f"""
        SELECT l.province_name, NULLIF(l.district_name, 'Unknown') AS district_name, c.category_label, pb.band_key AS price_band_key,
               pb.band_label AS ngan_sach, t.n_listings, t.median_area, t.median_rooms,
               t.median_price_per_m2 / 1e6 AS median_price_per_m2_tr, t.median_distance_km,
               k.lat, k.lon,
               CASE WHEN t.median_area < 50 THEN '1. Dưới 50 m²' WHEN t.median_area < 80 THEN '2. 50–80 m²'
                    WHEN t.median_area < 150 THEN '3. 80–150 m²' ELSE '4. Trên 150 m²' END AS dien_tich_nhan,
               lpad(format('%.1f', t.median_distance_km), 4, '0') || ' km · ' || l.district_name || ' · '
                   || CAST(t.n_listings AS varchar) || ' tin' AS quan_theo_khoang_cach
        FROM {G}.agg_budget_tradeoff t
        JOIN {G}.dim_location l ON l.location_key = t.location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = t.property_category_key
        JOIN {G}.dim_price_band pb ON pb.band_key = t.price_band_key
        LEFT JOIN (SELECT location_key, MAX(centroid_lat) AS lat, MAX(centroid_lon) AS lon FROM {G}.agg_commute_district GROUP BY 1) k
             ON k.location_key = t.location_key
        WHERE l.district_name IS NOT NULL AND l.district_name <> 'Unknown'""",
    "budget_pareto": f"""
        SELECT l.province_name, NULLIF(l.district_name, 'Unknown') AS district_name, c.category_label, pb.band_key AS price_band_key, pb.band_label AS ngan_sach,
               p.is_pareto_efficient, f.title, f.price / 1e9 AS gia_ty, f.area, f.rooms,
               f.title_has_frontage, f.title_has_car_access, f.ad_url,
               f.price_per_m2 / 1e6 AS gia_m2_tr, f.price_per_m2 / t.median_price_per_m2 AS so_voi_median_quan
        FROM {G}.fact_budget_pareto p
        JOIN {G}.fact_listing f ON f.source_id = p.source_id
        JOIN {G}.agg_budget_tradeoff t ON t.location_key = p.location_key AND t.property_category_key = p.property_category_key
             AND t.price_band_key = p.price_band_key
        JOIN {G}.dim_location l ON l.location_key = p.location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = p.property_category_key
        JOIN {G}.dim_price_band pb ON pb.band_key = p.price_band_key""",
    "commute_district": f"""
        SELECT d.destination_name AS noi_den, d.province_name, l.district_name, l.district_name AS quan_goc,
               c.category_label, ab.band_label AS area_band, d.commute_km, d.n_listings,
               d.estimated_cost_at_target_area / 1e9 AS chi_phi_ty,
               CASE WHEN d.is_commute_efficient THEN '1. Đáng cân nhắc' ELSE '2. Có quận vừa gần hơn vừa rẻ hơn' END AS lua_chon,
               lpad(format('%.1f', d.commute_km), 4, '0') || ' km · ' || l.district_name || ' · '
                   || CAST(d.n_listings AS varchar) || ' tin' AS quan_theo_quang_duong
        FROM {G}.agg_commute_district d
        JOIN {G}.dim_location l ON l.location_key = d.location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = d.property_category_key
        JOIN {G}.dim_area_band ab ON ab.band_key = d.area_band_key""",
    "commute_map": f"""
        SELECT d.destination_name AS noi_den, c.category_label, ab.band_label AS area_band,
               d.centroid_lat AS lat, d.centroid_lon AS lon, d.estimated_cost_at_target_area / 1e9 AS chi_phi_ty,
               d.estimated_cost_at_target_area / 1e9 + 1 AS kich_thuoc, d.commute_km,
               CASE WHEN d.is_commute_efficient THEN '1. Đáng cân nhắc' ELSE '2. Có quận vừa gần hơn vừa rẻ hơn' END AS lua_chon
        FROM {G}.agg_commute_district d
        JOIN {G}.dim_property_category c ON c.property_category_key = d.property_category_key
        JOIN {G}.dim_area_band ab ON ab.band_key = d.area_band_key
        UNION ALL
        SELECT DISTINCT d.destination_name, c.category_label, ab.band_label, d.destination_lat, d.destination_lon, 0e0, 12e0, 0e0, '0. Nơi làm/học'
        FROM {G}.agg_commute_district d
        JOIN {G}.dim_property_category c ON c.property_category_key = d.property_category_key
        JOIN {G}.dim_area_band ab ON ab.band_key = d.area_band_key""",
    "commute_substitution": f"""
        SELECT n.destination_name AS noi_den, o.province_name, o.district_name AS quan_goc, a.district_name AS quan_thay_the,
               c.category_label, ab.band_label AS area_band, s.price_gap_pct * 100 AS re_hon_pct,
               s.estimated_saving_at_target_area / 1e9 AS chenh_chi_phi_ty,
               a.district_name || ' · ' || format('%.1f', s.alternative_commute_km) || ' km (' || CASE WHEN s.commute_diff_km >= 0 THEN '+' ELSE '' END
                   || format('%.1f', s.commute_diff_km) || ') · ' || CAST(s.alternative_n_listings AS varchar) || ' tin' AS quan_thay_the_qd
        FROM {G}.agg_commute_substitution s
        JOIN (SELECT DISTINCT destination_id, destination_name FROM {G}.agg_commute_district) n ON n.destination_id = s.destination_id
        JOIN {G}.dim_location o ON o.location_key = s.origin_location_key
        JOIN {G}.dim_location a ON a.location_key = s.alternative_location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = s.property_category_key
        JOIN {G}.dim_area_band ab ON ab.band_key = s.area_band_key""",
    "commute_listings": f"""
        SELECT n.destination_name AS noi_den, o.district_name AS quan_goc, a.district_name AS quan_thay_the, c.category_label,
               ab.band_label AS area_band, s.alternative_commute_km AS quang_duong_km,
               f.title, f.price / 1e9 AS gia_ty, f.area, f.price_per_m2 / 1e6 AS gia_m2_tr,
               s.alternative_median_price_per_m2 / 1e6 AS median_quan_thay_the_tr, f.ad_url
        FROM {G}.agg_commute_substitution s
        JOIN (SELECT DISTINCT destination_id, destination_name FROM {G}.agg_commute_district) n ON n.destination_id = s.destination_id
        JOIN {G}.fact_listing f ON f.location_key = s.alternative_location_key AND f.property_category_key = s.property_category_key
             AND f.area_band_key = s.area_band_key AND f.is_dup_representative AND f.price_per_m2 > 0
        JOIN {G}.dim_location o ON o.location_key = s.origin_location_key
        JOIN {G}.dim_location a ON a.location_key = s.alternative_location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = s.property_category_key
        JOIN {G}.dim_area_band ab ON ab.band_key = s.area_band_key""",
    "data_quality": f"""
        SELECT k.source, CASE k.source_type WHEN 'historical' THEN 'Lịch sử (nạp một lần)' ELSE 'Crawl định kỳ' END AS loai_nguon,
               k.bronze_rows, k.current_rows, k.representative_rows,
               k.pass_rows, k.warn_rows, k.reject_rows, k.dup_suspect_rate,
               k.coverage_price_known, k.coverage_area_known, k.coverage_rooms_known, k.coverage_province,
               k.coverage_coordinate, k.coverage_legal_known, CAST(f.ngay_moi_nhat AS varchar) AS ngay_moi_nhat
        FROM {G}.agg_dq_kpi k
        LEFT JOIN (SELECT source, MAX(last_seen_date) AS ngay_moi_nhat FROM {G}.fact_listing_lifecycle GROUP BY source) f ON f.source = k.source""",
}


def m(expression: str, label: str) -> dict[str, Any]:
    return {"expressionType": "SQL", "sqlExpression": expression, "label": label}


# Dashboards with required filters open on a default scenario; their charts
# follow the filters, and the saved query context (used by the data API
# check) carries the default values.
BQ1_DEFAULT = {"province_name": HCM, "category_label": HOUSE, "ngan_sach": "3 - 5 tỷ"}
BQ2_DEFAULT = {"category_label": HOUSE, "province_name": HCM}
BQ3_DEFAULT = {"noi_den": "Khu công nghệ cao (Thủ Đức)", "quan_goc": "Quận Phú Nhuận", "category_label": HOUSE, "area_band": "50 - 80 m²"}
BQ3_PAIRS = f"""FROM {G}.agg_commute_substitution s JOIN {G}.agg_commute_district d ON d.destination_id = s.destination_id
    AND d.location_key = s.origin_location_key AND d.property_category_key = s.property_category_key AND d.area_band_key = s.area_band_key
    JOIN {G}.dim_location o ON o.location_key = s.origin_location_key JOIN {G}.dim_property_category c ON c.property_category_key = s.property_category_key
    JOIN {G}.dim_area_band ab ON ab.band_key = s.area_band_key
    WHERE d.destination_name = '{BQ3_DEFAULT["noi_den"]}' AND o.district_name = '{BQ3_DEFAULT["quan_goc"]}'
    AND c.category_label = '{HOUSE}' AND ab.band_label = '{BQ3_DEFAULT["area_band"]}'"""
BQ1_GROUPS = f"""FROM {G}.agg_budget_tradeoff t JOIN {G}.dim_location l ON l.location_key = t.location_key
    JOIN {G}.dim_property_category c ON c.property_category_key = t.property_category_key JOIN {G}.dim_price_band pb ON pb.band_key = t.price_band_key
    WHERE l.province_name = '{HCM}' AND c.category_label = '{HOUSE}' AND pb.band_label = '{BQ1_DEFAULT["ngan_sach"]}'
    AND l.district_name IS NOT NULL AND l.district_name <> 'Unknown' AND t.n_listings >= {MIN_GROUP}"""
BQ2_MODEL = f"""FROM {G}.fact_listing_price_model m JOIN {G}.fact_listing_price_assessment a ON a.source_id = m.source_id
    JOIN {G}.dim_property_category c ON c.property_category_key = a.property_category_key JOIN {G}.dim_location l ON l.location_key = a.location_key
    WHERE c.category_label = '{HOUSE}' AND l.province_name = '{HCM}'"""
OVER, UNDER = "chenh_uoc_tinh_pct >= 30", "chenh_uoc_tinh_pct <= -30"

# Colours carry meaning: red = expensive / priced high / small, green = cheap /
# priced low / large, grey = typical. Applied through the dashboards' label_colors.
LABEL_COLORS = {
    "1. Dưới 60 tr/m²": "#1a9850", "2. 60–100 tr/m²": "#d9ef8b", "3. 100–200 tr/m²": "#fc8d59", "4. Trên 200 tr/m²": "#d73027",
    "1. Rao thấp hơn ước tính ≥ 30%": "#1a9850", "2. Trong ±30% ước tính": "#bdbdbd", "3. Rao cao hơn ước tính ≥ 30%": "#d73027",
    "1. Dưới 50 m²": "#d73027", "2. 50–80 m²": "#fc8d59", "3. 80–150 m²": "#91cf60", "4. Trên 150 m²": "#1a9850",
    "0. Nơi làm/học": "#2b2b2b", "1. Đáng cân nhắc": "#1a9850", "2. Có quận vừa gần hơn vừa rẻ hơn": "#bdbdbd",
    "1. Chắc chắn (KTC 95% không chứa 0)": "#2c7fb8", "2. Chưa đủ chắc chắn": "#bdbdbd",
    "PASS": "#1a9850", "WARN": "#fee08b", "REJECT": "#d73027",
}

# Chart specs. viz: kpi | bar | hbar | funnel | table | map. "scenario": slug
# whose default filters the saved query context carries. Chart titles marked
# "title_from_insights" are replaced by a finding computed from Gold.
CHARTS: list[dict[str, Any]] = [
    # ---------------------------------------------------------- overview
    {"key": "ov_listings", "name": "Tin rao bán đang phân tích", "dataset": "market_overview", "viz": "kpi",
     "metric": m("SUM(n_listings)", "Tin"), "sub": "tin đại diện, đã bỏ nghi trùng giữa 9 nguồn",
     "check_sql": f"SELECT SUM(n_listings) FROM {G}.agg_market_overview"},
    {"key": "ov_hcm_share", "name": "Tỷ trọng TP.HCM", "dataset": "market_overview", "viz": "kpi", "format": ".1%",
     "metric": m(f"SUM(CASE WHEN province_name = '{HCM}' THEN n_listings ELSE 0 END) * 1e0 / SUM(n_listings)", "TP.HCM"),
     "sub": "số tin nằm ở TP.HCM"},
    {"key": "ov_median_m2", "name": "Median giá chào/m² (triệu)", "dataset": "price_check", "viz": "kpi", "format": ",.1f",
     "metric": m("APPROX_PERCENTILE(gia_m2_tr, 0.5)", "Triệu/m²"), "where": f"NOT {SUSPECT}", "sub": "theo bộ lọc đang chọn"},
    {"key": "ov_provinces", "name": "Tỉnh/thành có tin", "dataset": "market_overview", "viz": "kpi",
     "metric": m("COUNT(DISTINCT province_name)", "Tỉnh"), "where": "province_name <> 'Không rõ'", "sub": "trên 34 tỉnh/thành"},
    {"key": "ov_map_hcm", "name": "Giá/m² nhà phố TP.HCM theo vị trí", "title_from_insights": True, "dataset": "map_price", "viz": "map",
     "dimension": "muc_gia", "viewport": VIEW_HCM, "radius": 140, "where": f"tinh = '{HCM}' AND loai_hinh = 'nha_pho'", "width": 7, "height": 95},
    {"key": "ov_ring", "name": "Giá/m² theo khoảng cách tới trung tâm (TP.HCM, nhà phố)", "title_from_insights": True, "dataset": "map_price",
     "viz": "bar", "x": "vanh_dai", "metric": m("APPROX_PERCENTILE(gia_m2_tr, 0.5)", "Median triệu/m²"), "sort_x": True,
     "format": ",.0f", "where": f"tinh = '{HCM}' AND loai_hinh = 'nha_pho'", "width": 5, "height": 95},
    {"key": "ov_map_hn", "name": "Giá/m² nhà phố Hà Nội theo vị trí", "title_from_insights": True, "dataset": "map_price", "viz": "map",
     "dimension": "muc_gia", "viewport": VIEW_HN, "radius": 120, "where": "tinh = 'Hà Nội' AND loai_hinh = 'nha_pho'", "width": 7, "height": 95},
    {"key": "ov_price", "name": "Median giá/m² (triệu) theo tỉnh và loại hình – nhóm ≥ 30 tin", "title_from_insights": True,
     "dataset": "market_overview", "viz": "hbar", "x": "province_name", "groupby": ["category_label"],
     "metric": m("MAX(median_price_per_m2_tr)", "Triệu/m²"), "where": f"category_label IN ('{HOUSE}', '{APARTMENT}') AND n_listings >= 30",
     "top_provinces": 8, "width": 5, "height": 95},
    # ---------------------------------------------------------- BQ1
    {"key": "bq1_listings", "name": "Tin trong phân khúc", "dataset": "budget_tradeoff", "viz": "kpi", "scenario": "tlcn-bq1",
     "metric": m("SUM(n_listings)", "Tin"), "sub": "theo tỉnh, loại hình, ngân sách đã chọn",
     "check_sql": f"SELECT SUM(t.n_listings) {BQ1_GROUPS.replace(f' AND t.n_listings >= {MIN_GROUP}', '')}"},
    {"key": "bq1_districts", "name": f"Quận đủ dữ liệu (≥ {MIN_GROUP} tin)", "dataset": "budget_tradeoff", "viz": "kpi", "scenario": "tlcn-bq1",
     "metric": m("COUNT(*)", "Quận"), "where": f"n_listings >= {MIN_GROUP}", "sub": "chỉ các quận này được so sánh",
     "check_sql": f"SELECT COUNT(*) {BQ1_GROUPS}"},
    {"key": "bq1_pareto_share", "name": "Tin không bị trội", "dataset": "budget_pareto", "viz": "kpi", "format": ".1%", "scenario": "tlcn-bq1",
     "metric": m("AVG(CASE WHEN is_pareto_efficient THEN 1e0 ELSE 0e0 END)", "Pareto"), "sub": "danh sách nên xem trước ở cuối trang"},
    {"key": "bq1_map", "name": "Cùng ngân sách, mỗi quận cho bao nhiêu m²?", "title_from_insights": True, "dataset": "budget_tradeoff",
     "viz": "map", "dimension": "dien_tich_nhan", "radius_metric": m("SQRT(MAX(median_area))", "√ median diện tích"), "multiplier": 160,
     "autozoom": True, "viewport": VIEW_HCM, "scenario": "tlcn-bq1",
     "where": f"n_listings >= {MIN_GROUP} AND lat IS NOT NULL AND median_distance_km <= {MAP_RADIUS_KM}",
     "width": 7, "height": 110},
    {"key": "bq1_distance", "name": "Median diện tích (m²), quận xếp theo khoảng cách tới trung tâm", "title_from_insights": True,
     "dataset": "budget_tradeoff", "viz": "hbar", "x": "quan_theo_khoang_cach", "metric": m("MAX(median_area)", "m²"),
     "sort_x": True, "scenario": "tlcn-bq1", "where": f"n_listings >= {MIN_GROUP} AND median_distance_km IS NOT NULL", "width": 5, "height": 110},
    {"key": "bq1_table", "name": "Tin nên xem trước: không có tin cùng quận vừa rẻ hơn vừa không kém (bỏ tin giá/m² lệch > 3 lần median quận)",
     "dataset": "budget_pareto", "viz": "table", "scenario": "tlcn-bq1",
     "columns": ["district_name", "title", "gia_ty", "area", "gia_m2_tr", "rooms", "ad_url"],
     "order": [["district_name", True], ["gia_ty", True]], "row_limit": 500,
     "where": "is_pareto_efficient AND so_voi_median_quan BETWEEN 1e0 / 3 AND 3"},
    # ---------------------------------------------------------- BQ2
    {"key": "bq2_error", "name": "Sai số điển hình của giá ước tính (%)", "dataset": "price_check", "viz": "kpi", "format": ",.0f",
     "scenario": "tlcn-bq2", "metric": m("APPROX_PERCENTILE(ABS(chenh_uoc_tinh_pct), 0.5)", "%"),
     "where": "tap_mo_hinh = 'test' AND NOT is_outlier_trimmed", "sub": "nên chỉ tin vào mức lệch ≥ 30%"},
    {"key": "bq2_over", "name": "Rao cao hơn ước tính ≥ 30%", "dataset": "price_check", "viz": "kpi", "format": ".1%", "scenario": "tlcn-bq2",
     "metric": m(f"AVG(CASE WHEN {OVER} THEN 1e0 ELSE 0e0 END)", "Tỷ lệ"), "where": "chenh_uoc_tinh_pct IS NOT NULL",
     "sub": "nên thương lượng", "check_sql": f"SELECT AVG(CASE WHEN m.residual_pct >= 0.3 THEN 1e0 ELSE 0e0 END) {BQ2_MODEL} AND m.residual_pct IS NOT NULL"},
    {"key": "bq2_under", "name": "Rao thấp hơn ước tính ≥ 30%", "dataset": "price_check", "viz": "kpi", "format": ".1%", "scenario": "tlcn-bq2",
     "metric": m(f"AVG(CASE WHEN {UNDER} THEN 1e0 ELSE 0e0 END)", "Tỷ lệ"), "where": "chenh_uoc_tinh_pct IS NOT NULL",
     "sub": "giá tốt hoặc thiếu thông tin – kiểm tra kỹ",
     "check_sql": f"SELECT AVG(CASE WHEN m.residual_pct <= -0.3 THEN 1e0 ELSE 0e0 END) {BQ2_MODEL} AND m.residual_pct IS NOT NULL"},
    {"key": "bq2_map", "name": "Tin rao cao (đỏ) và thấp (xanh) so với giá ước tính", "title_from_insights": True, "dataset": "price_check",
     "viz": "map", "dimension": "muc_chenh", "radius": 140, "autozoom": True, "viewport": VIEW_HCM, "scenario": "tlcn-bq2",
     "where": f"lat IS NOT NULL AND muc_chenh IS NOT NULL AND kc_trung_tam <= {MAP_RADIUS_KM}", "width": 7, "height": 110},
    {"key": "bq2_district", "name": "Tỷ lệ tin rao cao hơn ước tính ≥ 30% theo quận (quận ≥ 100 tin)", "title_from_insights": True,
     "dataset": "price_check", "viz": "hbar", "x": "district_name", "metric": m(f"AVG(CASE WHEN {OVER} THEN 1e0 ELSE 0e0 END)", "Tỷ lệ"),
     "format": ".0%", "limit": 15, "where": "district_name IS NOT NULL AND chenh_uoc_tinh_pct IS NOT NULL", "having": "COUNT(*) >= 100",
     "scenario": "tlcn-bq2", "width": 5, "height": 110},
    {"key": "bq2_factor", "name": "Đặc điểm nêu trong tiêu đề đi kèm chênh giá/m² bao nhiêu %", "title_from_insights": True,
     "dataset": "price_factor", "viz": "hbar", "x": "dac_diem_nhan", "groupby": ["do_tin_cay"], "stack": "Stack", "show_value": True,
     "metric": m("MAX(chenh_gia_pct)", "% chênh giá/m²"), "format": ",.0f", "scenario": "tlcn-bq2", "height": 55},
    {"key": "bq2_table", "name": "Tra cứu tin: giá chào so với giá ước tính và P25–P75 nhóm tương đồng (bỏ tin lệch > 200%: thường là xếp nhầm loại hình)",
     "dataset": "price_check", "viz": "table", "scenario": "tlcn-bq2",
     "columns": ["khu_vuc", "title", "gia_ty", "area", "gia_m2_tr", "uoc_tinh_tr", "chenh_uoc_tinh_pct", "p25_nhom_tr", "p75_nhom_tr", "ad_url"],
     "order": [["khu_vuc", True], ["gia_ty", True]], "row_limit": 1000,
     "where": "uoc_tinh_tr IS NOT NULL AND NOT is_outlier_trimmed AND ABS(chenh_uoc_tinh_pct) <= 200"},
    # ---------------------------------------------------------- BQ3 (by place of work or study)
    {"key": "bq3_now", "name": "Quãng đường từ quận đang nhắm (km)", "dataset": "commute_district", "viz": "kpi", "format": ",.1f",
     "metric": m("MAX(commute_km)", "km"), "scenario": "tlcn-bq3", "sub": "đường chim bay tới nơi làm/học"},
    {"key": "bq3_options", "name": "Quận rẻ hơn, đường đi tương đương", "dataset": "commute_substitution", "viz": "kpi",
     "metric": m("COUNT(*)", "Quận"), "scenario": "tlcn-bq3", "sub": "xa hơn tối đa 2 km", "check_sql": f"SELECT COUNT(*) {BQ3_PAIRS}"},
    {"key": "bq3_best", "name": "Rẻ hơn nhiều nhất (%)", "dataset": "commute_substitution", "viz": "kpi", "format": ",.0f",
     "metric": m("MAX(re_hon_pct)", "%"), "scenario": "tlcn-bq3", "sub": "cho cùng diện tích",
     "check_sql": f"SELECT MAX(s.price_gap_pct) * 100 {BQ3_PAIRS}"},
    {"key": "bq3_saving", "name": "Tiết kiệm tối đa (tỷ đồng)", "dataset": "commute_substitution", "viz": "kpi", "format": ",.2f",
     "metric": m("MAX(chenh_chi_phi_ty)", "Tỷ"), "scenario": "tlcn-bq3", "sub": "ước tính, cùng diện tích"},
    {"key": "bq3_geo", "name": "Các quận quanh nơi làm/học (vòng tròn càng to càng đắt)", "title_from_insights": True, "dataset": "commute_map",
     "viz": "map", "dimension": "lua_chon", "radius_metric": m("MAX(kich_thuoc)", "Chi phí (tỷ) + 1"), "multiplier": 110,
     "autozoom": True, "viewport": VIEW_HCM, "scenario": "tlcn-bq3", "where": f"lat IS NOT NULL AND commute_km <= {MAP_RADIUS_KM}",
     "width": 7, "height": 110},
    {"key": "bq3_gap", "name": "Quận thay thế: rẻ hơn bao nhiêu %", "title_from_insights": True, "dataset": "commute_substitution",
     "viz": "hbar", "x": "quan_thay_the_qd", "metric": m("MAX(re_hon_pct)", "% thấp hơn"), "format": ",.0f", "scenario": "tlcn-bq3",
     "width": 5, "height": 110},
    {"key": "bq3_map", "name": "Mọi quận: quãng đường tới nơi làm/học và chi phí (tỷ đồng) cho cùng diện tích",
     "dataset": "commute_district", "viz": "hbar", "x": "quan_theo_quang_duong", "groupby": ["lua_chon"], "stack": "Stack", "show_value": True,
     "metric": m("MAX(chi_phi_ty)", "Tỷ đồng"), "format": ",.1f", "sort_x": True, "scenario": "tlcn-bq3", "height": 110},
    {"key": "bq3_listings", "name": "Tin rao giá điển hình ở các quận thay thế (giá/m² 0,8–1,25× median quận đó)",
     "dataset": "commute_listings", "viz": "table",
     "columns": ["quan_thay_the", "quang_duong_km", "title", "gia_ty", "area", "gia_m2_tr", "ad_url"],
     "order": [["quang_duong_km", True], ["gia_ty", True]], "row_limit": 300, "scenario": "tlcn-bq3",
     "where": "gia_m2_tr BETWEEN 0.8 * median_quan_thay_the_tr AND 1.25 * median_quan_thay_the_tr"},
    # ---------------------------------------------------------- data quality
    {"key": "dq_pass", "name": "Bản ghi đạt PASS", "dataset": "data_quality", "viz": "kpi", "format": ".1%",
     "metric": m("SUM(pass_rows) * 1e0 / SUM(pass_rows + warn_rows)", "PASS"), "sub": "còn lại là WARN: thiếu/đã sửa một trường",
     "check_sql": f"SELECT SUM(pass_rows) * 1e0 / SUM(pass_rows + warn_rows) FROM {G}.agg_dq_kpi"},
    {"key": "dq_reject", "name": "Bản ghi bị loại", "dataset": "data_quality", "viz": "kpi",
     "metric": m("SUM(reject_rows)", "Dòng"), "sub": "không vào phân tích"},
    {"key": "dq_dup", "name": "Tin nghi trùng giữa nguồn", "dataset": "data_quality", "viz": "kpi", "format": ".1%",
     "metric": m("SUM(dup_suspect_rate * current_rows) / SUM(current_rows)", "Tỷ lệ"), "sub": "chỉ giữ một tin đại diện"},
    {"key": "dq_status", "name": "Tỷ lệ PASS / WARN / REJECT theo nguồn", "title_from_insights": True, "dataset": "data_quality",
     "viz": "hbar", "x": "source", "metrics": [m("SUM(pass_rows)", "PASS"), m("SUM(warn_rows)", "WARN"), m("SUM(reject_rows)", "REJECT")],
     "stack": "Expand"},
    {"key": "dq_coverage", "name": "Độ phủ trường dữ liệu theo nguồn", "title_from_insights": True, "dataset": "data_quality",
     "viz": "hbar", "x": "source", "format": ".0%", "height": 90,
     "metrics": [m("MAX(coverage_price_known)", "Giá"), m("MAX(coverage_area_known)", "Diện tích"), m("MAX(coverage_rooms_known)", "Số phòng"),
                 m("MAX(coverage_coordinate)", "Tọa độ"), m("MAX(coverage_legal_known)", "Pháp lý")]},
    {"key": "dq_funnel", "name": "Từ dữ liệu crawl tới tin dùng để phân tích (số dòng mỗi tầng)", "dataset": "funnel", "viz": "hbar",
     "x": "stage", "metric": m("SUM(rows)", "Dòng"), "sort_x": True,
     "check_sql": f"SELECT SUM(bronze_rows) FROM {G}.agg_dq_kpi", "check_first_row": True},
    {"key": "dq_sources", "name": "Nguồn dữ liệu và ngày quan sát mới nhất", "dataset": "data_quality", "viz": "table",
     "columns": ["source", "loai_nguon", "ngay_moi_nhat", "current_rows", "representative_rows"],
     "order": [["representative_rows", False]], "row_limit": 20},
]

# Rows: chart keys, or "md" for the one-line header.
DASHBOARDS = [
    {"slug": "tlcn-tong-quan", "title": "Tổng quan thị trường BĐS đang chào bán",
     "rows": [["md"], ["ov_listings", "ov_hcm_share", "ov_median_m2", "ov_provinces"], ["ov_map_hcm", "ov_ring"], ["ov_map_hn", "ov_price"]]},
    {"slug": "tlcn-bq1", "title": "BQ1 – Cùng ngân sách, người mua đánh đổi gì?",
     "rows": [["md"], ["bq1_listings", "bq1_districts", "bq1_pareto_share"], ["bq1_map", "bq1_distance"], ["bq1_table"]]},
    {"slug": "tlcn-bq2", "title": "BQ2 – Tin này đắt hay rẻ so với căn tương đồng?",
     "rows": [["md"], ["bq2_error", "bq2_over", "bq2_under"], ["bq2_map", "bq2_district"], ["bq2_factor"], ["bq2_table"]]},
    {"slug": "tlcn-bq3", "title": "BQ3 – Đi làm/học ở đây: mua ở quận nào thì đường đi tương đương mà rẻ hơn?",
     "rows": [["md"], ["bq3_now", "bq3_options", "bq3_best", "bq3_saving"], ["bq3_geo", "bq3_gap"], ["bq3_map"], ["bq3_listings"]]},
    {"slug": "tlcn-chat-luong", "title": "Chất lượng dữ liệu – số liệu trên dashboard đáng tin đến đâu?",
     "rows": [["md"], ["dq_pass", "dq_reject", "dq_dup"], ["dq_status", "dq_coverage"], ["dq_funnel", "dq_sources"]]},
]
NATIVE_FILTERS = {
    "tlcn-tong-quan": [("Tỉnh/thành", "province_name"), ("Loại hình", "category_label")],
    "tlcn-bq1": [
        {"label": "Tỉnh/thành", "column": "province_name", "dataset": "budget_tradeoff", "default": BQ1_DEFAULT["province_name"]},
        {"label": "Loại hình", "column": "category_label", "dataset": "budget_tradeoff", "default": BQ1_DEFAULT["category_label"]},
        {"label": "Ngân sách", "column": "ngan_sach", "dataset": "budget_tradeoff", "default": BQ1_DEFAULT["ngan_sach"]},
    ],
    "tlcn-bq2": [
        {"label": "Loại hình", "column": "category_label", "dataset": "price_check", "default": BQ2_DEFAULT["category_label"]},
        {"label": "Tỉnh/thành", "column": "province_name", "dataset": "price_check", "default": BQ2_DEFAULT["province_name"]},
        {"label": "Quận/huyện", "column": "district_name", "dataset": "price_check", "parent": "province_name"},
    ],
    "tlcn-bq3": [
        {"label": "Nơi làm/học", "column": "noi_den", "dataset": "commute_district", "default": BQ3_DEFAULT["noi_den"]},
        {"label": "Quận đang nhắm", "column": "quan_goc", "dataset": "commute_district", "default": BQ3_DEFAULT["quan_goc"],
         "parent": "noi_den", "charts": ["bq3_now", "bq3_options", "bq3_best", "bq3_saving", "bq3_gap", "bq3_listings"]},
        {"label": "Loại hình", "column": "category_label", "dataset": "commute_district", "default": BQ3_DEFAULT["category_label"]},
        {"label": "Diện tích", "column": "area_band", "dataset": "commute_district", "default": BQ3_DEFAULT["area_band"]},
    ],
    "tlcn-chat-luong": [("Nguồn", "source")],
}


class Superset:
    def __init__(self) -> None:
        self.s = requests.Session()
        token = self.s.post(f"{BASE}/api/v1/security/login", json={
            "username": env("SUPERSET_ADMIN_USERNAME", "admin"), "password": env("SUPERSET_ADMIN_PASSWORD"),
            "provider": "db", "refresh": True}, timeout=60)
        token.raise_for_status()
        self.s.headers["Authorization"] = f"Bearer {token.json()['access_token']}"
        csrf = self.s.get(f"{BASE}/api/v1/security/csrf_token/", timeout=60)
        csrf.raise_for_status()
        self.s.headers["X-CSRFToken"] = csrf.json()["result"]
        self.s.headers["Referer"] = BASE

    def call(self, method: str, path: str, **kw) -> Any:
        import time

        for attempt in range(6):  # Superset rate-limits the API (429)
            r = self.s.request(method, f"{BASE}/api/v1/{path}", timeout=300, **kw)
            if r.status_code != 429:
                break
            time.sleep(1.5 * (attempt + 1))
        if r.status_code >= 400:
            raise RuntimeError(f"{method} {path} -> {r.status_code}: {r.text[:500]}")
        return r.json() if r.text else {}

    def find(self, resource: str, column: str, value: str) -> list[dict]:
        q = json.dumps({"filters": [{"col": column, "opr": "eq", "value": value}], "page_size": 100})
        return self.call("GET", f"{resource}/?q={requests.utils.quote(q)}")["result"]


DATASET_COLUMNS: dict[str, set[str]] = {}


def has_column(dataset: str, column: str) -> bool:
    return column in DATASET_COLUMNS.get(dataset, set())


def chart_params(spec: dict, dataset_id: int) -> tuple[dict, dict]:
    """Return (form_data params, query object for the query context)."""

    viz = {"kpi": "big_number_total", "bar": "echarts_timeseries_bar", "hbar": "echarts_timeseries_bar",
           "bubble": "bubble_v2", "funnel": "funnel", "table": "table", "map": "deck_scatter"}[spec["viz"]]
    adhoc = [{"expressionType": "SQL", "clause": "WHERE", "sqlExpression": spec["where"]}] if spec.get("where") else []
    if spec.get("having"):
        adhoc.append({"expressionType": "SQL", "clause": "HAVING", "sqlExpression": spec["having"]})
    extras = {k: v for k, v in (("where", spec.get("where")), ("having", spec.get("having"))) if v}
    base = {"datasource": f"{dataset_id}__table", "viz_type": viz, "adhoc_filters": adhoc}
    fmt = spec.get("format", "SMART_NUMBER")
    if spec["viz"] == "table":
        config = {}
        for column in spec["columns"]:
            if column in ("gia_ty", "gia_m2_tr", "median_nhom_tr", "price_ratio", "chenh_chi_phi_ty", "distance_diff_km", "feature_similarity",
                          "quang_duong_km", "median_quan_goc_tr", "median_quan_thay_the_tr", "uoc_tinh_tr", "p25_nhom_tr", "p75_nhom_tr"):
                config[column] = {"d3NumberFormat": ",.2~f"}
            elif column in ("re_hon_pct", "chenh_uoc_tinh_pct"):
                config[column] = {"d3NumberFormat": ",.0f"}
        params = {**base, "query_mode": "raw", "all_columns": spec["columns"], "column_config": config,
                  "order_by_cols": [json.dumps(o) for o in spec.get("order", [])], "row_limit": spec["row_limit"],
                  "server_pagination": False, "include_search": True, "page_length": 10, "show_cell_bars": False}
        query = {"columns": spec["columns"], "orderby": spec.get("order", []), "row_limit": spec["row_limit"], "extras": extras}
    elif spec["viz"] == "map":
        # deck.gl scatter on OpenStreetMap tiles (no Mapbox key). Colour comes
        # from the dimension through the dashboard's label_colors; the radius
        # is fixed (metres) or a metric x multiplier (metres).
        radius = ({"type": "metric", "value": spec["radius_metric"]} if spec.get("radius_metric")
                  else {"type": "fix", "value": spec.get("radius", 150)})
        params = {**base, "spatial": {"type": "latlong", "latCol": "lat", "lonCol": "lon"}, "row_limit": 50000, "filter_nulls": True,
                  "mapbox_style": OSM, "viewport": spec["viewport"], "autozoom": spec.get("autozoom", False),
                  "point_radius_fixed": radius, "point_unit": "radius_m", "min_radius": 2, "max_radius": 60,
                  "multiplier": spec.get("multiplier", 1), "dimension": spec["dimension"], "color_scheme": "supersetColors",
                  "legend_position": "tr", "js_columns": []}
        query = {"columns": ["lat", "lon", spec["dimension"]], "metrics": [spec["radius_metric"]] if spec.get("radius_metric") else [],
                 "row_limit": 50000, "extras": extras}
    elif spec["viz"] == "kpi":
        params = {**base, "metric": spec["metric"], "header_font_size": 0.5, "subheader_font_size": 0.18,
                  "y_axis_format": spec.get("format", ",d"), "subheader": spec.get("sub", ""), "subtitle": spec.get("sub", "")}
        query = {"columns": [], "metrics": [spec["metric"]], "extras": extras}
    elif spec["viz"] == "bubble":
        params = {**base, "entity": spec["entity"], "x": spec["x_metric"], "y": spec["y_metric"], "size": spec["size_metric"],
                  "max_bubble_size": "40", "row_limit": 100, "show_legend": False,
                  "x_axis_label": spec["x_metric"]["label"], "y_axis_label": spec["y_metric"]["label"],
                  "xAxisFormat": "SMART_NUMBER", "y_axis_format": "SMART_NUMBER", "truncateYAxis": False}
        query = {"columns": [spec["entity"]], "metrics": [spec["x_metric"], spec["y_metric"], spec["size_metric"]],
                 "row_limit": 100, "extras": extras}
    elif spec["viz"] == "funnel":
        params = {**base, "groupby": [spec["x"]], "metric": spec["metric"], "sort_by_metric": True, "show_legend": False,
                  "label_type": "key_value_percent", "number_format": ",d", "row_limit": 10}
        query = {"columns": [spec["x"]], "metrics": [spec["metric"]], "orderby": [[spec["metric"], False]], "extras": extras}
    else:  # bar / hbar with a categorical axis
        metrics = spec.get("metrics") or [spec["metric"]]
        horizontal = spec["viz"] == "hbar"
        sort_metric = not spec.get("sort_x")
        params = {**base, "x_axis": spec["x"], "metrics": metrics, "groupby": spec.get("groupby", []),
                  "orientation": "horizontal" if horizontal else "vertical", "row_limit": spec.get("limit", 1000),
                  "show_legend": bool(spec.get("groupby")) or len(metrics) > 1, "legendOrientation": "top", "rich_tooltip": True,
                  "stack": spec.get("stack"), "show_value": spec.get("show_value", spec.get("stack") is None),
                  "x_axis_sort": metrics[0]["label"] if sort_metric and not spec.get("groupby") else spec["x"],
                  # Horizontal bars draw bottom-up: ascending puts the largest on top.
                  "x_axis_sort_asc": horizontal if sort_metric and not spec.get("groupby") else not horizontal,
                  "sort_series_type": "name", "sort_series_ascending": True,
                  "y_axis_format": fmt, "truncateXAxis": True, "x_axis_title": "", "y_axis_title": "",
                  "zoomable": False, "minorTicks": False}
        query = {"columns": [spec["x"], *spec.get("groupby", [])], "metrics": metrics, "row_limit": spec.get("limit", 1000),
                 "orderby": [[metrics[0], False]] if sort_metric else [[spec["x"], True]], "extras": extras}
        if spec.get("limit"):
            query["series_limit"] = spec["limit"]
            query["series_limit_metric"] = metrics[0]
            params["limit"] = spec["limit"]
            params["timeseries_limit_metric"] = metrics[0]
    # Scenario charts take their scope from the dashboard filters; the saved
    # query context (used by the data API check) carries the default scenario.
    query["filters"] = [
        {"col": f["column"], "op": "IN", "val": [f["default"]]}
        for f in (NATIVE_FILTERS.get(spec["scenario"], []) if spec.get("scenario") else [])
        if isinstance(f, dict) and f.get("default") and has_column(spec["dataset"], f["column"])
        and spec["key"] in (f.get("charts") or [spec["key"]])
    ]
    return params, query


def position_json(rows: list[list[Any]], specs: dict[int, dict], markdown: str) -> dict:
    layout: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
    }
    heights = {"kpi": 22, "table": 80, "bubble": 70, "funnel": 50, "bar": 60, "hbar": 55, "map": 100}
    for r_index, row in enumerate(rows):
        row_id = f"ROW-{r_index}"
        layout["GRID_ID"]["children"].append(row_id)
        layout[row_id] = {"type": "ROW", "id": row_id, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                          "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        width = 12 // len(row)  # unless the chart sets its own "width"
        for item in row:
            if item == "md":
                cid = f"MARKDOWN-{r_index}"
                layout[cid] = {"type": "MARKDOWN", "id": cid, "children": [], "parents": ["ROOT_ID", "GRID_ID", row_id],
                               "meta": {"width": 12, "height": 12, "code": markdown}}
            else:
                cid = f"CHART-{item}"
                spec = specs[item]
                layout[cid] = {"type": "CHART", "id": cid, "children": [], "parents": ["ROOT_ID", "GRID_ID", row_id],
                               "meta": {"chartId": item, "width": spec.get("width", width), "height": spec.get("height", heights[spec["viz"]]),
                                        "sliceName": spec["name"]}}
            layout[row_id]["children"].append(cid)
    return layout


def native_filters(slug: str, dataset_ids: dict[str, int], chart_ids: list[int], dataset_of_chart: dict[int, str],
                   chart_ids_by_key: dict[str, int]) -> list[dict]:
    """Multi-select optional filters, or (with "default") a required single choice.

    A filter applies to the charts whose dataset has its column, or to the
    listed "charts"; "parent" narrows the options to the parent's choice.
    """

    def filter_id(column: str) -> str:
        return f"NATIVE_FILTER-{uuid.uuid5(uuid.NAMESPACE_URL, slug + column).hex[:10]}"

    configs = []
    for entry in NATIVE_FILTERS.get(slug, []):
        f = entry if isinstance(entry, dict) else {"label": entry[0], "column": entry[1]}
        column, default = f["column"], f.get("default")
        target = f.get("dataset") or next(
            name for name in dataset_ids if any(dataset_of_chart[c] == name for c in chart_ids) and has_column(name, column))
        in_scope = ([chart_ids_by_key[k] for k in f["charts"]] if f.get("charts")
                    else [c for c in chart_ids if has_column(dataset_of_chart[c], column)])
        mask = ({"filterState": {"value": [default], "label": default},
                 "extraFormData": {"filters": [{"col": column, "op": "IN", "val": [default]}]}} if default
                else {"filterState": {}, "extraFormData": {}})
        configs.append({
            "id": filter_id(column),
            "name": f["label"], "filterType": "filter_select", "type": "NATIVE_FILTER",
            "targets": [{"datasetId": dataset_ids[target], "column": {"name": column}}],
            "controlValues": {"multiSelect": not default, "enableEmptyFilter": bool(default), "defaultToFirstItem": False,
                              "searchAllOptions": True, "inverseSelection": False},
            "defaultDataMask": mask,
            "cascadeParentIds": [filter_id(f["parent"])] if f.get("parent") else [],
            "scope": {"rootPath": ["ROOT_ID"], "excluded": [c for c in chart_ids if c not in in_scope]}, "chartsInScope": in_scope,
            "description": "",
        })
    return configs


def trino_rows(sql: str) -> list[tuple]:
    import trino

    cursor = trino.dbapi.connect(host="localhost", port=8090, user="dashboard", catalog="iceberg").cursor()
    cursor.execute(sql)
    return cursor.fetchall()


def main() -> None:
    from src.dashboard.insights import compute_insights

    api = Superset()
    existing_db = api.find("database", "database_name", DB_NAME)
    db_id = existing_db[0]["id"] if existing_db else api.call("POST", "database/", json={
        "database_name": DB_NAME, "sqlalchemy_uri": DB_URI, "expose_in_sqllab": True,
        "allow_run_async": False, "extra": json.dumps({"allows_virtual_table_explore": True})})["id"]

    # Remove objects owned by this script so reruns start clean.
    for dash in DASHBOARDS:
        for found in api.find("dashboard", "slug", dash["slug"]):
            api.call("DELETE", f"dashboard/{found['id']}")
    owned = api.call("GET", "dataset/?q=" + requests.utils.quote(json.dumps(
        {"filters": [{"col": "table_name", "opr": "sw", "value": PREFIX}], "page_size": 100})))["result"]
    for dataset in owned:
        charts = api.call("GET", f"dataset/{dataset['id']}/related_objects").get("charts", {}).get("result", [])
        for chart in charts:
            api.call("DELETE", f"chart/{chart['id']}")
        api.call("DELETE", f"dataset/{dataset['id']}")
    dataset_ids: dict[str, int] = {}
    for name, sql in DATASETS.items():
        dataset_ids[name] = api.call("POST", "dataset/", json={
            "database": db_id, "schema": "gold", "table_name": PREFIX + name, "sql": " ".join(sql.split())})["id"]
        api.call("PUT", f"dataset/{dataset_ids[name]}/refresh")
        DATASET_COLUMNS[name] = {c["column_name"] for c in api.call("GET", f"dataset/{dataset_ids[name]}")["result"]["columns"]}

    top = [r[0] for r in trino_rows(f"SELECT province_name FROM {G}.agg_market_overview WHERE province_name <> 'Không rõ' "
                                    "GROUP BY 1 ORDER BY SUM(n_listings) DESC LIMIT 6")]
    insights = compute_insights(trino_rows)
    titles = {key: title for found in insights.values() for key, title in found.get("titles", {}).items()}

    chart_ids: dict[str, int] = {}
    specs_by_id: dict[int, dict] = {}
    checks = []
    for spec in CHARTS:
        if spec.get("title_from_insights") and spec["key"] in titles:
            spec["name"] = titles[spec["key"]]
        if spec.get("top_provinces"):
            spec["where"] = (spec.get("where", "1=1") + " AND province_name IN (" + ", ".join(f"'{p}'" for p in top) + ")")
        ds_id = dataset_ids[spec["dataset"]]
        params, query = chart_params(spec, ds_id)
        context = {"datasource": {"id": ds_id, "type": "table"}, "force": True, "queries": [query],
                   "form_data": params, "result_format": "json", "result_type": "full"}
        cid = api.call("POST", "chart/", json={
            "slice_name": spec["name"], "viz_type": params["viz_type"], "datasource_id": ds_id, "datasource_type": "table",
            "params": json.dumps(params), "query_context": json.dumps(context)})["id"]
        chart_ids[spec["key"]], specs_by_id[cid] = cid, spec
        result = api.call("GET", f"chart/{cid}/data/?format=json&type=full&force=true")["result"][0]
        entry = {"chart": spec["name"], "chart_id": cid, "viz": params["viz_type"], "status": result.get("status"),
                 "rows": result.get("rowcount"), "error": result.get("error")}
        data = result.get("data") or []
        if spec.get("check_sql"):
            expected = float(trino_rows(spec["check_sql"])[0][0])
            actual = float(max(r[spec["metric"]["label"]] for r in data)) if spec.get("check_first_row") else float(list(data[0].values())[0]) if data else None
            entry.update(expected=expected, actual=actual, match=actual is not None and abs(actual - expected) < 1e-6)
        else:
            entry["match"] = result.get("status") == "success" and len(data) > 0
        entry["sample"] = data[:3]
        checks.append(entry)
        print(f"{'OK ' if entry['match'] else 'ERR'} {spec['key']:18s} rows={entry['rows']}")

    dataset_of_chart = {chart_ids[s["key"]]: s["dataset"] for s in CHARTS}
    dashboards = []
    for dash in DASHBOARDS:
        rows = [[item if item == "md" else chart_ids[item] for item in row] for row in dash["rows"]]
        ids = [cid for row in rows for cid in row if cid != "md"]
        metadata = {"native_filter_configuration": native_filters(dash["slug"], dataset_ids, ids, dataset_of_chart, chart_ids),
                    "color_scheme": "supersetColors", "label_colors": LABEL_COLORS, "refresh_frequency": 0, "cross_filters_enabled": False}
        dash_id = api.call("POST", "dashboard/", json={
            "dashboard_title": dash["title"], "slug": dash["slug"], "published": True,
            "position_json": json.dumps(position_json(rows, specs_by_id, insights[dash["slug"]]["markdown"])),
            "json_metadata": json.dumps(metadata)})["id"]
        for cid in ids:
            api.call("PUT", f"chart/{cid}", json={"dashboards": [dash_id]})
        dashboards.append({"title": dash["title"], "url": f"{BASE}/superset/dashboard/{dash['slug']}/", "charts": len(ids),
                           "insights": insights[dash["slug"]]["facts"]})
        print(f"dashboard {dash['slug']}: {len(ids)} charts")

    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "superset": BASE, "database": DB_NAME, "datasets": {k: PREFIX + k for k in DATASETS},
        "dashboards": dashboards, "chart_checks": checks,
        "status": "PASS" if all(c["match"] for c in checks) else "FAIL",
    }
    out = PROJECT_ROOT / "docs/validation/superset_dashboard_check.json"
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print("status", summary["status"])
    if summary["status"] != "PASS":
        sys.exit(1)


if __name__ == "__main__":
    main()
