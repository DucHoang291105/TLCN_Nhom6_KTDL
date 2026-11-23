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
# Absolute percentage error with the actual price as denominator, as in
# price_model_summary.json (median_ape): |actual - predicted| / actual.
# residual_pct = actual / predicted - 1, so this is |residual| / (1 + residual).
APE_PCT = "abs(m.residual_pct) / (1 + m.residual_pct) * 100"
GAP_CLASS = """CASE WHEN m.residual_pct IS NULL THEN NULL WHEN m.residual_pct <= -0.3 THEN '1. Rao thấp hơn ước tính ≥ 30%'
               WHEN m.residual_pct < 0.3 THEN '2. Trong ±30% ước tính' ELSE '3. Rao cao hơn ước tính ≥ 30%' END"""
# BQ3: one row per listing × named place of work/study, road distance from fact_listing_commute.
COMMUTE_LISTING = f"""
        SELECT r.destination_name AS noi_den, r.province_name, r.district_name, c.category_label, ab.band_label AS area_band,
               pb.band_label AS ngan_sach, r.road_km, r.road_min, r.straight_km, r.ring AS vanh_dai,
               CASE WHEN r.straight_km >= 2 THEN r.road_km / r.straight_km END AS he_so_duong_vong,
               r.price / 1e9 AS gia_ty, r.area, r.price_per_m2 / 1e6 AS gia_m2_tr, r.lat, r.lon, f.title, f.ad_url
        FROM {G}.fact_listing_commute r
        JOIN {G}.fact_listing f ON f.source_id = r.source_id
        JOIN {G}.dim_property_category c ON c.property_category_key = r.property_category_key
        JOIN {G}.dim_area_band ab ON ab.band_key = r.area_band_key
        JOIN {G}.dim_price_band pb ON pb.band_key = r.price_band_key"""
ROAD_FRONTIER = "1. Đường biên: không quận nào vừa gần hơn vừa rẻ hơn"
ROAD_BEATEN = "2. Có quận vừa gần hơn vừa rẻ hơn"
# % lower median price/m² in the 10–15 km road ring than within 5 km (negative: farther is dearer).
FAR_GAP_SQL = ("(1 - APPROX_PERCENTILE(CASE WHEN vanh_dai LIKE '3.%' THEN gia_m2_tr END, 0.5)"
               " / APPROX_PERCENTILE(CASE WHEN vanh_dai LIKE '1.%' THEN gia_m2_tr END, 0.5)) * 100")
RINGS_ENOUGH = "COUNT_IF(vanh_dai LIKE '1.%') >= 30 AND COUNT_IF(vanh_dai LIKE '3.%') >= 30"
DATASETS: dict[str, str] = {
    "market_overview": f"""
        SELECT o.province_name, c.category_label, o.n_listings,
               o.median_price_per_m2 / 1e6 AS median_price_per_m2_tr, o.median_price / 1e9 AS median_price_ty, o.median_area
        FROM {G}.agg_market_overview o
        JOIN {G}.dim_property_category c ON c.property_category_key = o.property_category_key""",
    "map_price": f"""
        SELECT d.province_name, c.category_label, ll.lat, ll.lon, f.price_per_m2 / 1e6 AS gia_m2_tr,
               {PRICE_CLASS} AS muc_gia, {RING} AS vanh_dai, {RING} AS muc_vanh_dai, f.distance_to_center_km AS kc_trung_tam
        FROM {G}.fact_listing f
        JOIN {S}.listing_location ll ON ll.source_id = f.source_id
        JOIN {G}.dim_location d ON d.location_key = f.location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = f.property_category_key
        WHERE f.is_dup_representative AND ll.lat IS NOT NULL AND f.price_per_m2 > 0 AND f.distance_to_center_km IS NOT NULL""",
    # Fixed comparison of the three largest markets (does not follow filters).
    "city_compare": f"""
        SELECT d.province_name AS thanh_pho, f.price_per_m2 / 1e6 AS gia_m2_tr, {RING} AS vanh_dai
        FROM {G}.fact_listing f JOIN {G}.dim_location d ON d.location_key = f.location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = f.property_category_key
        WHERE f.is_dup_representative AND f.price_per_m2 > 0 AND f.distance_to_center_km IS NOT NULL AND c.model_category = 'nha_pho'
              AND d.province_name IN ('{HCM}', 'Hà Nội', 'Đà Nẵng')""",
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
               {APE_PCT} AS sai_so_pct,
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
               CASE WHEN k.ci95_low_pct > 0 OR k.ci95_high_pct < 0 THEN '1. Có ý nghĩa thống kê (KTC 95% không chứa 0)'
                    ELSE '2. Chưa có ý nghĩa thống kê (KTC 95% chứa 0)' END AS do_tin_cay,
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
    # BQ3 by place of work/study, on the road network (OpenStreetMap / OSRM, see src/gold/commute_road.py).
    "commute_listing": COMMUTE_LISTING,
    "commute_map": f"""
        SELECT noi_den, category_label, area_band, ngan_sach, lat, lon, vanh_dai, 1e0 AS kich_thuoc FROM ({COMMUTE_LISTING}) r
        UNION ALL
        SELECT p.destination_name, k.category_label, k.area_band, b.band_label, p.destination_lat, p.destination_lon, '0. Nơi làm/học', 12e0
        FROM (SELECT DISTINCT destination_name, destination_lat, destination_lon FROM {G}.agg_commute_district) p
        JOIN (SELECT DISTINCT r.destination_name, c.category_label, ab.band_label AS area_band FROM {G}.fact_listing_commute r
              JOIN {G}.dim_property_category c ON c.property_category_key = r.property_category_key
              JOIN {G}.dim_area_band ab ON ab.band_key = r.area_band_key) k ON k.destination_name = p.destination_name
        CROSS JOIN {G}.dim_price_band b""",
    "commute_frontier": f"""
        SELECT d.destination_name AS noi_den, l.district_name, c.category_label, ab.band_label AS area_band,
               d.road_km, d.road_min, d.road_n_listings, d.n_listings, d.target_area,
               d.estimated_cost_at_target_area / 1e9 AS chi_phi_ty,
               CASE WHEN d.is_road_frontier THEN '{ROAD_FRONTIER}' ELSE '{ROAD_BEATEN}' END AS lua_chon
        FROM {G}.agg_commute_district d
        JOIN {G}.dim_location l ON l.location_key = d.location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = d.property_category_key
        JOIN {G}.dim_area_band ab ON ab.band_key = d.area_band_key
        WHERE d.road_km IS NOT NULL""",
    "data_quality": f"""
        SELECT k.source, CASE k.source_type WHEN 'historical' THEN 'Lịch sử (nạp một lần)' ELSE 'Crawl định kỳ' END AS loai_nguon,
               k.bronze_rows, k.current_rows, k.representative_rows,
               k.pass_rows, k.warn_rows, k.reject_rows, k.dup_suspect_rate,
               k.coverage_price_known, k.coverage_area_known, k.coverage_rooms_known, k.coverage_province,
               k.coverage_coordinate, k.coverage_legal_known, CAST(f.ngay_moi_nhat AS varchar) AS ngay_moi_nhat
        FROM {G}.agg_dq_kpi k
        LEFT JOIN (SELECT source, MAX(last_seen_date) AS ngay_moi_nhat FROM {G}.fact_listing_lifecycle GROUP BY source) f ON f.source = k.source""",
}


# ----------------------------------------------------------------- evidence datasets (v4)
# Each BQ finding is backed by a chart, real listings and a validation. The
# heavy ones are Gold tables computed by build_bq_evidence.py in the pipeline.
DATASETS.update({
    "budget_curve": f"SELECT province_name, category_label, area, muc_ngan_sach, vung FROM {G}.ev_budget_curve",
    "budget_pairs": f"SELECT * FROM {G}.ev_budget_pairs",
    "reprice_first": f"SELECT province_name, district_name, category_label, da_ha_gia, vi_tri_luc_dang, vi_tri_luc_dang AS nhom_mau FROM {G}.ev_reprice",
    "reprice_examples": f"SELECT * FROM {G}.ev_reprice_examples",
    "flag_check": f"SELECT * FROM {G}.ev_flag_check",
    "calibration": f"SELECT * FROM {G}.ev_calibration",
})


# ----------------------------------------------------------------- finding cards
# Handlebars cards state each page's findings for the CURRENT filter selection:
# one row per filter combination, numbers pre-formatted Vietnamese style.
def vnf(expr: str, digits: int = 0) -> str:
    """Vietnamese number text: 1.234.567,9"""
    return f"translate(format('%,.{digits}f', CAST({expr} AS double)), ',.', '.,')"


def vnp(expr: str, digits: int = 0) -> str:
    return f"({vnf(f'({expr}) * 100', digits)} || '%')"


DIST = "f.distance_to_center_km"
CARD_DATASETS: dict[str, str] = {
    # Overview: price level and centre/outskirts gradient of the chosen province and type.
    "ov_card": f"""
        SELECT province_name, category_label, {vnf('n')} AS so_tin, {vnf('med')} AS median_m2,
               n_center >= 30 AND n_outer >= 30 AS co_vanh_dai, {vnf('center')} AS trung_tam, {vnf('outer_ppm2')} AS ngoai_thanh,
               {vnf('center / outer_ppm2', 1)} AS gap_lan, {vnp('share200')} AS tren_200, n_coord >= 30 AS co_toa_do, {vnp('n_coord * 1e0 / n')} AS ty_le_toa_do
        FROM (SELECT d.province_name, c.category_label, COUNT(*) AS n, MAX(o.median_price_per_m2) / 1e6 AS med,
                     approx_percentile(CASE WHEN {DIST} < 5 THEN f.price_per_m2 END, 0.5) / 1e6 AS center,
                     approx_percentile(CASE WHEN {DIST} >= 15 THEN f.price_per_m2 END, 0.5) / 1e6 AS outer_ppm2,
                     COUNT_IF({DIST} < 5) AS n_center, COUNT_IF({DIST} >= 15) AS n_outer, COUNT_IF({DIST} IS NOT NULL) AS n_coord,
                     AVG(CASE WHEN f.price_per_m2 >= 200e6 THEN 1e0 ELSE 0e0 END) AS share200
              FROM {G}.fact_listing f JOIN {G}.dim_location d ON d.location_key = f.location_key
              JOIN {G}.dim_property_category c ON c.property_category_key = f.property_category_key
              JOIN {G}.agg_market_overview o ON o.province_name = d.province_name AND o.property_category_key = f.property_category_key
              WHERE f.is_dup_representative AND f.price_per_m2 > 0 GROUP BY 1, 2) t""",
    # BQ1: extremes, slope per 10 km, dominated share for the chosen budget.
    "bq1_card": f"""
        WITH t AS (
            SELECT l.province_name, c.category_label, pb.band_label AS ngan_sach, l.district_name, t.n_listings, t.median_area, t.median_distance_km
            FROM {G}.agg_budget_tradeoff t JOIN {G}.dim_location l ON l.location_key = t.location_key
            JOIN {G}.dim_property_category c ON c.property_category_key = t.property_category_key
            JOIN {G}.dim_price_band pb ON pb.band_key = t.price_band_key
            WHERE l.district_name IS NOT NULL AND l.district_name <> 'Unknown' AND t.n_listings >= 5),
        s AS (
            SELECT province_name, category_label, ngan_sach, SUM(n_listings) AS n, COUNT(*) AS n_quan,
                   max_by(district_name, CASE WHEN n_listings >= 30 THEN median_area END) AS rong_nhat,
                   MAX(CASE WHEN n_listings >= 30 THEN median_area END) AS rong_m2,
                   min_by(district_name, CASE WHEN n_listings >= 30 THEN median_area END) AS hep_nhat,
                   MIN(CASE WHEN n_listings >= 30 THEN median_area END) AS hep_m2,
                   regr_slope(median_area, median_distance_km) AS slope, corr(median_area, median_distance_km) AS r,
                   COUNT(median_distance_km) AS n_dist
            FROM t GROUP BY 1, 2, 3),
        p AS (
            SELECT l.province_name, c.category_label, pb.band_label AS ngan_sach, AVG(CASE WHEN p.is_pareto_efficient THEN 0e0 ELSE 1e0 END) AS bi_troi
            FROM {G}.fact_budget_pareto p JOIN {G}.dim_location l ON l.location_key = p.location_key
            JOIN {G}.dim_property_category c ON c.property_category_key = p.property_category_key
            JOIN {G}.dim_price_band pb ON pb.band_key = p.price_band_key GROUP BY 1, 2, 3)
        SELECT s.province_name, s.category_label, s.ngan_sach, {vnf('s.n')} AS so_tin, s.n_quan AS so_quan,
               s.rong_m2 IS NOT NULL AND s.hep_m2 IS NOT NULL AND s.rong_nhat <> s.hep_nhat AS co_cuc_tri,
               s.rong_nhat, {vnf('s.rong_m2')} AS rong_m2, s.hep_nhat, {vnf('s.hep_m2')} AS hep_m2, {vnf('s.rong_m2 / s.hep_m2', 1)} AS gap_lan,
               s.n_dist >= 3 AND s.slope IS NOT NULL AS co_xu_huong, {vnf('s.slope * 10')} AS them_m2_10km, {vnf('s.r', 2)} AS r,
               {vnp('p.bi_troi')} AS bi_troi
        FROM s LEFT JOIN p ON p.province_name = s.province_name AND p.category_label = s.category_label AND p.ngan_sach = s.ngan_sach""",
    # BQ2: H1 with a two-proportion z-test, model error, raw check of the frontage premium.
    "bq2_card": f"""
        WITH r AS (
            SELECT province_name, category_label,
                   COUNT_IF(vi_tri_luc_dang LIKE '3.%') AS n1, SUM(CASE WHEN vi_tri_luc_dang LIKE '3.%' THEN da_ha_gia ELSE 0 END) AS c1,
                   COUNT_IF(vi_tri_luc_dang NOT LIKE '3.%') AS n0, SUM(CASE WHEN vi_tri_luc_dang NOT LIKE '3.%' THEN da_ha_gia ELSE 0 END) AS c0
            FROM {G}.ev_reprice GROUP BY 1, 2),
        z AS (
            SELECT r.*, c1 / n1 AS p1, c0 / n0 AS p0,
                   CASE WHEN n1 >= 30 AND n0 >= 30 AND c1 + c0 > 0 AND c1 + c0 < n1 + n0
                        THEN (c1 / n1 - c0 / n0) / sqrt(((c1 + c0) / (n1 + n0)) * (1 - (c1 + c0) / (n1 + n0)) * (1e0 / n1 + 1e0 / n0)) END AS zv
            FROM r WHERE n1 > 0 AND n0 > 0),
        m AS (
            SELECT l.province_name, c.category_label,
                   approx_percentile(CASE WHEN m.split = 'test' AND NOT m.is_outlier_trimmed THEN {APE_PCT} / 100 END, 0.5) AS err,
                   AVG(CASE WHEN m.residual_pct >= 0.3 THEN 1e0 ELSE 0e0 END) AS cao, AVG(CASE WHEN m.residual_pct <= -0.3 THEN 1e0 ELSE 0e0 END) AS thap
            FROM {G}.fact_listing_price_model m JOIN {G}.fact_listing_price_assessment a ON a.source_id = m.source_id
            JOIN {G}.dim_location l ON l.location_key = a.location_key JOIN {G}.dim_property_category c ON c.property_category_key = a.property_category_key
            WHERE m.residual_pct IS NOT NULL GROUP BY 1, 2),
        f AS (
            SELECT province_name, category_label, approx_percentile(chenh_tho_pct, 0.5) AS raw, MAX(he_so_mo_hinh_pct) AS model,
                   AVG(co_dat_hon) AS share, COUNT(*) AS groups
            FROM {G}.ev_flag_check WHERE dac_diem = 'Mặt tiền' GROUP BY 1, 2)
        SELECT z.province_name, z.category_label, {vnf('z.n1 + z.n0')} AS so_tin, {vnp('z.p1', 1)} AS ha_gia_tren_p75, {vnp('z.p0', 1)} AS ha_gia_con_lai,
               CASE WHEN z.zv IS NULL THEN 'chưa đủ dữ liệu để kiểm định (cần ≥ 30 tin mỗi nhóm)'
                    ELSE 'z = ' || {vnf('z.zv', 1)} || ', p ' || CASE WHEN 2 * (1 - normal_cdf(0, 1, abs(z.zv))) < 0.001 THEN '< 0,001'
                         ELSE '= ' || {vnf('2 * (1 - normal_cdf(0, 1, abs(z.zv)))', 3)} END END AS kiem_dinh,
               CASE WHEN z.zv IS NULL THEN 'chưa kết luận được'
                    WHEN 2 * (1 - normal_cdf(0, 1, abs(z.zv))) >= 0.05 THEN 'khác biệt chưa đủ bằng chứng thống kê'
                    WHEN z.zv > 0 THEN 'rao cao thì hay phải hạ giá hơn (gấp ' || {vnf('z.p1 / NULLIF(z.p0, 0)', 1)} || ' lần)'
                    ELSE 'ngược giả thuyết: tin rao cao lại ít hạ giá hơn' END AS ket_luan,
               m.err IS NOT NULL AS co_mo_hinh, {vnp('m.err')} AS sai_so, {vnp('m.cao')} AS rao_cao, {vnp('m.thap')} AS rao_thap,
               f.groups IS NOT NULL AS co_mat_tien, {vnf('f.raw')} AS mt_tho, {vnf('f.model')} AS mt_mo_hinh, {vnp('f.share')} AS mt_nhom,
               CAST(f.groups AS varchar) AS mt_so_nhom
        FROM z LEFT JOIN m ON m.province_name = z.province_name AND m.category_label = z.category_label
        LEFT JOIN f ON f.province_name = z.province_name AND f.category_label = z.category_label""",
    # BQ3: for the chosen place, type, area and budget: nearest affordable listing by road,
    # price per m² near vs 10–15 km away, and the districts on the road trade-off frontier.
    "bq3_card": f"""
        WITH r AS ({COMMUTE_LISTING}),
        k AS (SELECT DISTINCT noi_den, category_label, area_band FROM r),
        b AS (SELECT DISTINCT band_label AS ngan_sach FROM {G}.dim_price_band),
        nb AS (SELECT noi_den, category_label, area_band, ngan_sach, COUNT(*) AS n_ns, MIN(road_km) AS gan_km,
                      min_by(road_min, road_km) AS gan_phut, min_by(district_name, road_km) AS gan_quan, COUNT_IF(road_km < 10) AS n_10km
               FROM r GROUP BY 1, 2, 3, 4),
        ring AS (SELECT noi_den, category_label, area_band,
                        APPROX_PERCENTILE(CASE WHEN vanh_dai LIKE '1.%' THEN gia_m2_tr END, 0.5) AS m1, COUNT_IF(vanh_dai LIKE '1.%') AS n1,
                        APPROX_PERCENTILE(CASE WHEN vanh_dai LIKE '3.%' THEN gia_m2_tr END, 0.5) AS m3, COUNT_IF(vanh_dai LIKE '3.%') AS n3
                 FROM r GROUP BY 1, 2, 3),
        fr AS (SELECT d.destination_name AS noi_den, c.category_label, ab.band_label AS area_band, COUNT(*) AS n_bien,
                      array_join(slice(array_agg(l.district_name || ' ' || {vnf('d.road_km', 1)} || ' km – ' || {vnf('d.estimated_cost_at_target_area / 1e9', 1)}
                                                  || ' tỷ' ORDER BY d.road_km), 1, 4), '; ') AS bien
               FROM {G}.agg_commute_district d JOIN {G}.dim_location l ON l.location_key = d.location_key
               JOIN {G}.dim_property_category c ON c.property_category_key = d.property_category_key
               JOIN {G}.dim_area_band ab ON ab.band_key = d.area_band_key
               WHERE d.is_road_frontier GROUP BY 1, 2, 3)
        SELECT k.noi_den, k.category_label, k.area_band, b.ngan_sach,
               COALESCE(nb.n_ns, 0) > 0 AS co_tin, {vnf('nb.n_ns')} AS so_tin, {vnf('nb.gan_km', 1)} AS gan_km, {vnf('nb.gan_phut')} AS gan_phut,
               nb.gan_quan, {vnf('nb.n_10km')} AS n_10km,
               COALESCE(ring.n1 >= 30 AND ring.n3 >= 30, false) AS co_vanh, {vnf('ring.m1')} AS m1, {vnf('ring.m3')} AS m3,
               COALESCE(ring.m3 < ring.m1, false) AS re_hon, {vnp('abs(1 - ring.m3 / ring.m1)')} AS chenh,
               COALESCE(fr.n_bien, 0) > 0 AS co_bien, fr.bien
        FROM k CROSS JOIN b
        LEFT JOIN nb ON nb.noi_den = k.noi_den AND nb.category_label = k.category_label AND nb.area_band = k.area_band AND nb.ngan_sach = b.ngan_sach
        LEFT JOIN ring ON ring.noi_den = k.noi_den AND ring.category_label = k.category_label AND ring.area_band = k.area_band
        LEFT JOIN fr ON fr.noi_den = k.noi_den AND fr.category_label = k.category_label AND fr.area_band = k.area_band""",
}

# Superset sanitises <style>, so cards use plain HTML (bold, lists) only.
CARD_TEMPLATES = {
    "ov_card": """{{#each data}}<div><b>{{province_name}} · {{category_label}}</b> – {{so_tin}} tin có giá/m² hợp lệ, median {{median_m2}} triệu/m²<ul>
{{#if co_vanh_dai}}<li>Trong 5 km quanh trung tâm: <b>{{trung_tam}} triệu/m²</b>; từ 15 km trở ra: <b>{{ngoai_thanh}} triệu/m²</b> – chênh <b>{{gap_lan}} lần</b>.</li>
{{else}}<li>Chưa đủ tin có tọa độ ở cả trung tâm và ngoại thành để so sánh (cần ≥ 30 tin mỗi phía).</li>{{/if}}
<li>{{tren_200}} tin rao trên 200 triệu/m² (chấm đỏ trên bản đồ).</li></ul>
<div><small>Bản đồ và khoảng cách chỉ dùng tin có tọa độ: {{ty_le_toa_do}} số tin ở lựa chọn này.</small></div></div>{{/each}}""",
    "bq1_card": """{{#each data}}<div><b>{{province_name}} · {{category_label}} · {{ngan_sach}}</b> – {{so_tin}} tin, {{so_quan}} quận đủ dữ liệu<ul>
{{#if co_cuc_tri}}<li>Cùng số tiền: <b>{{hep_nhat}}</b> chỉ được <b>{{hep_m2}} m²</b>, <b>{{rong_nhat}}</b> được <b>{{rong_m2}} m²</b> – gấp {{gap_lan}} lần (so các quận ≥ 30 tin).</li>
{{else}}<li>Chưa có đủ quận ≥ 30 tin để so diện tích giữa các quận.</li>{{/if}}
{{#if co_xu_huong}}<li>Mỗi 10 km xa trung tâm, cùng số tiền được thêm khoảng <b>{{them_m2_10km}} m²</b> (tương quan r = {{r}}, giữa các quận).</li>{{/if}}
<li><b>{{bi_troi}}</b> tin có một tin cùng quận rẻ hơn mà không kém – danh sách nên xem và ví dụ cặp tin ở cuối trang.</li></ul></div>{{/each}}""",
    "bq2_card": """{{#each data}}<div><b>{{province_name}} · {{category_label}}</b> – {{so_tin}} tin quan sát ≥ 2 lần. Thẻ tổng hợp theo tỉnh và loại hình; bộ lọc quận không áp dụng cho thẻ này.<ul>
<li>Giá đầu tiên đã quan sát so với <b>P75 nhóm hiện tại</b>: nhóm trên P75 có <b>{{ha_gia_tren_p75}}</b> hạ giá, còn lại <b>{{ha_gia_con_lai}}</b> → <b>{{ket_luan}}</b> ({{kiem_dinh}}). Đây là mô tả hồi cứu, chưa kiểm soát nguồn và thời gian quan sát.</li>
{{#if co_mo_hinh}}<li>Giá ước tính lệch trung vị <b>{{sai_so}}</b> so với giá thật (MdAPE, tin mô hình chưa thấy). Vì vậy chỉ coi là rao cao/thấp bất thường khi lệch ≥ 30% so với ước tính. <b>{{rao_cao}}</b> tin rao cao hơn ước tính ≥ 30%, <b>{{rao_thap}}</b> thấp hơn ≥ 30%.</li>{{/if}}
{{#if co_mat_tien}}<li>Mặt tiền: dữ liệu thô <b>+{{mt_tho}}%</b> giá/m², mô hình (toàn quốc) <b>+{{mt_mo_hinh}}%</b>; đắt hơn ở {{mt_nhom}} trong {{mt_so_nhom}} nhóm cùng quận–diện tích.</li>{{/if}}
</ul></div>{{/each}}""",
    "bq3_card": """{{#each data}}<div><b>Làm/học ở {{noi_den}}</b>, tìm {{category_label}} {{area_band}}, ngân sách <b>{{ngan_sach}}</b> (quãng đường theo đường bộ OpenStreetMap)<ul>
{{#if co_tin}}<li>Tin vừa ngân sách gần nhất ở <b>{{gan_quan}}</b>, cách <b>{{gan_km}} km</b> đường bộ (khoảng {{gan_phut}} phút khi đường thông thoáng). Trong 10 km có <b>{{n_10km}}</b>/{{so_tin}} tin vừa ngân sách.</li>
{{else}}<li>Chưa có tin nào ở mức ngân sách này có tọa độ để tính quãng đường; thử mức ngân sách hoặc diện tích khác.</li>{{/if}}
{{#if co_vanh}}<li>Giá/m² trung vị (mọi mức ngân sách): trong 5 km <b>{{m1}} triệu</b>, vành 10–15 km <b>{{m3}} triệu</b> – {{#if re_hon}}xa thêm thì rẻ hơn <b>{{chenh}}</b>.{{else}}xa thêm lại <b>đắt hơn {{chenh}}</b>, ở gần nơi làm/học không phải trả thêm.{{/if}}</li>
{{else}}<li>Chưa đủ tin (cần ≥ 30 tin mỗi vành) để so giá trong 5 km với vành 10–15 km.</li>{{/if}}
{{#if co_bien}}<li>Đường biên đánh đổi theo quận (không quận nào vừa gần hơn vừa rẻ hơn, chi phí cho cùng diện tích): {{bien}}.</li>{{/if}}
</ul></div>{{/each}}""",
}
DATASETS.update(CARD_DATASETS)


def values_sql(columns: list[str], rows: list[tuple]) -> str:
    """A literal table for evidence kept in files (label accuracy, ground truth)."""

    def lit(value: Any) -> str:
        return "'" + str(value).replace("'", "''") + "'" if isinstance(value, str) else f"CAST({float(value)!r} AS double)"

    if not rows:
        return "SELECT " + ", ".join(f"CAST(NULL AS varchar) AS {c}" for c in columns) + " WHERE false"
    body = ", ".join("(" + ", ".join(lit(v) for v in row) + ")" for row in rows)
    return f"SELECT * FROM (VALUES {body}) AS t({', '.join(columns)})"


def evidence_rows() -> list[tuple]:
    """Label accuracy before/after the rule fix on the test split."""

    compare = PROJECT_ROOT / "docs/validation/label_accuracy_rules_compare.json"
    accuracy = []
    if compare.exists():
        test = json.loads(compare.read_text(encoding="utf-8"))["parts"]["test"]
        after = test["after"]
        accuracy.append((f"Loại hình – độ chính xác ({after.get('category_labelled', test['rows'])} tin)",
                         test["before"]["category_accuracy"], after["category_accuracy"]))
        for flag, label in (("legal", "Pháp lý"), ("frontage", "Mặt tiền"), ("car_access", "Ô tô tiếp cận"),
                            ("furnished", "Nội thất"), ("elevator", "Thang máy")):
            support = after.get(f"{flag}_support")
            name = f"{label} – recall" + (f" ({support} ca)" if support is not None else "")
            accuracy.append((name, test["before"][f"{flag}_recall"], after[f"{flag}_recall"]))
    return accuracy


_ACCURACY = evidence_rows()
DATASETS["label_accuracy"] = values_sql(["chi_so", "truoc", "sau"], _ACCURACY)


def m(expression: str, label: str) -> dict[str, Any]:
    return {"expressionType": "SQL", "sqlExpression": expression, "label": label}


# Dashboards with required filters open on a default scenario; their charts
# follow the filters, and the saved query context (used by the data API
# check) carries the default values.
BQ1_DEFAULT = {"province_name": HCM, "category_label": HOUSE, "ngan_sach": "3 - 5 tỷ"}
BQ2_DEFAULT = {"category_label": HOUSE, "province_name": HCM}
BQ3_DEFAULT = {"noi_den": "Khu công nghệ cao (Thủ Đức)", "category_label": HOUSE, "area_band": "50 - 80 m²", "ngan_sach": "3 - 5 tỷ"}
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
    ROAD_FRONTIER: "#1a9850",
    # BQ3 road rings: warm = close to the place of work/study, cool = far.
    "1. Dưới 5 km": "#d73027", "2. 5–10 km": "#fc8d59", "3. 10–15 km": "#fee08b", "4. 15–20 km": "#91cf60",
    "5. 20–30 km": "#1a9850", "6. Từ 30 km": "#4575b4",
    "1. Dưới P25 nhóm": "#1a9850", "2. Trong P25–P75": "#bdbdbd", "3. Trên P75 nhóm": "#d73027",
    "1. Quận trung tâm (< 5 km)": "#d73027", "2. Quận cách 5–10 km": "#fc8d59", "3. Quận xa hơn 10 km": "#1a9850",
    HCM: "#2c7fb8", "Hà Nội": "#d73027",
    "Giá chào thật (median)": "#2b2b2b", "Giá ước tính (median)": "#2c7fb8",
    "Chênh thô (cùng quận, cùng diện tích)": "#bdbdbd", "Mô hình (đã kiểm soát vị trí, diện tích, phòng)": "#2c7fb8",
    "Luật cũ (v4)": "#bdbdbd", "Luật mới (v5)": "#1a9850",
    "PASS": "#1a9850", "WARN": "#fee08b", "REJECT": "#d73027",
}

# Chart specs. viz: kpi | bar | hbar | line | funnel | table | map. "scenario":
# slug whose default filters the saved query context carries. Charts marked
# "title_from_insights" get a finding computed from Gold as their title.
CHARTS: list[dict[str, Any]] = [
    # ---------------------------------------------------------- overview
    {"key": "ov_listings", "name": "Tin rao bán đang phân tích", "dataset": "market_overview", "viz": "kpi",
     "metric": m("SUM(n_listings)", "Tin"), "sub": "tin đại diện ở tỉnh, loại hình đang chọn",
     "check_sql": f"SELECT SUM(n_listings) FROM {G}.agg_market_overview"},
    {"key": "ov_median_m2", "name": "Median giá chào/m² (triệu)", "dataset": "market_overview", "viz": "kpi", "format": ",.1f",
     "metric": m("MAX(median_price_per_m2_tr)", "Triệu/m²"), "scenario": "tlcn-tong-quan",
     "sub": "median chính xác từ Gold, ở tỉnh và loại hình đang chọn"},
    {"key": "ov_gradient", "name": "Trung tâm / ngoại thành", "dataset": "map_price", "viz": "kpi", "format": ",.1f", "scenario": "tlcn-tong-quan",
     "metric": m("APPROX_PERCENTILE(CASE WHEN vanh_dai = '1. Dưới 5 km' THEN gia_m2_tr END, 0.5)"
                 " / APPROX_PERCENTILE(CASE WHEN vanh_dai IN ('4. 15–25 km', '5. Trên 25 km') THEN gia_m2_tr END, 0.5)", "lần"),
     "sub": "lần – giá/m² trong 5 km so với từ 15 km trở ra"},
    {"key": "ov_card", "name": "Kết luận cho lựa chọn hiện tại", "dataset": "ov_card", "viz": "card", "scenario": "tlcn-tong-quan"},
    {"key": "ov_map", "name": "Giá/m² theo vị trí – mỗi chấm một tin có tọa độ (trong 30 km quanh trung tâm tỉnh)", "dataset": "map_price",
     "viz": "map", "dimension": "muc_gia", "viewport": VIEW_HCM, "autozoom": True, "radius": 140, "scenario": "tlcn-tong-quan",
     "where": f"kc_trung_tam <= {MAP_RADIUS_KM}", "width": 7, "height": 100},
    {"key": "ov_ring", "name": "Median giá/m² theo khoảng cách tới trung tâm tỉnh (mỗi cột ≥ 30 tin)", "dataset": "map_price",
     "viz": "bar", "x": "vanh_dai", "groupby": ["muc_vanh_dai"], "stack": "Stack", "show_value": True,
     "metric": m("APPROX_PERCENTILE(gia_m2_tr, 0.5)", "Median triệu/m²"), "sort_x": True, "format": ",.0f",
     "scenario": "tlcn-tong-quan", "having": "COUNT(*) >= 30", "width": 5, "height": 100},
    {"key": "ov_compare", "name": "(Cố định) Cùng khoảng cách tới trung tâm: giá/m² nhà phố 3 thành phố lớn", "title_from_insights": True,
     "dataset": "city_compare", "viz": "bar", "x": "vanh_dai", "groupby": ["thanh_pho"], "metric": m("APPROX_PERCENTILE(gia_m2_tr, 0.5)", "Median triệu/m²"),
     "sort_x": True, "format": ",.0f", "where": "vanh_dai <> '5. Trên 25 km'", "having": "COUNT(*) >= 30", "width": 7, "height": 90},
    {"key": "ov_price", "name": "(Cố định) Median giá/m² theo tỉnh và loại hình – nhóm ≥ 30 tin", "title_from_insights": True,
     "dataset": "market_overview", "viz": "hbar", "x": "province_name", "groupby": ["category_label"],
     "metric": m("MAX(median_price_per_m2_tr)", "Triệu/m²"), "where": f"category_label IN ('{HOUSE}', '{APARTMENT}') AND n_listings >= 30",
     "top_provinces": 8, "width": 5, "height": 90},
    # ---------------------------------------------------------- BQ1
    {"key": "bq1_listings", "name": "Tin trong phân khúc", "dataset": "budget_tradeoff", "viz": "kpi", "scenario": "tlcn-bq1",
     "metric": m("SUM(n_listings)", "Tin"), "sub": "theo tỉnh, loại hình, ngân sách đã chọn",
     "check_sql": f"SELECT SUM(t.n_listings) {BQ1_GROUPS.replace(f' AND t.n_listings >= {MIN_GROUP}', '')}"},
    {"key": "bq1_center", "name": "Quận trung tâm (< 5 km) được", "dataset": "budget_tradeoff", "viz": "kpi", "format": ",.0f",
     "scenario": "tlcn-bq1", "metric": m("APPROX_PERCENTILE(CASE WHEN median_distance_km < 5 THEN median_area END, 0.5)", "m²"),
     "where": f"n_listings >= {MIN_GROUP}", "sub": "m² – median diện tích nhận được"},
    {"key": "bq1_spread", "name": "Quận rộng nhất / hẹp nhất", "dataset": "budget_tradeoff", "viz": "kpi", "format": ",.1f",
     "scenario": "tlcn-bq1", "metric": m("MAX(median_area) / MIN(median_area)", "lần"), "where": f"n_listings >= {MIN_GROUP}",
     "sub": "lần – median diện tích, cùng số tiền"},
    {"key": "bq1_dominated", "name": "Tin có lựa chọn tốt hơn cùng quận", "dataset": "budget_pareto", "viz": "kpi", "format": ".0%",
     "scenario": "tlcn-bq1", "metric": m("AVG(CASE WHEN is_pareto_efficient THEN 0e0 ELSE 1e0 END)", "Tỷ lệ"),
     "sub": "có tin rẻ hơn mà không kém – ví dụ thật bên dưới"},
    {"key": "bq1_card", "name": "Kết luận cho lựa chọn hiện tại", "dataset": "bq1_card", "viz": "card", "scenario": "tlcn-bq1"},
    {"key": "bq1_map", "name": "Diện tích nhận được theo quận (vòng càng to càng rộng; đỏ < 50 m², xanh > 150 m²)", "dataset": "budget_tradeoff",
     "viz": "map", "dimension": "dien_tich_nhan", "radius_metric": m("SQRT(MAX(median_area))", "√ median diện tích"), "multiplier": 110,
     "autozoom": True, "viewport": VIEW_HCM, "scenario": "tlcn-bq1",
     "where": f"n_listings >= {MIN_GROUP} AND lat IS NOT NULL AND median_distance_km <= {MAP_RADIUS_KM}", "width": 6, "height": 105},
    {"key": "bq1_distance", "name": "Median diện tích theo quận, xếp theo khoảng cách tới trung tâm (nhãn: km · quận · số tin)",
     "dataset": "budget_tradeoff", "viz": "hbar", "x": "quan_theo_khoang_cach", "metric": m("MAX(median_area)", "m²"),
     "sort_x": True, "scenario": "tlcn-bq1", "where": f"n_listings >= {MIN_GROUP} AND median_distance_km IS NOT NULL", "width": 6, "height": 105},
    {"key": "bq1_curve", "name": "Tăng ngân sách thì được thêm bao nhiêu m²? Median diện tích theo vùng khoảng cách (mỗi điểm ≥ 50 tin)",
     "dataset": "budget_curve", "viz": "line", "x": "muc_ngan_sach", "groupby": ["vung"], "metric": m("APPROX_PERCENTILE(area, 0.5)", "m²"),
     "sort_x": True, "format": ",.0f", "scenario": "tlcn-bq1", "having": "COUNT(*) >= 50", "height": 70,
     # Open-ended bands (under 1 tỷ, from 20 tỷ) are not comparable across zones.
     "where": "muc_ngan_sach NOT IN ('1. Dưới 1 tỷ', '8. Từ 20 tỷ')"},
    {"key": "bq1_pairs", "name": "Ví dụ thật mỗi quận: tin B rẻ hơn ≥ 10%, rộng hơn ≥ 20%, không kém về phòng/mặt tiền/ô tô (giá/m² trong P25–P75 cùng nhóm quận, loại hình, ngân sách)",
     "dataset": "budget_pairs", "viz": "table", "scenario": "tlcn-bq1",
     "columns": ["district_name", "gia_a_ty", "dt_a", "phong_a", "gia_b_ty", "dt_b", "phong_b", "tin_a", "tin_b", "link_a", "link_b"],
     "order": [["district_name", True]], "row_limit": 200},
    {"key": "bq1_table", "name": "Danh sách nên xem trước: tin không bị tin nào cùng quận vừa rẻ hơn vừa không kém (bỏ tin giá/m² lệch > 3 lần median quận)",
     "dataset": "budget_pareto", "viz": "table", "scenario": "tlcn-bq1",
     "columns": ["district_name", "title", "gia_ty", "area", "gia_m2_tr", "rooms", "ad_url"],
     "order": [["district_name", True], ["gia_ty", True]], "row_limit": 500,
     "where": "is_pareto_efficient AND so_voi_median_quan BETWEEN 1e0 / 3 AND 3"},
    # ---------------------------------------------------------- BQ2
    {"key": "bq2_h1", "name": "Tỷ lệ hạ giá: trên P75 / còn lại", "dataset": "reprice_first", "viz": "kpi", "format": ",.1f",
     "scenario": "tlcn-bq2", "metric": m("AVG(CASE WHEN vi_tri_luc_dang = '3. Trên P75 nhóm' THEN da_ha_gia END)"
                                       " / NULLIF(AVG(CASE WHEN vi_tri_luc_dang <> '3. Trên P75 nhóm' THEN da_ha_gia END), 0)", "lần"),
     "sub": "giá đầu quan sát so với nhóm hiện tại – mô tả hồi cứu"},
    {"key": "bq2_front", "name": "Mặt tiền đắt hơn ở", "dataset": "flag_check", "viz": "kpi", "format": ".0%", "scenario": "tlcn-bq2",
     "metric": m("AVG(co_dat_hon)", "Nhóm"), "where": "dac_diem = 'Mặt tiền'", "sub": "số nhóm cùng quận, cùng diện tích"},
    {"key": "bq2_error", "name": "Sai số trung vị của giá ước tính (MdAPE, %)", "dataset": "price_check", "viz": "kpi", "format": ",.0f",
     "scenario": "tlcn-bq2", "metric": m("APPROX_PERCENTILE(sai_so_pct, 0.5)", "%"),
     "where": "tap_mo_hinh = 'test' AND NOT is_outlier_trimmed",
     "sub": "|giá thật − ước tính| / giá thật, tin test trong khoảng trim"},
    {"key": "bq2_over", "name": "Rao cao hơn ước tính ≥ 30%", "dataset": "price_check", "viz": "kpi", "format": ".1%", "scenario": "tlcn-bq2",
     "metric": m(f"AVG(CASE WHEN {OVER} THEN 1e0 ELSE 0e0 END)", "Tỷ lệ"), "where": "chenh_uoc_tinh_pct IS NOT NULL",
     "sub": "nên thương lượng", "check_sql": f"SELECT AVG(CASE WHEN m.residual_pct >= 0.3 THEN 1e0 ELSE 0e0 END) {BQ2_MODEL} AND m.residual_pct IS NOT NULL"},
    {"key": "bq2_card", "name": "Kết luận cho lựa chọn hiện tại", "dataset": "bq2_card", "viz": "card", "scenario": "tlcn-bq2"},
    {"key": "bq2_reprice", "name": "Tỷ lệ hạ giá (1–30%): giá đầu quan sát so với P25–P75 nhóm hiện tại",
     "dataset": "reprice_first", "viz": "bar", "x": "vi_tri_luc_dang", "groupby": ["nhom_mau"], "stack": "Stack", "show_value": True,
     "metric": m("AVG(da_ha_gia)", "Đã hạ giá"), "format": ".1%",
     "sort_x": True, "scenario": "tlcn-bq2", "width": 5, "height": 65},
    {"key": "bq2_calib", "name": "Kiểm chứng mô hình trên tập test: giá ước tính so với giá chào thật theo 10 nhóm giá",
     "dataset": "calibration", "viz": "line", "x": "nhom_gia_uoc_tinh", "sort_x": True, "format": ",.0f", "scenario": "tlcn-bq2", "show_value": False,
     "metrics": [m("APPROX_PERCENTILE(gia_that_tr, 0.5)", "Giá chào thật (median)"), m("APPROX_PERCENTILE(uoc_tinh_tr, 0.5)", "Giá ước tính (median)")],
     "width": 7, "height": 65},
    {"key": "bq2_flags", "name": "Dữ liệu thô có xác nhận mô hình không? Chênh giá/m² khi tiêu đề nêu đặc điểm (%)",
     "dataset": "flag_check", "viz": "hbar", "x": "dac_diem", "format": ",.0f", "scenario": "tlcn-bq2", "show_value": True,
     "metrics": [m("APPROX_PERCENTILE(chenh_tho_pct, 0.5)", "Chênh thô (cùng quận, cùng diện tích)"),
                 m("MAX(he_so_mo_hinh_pct)", "Mô hình (đã kiểm soát vị trí, diện tích, phòng)")], "height": 60},
    {"key": "bq2_map", "name": "Tin rao cao (đỏ) và thấp (xanh) so với giá ước tính – lệch ≥ 30%", "dataset": "price_check",
     "viz": "map", "dimension": "muc_chenh", "radius": 140, "autozoom": True, "viewport": VIEW_HCM, "scenario": "tlcn-bq2",
     "where": f"lat IS NOT NULL AND muc_chenh IS NOT NULL AND kc_trung_tam <= {MAP_RADIUS_KM}", "width": 7, "height": 105},
    {"key": "bq2_district", "name": "Tỷ lệ tin rao cao hơn ước tính ≥ 30% theo quận (quận ≥ 100 tin)",
     "dataset": "price_check", "viz": "hbar", "x": "district_name", "metric": m(f"AVG(CASE WHEN {OVER} THEN 1e0 ELSE 0e0 END)", "Tỷ lệ"),
     "format": ".0%", "limit": 15, "where": "district_name IS NOT NULL AND chenh_uoc_tinh_pct IS NOT NULL", "having": "COUNT(*) >= 100",
     "scenario": "tlcn-bq2", "width": 5, "height": 105},
    {"key": "bq2_cuts", "name": "Ví dụ thật: giá đầu quan sát trên P75 nhóm hiện tại, sau đó hạ giá 1–30%",
     "dataset": "reprice_examples", "viz": "table", "scenario": "tlcn-bq2",
     "columns": ["district_name", "title", "gia_luc_dang_ty", "gia_hien_tai_ty", "thay_doi_pct", "vuot_p75_luc_dang_pct", "ngay_ha_gia", "ad_url"],
     "order": [["thay_doi_pct", True]], "row_limit": 300},
    {"key": "bq2_table", "name": "Tra cứu tin: giá chào so với giá ước tính và P25–P75 nhóm tương đồng (bỏ tin lệch > 200%: thường là xếp nhầm loại hình)",
     "dataset": "price_check", "viz": "table", "scenario": "tlcn-bq2",
     "columns": ["khu_vuc", "title", "gia_ty", "area", "gia_m2_tr", "uoc_tinh_tr", "chenh_uoc_tinh_pct", "p25_nhom_tr", "p75_nhom_tr", "ad_url"],
     "order": [["khu_vuc", True], ["gia_ty", True]], "row_limit": 1000,
     "where": "uoc_tinh_tr IS NOT NULL AND NOT is_outlier_trimmed AND ABS(chenh_uoc_tinh_pct) <= 200"},
    # ---------------------------------------------------------- BQ3 (by place of work or study, road network)
    {"key": "bq3_nearest", "name": "Tin vừa ngân sách gần nhất (km đường bộ)", "dataset": "commute_listing", "viz": "kpi",
     "format": ",.1f", "metric": m("MIN(road_km)", "km"), "scenario": "tlcn-bq3", "sub": "quãng đường theo bản đồ OpenStreetMap"},
    {"key": "bq3_within10", "name": "Tin vừa ngân sách trong 10 km đường bộ", "dataset": "commute_listing", "viz": "kpi",
     "metric": m("COUNT_IF(road_km < 10)", "Tin"), "scenario": "tlcn-bq3", "sub": "đúng loại hình, diện tích, ngân sách đang chọn"},
    {"key": "bq3_far_gap", "name": "Ra vành 10–15 km: giá/m² thấp hơn (%)", "dataset": "commute_listing", "viz": "kpi", "format": ",.0f",
     "metric": m(FAR_GAP_SQL, "%"), "scenario": "tlcn-bq3", "sub": "so với trong 5 km; số âm: xa hơn lại đắt hơn"},
    {"key": "bq3_detour", "name": "Đường bộ dài hơn đường chim bay (lần)", "dataset": "commute_listing", "viz": "kpi", "format": ",.2f",
     "metric": m("APPROX_PERCENTILE(he_so_duong_vong, 0.5)", "Lần"), "scenario": "tlcn-bq3", "sub": "trung vị các tin quanh nơi làm/học này"},
    {"key": "bq3_card", "name": "Kết luận cho lựa chọn hiện tại", "dataset": "bq3_card", "viz": "card", "scenario": "tlcn-bq3"},
    {"key": "bq3_geo", "name": "Tin vừa ngân sách quanh nơi làm/học, tô màu theo vành đai quãng đường đường bộ (đen: nơi làm/học)",
     "dataset": "commute_map", "viz": "map", "dimension": "vanh_dai", "radius_metric": m("MAX(kich_thuoc)", "Kích thước"),
     "multiplier": 80, "autozoom": True, "viewport": VIEW_HCM, "scenario": "tlcn-bq3", "width": 7, "height": 105},
    {"key": "bq3_rings", "name": "Giá/m² trung vị theo vành đai quãng đường đường bộ (triệu đồng/m², mọi mức ngân sách)",
     "dataset": "commute_listing", "viz": "bar", "x": "vanh_dai", "metric": m("APPROX_PERCENTILE(gia_m2_tr, 0.5)", "Triệu/m²"),
     "format": ",.0f", "show_value": True, "sort_x": True, "having": "COUNT(*) >= 30", "scenario": "tlcn-bq3", "width": 5, "height": 105},
    {"key": "bq3_tradeoff", "name": "Đánh đổi theo quận: km đường bộ (ngang) và chi phí cho cùng diện tích (dọc); xanh: đường biên, bong bóng: số tin",
     "dataset": "commute_frontier", "viz": "bubble", "entity": "district_name", "series": "lua_chon",
     "x_metric": m("MAX(road_km)", "Km đường bộ tới nơi làm/học"), "y_metric": m("MAX(chi_phi_ty)", "Tỷ đồng cho cùng diện tích"),
     "size_metric": m("MAX(n_listings)", "Số tin"), "scenario": "tlcn-bq3", "width": 7, "height": 95},
    {"key": "bq3_frontier", "name": "Các quận trên đường biên đánh đổi: không quận nào vừa gần hơn vừa rẻ hơn",
     "dataset": "commute_frontier", "viz": "table", "columns": ["district_name", "road_km", "road_min", "chi_phi_ty", "n_listings"],
     "where": "lua_chon LIKE '1.%'", "order": [["road_km", True]], "row_limit": 30, "scenario": "tlcn-bq3", "width": 5, "height": 95},
    {"key": "bq3_all_places", "name": "Mọi nơi làm/học: ra vành 10–15 km đường bộ thì giá/m² thấp hơn bao nhiêu % so với trong 5 km",
     "dataset": "commute_listing", "viz": "hbar", "x": "noi_den", "metric": m(FAR_GAP_SQL, "% thấp hơn"), "format": ",.0f",
     "show_value": True, "having": RINGS_ENOUGH, "scenario": "tlcn-bq3", "width": 7, "height": 90},
    {"key": "bq3_detour_all", "name": "(Cố định) Kiểm chứng quãng đường: đường bộ dài hơn đường chim bay bao nhiêu lần", "title_from_insights": True,
     "dataset": "commute_listing", "viz": "hbar", "x": "noi_den", "metric": m("APPROX_PERCENTILE(he_so_duong_vong, 0.5)", "Lần"),
     "format": ",.2f", "show_value": True, "width": 5, "height": 90},
    {"key": "bq3_listings", "name": "Tin vừa ngân sách gần nơi làm/học nhất (km đường bộ; phút khi đường thông thoáng)",
     "dataset": "commute_listing", "viz": "table",
     "columns": ["district_name", "road_km", "road_min", "title", "gia_ty", "area", "gia_m2_tr", "ad_url"],
     "order": [["road_km", True], ["gia_ty", True]], "row_limit": 100, "scenario": "tlcn-bq3"},
    # ---------------------------------------------------------- data quality
    {"key": "dq_pass", "name": "Bản ghi đạt PASS", "dataset": "data_quality", "viz": "kpi", "format": ".1%",
     "metric": m("SUM(pass_rows) * 1e0 / SUM(pass_rows + warn_rows)", "PASS"), "sub": "còn lại là WARN: thiếu/đã sửa một trường",
     "check_sql": f"SELECT SUM(pass_rows) * 1e0 / SUM(pass_rows + warn_rows) FROM {G}.agg_dq_kpi"},
    {"key": "dq_reject", "name": "Bản ghi bị loại", "dataset": "data_quality", "viz": "kpi",
     "metric": m("SUM(reject_rows)", "Dòng"), "sub": "không vào phân tích"},
    {"key": "dq_dup", "name": "Tin nghi trùng giữa nguồn", "dataset": "data_quality", "viz": "kpi", "format": ".1%",
     "metric": m("SUM(dup_suspect_rate * current_rows) / SUM(current_rows)", "Tỷ lệ"), "sub": "chỉ giữ một tin đại diện"},
    {"key": "dq_g4", "name": "Độ chính xác đọc đặc điểm từ tiêu đề, trước và sau khi sửa luật bằng nhãn thủ công (tập test)", "title_from_insights": True,
     "dataset": "label_accuracy", "viz": "hbar", "x": "chi_so", "format": ".0%", "show_value": True,
     "metrics": [m("MAX(truoc)", "Luật cũ (v4)"), m("MAX(sau)", "Luật mới (v5)")], "bounds": [0, 1.12], "width": 6, "height": 70},
    {"key": "dq_status", "name": "Tỷ lệ PASS / WARN / REJECT theo nguồn", "title_from_insights": True, "dataset": "data_quality",
     "viz": "hbar", "x": "source", "metrics": [m("SUM(pass_rows)", "PASS"), m("SUM(warn_rows)", "WARN"), m("SUM(reject_rows)", "REJECT")],
     "stack": "Expand", "width": 6, "height": 70},
    {"key": "dq_coverage", "name": "Độ phủ trường dữ liệu theo nguồn", "title_from_insights": True, "dataset": "data_quality",
     "viz": "hbar", "x": "source", "format": ".0%", "height": 90, "width": 6,
     "metrics": [m("MAX(coverage_price_known)", "Giá"), m("MAX(coverage_area_known)", "Diện tích"), m("MAX(coverage_rooms_known)", "Số phòng"),
                 m("MAX(coverage_coordinate)", "Tọa độ"), m("MAX(coverage_legal_known)", "Pháp lý")]},
    {"key": "dq_funnel", "name": "Từ dữ liệu crawl tới tin dùng để phân tích (số dòng mỗi tầng)", "dataset": "funnel", "viz": "hbar",
     "x": "stage", "metric": m("SUM(rows)", "Dòng"), "sort_x": True, "width": 6, "height": 90,
     "check_sql": f"SELECT SUM(bronze_rows) FROM {G}.agg_dq_kpi", "check_first_row": True},
    {"key": "dq_sources", "name": "Nguồn dữ liệu và ngày quan sát mới nhất", "dataset": "data_quality", "viz": "table",
     "columns": ["source", "loai_nguon", "ngay_moi_nhat", "current_rows", "representative_rows"],
     "order": [["representative_rows", False]], "row_limit": 20},
]

# Rows: chart keys, or "md" for the one-line header. Each BQ page reads:
# finding (KPIs) -> charts that show it -> real examples -> validation.
DASHBOARDS = [
    {"slug": "tlcn-tong-quan", "title": "Tổng quan thị trường BĐS đang chào bán",
     "rows": [["md"], ["ov_listings", "ov_median_m2", "ov_gradient"], ["ov_card"], ["ov_map", "ov_ring"], ["ov_compare", "ov_price"]]},
    {"slug": "tlcn-bq1", "title": "BQ1 – Cùng ngân sách, người mua đánh đổi gì?",
     "rows": [["md"], ["bq1_listings", "bq1_center", "bq1_spread", "bq1_dominated"], ["bq1_card"], ["bq1_map", "bq1_distance"], ["bq1_curve"],
              ["bq1_pairs"], ["bq1_table"]]},
    {"slug": "tlcn-bq2", "title": "BQ2 – Tin này đắt hay rẻ so với căn tương đồng?",
     "rows": [["md"], ["bq2_h1", "bq2_front", "bq2_error", "bq2_over"], ["bq2_card"], ["bq2_reprice", "bq2_calib"], ["bq2_flags"],
              ["bq2_map", "bq2_district"], ["bq2_cuts"], ["bq2_table"]]},
    {"slug": "tlcn-bq3", "title": "BQ3 – Đi làm/học ở đây: ở cách bao xa thì vừa ngân sách, xa thêm thì rẻ hơn bao nhiêu?",
     "rows": [["md"], ["bq3_nearest", "bq3_within10", "bq3_far_gap", "bq3_detour"], ["bq3_card"], ["bq3_geo", "bq3_rings"],
              ["bq3_tradeoff", "bq3_frontier"], ["bq3_all_places", "bq3_detour_all"], ["bq3_listings"]]},
    {"slug": "tlcn-chat-luong", "title": "Chất lượng dữ liệu – số liệu trên dashboard đáng tin đến đâu?",
     "rows": [["md"], ["dq_pass", "dq_reject", "dq_dup"], ["dq_g4", "dq_status"], ["dq_coverage", "dq_funnel"], ["dq_sources"]]},
]
# Overview charts that follow the province / type filters (the others are fixed comparisons).
OV_FILTERED = ["ov_listings", "ov_median_m2", "ov_gradient", "ov_card", "ov_map", "ov_ring"]
# BQ3 charts about the chosen place (bq3_all_places compares every place; bq3_detour_all is a fixed check).
BQ3_PLACE_CHARTS = ["bq3_nearest", "bq3_within10", "bq3_far_gap", "bq3_detour", "bq3_card", "bq3_geo", "bq3_rings",
                    "bq3_tradeoff", "bq3_frontier", "bq3_listings"]
NATIVE_FILTERS = {
    "tlcn-tong-quan": [
        {"label": "Tỉnh/thành", "column": "province_name", "dataset": "market_overview", "default": HCM, "charts": OV_FILTERED},
        {"label": "Loại hình", "column": "category_label", "dataset": "market_overview", "default": HOUSE, "charts": OV_FILTERED},
    ],
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
        {"label": "Nơi làm/học", "column": "noi_den", "dataset": "commute_listing", "default": BQ3_DEFAULT["noi_den"],
         "charts": BQ3_PLACE_CHARTS},
        {"label": "Loại hình", "column": "category_label", "dataset": "commute_listing", "default": BQ3_DEFAULT["category_label"],
         "charts": BQ3_PLACE_CHARTS + ["bq3_all_places"]},
        {"label": "Diện tích", "column": "area_band", "dataset": "commute_listing", "default": BQ3_DEFAULT["area_band"],
         "charts": BQ3_PLACE_CHARTS + ["bq3_all_places"]},
        {"label": "Ngân sách", "column": "ngan_sach", "dataset": "commute_listing", "default": BQ3_DEFAULT["ngan_sach"],
         "charts": ["bq3_nearest", "bq3_within10", "bq3_card", "bq3_geo", "bq3_listings"]},
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
# Table headers: Superset shows a column's verbose name.
COLUMN_LABELS = {
    "district_name": "Quận/huyện", "title": "Tiêu đề tin", "gia_ty": "Giá (tỷ)", "area": "m²", "gia_m2_tr": "Giá/m² (triệu)",
    "rooms": "Phòng ngủ", "ad_url": "Link tin", "khu_vuc": "Khu vực", "uoc_tinh_tr": "Ước tính/m² (triệu)",
    "chenh_uoc_tinh_pct": "Lệch so với ước tính (%)", "p25_nhom_tr": "P25 nhóm (tr/m²)", "p75_nhom_tr": "P75 nhóm (tr/m²)",
    "gia_a_ty": "A: giá (tỷ)", "dt_a": "A: m²", "phong_a": "A: phòng", "tin_a": "Tin A", "link_a": "Link A",
    "gia_b_ty": "B: giá (tỷ)", "dt_b": "B: m²", "phong_b": "B: phòng", "tin_b": "Tin B (rẻ hơn, rộng hơn)", "link_b": "Link B",
    "gia_luc_dang_ty": "Giá lúc đăng (tỷ)", "gia_hien_tai_ty": "Giá hiện tại (tỷ)", "thay_doi_pct": "Thay đổi (%)",
    "vuot_p75_luc_dang_pct": "Vượt P75 lúc đăng (%)", "ngay_ha_gia": "Ngày hạ giá",
    "quan_thay_the": "Quận thay thế", "quang_duong_km": "Quãng đường (km)",
    "road_km": "Đường bộ (km)", "road_min": "Phút (đường thông thoáng)", "chi_phi_ty": "Chi phí cùng diện tích (tỷ)",
    "n_listings": "Số tin", "noi_den": "Nơi làm/học", "vanh_dai": "Vành đai", "he_so_duong_vong": "Đường bộ / chim bay",
    "source": "Nguồn", "loai_nguon": "Loại nguồn", "ngay_moi_nhat": "Ngày quan sát mới nhất",
    "current_rows": "Tin current", "representative_rows": "Tin đại diện",
}


def has_column(dataset: str, column: str) -> bool:
    return column in DATASET_COLUMNS.get(dataset, set())


def chart_params(spec: dict, dataset_id: int) -> tuple[dict, dict]:
    """Return (form_data params, query object for the query context)."""

    viz = {"kpi": "big_number_total", "bar": "echarts_timeseries_bar", "hbar": "echarts_timeseries_bar",
           "bubble": "bubble_v2", "funnel": "funnel", "table": "table", "map": "deck_scatter",
           "line": "echarts_timeseries_line", "card": "handlebars"}[spec["viz"]]
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
                          "quang_duong_km", "median_quan_goc_tr", "median_quan_thay_the_tr", "uoc_tinh_tr", "p25_nhom_tr", "p75_nhom_tr",
                          "gia_a_ty", "gia_b_ty", "gia_luc_dang_ty", "gia_hien_tai_ty", "chi_phi_ty"):
                config[column] = {"d3NumberFormat": ",.2~f"}
            elif column in ("road_km",):
                config[column] = {"d3NumberFormat": ",.1f"}
            elif column in ("road_min", "n_listings"):
                config[column] = {"d3NumberFormat": ",.0f"}
            elif column in ("re_hon_pct", "chenh_uoc_tinh_pct", "thay_doi_pct", "vuot_p75_luc_dang_pct", "dt_a", "dt_b", "area"):
                config[column] = {"d3NumberFormat": ",.0f"}
            elif column in ("khoang_cach_km",):
                config[column] = {"d3NumberFormat": ",.1f"}
        params = {**base, "query_mode": "raw", "all_columns": spec["columns"], "column_config": config,
                  "order_by_cols": [json.dumps(o) for o in spec.get("order", [])], "row_limit": spec["row_limit"],
                  "server_pagination": False, "include_search": True, "page_length": 10, "show_cell_bars": False}
        query = {"columns": spec["columns"], "orderby": spec.get("order", []), "row_limit": spec["row_limit"], "extras": extras}
    elif spec["viz"] == "card":
        columns = sorted(DATASET_COLUMNS[spec["dataset"]])
        params = {**base, "query_mode": "raw", "all_columns": columns, "order_by_cols": [], "row_limit": 5,
                  "handlebarsTemplate": CARD_TEMPLATES[spec["dataset"]], "styleTemplate": ""}
        query = {"columns": columns, "row_limit": 5, "extras": extras}
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
        series = [spec["series"]] if spec.get("series") else []
        params = {**base, "entity": spec["entity"], "x": spec["x_metric"], "y": spec["y_metric"], "size": spec["size_metric"],
                  "max_bubble_size": "40", "row_limit": 100, "show_legend": bool(series),
                  "x_axis_label": spec["x_metric"]["label"], "y_axis_label": spec["y_metric"]["label"],
                  "xAxisFormat": "SMART_NUMBER", "y_axis_format": "SMART_NUMBER", "truncateYAxis": False,
                  **({"series": spec["series"]} if series else {})}
        query = {"columns": [spec["entity"], *series], "metrics": [spec["x_metric"], spec["y_metric"], spec["size_metric"]],
                 "row_limit": 100, "extras": extras}
    elif spec["viz"] == "funnel":
        params = {**base, "groupby": [spec["x"]], "metric": spec["metric"], "sort_by_metric": True, "show_legend": False,
                  "label_type": "key_value_percent", "number_format": ",d", "row_limit": 10}
        query = {"columns": [spec["x"]], "metrics": [spec["metric"]], "orderby": [[spec["metric"], False]], "extras": extras}
    else:  # bar / hbar / line with a categorical axis
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
                  "zoomable": False, "minorTicks": False, "markerEnabled": True, "markerSize": 7,
                  "y_axis_bounds": spec.get("bounds", [None, None])}
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
    heights = {"kpi": 22, "table": 80, "bubble": 70, "funnel": 50, "bar": 60, "hbar": 55, "line": 60, "map": 100, "card": 34}
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
        columns = api.call("GET", f"dataset/{dataset_ids[name]}")["result"]["columns"]
        DATASET_COLUMNS[name] = {c["column_name"] for c in columns}
        # The PUT replaces the column list, so every column is sent back.
        if any(c["column_name"] in COLUMN_LABELS for c in columns):
            keep = ("id", "column_name", "type", "is_dttm", "filterable", "groupby", "expression", "description", "python_date_format")
            api.call("PUT", f"dataset/{dataset_ids[name]}", json={"columns": [
                {**{k: c[k] for k in keep if k in c}, "verbose_name": COLUMN_LABELS.get(c["column_name"], c.get("verbose_name"))}
                for c in columns]})

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
