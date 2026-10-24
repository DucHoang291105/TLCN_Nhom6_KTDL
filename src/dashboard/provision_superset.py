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

# ----------------------------------------------------------------- datasets
# SQL is sent as one line, so no "--" comments inside these strings.
# quan_theo_khoang_cach ("07.5 km · Quận 7") sorts districts by distance and
# shows the distance on the axis; quan_thay_the_km adds the extra distance.
G = "iceberg.gold"
DATASETS: dict[str, str] = {
    "market_overview": f"""
        SELECT o.province_name, c.category_label, o.n_listings,
               o.median_price_per_m2 / 1e6 AS median_price_per_m2_tr, o.median_price / 1e9 AS median_price_ty, o.median_area
        FROM {G}.agg_market_overview o
        JOIN {G}.dim_property_category c ON c.property_category_key = o.property_category_key""",
    "funnel": f"""
        SELECT stage, rows FROM (
            SELECT 1 AS ord, '1. Bronze (đã crawl)' AS stage, SUM(bronze_rows) AS rows FROM {G}.agg_dq_kpi
            UNION ALL SELECT 2, '2. Silver observation (hợp lệ, bỏ trùng batch)', SUM(observation_rows) FROM {G}.agg_dq_kpi
            UNION ALL SELECT 3, '3. Tin current (mới nhất mỗi tin)', SUM(current_rows) FROM {G}.agg_dq_kpi
            UNION ALL SELECT 4, '4. Tin bán (fact_listing)', SUM(fact_rows) FROM {G}.agg_dq_kpi
            UNION ALL SELECT 5, '5. Tin đại diện (bỏ nghi trùng nguồn)', SUM(representative_rows) FROM {G}.agg_dq_kpi
        ) t""",
    "price_assessment": f"""
        SELECT l.province_name, NULLIF(l.district_name, 'Unknown') AS district_name, c.category_label, ab.band_label AS area_band,
               a.price_position,
               CASE a.price_position WHEN 'duoi_p25' THEN '1. Dưới P25' WHEN 'p25_p75' THEN '2. Trong P25–P75'
                    WHEN 'tren_p75' THEN '3. Trên P75' ELSE '4. Không đủ dữ liệu' END AS vi_tri_gia,
               CASE WHEN a.price_ratio IS NULL THEN NULL
                    WHEN a.price_ratio < 0.5 THEN '1. < 0,5×' WHEN a.price_ratio < 0.8 THEN '2. 0,5–0,8×'
                    WHEN a.price_ratio < 1.25 THEN '3. 0,8–1,25×' WHEN a.price_ratio < 2 THEN '4. 1,25–2×'
                    WHEN a.price_ratio <= 10 THEN '5. 2–10×' ELSE '6. > 10× (outlier)' END AS muc_lech,
               a.price / 1e9 AS gia_ty, a.area, a.price_per_m2 / 1e6 AS gia_m2_tr,
               a.peer_median_price_per_m2 / 1e6 AS median_nhom_tr, a.price_ratio, a.peer_n,
               a.is_low_price_missing_legal, f.title
        FROM {G}.fact_listing_price_assessment a
        JOIN {G}.dim_location l ON l.location_key = a.location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = a.property_category_key
        JOIN {G}.dim_area_band ab ON ab.band_key = a.area_band_key
        JOIN {G}.fact_listing f ON f.source_id = a.source_id""",
    "budget_tradeoff": f"""
        SELECT l.province_name, NULLIF(l.district_name, 'Unknown') AS district_name, c.category_label, pb.band_key AS price_band_key,
               pb.band_label AS ngan_sach, t.n_listings, t.median_area, t.median_rooms,
               t.median_price_per_m2 / 1e6 AS median_price_per_m2_tr, t.median_distance_km,
               t.share_legal, t.share_frontage, t.share_car_access, t.share_elevator,
               lpad(format('%.1f', t.median_distance_km), 4, '0') || ' km · ' || l.district_name AS quan_theo_khoang_cach
        FROM {G}.agg_budget_tradeoff t
        JOIN {G}.dim_location l ON l.location_key = t.location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = t.property_category_key
        JOIN {G}.dim_price_band pb ON pb.band_key = t.price_band_key
        WHERE l.district_name IS NOT NULL AND l.district_name <> 'Unknown'""",
    "budget_pareto": f"""
        SELECT l.province_name, NULLIF(l.district_name, 'Unknown') AS district_name, c.category_label, pb.band_key AS price_band_key, pb.band_label AS ngan_sach,
               p.is_pareto_efficient, p.group_size, p.frontier_size, f.title, f.price / 1e9 AS gia_ty, f.area, f.rooms,
               f.title_has_legal, f.title_has_frontage, f.title_has_car_access
        FROM {G}.fact_budget_pareto p
        JOIN {G}.fact_listing f ON f.source_id = p.source_id
        JOIN {G}.dim_location l ON l.location_key = p.location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = p.property_category_key
        JOIN {G}.dim_price_band pb ON pb.band_key = p.price_band_key""",
    "area_substitution": f"""
        SELECT o.province_name, NULLIF(o.district_name, 'Unknown') AS quan_goc, NULLIF(a.district_name, 'Unknown') AS quan_thay_the, c.category_label,
               ab.band_label AS area_band, s.substitution_rank, s.price_gap_pct * 100 AS re_hon_pct,
               s.target_area, s.estimated_saving_at_target_area / 1e9 AS chenh_chi_phi_ty,
               s.distance_diff_km, s.feature_similarity, s.origin_n_listings, s.alternative_n_listings,
               a.district_name || ' (' || CASE WHEN s.distance_diff_km >= 0 THEN '+' ELSE '' END
                   || format('%.1f', s.distance_diff_km) || ' km)' AS quan_thay_the_km
        FROM {G}.agg_area_substitution s
        JOIN {G}.dim_location o ON o.location_key = s.origin_location_key
        JOIN {G}.dim_location a ON a.location_key = s.alternative_location_key
        JOIN {G}.dim_property_category c ON c.property_category_key = s.property_category_key
        JOIN {G}.dim_area_band ab ON ab.band_key = s.area_band_key""",
}


def m(expression: str, label: str) -> dict[str, Any]:
    return {"expressionType": "SQL", "sqlExpression": expression, "label": label}


BQ1_SCOPE = f"province_name = '{HCM}' AND category_label = '{HOUSE}' AND price_band_key = 4 AND n_listings >= 20"
BQ3_SCOPE = f"quan_goc = 'Quận Bình Tân' AND category_label = '{HOUSE}' AND area_band = '50 - 80 m²'"
PCT_P75 = m("AVG(CASE WHEN price_position = 'tren_p75' THEN 1e0 ELSE 0e0 END)", "Tỷ lệ trên P75")

# Chart specs. viz: kpi | bar | hbar | bubble | funnel | table.
CHARTS: list[dict[str, Any]] = [
    # ---------------------------------------------------------- overview
    {"key": "ov_listings", "name": "Tin rao bán đang phân tích", "dataset": "market_overview", "viz": "kpi",
     "metric": m("SUM(n_listings)", "Tin"), "sub": "tin đại diện, đã bỏ nghi trùng giữa 9 nguồn",
     "check_sql": f"SELECT SUM(n_listings) FROM {G}.agg_market_overview"},
    {"key": "ov_hcm_share", "name": "Tỷ trọng TP.HCM", "dataset": "market_overview", "viz": "kpi", "format": ".1%",
     "metric": m(f"SUM(CASE WHEN province_name = '{HCM}' THEN n_listings ELSE 0 END) * 1e0 / SUM(n_listings)", "TP.HCM"),
     "sub": "số tin nằm ở TP.HCM"},
    {"key": "ov_median_m2", "name": "Median giá chào/m² (triệu)", "dataset": "price_assessment", "viz": "kpi", "format": ",.1f",
     "metric": m("APPROX_PERCENTILE(gia_m2_tr, 0.5)", "Triệu/m²"), "where": f"NOT {SUSPECT}", "sub": "toàn bộ tin có giá, cả nước"},
    {"key": "ov_provinces", "name": "Tỉnh/thành có tin", "dataset": "market_overview", "viz": "kpi",
     "metric": m("COUNT(DISTINCT province_name)", "Tỉnh"), "where": "province_name <> 'Không rõ'", "sub": "trên 34 tỉnh/thành (mô hình 2025)"},
    {"key": "ov_by_province", "name": "Tin tập trung ở đâu? Số tin theo tỉnh (10 tỉnh nhiều nhất)", "dataset": "market_overview",
     "viz": "hbar", "x": "province_name", "metric": m("SUM(n_listings)", "Số tin"), "limit": 10, "where": "province_name <> 'Không rõ'"},
    {"key": "ov_by_category", "name": "Loại hình nào được rao nhiều?", "dataset": "market_overview",
     "viz": "hbar", "x": "category_label", "metric": m("SUM(n_listings)", "Số tin"), "where": "category_label <> 'Không rõ'"},
    {"key": "ov_price", "name": "Ở đâu đắt nhất? Median giá/m² (triệu) theo tỉnh và loại hình", "dataset": "market_overview",
     "viz": "bar", "x": "province_name", "groupby": ["category_label"], "metric": m("MAX(median_price_per_m2_tr)", "Triệu/m²"),
     "where": f"category_label IN ('{HOUSE}', '{APARTMENT}', 'Đất')", "top_provinces": 6},
    {"key": "ov_funnel", "name": "Từ dữ liệu crawl tới tin dùng để phân tích (số dòng mỗi tầng)", "dataset": "funnel", "viz": "hbar",
     "x": "stage", "metric": m("SUM(rows)", "Dòng"), "sort_x": True,
     "check_sql": f"SELECT SUM(bronze_rows) FROM {G}.agg_dq_kpi", "check_first_row": True},
    # ---------------------------------------------------------- BQ2
    {"key": "bq2_high", "name": "Rao cao hơn median nhóm ≥ 25%", "dataset": "price_assessment", "viz": "kpi", "format": ".1%",
     "metric": m("AVG(CASE WHEN price_ratio >= 1.25 THEN 1e0 ELSE 0e0 END)", "≥ 1,25×"), "where": f"price_ratio IS NOT NULL AND NOT {SUSPECT}",
     "sub": "so với các căn cùng quận, loại hình, diện tích, số phòng",
     "check_sql": f"SELECT AVG(CASE WHEN price_ratio >= 1.25 THEN 1e0 ELSE 0e0 END) FROM {G}.fact_listing_price_assessment WHERE price_ratio IS NOT NULL AND NOT {SUSPECT}"},
    {"key": "bq2_low", "name": "Rao gấp đôi median nhóm", "dataset": "price_assessment", "viz": "kpi", "format": ".1%",
     "metric": m("AVG(CASE WHEN price_ratio >= 2 THEN 1e0 ELSE 0e0 END)", "≥ 2×"), "where": f"price_ratio IS NOT NULL AND NOT {SUSPECT}",
     "sub": "ứng viên 'định giá cao' cần thương lượng"},
    {"key": "bq2_low_legal", "name": "Giá thấp nhưng không nêu pháp lý", "dataset": "price_assessment", "viz": "kpi", "format": ".0%",
     "metric": m("AVG(CASE WHEN is_low_price_missing_legal THEN 1e0 ELSE 0e0 END)", "Tỷ lệ"), "where": "price_position = 'duoi_p25'",
     "sub": "trong các tin dưới P25 – cần hỏi giấy tờ"},
    {"key": "bq2_suspect", "name": "Outlier: giá/m² > 10× median nhóm", "dataset": "price_assessment", "viz": "kpi",
     "metric": m(f"SUM(CASE WHEN {SUSPECT} THEN 1 ELSE 0 END)", "Tin"), "sub": "giữ lại, cần xem riêng (mặt tiền đắc địa, nhóm so sánh không đồng nhất)",
     "check_sql": f"SELECT SUM(CASE WHEN {SUSPECT} THEN 1 ELSE 0 END) FROM {G}.fact_listing_price_assessment"},
    {"key": "bq2_positions", "name": "Loại hình nào hay bị rao cao? (tỷ lệ tin theo vị trí giá)", "dataset": "price_assessment",
     "viz": "hbar", "x": "category_label", "groupby": ["vi_tri_gia"], "metric": m("COUNT(*)", "Số tin"), "stack": "Expand",
     "where": "category_label <> 'Không rõ'"},
    {"key": "bq2_ratio", "name": "Giá/m² lệch bao nhiêu so với median nhóm? (số tin)", "dataset": "price_assessment",
     "viz": "bar", "x": "muc_lech", "metric": m("COUNT(*)", "Số tin"), "where": "muc_lech IS NOT NULL", "sort_x": True},
    {"key": "bq2_district", "name": f"Căn hộ TP.HCM: quận nào có nhiều tin rao cao hơn P75? (quận ≥ 100 tin)", "dataset": "price_assessment",
     "viz": "hbar", "x": "district_name", "metric": PCT_P75, "format": ".0%", "limit": 12,
     "where": f"province_name = '{HCM}' AND category_label = '{APARTMENT}' AND district_name IS NOT NULL", "having": "COUNT(*) >= 100"},
    {"key": "bq2_table", "name": "Chi tiết: tin rao cao nhất so với nhóm (≤ 10× median)", "dataset": "price_assessment", "viz": "table",
     "columns": ["district_name", "category_label", "title", "gia_ty", "area", "gia_m2_tr", "median_nhom_tr", "price_ratio"],
     "order": [["price_ratio", False]], "row_limit": 50, "where": f"price_position = 'tren_p75' AND NOT {SUSPECT}"},
    # ---------------------------------------------------------- BQ1
    {"key": "bq1_districts", "name": "Quận có đủ tin (≥ 20)", "dataset": "budget_tradeoff", "viz": "kpi",
     "metric": m("COUNT(*)", "Quận"), "where": BQ1_SCOPE, "sub": "nhà phố 3–5 tỷ tại TP.HCM"},
    {"key": "bq1_area_range", "name": "Chênh diện tích giữa các quận", "dataset": "budget_tradeoff", "viz": "kpi", "format": ",.1f",
     "metric": m("MAX(median_area) / MIN(median_area)", "lần"), "where": BQ1_SCOPE, "sub": "quận rộng nhất / quận hẹp nhất (lần)"},
    {"key": "bq1_pareto_share", "name": "Tin không bị trội", "dataset": "budget_pareto", "viz": "kpi", "format": ".1%",
     "metric": m("AVG(CASE WHEN is_pareto_efficient THEN 1e0 ELSE 0e0 END)", "Pareto"),
     "where": f"province_name = '{HCM}' AND category_label = '{HOUSE}' AND price_band_key = 4", "sub": "nên xem trước; số còn lại có lựa chọn tốt hơn cùng quận"},
    {"key": "bq1_distance", "name": "Cùng 3–5 tỷ: càng xa trung tâm, nhà càng rộng – median diện tích (m²), quận xếp theo khoảng cách",
     "dataset": "budget_tradeoff", "viz": "bar", "x": "quan_theo_khoang_cach", "metric": m("MAX(median_area)", "Median diện tích (m²)"),
     "sort_x": True, "where": BQ1_SCOPE + " AND median_distance_km IS NOT NULL"},
    {"key": "bq1_price_m2", "name": "Giá/m² (triệu) theo quận – phần tiền trả cho vị trí", "dataset": "budget_tradeoff",
     "viz": "hbar", "x": "district_name", "metric": m("MAX(median_price_per_m2_tr)", "Triệu/m²"), "where": BQ1_SCOPE, "height": 85},
    {"key": "bq1_car", "name": "Tỷ lệ tin nêu ô tô tiếp cận theo quận", "dataset": "budget_tradeoff",
     "viz": "hbar", "x": "district_name", "metric": m("MAX(share_car_access)", "Ô tô tiếp cận"), "format": ".0%", "where": BQ1_SCOPE, "height": 85},
    {"key": "bq1_table", "name": "Chi tiết: tin không bị trội, nhà phố 3–5 tỷ TP.HCM",
     "dataset": "budget_pareto", "viz": "table",
     "columns": ["district_name", "title", "gia_ty", "area", "rooms", "title_has_frontage", "title_has_car_access", "title_has_legal"],
     "order": [["area", False]], "row_limit": 100,
     "where": f"is_pareto_efficient AND province_name = '{HCM}' AND category_label = '{HOUSE}' AND price_band_key = 4"},
    # ---------------------------------------------------------- BQ3
    {"key": "bq3_options", "name": "Lựa chọn thay thế", "dataset": "area_substitution", "viz": "kpi",
     "metric": m("COUNT(*)", "Quận"), "where": BQ3_SCOPE, "sub": "cho nhà phố 50–80 m² ở Quận Bình Tân"},
    {"key": "bq3_best_near", "name": "Rẻ hơn nhiều nhất trong phạm vi +5 km", "dataset": "area_substitution", "viz": "kpi", "format": ",.0f",
     "metric": m("MAX(re_hon_pct)", "%"), "where": BQ3_SCOPE + " AND distance_diff_km <= 5", "sub": "% chi phí thấp hơn cho cùng diện tích"},
    {"key": "bq3_gap", "name": "Chi phí thấp hơn bao nhiêu % cho cùng diện tích – và xa trung tâm thêm bao nhiêu km",
     "dataset": "area_substitution", "viz": "hbar", "x": "quan_thay_the_km", "metric": m("MAX(re_hon_pct)", "% thấp hơn"),
     "format": ",.0f", "where": BQ3_SCOPE},
    {"key": "bq3_saving", "name": "Ước tính chênh chi phí (tỷ đồng) cho căn 62 m² điển hình", "dataset": "area_substitution",
     "viz": "hbar", "x": "quan_thay_the", "metric": m("MAX(chenh_chi_phi_ty)", "Tỷ đồng"), "format": ",.2f", "where": BQ3_SCOPE},
    {"key": "bq3_by_origin", "name": "Quận gốc nào có nhiều lựa chọn thay thế? (TP.HCM, nhà phố 50–80 m²)", "dataset": "area_substitution",
     "viz": "hbar", "x": "quan_goc", "metric": m("COUNT(*)", "Số quận thay thế"), "limit": 12,
     "where": f"province_name = '{HCM}' AND category_label = '{HOUSE}' AND area_band = '50 - 80 m²' AND quan_goc IS NOT NULL"},
    {"key": "bq3_table", "name": "Chi tiết: khu vực thay thế (dùng bộ lọc Quận gốc)", "dataset": "area_substitution", "viz": "table",
     "columns": ["quan_goc", "category_label", "area_band", "quan_thay_the", "re_hon_pct", "chenh_chi_phi_ty", "distance_diff_km", "feature_similarity"],
     "order": [["origin_n_listings", False]], "row_limit": 500},
]

# Rows: chart keys, or "md" for the insight header.
DASHBOARDS = [
    {"slug": "tlcn-tong-quan", "title": "Tổng quan thị trường BĐS đang chào bán",
     "rows": [["md"], ["ov_listings", "ov_hcm_share", "ov_median_m2", "ov_provinces"], ["ov_by_province", "ov_by_category"], ["ov_price"], ["ov_funnel"]]},
    {"slug": "tlcn-bq1", "title": "BQ1 – Cùng ngân sách, người mua đánh đổi gì?",
     "rows": [["md"], ["bq1_districts", "bq1_area_range", "bq1_pareto_share"], ["bq1_distance"], ["bq1_price_m2", "bq1_car"], ["bq1_table"]]},
    {"slug": "tlcn-bq2", "title": "BQ2 – Tin nào rao cao/thấp bất thường so với căn tương đồng?",
     "rows": [["md"], ["bq2_high", "bq2_low", "bq2_low_legal", "bq2_suspect"], ["bq2_positions", "bq2_ratio"], ["bq2_district"], ["bq2_table"]]},
    {"slug": "tlcn-bq3", "title": "BQ3 – Không mua được ở quận này thì xem quận nào?",
     "rows": [["md"], ["bq3_options", "bq3_best_near"], ["bq3_gap", "bq3_saving"], ["bq3_by_origin"], ["bq3_table"]]},
]
NATIVE_FILTERS = {
    "tlcn-tong-quan": [("Tỉnh/thành", "province_name"), ("Loại hình", "category_label")],
    "tlcn-bq2": [("Tỉnh/thành", "province_name"), ("Quận/huyện", "district_name"), ("Loại hình", "category_label")],
    "tlcn-bq1": [("Quận/huyện", "district_name")],
    "tlcn-bq3": [("Quận gốc", "quan_goc"), ("Loại hình", "category_label")],
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


def chart_params(spec: dict, dataset_id: int) -> tuple[dict, dict]:
    """Return (form_data params, query object for the query context)."""

    viz = {"kpi": "big_number_total", "bar": "echarts_timeseries_bar", "hbar": "echarts_timeseries_bar",
           "bubble": "bubble_v2", "funnel": "funnel", "table": "table"}[spec["viz"]]
    adhoc = [{"expressionType": "SQL", "clause": "WHERE", "sqlExpression": spec["where"]}] if spec.get("where") else []
    if spec.get("having"):
        adhoc.append({"expressionType": "SQL", "clause": "HAVING", "sqlExpression": spec["having"]})
    extras = {k: v for k, v in (("where", spec.get("where")), ("having", spec.get("having"))) if v}
    base = {"datasource": f"{dataset_id}__table", "viz_type": viz, "adhoc_filters": adhoc}
    fmt = spec.get("format", "SMART_NUMBER")
    if spec["viz"] == "table":
        config = {}
        for column in spec["columns"]:
            if column in ("gia_ty", "gia_m2_tr", "median_nhom_tr", "price_ratio", "chenh_chi_phi_ty", "distance_diff_km", "feature_similarity"):
                config[column] = {"d3NumberFormat": ",.2~f"}
            elif column in ("re_hon_pct",):
                config[column] = {"d3NumberFormat": ",.0f"}
        params = {**base, "query_mode": "raw", "all_columns": spec["columns"], "column_config": config,
                  "order_by_cols": [json.dumps(o) for o in spec.get("order", [])], "row_limit": spec["row_limit"],
                  "server_pagination": False, "include_search": True, "page_length": 10}
        query = {"columns": spec["columns"], "orderby": spec.get("order", []), "row_limit": spec["row_limit"], "extras": extras}
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
                  "stack": spec.get("stack"), "show_value": spec.get("stack") is None,
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
    query["filters"] = []
    return params, query


def position_json(rows: list[list[Any]], specs: dict[int, dict], markdown: str) -> dict:
    layout: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
    }
    heights = {"kpi": 24, "table": 80, "bubble": 70, "funnel": 50, "bar": 60, "hbar": 55}
    for r_index, row in enumerate(rows):
        row_id = f"ROW-{r_index}"
        layout["GRID_ID"]["children"].append(row_id)
        layout[row_id] = {"type": "ROW", "id": row_id, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                          "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        width = 12 // len(row)
        for item in row:
            if item == "md":
                cid = f"MARKDOWN-{r_index}"
                layout[cid] = {"type": "MARKDOWN", "id": cid, "children": [], "parents": ["ROOT_ID", "GRID_ID", row_id],
                               "meta": {"width": 12, "height": 26, "code": markdown}}
            else:
                cid = f"CHART-{item}"
                spec = specs[item]
                layout[cid] = {"type": "CHART", "id": cid, "children": [], "parents": ["ROOT_ID", "GRID_ID", row_id],
                               "meta": {"chartId": item, "width": width, "height": spec.get("height", heights[spec["viz"]]), "sliceName": spec["name"]}}
            layout[row_id]["children"].append(cid)
    return layout


def native_filters(slug: str, dataset_ids: dict[str, int], chart_ids: list[int], dataset_of_chart: dict[int, str]) -> list[dict]:
    configs = []
    for label, column in NATIVE_FILTERS.get(slug, []):
        target = next(name for name in dataset_ids if any(dataset_of_chart[c] == name for c in chart_ids) and column in DATASETS[name])
        configs.append({
            "id": f"NATIVE_FILTER-{uuid.uuid5(uuid.NAMESPACE_URL, slug + column).hex[:10]}",
            "name": label, "filterType": "filter_select", "type": "NATIVE_FILTER",
            "targets": [{"datasetId": dataset_ids[target], "column": {"name": column}}],
            "controlValues": {"multiSelect": True, "enableEmptyFilter": False, "searchAllOptions": True, "inverseSelection": False},
            "defaultDataMask": {"filterState": {}, "extraFormData": {}},
            "cascadeParentIds": [], "scope": {"rootPath": ["ROOT_ID"], "excluded": []}, "chartsInScope": chart_ids,
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

    top = [r[0] for r in trino_rows(f"SELECT province_name FROM {G}.agg_market_overview WHERE province_name <> 'Không rõ' "
                                    "GROUP BY 1 ORDER BY SUM(n_listings) DESC LIMIT 6")]
    insights = compute_insights(trino_rows)

    chart_ids: dict[str, int] = {}
    specs_by_id: dict[int, dict] = {}
    checks = []
    for spec in CHARTS:
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
        metadata = {"native_filter_configuration": native_filters(dash["slug"], dataset_ids, ids, dataset_of_chart),
                    "color_scheme": "supersetColors", "refresh_frequency": 0, "cross_filters_enabled": False}
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
