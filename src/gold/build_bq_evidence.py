"""Evidence tables behind the BQ1-BQ3 findings shown on the dashboards.

Each finding on a dashboard is backed by a chart, real listings and a check.
The heavier ones are computed here, once per pipeline run, instead of inside
Superset virtual datasets on every page load (a single 2 GB Trino node could
not serve ten such charts at once).

- ``ev_budget_curve``   BQ1 – listing area by budget band and distance zone
                        of the district (centre < 5 km, 5-10 km, farther).
- ``ev_budget_pairs``   BQ1 – one real pair per district and budget: listing B
                        is >= 10% cheaper, 20-100% larger, has as many rooms
                        and the same frontage / car access as listing A; both
                        priced per m2 within the district's P25-P75.
- ``ev_reprice``        BQ2 – position of each listing at its FIRST observed
                        price (the current price is already cut) and whether it
                        later made a genuine cut (1-30%; larger drops are
                        mostly typo fixes such as 114 tỷ -> 11,4 tỷ).
- ``ev_reprice_examples`` BQ2 – listings above P75 when first seen that cut.
- ``ev_flag_check``     BQ2 – raw check of the model: median price/m2 with vs
                        without each title flag inside the same district and
                        area band (>= 5 listings each side), next to the model.
- ``ev_calibration``    BQ2 – test-split listings with the decile of their
                        estimate inside their province and category.
"""

from __future__ import annotations

import math
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.gold.gold_common import FLAG_COLUMNS, write_summary

GENUINE_CUT = "c.direction = 'giam' AND c.new_price BETWEEN 0.70 * c.previous_price AND 0.99 * c.previous_price"
FLAG_LABELS = {"title_has_frontage": "Mặt tiền", "title_has_elevator": "Thang máy", "title_has_car_access": "Ô tô tiếp cận",
               "title_has_furnished": "Nội thất", "title_has_legal": "Pháp lý (sổ)"}
POSITIONS = ("1. Dưới P25 nhóm", "2. Trong P25–P75", "3. Trên P75 nhóm")


def evidence_sql(gold: str, silver: str) -> dict[str, str]:
    labels = f"""JOIN {gold}.dim_location l ON l.location_key = {{t}}.location_key
        JOIN {gold}.dim_property_category k ON k.property_category_key = {{t}}.property_category_key"""
    flag_parts = " UNION ALL ".join(f"""
        SELECT g.location_key, g.property_category_key, g.area_band_key, g.province_name, g.category_label, '{label}' AS dac_diem, g.n_co, g.n_khong,
               (g.gia_co / g.gia_khong - 1) * 100 AS chenh_tho_pct, CASE WHEN g.gia_co > g.gia_khong THEN 1D ELSE 0D END AS co_dat_hon,
               m.premium_pct * 100 AS he_so_mo_hinh_pct
        FROM (SELECT l.province_name, k.category_label, f.property_category_key, f.location_key, f.area_band_key,
                     percentile(CASE WHEN f.{flag} THEN f.price_per_m2 END, 0.5) AS gia_co,
                     percentile(CASE WHEN NOT f.{flag} THEN f.price_per_m2 END, 0.5) AS gia_khong,
                     COUNT_IF(f.{flag}) AS n_co, COUNT_IF(NOT f.{flag}) AS n_khong
              FROM {gold}.fact_listing f {labels.format(t="f")}
              WHERE f.is_dup_representative AND f.price_per_m2 > 0 AND l.district_name IS NOT NULL AND l.district_name <> 'Unknown'
              GROUP BY 1, 2, 3, 4, 5) g
        LEFT JOIN {gold}.agg_price_model_coefficient m ON m.property_category_key = g.property_category_key
             AND m.factor = 'flag' AND m.level = '{flag}'
        WHERE g.n_co >= 5 AND g.n_khong >= 5""" for flag, label in FLAG_LABELS.items())
    return {
        "ev_budget_curve": f"""
            WITH dist AS (SELECT location_key, percentile(distance_to_center_km, 0.5) AS km FROM {gold}.fact_listing
                          WHERE is_dup_representative AND distance_to_center_km IS NOT NULL GROUP BY 1 HAVING COUNT(*) >= 10)
            SELECT f.source_id, l.province_name, k.category_label, f.area,
                   CONCAT(CAST(pb.band_key AS STRING), '. ', pb.band_label) AS muc_ngan_sach,
                   CASE WHEN dist.km < 5 THEN '1. Quận trung tâm (< 5 km)' WHEN dist.km < 10 THEN '2. Quận cách 5–10 km'
                        ELSE '3. Quận xa hơn 10 km' END AS vung
            FROM {gold}.fact_budget_pareto p JOIN {gold}.fact_listing f ON f.source_id = p.source_id
            JOIN dist ON dist.location_key = p.location_key {labels.format(t="p")}
            JOIN {gold}.dim_price_band pb ON pb.band_key = p.price_band_key
            WHERE f.area > 0""",
        "ev_budget_pairs": f"""
            WITH x AS (
                SELECT f.source_id, p.location_key, p.price_band_key, p.property_category_key, f.price, f.area, f.rooms,
                       f.price_per_m2 AS ppm2, f.title_has_frontage AS fr, f.title_has_car_access AS car, f.title, f.ad_url
                FROM {gold}.fact_budget_pareto p JOIN {gold}.fact_listing f ON f.source_id = p.source_id
                WHERE f.area BETWEEN 30 AND 200 AND f.rooms > 0 AND f.ad_url IS NOT NULL),
            q AS (SELECT location_key, price_band_key, property_category_key,
                         percentile(ppm2, 0.25) AS p25, percentile(ppm2, 0.75) AS p75
                  FROM x GROUP BY 1, 2, 3 HAVING COUNT(*) >= 20),
            t AS (SELECT x.* FROM x JOIN q ON q.location_key = x.location_key AND q.price_band_key = x.price_band_key
                       AND q.property_category_key = x.property_category_key WHERE x.ppm2 BETWEEN q.p25 AND q.p75),
            pairs AS (
                SELECT a.location_key, a.price_band_key, a.property_category_key,
                       a.title AS tin_a, a.price / 1e9 AS gia_a_ty, a.area AS dt_a, a.rooms AS phong_a, a.ad_url AS link_a,
                       b.title AS tin_b, b.price / 1e9 AS gia_b_ty, b.area AS dt_b, b.rooms AS phong_b, b.ad_url AS link_b,
                       row_number() OVER (PARTITION BY a.location_key, a.price_band_key, a.property_category_key
                                          ORDER BY (b.area / a.area) * (a.price / b.price) DESC, b.source_id, a.source_id) AS rk
                FROM t a JOIN t b ON a.location_key = b.location_key AND a.price_band_key = b.price_band_key
                     AND a.property_category_key = b.property_category_key AND a.source_id <> b.source_id
                WHERE b.price <= 0.9 * a.price AND b.area BETWEEN 1.2 * a.area AND 2 * a.area AND b.rooms >= a.rooms
                      AND (b.fr OR NOT a.fr) AND (b.car OR NOT a.car))
            SELECT pairs.location_key, pairs.price_band_key, pairs.property_category_key, l.province_name, l.district_name, k.category_label, pb.band_label AS ngan_sach,
                   tin_a, gia_a_ty, dt_a, phong_a, link_a, tin_b, gia_b_ty, dt_b, phong_b, link_b
            FROM pairs {labels.format(t="pairs")} JOIN {gold}.dim_price_band pb ON pb.band_key = pairs.price_band_key
            WHERE rk = 1""",
        "ev_reprice": f"""
            WITH ch AS (SELECT source_id, MIN_BY(previous_price, version_number) AS first_price FROM {gold}.fact_listing_price_change GROUP BY 1),
            cuts AS (SELECT DISTINCT c.source_id FROM {gold}.fact_listing_price_change c WHERE {GENUINE_CUT}),
            base AS (
                SELECT a.source_id, a.location_key, a.property_category_key, COALESCE(ch.first_price, a.price) / a.area AS first_ppm2,
                       a.peer_p25_price_per_m2 AS p25, a.peer_p75_price_per_m2 AS p75,
                       CASE WHEN cuts.source_id IS NULL THEN 0D ELSE 1D END AS da_ha_gia
                FROM {gold}.fact_listing_price_assessment a
                JOIN {gold}.fact_listing_lifecycle lc ON lc.source_id = a.source_id
                LEFT JOIN ch ON ch.source_id = a.source_id LEFT JOIN cuts ON cuts.source_id = a.source_id
                WHERE lc.observation_count >= 2 AND a.peer_p75_price_per_m2 IS NOT NULL AND a.area > 0)
            SELECT base.source_id, l.province_name, NULLIF(l.district_name, 'Unknown') AS district_name, k.category_label, da_ha_gia,
                   CASE WHEN first_ppm2 > p75 THEN '{POSITIONS[2]}' WHEN first_ppm2 < p25 THEN '{POSITIONS[0]}' ELSE '{POSITIONS[1]}' END AS vi_tri_luc_dang
            FROM base {labels.format(t="base")}""",
        "ev_reprice_examples": f"""
            WITH ch AS (SELECT c.source_id, MIN_BY(c.previous_price, c.version_number) AS p0, MAX_BY(c.new_price, c.version_number) AS p1,
                               MAX(c.changed_at) AS ngay_ha, BOOL_AND(c.new_price >= 0.70 * c.previous_price) AS hop_le
                        FROM {gold}.fact_listing_price_change c GROUP BY 1)
            SELECT a.source_id, l.province_name, NULLIF(l.district_name, 'Unknown') AS district_name, k.category_label, f.title,
                   ch.p0 / 1e9 AS gia_luc_dang_ty, ch.p1 / 1e9 AS gia_hien_tai_ty, (ch.p1 / ch.p0 - 1) * 100 AS thay_doi_pct,
                   (ch.p0 / a.area / a.peer_p75_price_per_m2 - 1) * 100 AS vuot_p75_luc_dang_pct, a.peer_n,
                   CAST(CAST(ch.ngay_ha AS DATE) AS STRING) AS ngay_ha_gia, f.ad_url
            FROM ch JOIN {gold}.fact_listing_price_assessment a ON a.source_id = ch.source_id JOIN {gold}.fact_listing f ON f.source_id = a.source_id
            {labels.format(t="a")}
            WHERE ch.hop_le AND ch.p1 BETWEEN 0.70 * ch.p0 AND 0.99 * ch.p0 AND ch.p0 / a.area > a.peer_p75_price_per_m2 AND a.peer_n >= 10""",
        "ev_flag_check": flag_parts,
        "ev_calibration": f"""
            SELECT source_id, province_name, category_label, CONCAT('D', lpad(CAST(dec AS STRING), 2, '0')) AS nhom_gia_uoc_tinh, gia_that_tr, uoc_tinh_tr
            FROM (SELECT m.source_id, l.province_name, k.category_label, m.price_per_m2 / 1e6 AS gia_that_tr, m.predicted_price_per_m2 / 1e6 AS uoc_tinh_tr,
                         ntile(10) OVER (PARTITION BY l.province_name, k.category_label ORDER BY m.predicted_price_per_m2, m.source_id) AS dec
                  FROM {gold}.fact_listing_price_model m JOIN {gold}.fact_listing_price_assessment a ON a.source_id = m.source_id
                  {labels.format(t="a")}
                  WHERE m.split = 'test' AND NOT m.is_outlier_trimmed) t""",
    }


def two_proportion_z(events_a: float, n_a: float, events_b: float, n_b: float) -> tuple[float | None, float | None]:
    """(z, two-sided p) for the difference of two proportions."""

    if n_a <= 0 or n_b <= 0:
        return None, None
    p_a, p_b, pooled = events_a / n_a, events_b / n_b, (events_a + events_b) / (n_a + n_b)
    if pooled in (0, 1):
        return 0.0, 1.0
    z = (p_a - p_b) / math.sqrt(pooled * (1 - pooled) * (1 / n_a + 1 / n_b))
    return z, math.erfc(abs(z) / math.sqrt(2))


def main() -> None:
    from pyspark.sql import functions as F

    from src.common.spark_session import build_spark_session, ensure_gold_namespace, ensure_silver_namespace, write_iceberg_table

    spark = build_spark_session("GoldBQEvidence")
    gold = ensure_gold_namespace(spark)
    silver = ensure_silver_namespace(spark)
    started_at = datetime.now(timezone.utc)
    assert set(FLAG_LABELS) == set(FLAG_COLUMNS)

    counts = {}
    for table, sql in evidence_sql(gold, silver).items():
        write_iceberg_table(spark.sql(sql), f"{gold}.{table}")
        counts[table] = spark.table(f"{gold}.{table}").count()

    reprice = {r["vi_tri_luc_dang"]: (r["n"], r["cuts"]) for r in spark.table(f"{gold}.ev_reprice")
               .groupBy("vi_tri_luc_dang").agg(F.count("*").alias("n"), F.sum("da_ha_gia").alias("cuts")).collect()}
    top = reprice.get(POSITIONS[2], (0, 0))
    rest = (sum(v[0] for k, v in reprice.items() if k != POSITIONS[2]), sum(v[1] for k, v in reprice.items() if k != POSITIONS[2]))
    z, p = two_proportion_z(top[1], top[0], rest[1], rest[0]) if top[0] and rest[0] else (None, None)
    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "table_rows": counts,
        "genuine_price_cut": "1-30% below the previous price",
        "comparison_scope": "First observed price divided by current area, compared with current peer quartiles. Retrospective descriptive comparison; not a point-in-time benchmark or a causal/predictive test. Source and exposure duration are not adjusted.",
        "h1_price_cut_by_first_position": {
            k: {"listings": int(v[0]), "with_cut": int(v[1]), "rate": round(v[1] / v[0], 6)} for k, v in sorted(reprice.items())
        },
        "h1_above_p75_vs_rest": {"rate_ratio": round((top[1] / top[0]) / (rest[1] / rest[0]), 4) if top[0] and rest[1] else None,
                                 "z": round(z, 3) if z is not None else None, "p_two_sided": p},
        "status": "PASS",
    }
    write_summary("gold_bq_evidence_summary.json", summary)
    spark.stop()


if __name__ == "__main__":
    main()
