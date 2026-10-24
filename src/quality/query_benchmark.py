"""Benchmark: answer each BQ from Gold vs. compute it from Silver, via Trino.

For every question the Gold query reads the pre-aggregated table; the Silver
query computes a comparable aggregate on the fly from silver_listings_current_27
+ listing_feature + listing_location. Silver versions approximate the Gold
logic (no cross-source dedup, no Pareto, approximate percentiles), so they are
a *lower bound* on the real cost of skipping Gold. Each query runs once to warm
up, then RUNS times; the median wall-clock time is reported.

Requires Trino (docker compose --profile query up -d trino) and `pip install trino`.
Run: python -m src.quality.query_benchmark
"""

from __future__ import annotations

import json
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RUNS = 5
PROVINCE = "Hồ Chí Minh"

SILVER_BASE = """
    FROM iceberg.silver.silver_listings_current_27 c
    JOIN iceberg.silver.listing_feature f ON f.source_id = c.source_id
    JOIN iceberg.silver.listing_location l ON l.source_id = c.source_id
    WHERE c.is_rent = false AND f.dq_status <> 'REJECT'
"""

QUERIES = {
    "market_overview_province": {
        "question": "Mặt bằng giá theo loại hình tại TP.HCM",
        "gold": f"""SELECT c.category_label, o.n_listings, o.median_price_per_m2
            FROM iceberg.gold.agg_market_overview o
            JOIN iceberg.gold.dim_property_category c ON c.property_category_key = o.property_category_key
            WHERE o.province_name = '{PROVINCE}'""",
        "silver": f"""SELECT f.model_category, count(*) AS n_listings,
                approx_percentile(c.price_per_m2, 0.5) AS median_price_per_m2
            {SILVER_BASE} AND l.province_name_model = '{PROVINCE}'
            GROUP BY f.model_category""",
    },
    "bq1_budget_tradeoff": {
        "question": "BQ1 – Nhà phố 3–5 tỷ tại TP.HCM: diện tích, giá/m² theo quận",
        "gold": f"""SELECT l.district_name, t.n_listings, t.median_area, t.median_price_per_m2, t.share_frontage
            FROM iceberg.gold.agg_budget_tradeoff t
            JOIN iceberg.gold.dim_location l ON l.location_key = t.location_key
            JOIN iceberg.gold.dim_property_category c ON c.property_category_key = t.property_category_key
            WHERE l.province_name = '{PROVINCE}' AND c.model_category = 'nha_pho' AND t.price_band_key = 4""",
        "silver": f"""SELECT l.district_name_model, count(*) AS n_listings,
                approx_percentile(c.area, 0.5) AS median_area, approx_percentile(c.price_per_m2, 0.5) AS median_price_per_m2,
                avg(CAST(f.title_has_frontage AS double)) AS share_frontage
            {SILVER_BASE} AND l.province_name_model = '{PROVINCE}' AND f.model_category = 'nha_pho'
                AND c.price >= 3e9 AND c.price < 5e9
            GROUP BY l.district_name_model""",
    },
    "bq2_listing_position": {
        "question": "BQ2 – Vị trí giá/m² của từng căn hộ TP.HCM trong nhóm (quận, band diện tích)",
        "gold": f"""SELECT a.source_id, a.price_per_m2, a.peer_median_price_per_m2, a.price_position
            FROM iceberg.gold.fact_listing_price_assessment a
            JOIN iceberg.gold.dim_location l ON l.location_key = a.location_key
            JOIN iceberg.gold.dim_property_category c ON c.property_category_key = a.property_category_key
            WHERE l.province_name = '{PROVINCE}' AND c.model_category = 'can_ho'""",
        "silver": f"""WITH base AS (
                SELECT c.source_id, c.price_per_m2, l.district_name_model AS district,
                    CASE WHEN c.area < 30 THEN 1 WHEN c.area < 50 THEN 2 WHEN c.area < 80 THEN 3
                         WHEN c.area < 120 THEN 4 WHEN c.area < 200 THEN 5 ELSE 6 END AS area_band
                {SILVER_BASE} AND l.province_name_model = '{PROVINCE}' AND f.model_category = 'can_ho' AND c.price_per_m2 > 0),
            peer AS (
                SELECT district, area_band, approx_percentile(price_per_m2, 0.25) p25,
                    approx_percentile(price_per_m2, 0.5) p50, approx_percentile(price_per_m2, 0.75) p75
                FROM base GROUP BY district, area_band HAVING count(*) >= 5)
            SELECT b.source_id, b.price_per_m2, p.p50,
                CASE WHEN b.price_per_m2 < p.p25 THEN 'duoi_p25' WHEN b.price_per_m2 > p.p75 THEN 'tren_p75' ELSE 'p25_p75' END
            FROM base b JOIN peer p ON p.district = b.district AND p.area_band = b.area_band""",
    },
    "bq3_area_substitution": {
        "question": "BQ3 – Quận thay thế rẻ hơn cho nhà phố 50–80 m² tại TP.HCM",
        "gold": f"""SELECT o.district_name AS origin, a.district_name AS alternative, s.price_gap_pct
            FROM iceberg.gold.agg_area_substitution s
            JOIN iceberg.gold.dim_location o ON o.location_key = s.origin_location_key
            JOIN iceberg.gold.dim_location a ON a.location_key = s.alternative_location_key
            JOIN iceberg.gold.dim_property_category c ON c.property_category_key = s.property_category_key
            WHERE o.province_name = '{PROVINCE}' AND c.model_category = 'nha_pho' AND s.area_band_key = 3""",
        "silver": f"""WITH g AS (
                SELECT l.district_name_model AS district, approx_percentile(c.price_per_m2, 0.5) AS p50,
                    avg(CAST(f.title_has_frontage AS double)) AS frontage, count(*) AS n
                {SILVER_BASE} AND l.province_name_model = '{PROVINCE}' AND f.model_category = 'nha_pho'
                    AND c.area >= 50 AND c.area < 80 AND c.price_per_m2 > 0
                GROUP BY l.district_name_model HAVING count(*) >= 5)
            SELECT o.district, a.district, 1 - a.p50 / o.p50
            FROM g o JOIN g a ON a.district <> o.district AND a.p50 < o.p50 AND abs(a.frontage - o.frontage) <= 0.3""",
    },
}


def timed(cursor, sql: str) -> tuple[float, int]:
    start = time.perf_counter()
    cursor.execute(sql)
    rows = cursor.fetchall()
    return time.perf_counter() - start, len(rows)


def main() -> None:
    import trino

    connection = trino.dbapi.connect(host="localhost", port=8090, user="benchmark", catalog="iceberg")
    cursor = connection.cursor()
    results = {}
    for name, spec in QUERIES.items():
        entry = {"question": spec["question"]}
        for layer in ("gold", "silver"):
            timed(cursor, spec[layer])  # warm-up
            runs = [timed(cursor, spec[layer]) for _ in range(RUNS)]
            entry[layer] = {
                "median_seconds": round(statistics.median(t for t, _ in runs), 3),
                "min_seconds": round(min(t for t, _ in runs), 3),
                "rows": runs[0][1],
            }
        entry["speedup"] = round(entry["silver"]["median_seconds"] / entry["gold"]["median_seconds"], 2)
        results[name] = entry
        print(f"{name:28s} gold={entry['gold']['median_seconds']}s silver={entry['silver']['median_seconds']}s x{entry['speedup']}")
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "engine": "Trino (docker, single node)",
        "runs_per_query": RUNS,
        "method": "1 warm-up + median of RUNS wall-clock runs incl. fetching all rows; Silver queries approximate Gold logic (lower bound on its cost)",
        "queries": results,
        "sql": {name: {layer: " ".join(spec[layer].split()) for layer in ("gold", "silver")} for name, spec in QUERIES.items()},
    }
    out = PROJECT_ROOT / "docs/validation/query_benchmark.json"
    out.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {out}")


if __name__ == "__main__":
    main()
