"""Content fingerprints of every Silver and Gold table, for rebuild comparison.

For each table: row count, number of distinct grain keys and an
order-independent hash of all row contents (columns that only record when a
build ran, ``*_built_at``, are excluded). Two rebuilds from the same Bronze
input must produce identical fingerprints, which checks content and keys, not
just row counts.

Usage (Spark): .\\scripts\\run_spark.ps1 "src\\common\\table_fingerprints.py" <out.json>
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

GRAINS = {
    "silver.listing_observation": ["source_id", "batch_id"],
    "silver.listing_dq_quarantine": ["bronze_path", "source_id", "batch_id", "record_hash"],
    "silver.crawl_current_27": ["source_id"],
    "silver.historical_current_27": ["source_id"],
    "silver.silver_listings_current_27": ["source_id"],
    "silver.listing_history": ["source_id", "version_number"],
    "silver.listing_location": ["source_id"],
    "silver.listing_feature": ["source_id"],
    "gold.dim_source": ["source_key"],
    "gold.dim_date": ["date_key"],
    "gold.dim_location": ["location_key"],
    "gold.dim_property_category": ["property_category_key"],
    "gold.dim_price_band": ["band_key"],
    "gold.dim_area_band": ["band_key"],
    "gold.dim_unit_price_band": ["band_key"],
    "gold.dim_room_band": ["band_key"],
    "gold.dim_dq_status": ["dq_status_key"],
    "gold.fact_listing": ["source_id"],
    "gold.agg_peer_group_benchmark": ["peer_group_id"],
    "gold.fact_listing_price_assessment": ["source_id"],
    "gold.agg_budget_tradeoff": ["price_band_key", "location_key", "property_category_key"],
    "gold.fact_budget_pareto": ["source_id"],
    "gold.agg_area_substitution": ["origin_location_key", "alternative_location_key", "property_category_key", "area_band_key"],
    "gold.agg_dq_kpi": ["source_key"],
    "gold.fact_listing_price_change": ["source_id", "version_number"],
    "gold.agg_market_overview": ["province_name", "property_category_key"],
}


def is_build_time(column: str) -> bool:
    return column.endswith("built_at")


def main(output: str) -> None:
    from pyspark.sql import functions as F

    from src.common.spark_session import CATALOG_NAME, build_spark_session

    spark = build_spark_session("TableFingerprints")
    result = {}
    for name, grain in GRAINS.items():
        table = spark.table(f"{CATALOG_NAME}.{name}")
        columns = sorted(c for c in table.columns if not is_build_time(c))
        row_hash = F.sha2(F.to_json(F.struct(*[F.col(c) for c in columns])), 256)
        stats = table.agg(
            F.count("*").alias("rows"),
            F.countDistinct(*[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in grain]).alias("keys"),
            F.sum(F.conv(F.substring(row_hash, 1, 15), 16, 10).cast("decimal(38,0)")).cast("string").alias("content_hash"),
        ).first()
        result[name] = {
            "grain": grain,
            "rows": int(stats["rows"]),
            "distinct_keys": int(stats["keys"]),
            "content_hash": stats["content_hash"],
            "excluded_columns": [c for c in table.columns if is_build_time(c)],
        }
    output_path = Path(output) if Path(output).is_absolute() else PROJECT_ROOT / output
    payload = {"generated_at": datetime.now(timezone.utc).isoformat(), "tables": result}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {output_path}")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else str(PROJECT_ROOT / "outputs/determinism/fingerprints.json"))
