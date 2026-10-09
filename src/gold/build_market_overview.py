"""Market overview: asking-price level per province and model_category.

``agg_market_overview``: grain (province, property_category_key) over
representative sale listings; listings without a province fall under the
dim_location unknown row (``location_key = -1``, province "Không rõ").
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.gold.gold_common import FLAG_COLUMNS, quartiles, representatives, share, write_summary


def main() -> None:
    from pyspark.sql import functions as F

    from src.common.spark_session import build_spark_session, ensure_gold_namespace, write_iceberg_table

    spark = build_spark_session("GoldMarketOverview")
    gold = ensure_gold_namespace(spark)
    started_at = datetime.now(timezone.utc)

    provinces = spark.table(f"{gold}.dim_location").select("location_key", "province_name")
    listings = representatives(spark, gold).join(provinces, "location_key")
    overview = listings.groupBy("province_name", "property_category_key").agg(
        F.count("*").alias("n_listings"),
        F.sum(F.col("price_known").cast("int")).alias("n_with_price"),
        quartiles("price_per_m2").alias("_q"),
        F.expr("percentile(price, 0.5)").alias("median_price"),
        F.expr("percentile(area, 0.5)").alias("median_area"),
        F.expr("percentile(rooms, 0.5)").alias("median_rooms"),
        *[share(c).alias(c.replace("title_has_", "share_")) for c in FLAG_COLUMNS],
    ).select(
        "province_name", "property_category_key", "n_listings", "n_with_price",
        F.col("_q")[0].alias("p25_price_per_m2"), F.col("_q")[1].alias("median_price_per_m2"), F.col("_q")[2].alias("p75_price_per_m2"),
        "median_price", "median_area", "median_rooms", *[c.replace("title_has_", "share_") for c in FLAG_COLUMNS],
    )
    write_iceberg_table(overview, f"{gold}.agg_market_overview")
    written = spark.table(f"{gold}.agg_market_overview")

    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "rows": written.count(),
        "provinces": written.select("province_name").distinct().count(),
        "listings": int(written.agg(F.sum("n_listings")).first()[0] or 0),
        "top_provinces": {
            str(r["province_name"]): int(r["n"])
            for r in written.groupBy("province_name").agg(F.sum("n_listings").alias("n")).orderBy(F.desc("n")).limit(5).collect()
        },
        "status": "PASS",
    }
    write_summary("gold_market_overview_summary.json", summary)
    spark.stop()


if __name__ == "__main__":
    main()
