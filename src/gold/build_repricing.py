"""Observed price changes from ``listing_history`` (back-check for BQ2).

``fact_listing_price_change``: one row per history version whose price differs
from the previous version of the same sale listing. Only listings present in
``fact_listing`` are kept. The summary cross-tabulates repricing with the
current BQ2 price position; this is an association, not a forecast.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.gold.gold_common import write_summary


def main() -> None:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from src.common.spark_session import build_spark_session, ensure_gold_namespace, ensure_silver_namespace, write_iceberg_table

    spark = build_spark_session("GoldRepricing")
    silver = ensure_silver_namespace(spark)
    gold = ensure_gold_namespace(spark)
    started_at = datetime.now(timezone.utc)

    fact = spark.table(f"{gold}.fact_listing").select("source_id", "source_key", "location_key", "property_category_key", "is_dup_representative")
    history = spark.table(f"{silver}.listing_history").select("source_id", "version_number", "valid_from", "price")
    versions = Window.partitionBy("source_id").orderBy("version_number")
    changes = (
        history.join(fact.select("source_id"), "source_id")
        .withColumn("previous_price", F.lag("price").over(versions))
        .filter((F.col("previous_price") > 0) & (F.col("price") > 0) & (F.col("price") != F.col("previous_price")))
        .join(fact, "source_id")
        .select(
            "source_id", "version_number", "source_key", "location_key", "property_category_key", "is_dup_representative",
            F.col("valid_from").alias("changed_at"),
            F.date_format("valid_from", "yyyyMMdd").cast("int").alias("date_key"),
            "previous_price", F.col("price").alias("new_price"),
            (F.col("price") - F.col("previous_price")).alias("price_change"),
            F.round(F.col("price") / F.col("previous_price") - 1, 6).alias("price_change_pct"),
            F.when(F.col("price") < F.col("previous_price"), "giam").otherwise("tang").alias("direction"),
        )
    )
    write_iceberg_table(changes, f"{gold}.fact_listing_price_change")
    written = spark.table(f"{gold}.fact_listing_price_change")

    per_listing = written.filter("is_dup_representative").groupBy("source_id").agg(
        F.max(F.when(F.col("direction") == "giam", 1).otherwise(0)).alias("has_price_drop")
    )
    assessment = spark.table(f"{gold}.fact_listing_price_assessment").select("source_id", "price_position")
    crosstab = (
        assessment.join(per_listing, "source_id", "left").fillna(0, subset=["has_price_drop"])
        .groupBy("price_position").agg(F.count("*").alias("listings"), F.sum("has_price_drop").alias("with_price_drop"))
        .collect()
    )
    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "price_change_events": written.count(),
        "listings_with_change": written.select("source_id").distinct().count(),
        "direction_counts": {str(r["direction"]): int(r["count"]) for r in written.groupBy("direction").count().collect()},
        "median_price_change_pct": {
            str(r["direction"]): r["median"] for r in written.groupBy("direction").agg(F.expr("percentile(price_change_pct, 0.5)").alias("median")).collect()
        },
        "price_drop_by_current_position": {
            str(r["price_position"]): {
                "listings": int(r["listings"]),
                "with_price_drop": int(r["with_price_drop"]),
                "share": round(int(r["with_price_drop"]) / int(r["listings"]), 6) if r["listings"] else 0.0,
            }
            for r in crosstab
        },
        "status": "PASS",
    }
    write_summary("gold_repricing_summary.json", summary)
    spark.stop()


if __name__ == "__main__":
    main()
