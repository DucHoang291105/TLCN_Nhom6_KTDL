"""Observed listing lifecycle; absence from a crawl never means a completed sale."""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
CRAWL_SOURCES = ("batdongsan", "guland", "nhadatvui")


def calendar_from_audit(audit: dict) -> list[tuple[str, str]]:
    """Use audited batches, including batches with no accepted Silver rows."""
    if audit.get("status") != "PASS":
        raise ValueError("Lifecycle requires a PASS Bronze audit")
    rows = sorted({(r["source"], r["batch_id"]) for r in audit["batches"]
                   if r["branch"] == "snapshots" and r["status"] == "READABLE"
                   and r["source"] in CRAWL_SOURCES})
    for _, batch in rows:
        if len(batch) != 8 or datetime.strptime(batch, "%Y%m%d").strftime("%Y%m%d") != batch:
            raise ValueError(f"Invalid calendar batch: {batch}")
    if {s for s, _ in rows} != set(CRAWL_SOURCES):
        raise ValueError("All three crawl sources must have audited batches")
    return rows


def build_lifecycle_frame(observations, calendar, sources):
    from pyspark.sql import Window, functions as F
    ordered = Window.partitionBy("source_id").orderBy(F.col("snapshot_date").asc_nulls_last(),
        F.col("scraped_at").asc_nulls_last(), "batch_id", "record_hash")
    observed = observations.withColumn("_previous_price", F.lag("price").over(ordered))
    price_changed = ((F.col("price") > 0) & (F.col("_previous_price") > 0) &
                     (F.col("price") != F.col("_previous_price")))
    # One observation scan and aggregate replace the separate event branch/join.
    # NULL/invalid prices still break a sequence; never bridge missing prices.
    stats = observed.groupBy("source", "source_id").agg(
        F.min("snapshot_date").alias("first_seen_date"), F.max("snapshot_date").alias("last_seen_date"),
        F.min("batch_id").alias("first_seen_batch_id"), F.max("batch_id").alias("last_seen_batch_id"),
        F.collect_set("batch_id").alias("_observed_batches"),
        F.sum(F.when(price_changed, 1).otherwise(0)).alias("price_change_count"))
    # The set already contains exactly the distinct non-null batch IDs. Reuse
    # it instead of a second distinct aggregate/extra shuffle; retain BIGINT.
    stats = stats.withColumn("observation_count", F.size("_observed_batches").cast("long"))
    batches = calendar.groupBy("source").agg(F.sort_array(F.collect_set("batch_id"), asc=False).alias("_source_batches"))
    batches = batches.withColumn("_latest_two", F.slice("_source_batches", 1, 2))
    frame = stats.join(F.broadcast(batches), "source", "left").join(F.broadcast(sources.select("source", "source_key")), "source", "left")
    crawl = F.col("source").isin(*CRAWL_SOURCES)
    frame = (frame.withColumn("listed_days", F.datediff("last_seen_date", "first_seen_date"))
        .withColumn("source_batch_count", F.when(crawl, F.size("_source_batches")).otherwise(F.lit(None).cast("int")))
        .withColumn("latest_source_batch_id", F.element_at("_source_batches", 1))
        .withColumn("previous_source_batch_id", F.element_at("_source_batches", 2))
        .withColumn("missed_latest_batch_count", F.when(crawl, F.size("_latest_two") - F.size(F.array_intersect("_latest_two", "_observed_batches"))))
        .withColumn("is_disappeared", F.when(crawl & (F.col("source_batch_count") >= 2), F.col("missed_latest_batch_count") == 2).otherwise(F.lit(None).cast("boolean")))
        .withColumn("lifecycle_status", F.when(~crawl, "NOT_APPLICABLE").when(F.col("source_batch_count") < 2, "INSUFFICIENT_BATCHES")
            .when(F.array_contains(F.col("_observed_batches"), F.col("latest_source_batch_id")), "OBSERVED_LATEST")
            .when(F.col("is_disappeared"), "NOT_OBSERVED_2_BATCHES").otherwise("NOT_OBSERVED_LATEST"))
        .withColumn("lifecycle_built_at", F.current_timestamp()))
    return frame.select("source_id", "source_key", "source", "first_seen_date", "last_seen_date", "first_seen_batch_id",
        "last_seen_batch_id", "observation_count", "listed_days", "price_change_count", "source_batch_count",
        "latest_source_batch_id", "previous_source_batch_id", "missed_latest_batch_count", "is_disappeared", "lifecycle_status", "lifecycle_built_at")


def main():
    from src.common.spark_session import build_spark_session, ensure_silver_namespace, ensure_gold_namespace, write_iceberg_table
    from src.gold.gold_common import write_summary
    spark = build_spark_session("GoldListingLifecycle")
    silver, gold = ensure_silver_namespace(spark), ensure_gold_namespace(spark)
    audit = json.loads((PROJECT_ROOT / "docs/validation/bronze_state_audit.json").read_text(encoding="utf-8"))
    calendar_rows = calendar_from_audit(audit)
    calendar = spark.createDataFrame(calendar_rows, "source string, batch_id string")
    observations = spark.table(f"{silver}.listing_observation")
    unknown = observations.filter(observations.source.isin(*CRAWL_SOURCES)).select("source", "batch_id").distinct().join(calendar, ["source", "batch_id"], "left_anti")
    if unknown.limit(1).count():
        raise RuntimeError("Observation batches missing from Bronze audit; rerun audit")
    frame = build_lifecycle_frame(observations, calendar, spark.table(f"{gold}.dim_source"))
    table = f"{gold}.fact_listing_lifecycle"
    write_iceberg_table(frame, table)
    written = spark.table(table)
    counts = written.groupBy("source", "lifecycle_status").count().orderBy("source", "lifecycle_status").collect()
    summary = {"generated_at": datetime.now(timezone.utc).isoformat(), "output_table": table,
        "input_table": f"{silver}.listing_observation", "rows": sum(r["count"] for r in counts), "grain": ["source_id"],
        "source_batch_calendar": {s: [b for src, b in calendar_rows if src == s] for s in CRAWL_SOURCES},
        "status_counts": [r.asDict() for r in counts],
        "sample": [json.loads(r) for r in written.orderBy("source", "source_id").limit(20).toJSON().collect()],
        "definitions": {"listed_days": "last_seen_date - first_seen_date; observed span, not time to sale",
            "is_disappeared": "Absent from both latest audited batches of its own source; not sold",
            "historical": "NOT_APPLICABLE and NULL disappearance fields",
            "price_changes": "Adjacent valid positive observed prices differ; NULL does not bridge a gap"}, "status": "PASS"}
    write_summary("gold_listing_lifecycle_summary.json", summary)
    spark.stop()


if __name__ == "__main__":
    main()
