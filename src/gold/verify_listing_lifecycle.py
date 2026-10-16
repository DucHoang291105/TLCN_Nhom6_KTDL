"""Independent reconciliation of published lifecycle fields against Silver observations."""
import json
from pathlib import Path


def verify_lifecycle(spark, silver, gold):
    from pyspark.sql import Window, functions as F
    from src.gold.build_listing_lifecycle import CRAWL_SOURCES, calendar_from_audit
    life = spark.table(f"{gold}.fact_listing_lifecycle")
    obs = spark.table(f"{silver}.listing_observation")
    root = Path(__file__).resolve().parents[2]
    audit = json.loads((root / "docs/validation/bronze_state_audit.json").read_text(encoding="utf-8"))
    calendar = calendar_from_audit(audit)
    expected = obs.groupBy("source_id", "source").agg(
        F.min("snapshot_date").alias("first_seen_date"), F.max("snapshot_date").alias("last_seen_date"),
        F.min("batch_id").alias("first_seen_batch_id"), F.max("batch_id").alias("last_seen_batch_id"),
        F.countDistinct("batch_id").alias("observation_count"))
    columns = expected.columns
    actual = life.select(*columns)
    checks = {"lifecycle_observed_metrics_recomputed": actual.exceptAll(expected).count() == 0 and expected.exceptAll(actual).count() == 0,
              "lifecycle_ids_cover_all_observed_listings": life.select("source_id").exceptAll(obs.select("source_id").distinct()).count() == 0
                  and obs.select("source_id").distinct().exceptAll(life.select("source_id")).count() == 0,
              "lifecycle_non_null_ids": life.filter("source_id IS NULL OR source_key IS NULL").count() == 0,
              "lifecycle_dates_and_counts_valid": life.filter("first_seen_date IS NULL OR last_seen_date IS NULL OR observation_count IS NULL OR listed_days IS NULL OR price_change_count IS NULL OR first_seen_date>last_seen_date OR observation_count<1 OR listed_days<0 OR price_change_count<0 OR listed_days<>datediff(last_seen_date,first_seen_date)").count() == 0}
    w = Window.partitionBy("source_id").orderBy(F.col("snapshot_date").asc_nulls_last(), F.col("scraped_at").asc_nulls_last(), "batch_id", "record_hash")
    changes = obs.withColumn("prev", F.lag("price").over(w)).filter("prev>0 AND price>0 AND prev<>price").groupBy("source_id").count()
    mismatch = life.join(changes, "source_id", "left").filter(~F.col("price_change_count").eqNullSafe(F.coalesce(F.col("count"), F.lit(0)))).count()
    checks["lifecycle_price_changes_recomputed"] = mismatch == 0
    sale_events = life.join(spark.table(f"{gold}.fact_listing").select("source_id"), "source_id").agg(F.sum("price_change_count")).first()[0] or 0
    checks["lifecycle_sale_changes_match_repricing_events"] = sale_events == spark.table(f"{gold}.fact_listing_price_change").count()
    checks["lifecycle_historical_not_evaluated"] = life.filter(~F.col("source").isin(*CRAWL_SOURCES)).filter(
        "lifecycle_status IS NULL OR lifecycle_status<>'NOT_APPLICABLE' OR is_disappeared IS NOT NULL OR missed_latest_batch_count IS NOT NULL OR latest_source_batch_id IS NOT NULL OR source_batch_count IS NOT NULL").count() == 0
    mismatches = 0
    calendar_mismatches = 0
    for source in CRAWL_SOURCES:
        batches = sorted({b for s, b in calendar if s == source}, reverse=True)
        latest = batches[:2]
        observed = obs.filter((F.col("source") == source) & F.col("batch_id").isin(*latest)).groupBy("source_id").agg(
            F.countDistinct("batch_id").alias("seen"), F.max(F.when(F.col("batch_id") == latest[0], 1).otherwise(0)).alias("seen_latest"))
        joined = life.filter(F.col("source") == source).join(observed, "source_id", "left").fillna(0, subset=["seen", "seen_latest"])
        expected_disappeared = (F.col("seen") == 0) if len(latest) == 2 else F.lit(None).cast("boolean")
        status = (F.lit("INSUFFICIENT_BATCHES") if len(latest) < 2 else F.when(F.col("seen_latest") == 1, "OBSERVED_LATEST")
                  .when(F.col("seen") == 0, "NOT_OBSERVED_2_BATCHES").otherwise("NOT_OBSERVED_LATEST"))
        mismatches += joined.filter(~F.col("is_disappeared").eqNullSafe(expected_disappeared)
            | ~F.col("missed_latest_batch_count").eqNullSafe(F.lit(len(latest)) - F.col("seen"))
            | ~F.col("lifecycle_status").eqNullSafe(status)).count()
        calendar_mismatches += joined.filter(~F.col("latest_source_batch_id").eqNullSafe(F.lit(latest[0]))
            | ~F.col("previous_source_batch_id").eqNullSafe(F.lit(latest[1] if len(latest) > 1 else None))
            | ~F.col("source_batch_count").eqNullSafe(F.lit(len(batches)))).count()
    checks["lifecycle_disappearance_and_status_recomputed"] = mismatches == 0
    checks["lifecycle_per_source_batch_calendar_correct"] = calendar_mismatches == 0
    return checks, {"lifecycle_status_mismatches": mismatches, "lifecycle_price_count_mismatches": mismatch,
                    "lifecycle_calendar_mismatches": calendar_mismatches,
                    "lifecycle_disappeared_rows": life.filter("is_disappeared").count()}
