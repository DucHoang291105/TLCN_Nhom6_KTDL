"""Data Quality KPI per source, tracing every row from Bronze to Gold.

``agg_dq_kpi``: one row per source. Funnel Bronze -> normalized -> accepted ->
observation -> current -> fact_listing -> representative, DQ status counts,
top WARN reasons, cross-source duplicate rate and feature coverage.
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.bronze.audit_bronze_state import read_sources_config
from src.gold.gold_common import write_summary

BRONZE_ROOT = "s3a://lakehouse-bronze/real_estate"
TOP_WARN_REASONS = 3


def main() -> None:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from src.common.spark_session import build_spark_session, ensure_gold_namespace, ensure_silver_namespace, write_iceberg_table

    spark = build_spark_session("GoldDataQualityKpi")
    silver = ensure_silver_namespace(spark)
    gold = ensure_gold_namespace(spark)
    started_at = datetime.now(timezone.utc)

    sources = read_sources_config(PROJECT_ROOT / "config/sources.yaml")
    bronze_rows = [
        (source, spark.read.parquet(f"{BRONZE_ROOT}/{branch}/{source}").count())
        for branch, names in sources.items() for source in names
    ]
    bronze = spark.createDataFrame(bronze_rows, "source STRING, bronze_rows LONG")

    def counts(table: str, name: str, condition: str = "true"):
        return spark.table(table).filter(condition).groupBy("source").agg(F.count("*").alias(name))

    observation = spark.table(f"{silver}.listing_observation")
    dq = observation.groupBy("source").agg(
        F.sum(F.when(F.col("dq_status") == "PASS", 1).otherwise(0)).alias("pass_rows"),
        F.sum(F.when(F.col("dq_status") == "WARN", 1).otherwise(0)).alias("warn_rows"),
    )
    reason_rank = Window.partitionBy("source").orderBy(F.desc("n"), F.col("reason"))
    top_reasons = (
        observation.filter("dq_status = 'WARN'").select("source", F.explode("dq_reasons").alias("reason"))
        .groupBy("source", "reason").agg(F.count("*").alias("n"))
        .withColumn("_rank", F.row_number().over(reason_rank)).filter(F.col("_rank") <= TOP_WARN_REASONS)
        .groupBy("source").agg(F.sort_array(F.collect_list(F.struct(F.col("_rank"), F.concat_ws(":", "reason", F.col("n").cast("string")).alias("v")))).alias("_r"))
        .select("source", F.col("_r.v").alias("top_warn_reasons"))
    )
    dim_source = spark.table(f"{gold}.dim_source").select("source_key", "source", "source_type")
    fact = spark.table(f"{gold}.fact_listing").join(dim_source.select("source_key", "source"), "source_key")
    fact_stats = fact.groupBy("source").agg(
        F.count("*").alias("fact_rows"),
        F.sum(F.col("is_dup_representative").cast("int")).alias("representative_rows"),
        F.sum(F.col("is_cross_source_dup_suspect").cast("int")).alias("dup_suspect_rows"),
    )
    feature = spark.table(f"{silver}.listing_feature").groupBy("source").agg(
        *[F.round(F.avg(F.col(c).cast("double")), 6).alias(f"coverage_{c}") for c in ("price_known", "area_known", "rooms_known", "location_known", "legal_known")],
        F.round(F.avg("feature_completeness_score"), 6).alias("avg_feature_completeness"),
    )
    location = spark.table(f"{silver}.listing_location").groupBy("source").agg(
        F.round(F.avg(F.col("has_coord").cast("double")), 6).alias("coverage_coordinate"),
        F.round(F.avg(F.col("province_name_model").isNotNull().cast("double")), 6).alias("coverage_province"),
    )

    # Quarantined rows are mostly CSV rows broken by multi-line HTML, so their
    # ``source`` column holds garbage; attribute them through Bronze lineage.
    rejects = (
        spark.table(f"{silver}.listing_dq_quarantine")
        .withColumn("source", F.regexp_extract("bronze_path", r"/(?:historical|snapshots)/([^/]+)/", 1))
        .groupBy("source").agg(F.count("*").alias("reject_rows"))
    )
    kpi = (
        dim_source.filter("source_key <> -1")
        .join(bronze, "source", "left")
        .join(rejects, "source", "left")
        .join(counts(f"{silver}.listing_observation", "observation_rows"), "source", "left")
        .join(counts(f"{silver}.silver_listings_current_27", "current_rows"), "source", "left")
        .join(counts(f"{silver}.silver_listings_current_27", "rent_rows", "is_rent"), "source", "left")
        .join(dq, "source", "left").join(top_reasons, "source", "left")
        .join(fact_stats, "source", "left").join(feature, "source", "left").join(location, "source", "left")
        .fillna(0, subset=["bronze_rows", "reject_rows", "observation_rows", "current_rows", "rent_rows", "pass_rows", "warn_rows", "fact_rows", "representative_rows", "dup_suspect_rows"])
        .withColumn("accepted_rows", F.col("bronze_rows") - F.col("reject_rows"))
        .withColumn("duplicate_rows_dropped", F.col("accepted_rows") - F.col("observation_rows"))
        .withColumn("pass_rate", F.round(F.col("pass_rows") / (F.col("observation_rows") + F.col("reject_rows")), 6))
        .withColumn("warn_rate", F.round(F.col("warn_rows") / (F.col("observation_rows") + F.col("reject_rows")), 6))
        .withColumn("reject_rate", F.round(F.col("reject_rows") / (F.col("observation_rows") + F.col("reject_rows")), 6))
        .withColumn("dup_suspect_rate", F.round(F.col("dup_suspect_rows") / F.col("fact_rows"), 6))
        .select(
            "source_key", "source", "source_type", "bronze_rows", "accepted_rows", "reject_rows",
            "duplicate_rows_dropped", "observation_rows", "current_rows", "rent_rows", "fact_rows",
            "representative_rows", "pass_rows", "warn_rows", "pass_rate", "warn_rate", "reject_rate",
            "top_warn_reasons", "dup_suspect_rows", "dup_suspect_rate",
            "coverage_price_known", "coverage_area_known", "coverage_rooms_known", "coverage_location_known",
            "coverage_legal_known", "coverage_coordinate", "coverage_province", "avg_feature_completeness",
        )
    )
    write_iceberg_table(kpi, f"{gold}.agg_dq_kpi")
    written = spark.table(f"{gold}.agg_dq_kpi")
    totals = written.agg(*[F.sum(c).alias(c) for c in (
        "bronze_rows", "accepted_rows", "reject_rows", "duplicate_rows_dropped", "observation_rows",
        "current_rows", "fact_rows", "representative_rows", "pass_rows", "warn_rows", "dup_suspect_rows",
    )]).first().asDict()
    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "rows": written.count(),
        "totals": {k: int(v or 0) for k, v in totals.items()},
        "status": "PASS",
    }
    write_summary("gold_data_quality_summary.json", summary)
    spark.stop()


if __name__ == "__main__":
    main()
