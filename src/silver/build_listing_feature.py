"""Build Silver listing features used by the Gold Business Questions.

Input is Silver only: ``silver_listings_current_27`` (grain), ``listing_location``
and the ``listing_observation`` row that produced each current row (for
``dq_status`` and ``record_hash``). Rules live in ``feature_rules.py`` and are
applied through ``mapPartitions`` so Spark output matches the unit tests.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.silver.feature_rules import FEATURE_COLUMNS, FEATURE_RULE_VERSION, build_feature_record

OUTPUT_PARTITIONS = max(1, int(os.getenv("SILVER_FEATURE_PARTITIONS", "8")))
MAX_UNMAPPED_CATEGORY_SHARE = 0.02
FLAG_COLUMNS = [
    "title_has_legal", "title_has_furnished", "title_has_frontage",
    "title_has_elevator", "title_has_car_access",
]
KNOWN_COLUMNS = ["price_known", "area_known", "rooms_known", "location_known", "legal_known"]


def transform_partition(rows: Iterator[Any]) -> Iterator[tuple[Any, ...]]:
    for row in rows:
        output = build_feature_record(row.asDict(recursive=True))
        yield tuple(output[column] for column in FEATURE_COLUMNS)


def main() -> None:
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.storagelevel import StorageLevel

    from src.common.spark_session import build_spark_session, ensure_silver_namespace, write_iceberg_table

    spark = build_spark_session("SilverListingFeature")
    spark.conf.set("spark.sql.shuffle.partitions", str(OUTPUT_PARTITIONS))
    namespace = ensure_silver_namespace(spark)
    current_table = f"{namespace}.silver_listings_current_27"
    location_table = f"{namespace}.listing_location"
    observation_table = f"{namespace}.listing_observation"
    output_table = f"{namespace}.listing_feature"
    started_at = datetime.now(timezone.utc)

    current = spark.table(current_table).select(
        "source_id", "source", "title", "category_name", "price", "area", "rooms", "has_coord", "is_rent"
    )
    location = spark.table(location_table).select(
        "source_id", "province_name_model", "district_name_model"
    )
    # Same ordering as current_from() in build_listing_core_spark.py, so the
    # selected observation is the one that produced the current row.
    latest = Window.partitionBy("source_id").orderBy(
        F.col("snapshot_date").desc_nulls_last(), F.col("scraped_at").desc_nulls_last(),
        F.col("completeness_score").desc_nulls_last(), F.col("record_hash").asc_nulls_last(),
    )
    observation = (
        spark.table(observation_table)
        .withColumn("_rank", F.row_number().over(latest)).filter("_rank = 1")
        .select("source_id", "dq_status", "record_hash")
    )

    input_rows = current.count()
    joined = current.join(location, "source_id", "left").join(observation, "source_id", "left")
    missing_observation = joined.filter(F.col("record_hash").isNull()).count()
    if missing_observation:
        raise RuntimeError(f"{missing_observation:,} current rows have no source observation")

    boolean_columns = set(FLAG_COLUMNS + KNOWN_COLUMNS) | {"is_rent"}
    schema = T.StructType([
        T.StructField(
            column,
            T.BooleanType() if column in boolean_columns
            else T.DoubleType() if column == "feature_completeness_score"
            else T.StringType(),
            column in {"is_rent", "dq_status", "record_hash", "category_name", "source"},
        )
        for column in FEATURE_COLUMNS
    ])
    feature = (
        spark.createDataFrame(joined.rdd.mapPartitions(transform_partition), schema)
        .withColumn("feature_built_at", F.lit(started_at.replace(tzinfo=None)).cast("timestamp"))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    output_rows = feature.count()
    if output_rows != input_rows:
        raise RuntimeError(f"Feature row mismatch: input={input_rows:,}, output={output_rows:,}")

    write_iceberg_table(feature, output_table)
    verified = spark.table(output_table)
    if verified.count() != output_rows:
        raise RuntimeError("Feature read-back row count mismatch")

    def grouped_counts(column: str) -> dict[str, int]:
        return {str(r[column]): int(r["count"]) for r in feature.groupBy(column).count().collect()}

    rates = feature.agg(*[
        F.avg(F.col(column).cast("double")).alias(column) for column in FLAG_COLUMNS + KNOWN_COLUMNS
    ]).first().asDict()
    category_counts = grouped_counts("model_category")
    unmapped_share = category_counts.get("khong_ro", 0) / output_rows
    warnings = [
        f"{column} TRUE rate {rates[column]:.2%} outside (0%, 80%]"
        for column in FLAG_COLUMNS if not 0 < rates[column] <= 0.8
    ]
    if unmapped_share >= MAX_UNMAPPED_CATEGORY_SHARE:
        warnings.append(f"model_category khong_ro share {unmapped_share:.2%} >= 2%")
    raw_categories = {
        str(r["category_name"]): int(r["count"])
        for r in current.groupBy("category_name").count().orderBy(F.desc("count")).collect()
    }
    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "inputs": [current_table, location_table, observation_table],
        "output": output_table,
        "feature_rule_version": FEATURE_RULE_VERSION,
        "input_rows": input_rows,
        "output_rows": output_rows,
        "category_name_counts": raw_categories,
        "model_category_counts": category_counts,
        "model_category_method_counts": grouped_counts("model_category_method"),
        "model_category_unmapped_share": round(unmapped_share, 6),
        "true_rates": {column: round(float(rates[column]), 6) for column in FLAG_COLUMNS + KNOWN_COLUMNS},
        "is_rent_counts": grouped_counts("is_rent"),
        "dq_status_counts": grouped_counts("dq_status"),
        "warnings": warnings,
        "status": "PASS",
    }
    for output_dir in (PROJECT_ROOT / "outputs/validation", PROJECT_ROOT / "docs/validation"):
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "silver_feature_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    feature.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
