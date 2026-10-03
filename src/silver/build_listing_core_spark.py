"""Spark Bronze -> Silver Core pipeline for the three snapshot crawlers.

The job reads only Bronze Parquet from MinIO. It never reads crawler CSVs.
Run inside the project Spark container with ``scripts/run_spark.ps1``.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from pyspark.sql import DataFrame, Row, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.storagelevel import StorageLevel

# ``spark-submit`` adds the application's directory (``src/silver``) to
# ``sys.path``, not the repository root.  Add the mounted project root before
# importing shared project modules.  Executors receive the same path in
# ``main`` below.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.utils import CANONICAL_COLUMNS
from src.silver.build_listing_core import (
    OBSERVATION_METADATA_COLUMNS,
    build_batdongsan_observation,
    build_guland_observation,
    build_nhadatvui_observation,
)


BRONZE_ROOT = os.getenv(
    "BRONZE_SNAPSHOT_ROOT",
    "s3a://lakehouse-bronze/real_estate/snapshots",
).rstrip("/")
SILVER_ROOT = os.getenv(
    "SILVER_CORE_ROOT",
    "s3a://lakehouse-silver/real_estate/core",
).rstrip("/")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
SILVER_SHUFFLE_PARTITIONS = max(
    1,
    int(os.getenv("SILVER_SHUFFLE_PARTITIONS", "8")),
)

MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
if not MINIO_USER or not MINIO_PASSWORD:
    raise RuntimeError("Missing MINIO_ROOT_USER or MINIO_ROOT_PASSWORD")


ObservationBuilder = Callable[..., dict[str, Any]]
SOURCE_BUILDERS: dict[str, ObservationBuilder] = {
    "batdongsan": build_batdongsan_observation,
    "guland": build_guland_observation,
    "nhadatvui": build_nhadatvui_observation,
}


OBSERVATION_SCHEMA = T.StructType(
    [
        T.StructField("source", T.StringType(), False),
        T.StructField("source_group", T.StringType(), True),
        T.StructField("source_id", T.StringType(), True),
        T.StructField("ad_id", T.StringType(), True),
        T.StructField("title", T.StringType(), True),
        T.StructField("price", T.DoubleType(), True),
        T.StructField("price_str", T.StringType(), True),
        T.StructField("area", T.DoubleType(), True),
        T.StructField("rooms", T.IntegerType(), True),
        T.StructField("address", T.StringType(), True),
        T.StructField("ward", T.StringType(), True),
        T.StructField("district_id", T.StringType(), True),
        T.StructField("district_name", T.StringType(), True),
        T.StructField("category_id", T.StringType(), True),
        T.StructField("category_name", T.StringType(), True),
        T.StructField("lat", T.DoubleType(), True),
        T.StructField("lon", T.DoubleType(), True),
        T.StructField("image", T.StringType(), True),
        T.StructField("ad_url", T.StringType(), True),
        T.StructField("source_url", T.StringType(), True),
        T.StructField("posted_at", T.TimestampType(), True),
        T.StructField("scraped_at", T.TimestampType(), True),
        T.StructField("page_fetched", T.IntegerType(), True),
        T.StructField("price_m", T.DoubleType(), True),
        T.StructField("price_per_m2", T.DoubleType(), True),
        T.StructField("has_coord", T.BooleanType(), False),
        T.StructField("is_rent", T.BooleanType(), True),
        T.StructField("batch_id", T.StringType(), False),
        T.StructField("snapshot_date", T.DateType(), True),
        T.StructField("bronze_ingested_at", T.TimestampType(), True),
        T.StructField("bronze_source_file", T.StringType(), True),
        T.StructField("bronze_path", T.StringType(), True),
        T.StructField("record_hash", T.StringType(), True),
        T.StructField("dq_status", T.StringType(), False),
        T.StructField("dq_reasons", T.ArrayType(T.StringType(), False), False),
        T.StructField("completeness_score", T.DoubleType(), False),
    ]
)


def configure_minio(spark: SparkSession) -> None:
    spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.access.key", MINIO_USER)
    hadoop_conf.set("fs.s3a.secret.key", MINIO_PASSWORD)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )


def transform_partition(
    rows: Iterator[Row],
    builder: ObservationBuilder,
) -> Iterator[tuple[Any, ...]]:
    columns = CANONICAL_COLUMNS + OBSERVATION_METADATA_COLUMNS
    for row in rows:
        raw = row.asDict(recursive=True)
        bronze_path = raw.pop("_bronze_path", None)
        batch_id = str(raw.get("_batch_id") or "")
        observation = builder(
            raw,
            batch_id=batch_id,
            bronze_path=bronze_path,
        )
        yield tuple(observation.get(column) for column in columns)


def read_and_transform_source(
    spark: SparkSession,
    source: str,
    builder: ObservationBuilder,
) -> tuple[DataFrame, int]:
    bronze_path = f"{BRONZE_ROOT}/{source}/batch_id=*"
    try:
        bronze = (
            spark.read
            .option("basePath", f"{BRONZE_ROOT}/{source}")
            .parquet(bronze_path)
            .withColumn("_bronze_path", F.input_file_name())
        )
    except Exception as exc:
        raise RuntimeError(
            f"Cannot read Bronze for {source}: {bronze_path}. "
            "Run ingest_snapshots_bronze.py first."
        ) from exc

    input_count = bronze.count()
    if input_count == 0:
        raise RuntimeError(f"Bronze source is empty: {source}")

    transformed_rdd = bronze.rdd.mapPartitions(
        lambda rows: transform_partition(rows, builder)
    )
    return spark.createDataFrame(transformed_rdd, OBSERVATION_SCHEMA), input_count


def collect_counts(frame: DataFrame, column: str) -> dict[str, int]:
    return {
        str(row[column]): int(row["count"])
        for row in frame.groupBy(column).count().collect()
    }


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("BronzeToSilverListingCore")
        .config("spark.executorEnv.PYTHONPATH", str(PROJECT_ROOT))
        .config(
            "spark.sql.shuffle.partitions",
            str(SILVER_SHUFFLE_PARTITIONS),
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    configure_minio(spark)

    started_at = datetime.now(timezone.utc)
    source_counts: dict[str, int] = {}
    normalized: DataFrame | None = None

    for source, builder in SOURCE_BUILDERS.items():
        source_frame, source_count = read_and_transform_source(
            spark, source, builder
        )
        source_counts[source] = source_count
        normalized = (
            source_frame
            if normalized is None
            else normalized.unionByName(source_frame)
        )

    if normalized is None:
        raise RuntimeError("No Bronze snapshot sources were loaded")

    # The Python transformations are the most expensive part of this job.
    # Persist their result so counts, DQ splits, windowing and writes do not
    # execute the source transformations repeatedly.
    normalized = normalized.persist(StorageLevel.MEMORY_AND_DISK)

    quarantine = normalized.filter(
        F.col("dq_status") == F.lit("REJECT")
    ).persist(StorageLevel.MEMORY_AND_DISK)
    accepted = normalized.filter(F.col("dq_status") != F.lit("REJECT"))

    dedup_window = Window.partitionBy("source", "ad_id", "batch_id").orderBy(
        F.col("completeness_score").desc_nulls_last(),
        F.col("scraped_at").desc_nulls_last(),
        F.col("page_fetched").asc_nulls_last(),
        F.col("record_hash").desc_nulls_last(),
    )
    observations = (
        accepted
        .withColumn("_dedup_rank", F.row_number().over(dedup_window))
        .filter(F.col("_dedup_rank") == 1)
        .drop("_dedup_rank")
    ).persist(StorageLevel.MEMORY_AND_DISK)

    current_window = Window.partitionBy("source_id").orderBy(
        F.col("snapshot_date").desc_nulls_last(),
        F.col("scraped_at").desc_nulls_last(),
        F.col("completeness_score").desc_nulls_last(),
        F.col("record_hash").desc_nulls_last(),
    )
    current = (
        observations
        .withColumn("_current_rank", F.row_number().over(current_window))
        .filter(F.col("_current_rank") == 1)
        .select(*CANONICAL_COLUMNS)
    ).persist(StorageLevel.MEMORY_AND_DISK)

    input_rows = sum(source_counts.values())
    accepted_rows = accepted.count()
    observation_rows = observations.count()
    current_rows = current.count()
    quarantine_rows = quarantine.count()
    duplicates_dropped = accepted_rows - observation_rows

    observation_path = f"{SILVER_ROOT}/listing_observation"
    current_path = f"{SILVER_ROOT}/listings_current_27"
    quarantine_path = f"{SILVER_ROOT}/listing_dq_quarantine"

    observations.write.mode("overwrite").parquet(observation_path)
    current.write.mode("overwrite").parquet(current_path)
    quarantine.write.mode("overwrite").parquet(quarantine_path)

    verified_observation = spark.read.parquet(observation_path)
    verified_current = spark.read.parquet(current_path)
    verified_quarantine = spark.read.parquet(quarantine_path)
    if verified_observation.count() != observation_rows:
        raise RuntimeError("Silver observation read-back count mismatch")
    if verified_current.count() != current_rows:
        raise RuntimeError("Silver current read-back count mismatch")
    if verified_quarantine.count() != quarantine_rows:
        raise RuntimeError("Silver quarantine read-back count mismatch")
    if verified_current.columns != CANONICAL_COLUMNS:
        raise RuntimeError(
            "Silver current schema/order mismatch: "
            f"{verified_current.columns}"
        )

    status_counts = collect_counts(observations.unionByName(quarantine), "dq_status")
    reason_counts = {
        str(row["reason"]): int(row["count"])
        for row in (
            observations.unionByName(quarantine)
            .select(F.explode("dq_reasons").alias("reason"))
            .groupBy("reason")
            .count()
            .collect()
        )
    }

    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "bronze_root": BRONZE_ROOT,
        "silver_root": SILVER_ROOT,
        "source_input_rows": source_counts,
        "input_rows": input_rows,
        "duplicates_dropped": duplicates_dropped,
        "observation_rows": observation_rows,
        "current_rows": current_rows,
        "quarantine_rows": quarantine_rows,
        "status_counts": status_counts,
        "dq_reason_counts": reason_counts,
        "outputs": {
            "listing_observation": observation_path,
            "listings_current_27": current_path,
            "listing_dq_quarantine": quarantine_path,
        },
        "status": "SUCCESS",
    }

    summary_path = PROJECT_ROOT / "outputs/validation/silver_core_spark_summary.json"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    current.unpersist()
    observations.unpersist()
    quarantine.unpersist()
    normalized.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
