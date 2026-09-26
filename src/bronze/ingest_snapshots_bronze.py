import os
import json
import re
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = "/opt/project"

INPUT_ROOT = Path(
    f"{PROJECT_ROOT}/data/incoming/snapshots"
)

BRONZE_ROOT = (
    "s3a://lakehouse-bronze/"
    "real_estate/snapshots"
)

SOURCE_FILES = {
    "batdongsan": "batdongsan_raw.csv",
    "guland": "guland_raw.csv",
    "nhadatvui": "nhadatvui_raw.csv",
}


# ============================================================
# MINIO CREDENTIALS
# ============================================================

MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

if not MINIO_USER or not MINIO_PASSWORD:
    raise RuntimeError(
        "Khong tim thay MINIO_ROOT_USER "
        "hoac MINIO_ROOT_PASSWORD"
    )


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("BronzeSnapshotIngestion")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# S3A / MINIO CONFIG
# ============================================================

hadoop_conf = (
    spark.sparkContext
    ._jsc
    .hadoopConfiguration()
)

hadoop_conf.set(
    "fs.s3a.endpoint",
    "http://minio:9000"
)

hadoop_conf.set(
    "fs.s3a.access.key",
    MINIO_USER
)

hadoop_conf.set(
    "fs.s3a.secret.key",
    MINIO_PASSWORD
)

hadoop_conf.set(
    "fs.s3a.path.style.access",
    "true"
)

hadoop_conf.set(
    "fs.s3a.connection.ssl.enabled",
    "false"
)

hadoop_conf.set(
    "fs.s3a.impl",
    "org.apache.hadoop.fs.s3a.S3AFileSystem"
)

hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)


# ============================================================
# SUMMARY
# ============================================================

summary = {
    "ingestion_mode": "snapshot_crawl",
    "started_at": datetime.now(
        timezone.utc
    ).isoformat(),
    "sources": {},
}

total_input = 0
total_output = 0


print("=" * 75)
print("BRONZE SNAPSHOT INGESTION")
print("=" * 75)


# ============================================================
# INGEST
# ============================================================

for source_name, file_name in SOURCE_FILES.items():

    source_dir = INPUT_ROOT / source_name

    if not source_dir.exists():
        raise RuntimeError(
            f"Khong tim thay folder: {source_dir}"
        )

    # Chỉ lấy folder dạng YYYYMMDD
    # Ví dụ: 20260922, 20260925
    batch_dirs = sorted(
        [
            p
            for p in source_dir.iterdir()
            if p.is_dir()
            and re.fullmatch(r"\d{8}", p.name)
        ]
    )

    if not batch_dirs:
        raise RuntimeError(
            f"Khong tim thay batch YYYYMMDD "
            f"trong {source_dir}"
        )

    summary["sources"][source_name] = {}

    for batch_dir in batch_dirs:

        batch_id = batch_dir.name

        input_path = (
            batch_dir / file_name
        )

        if not input_path.exists():
            raise RuntimeError(
                f"Khong tim thay file: {input_path}"
            )

        output_path = (
            f"{BRONZE_ROOT}/"
            f"{source_name}/"
            f"batch_id={batch_id}"
        )

        print()
        print("-" * 75)
        print(
            f"[START] {source_name} - {batch_id}"
        )
        print(
            f"INPUT : {input_path}"
        )
        print(
            f"OUTPUT: {output_path}"
        )

        # ====================================================
        # READ RAW CSV
        # ====================================================

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .option("encoding", "UTF-8")
            .option("multiLine", "true")
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "PERMISSIVE")
            .csv(str(input_path))
        )

        if len(df.columns) == 0:
            raise RuntimeError(
                f"{source_name} {batch_id}: "
                "CSV khong co cot"
            )

        input_count = df.count()

        print(
            f"COLUMNS    : {len(df.columns)}"
        )

        print(
            f"INPUT ROWS : {input_count:,}"
        )

        # ====================================================
        # ADD BRONZE METADATA
        # ====================================================

        bronze_df = (
            df
            .withColumn(
                "_source_name",
                F.lit(source_name)
            )
            .withColumn(
                "_source_file",
                F.lit(file_name)
            )
            .withColumn(
                "_batch_id",
                F.lit(batch_id)
            )
            .withColumn(
                "_snapshot_date",
                F.to_date(
                    F.lit(batch_id),
                    "yyyyMMdd"
                )
            )
            .withColumn(
                "_ingestion_mode",
                F.lit("snapshot_crawl")
            )
            .withColumn(
                "_ingested_at",
                F.current_timestamp()
            )
        )

        # ====================================================
        # WRITE BRONZE
        # ====================================================

        (
            bronze_df.write
            .mode("overwrite")
            .parquet(output_path)
        )

        # ====================================================
        # VERIFY READ BACK
        # ====================================================

        verify_df = (
            spark.read
            .parquet(output_path)
        )

        output_count = verify_df.count()

        print(
            f"OUTPUT ROWS: {output_count:,}"
        )

        if input_count != output_count:
            raise RuntimeError(
                f"{source_name} {batch_id}: "
                "WRITE VERIFICATION FAILED. "
                f"Input={input_count:,}, "
                f"Output={output_count:,}"
            )

        print(
            f"[OK] {source_name} - "
            f"{batch_id}: "
            f"{output_count:,} rows"
        )

        total_input += input_count
        total_output += output_count

        summary["sources"][
            source_name
        ][batch_id] = {
            "file": file_name,
            "input_path": str(
                input_path
            ),
            "output_path": output_path,
            "column_count": len(
                df.columns
            ),
            "input_rows": input_count,
            "output_rows": output_count,
            "status": "SUCCESS",
        }


# ============================================================
# FINAL VALIDATION
# ============================================================

print()
print("=" * 75)
print("FINAL VALIDATION")
print("=" * 75)

print(
    f"TOTAL INPUT : {total_input:,}"
)

print(
    f"TOTAL OUTPUT: {total_output:,}"
)

if total_input != total_output:
    raise RuntimeError(
        "Tong Input va Output khong khop"
    )


summary["total_input_rows"] = total_input
summary["total_output_rows"] = total_output
summary["status"] = "SUCCESS"

summary["finished_at"] = datetime.now(
    timezone.utc
).isoformat()


# ============================================================
# SAVE SUMMARY LOCAL
# ============================================================

summary_path = Path(
    f"{PROJECT_ROOT}/"
    "outputs/validation/"
    "bronze_snapshot_ingestion_summary.json"
)

summary_path.parent.mkdir(
    parents=True,
    exist_ok=True
)

with open(
    summary_path,
    "w",
    encoding="utf-8"
) as f:

    json.dump(
        summary,
        f,
        ensure_ascii=False,
        indent=2
    )


print()
print(
    f"SUMMARY: {summary_path}"
)

print()
print("=" * 75)
print("BRONZE SNAPSHOT INGESTION SUCCESS")
print(
    f"{total_output:,} ROWS WRITTEN"
)
print("=" * 75)


spark.stop()