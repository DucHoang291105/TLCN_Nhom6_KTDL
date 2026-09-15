import os
import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = "/opt/project"

INPUT_ROOT = (
    f"{PROJECT_ROOT}/data/incoming/historical"
)

BRONZE_ROOT = (
    "s3a://lakehouse-bronze/"
    "real_estate/historical"
)

BATCH_ID = os.getenv(
    "BATCH_ID",
    "historical_backfill_v1"
)

MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

if not MINIO_USER or not MINIO_PASSWORD:
    raise RuntimeError(
        "Khong tim thay MINIO_ROOT_USER "
        "hoac MINIO_ROOT_PASSWORD"
    )


SOURCES = {
    "alonhadat": {
        "file": "alonhadat_local_schema_chuan.csv",
        "expected_rows": 1278,
    },
    "chotot": {
        "file": "chotot_schema_chuan.csv",
        "expected_rows": 8152,
    },
    "homedy": {
        "file": "homedy_schema_chuan.csv",
        "expected_rows": 566,
    },
    "luachonnhadat": {
        "file": "luachonnhadat_schema_chuan.csv",
        "expected_rows": 883,
    },
    "mogi": {
        "file": "mogi_schema_chuan.csv",
        "expected_rows": 19075,
    },
    "muaban": {
        "file": "muaban_schema_chuan.csv",
        "expected_rows": 363,
    },
}


EXPECTED_COLUMNS = [
    "source",
    "source_group",
    "source_id",
    "ad_id",
    "title",
    "price",
    "price_str",
    "area",
    "rooms",
    "address",
    "ward",
    "district_id",
    "district_name",
    "category_id",
    "category_name",
    "lat",
    "lon",
    "image",
    "ad_url",
    "source_url",
    "posted_at",
    "scraped_at",
    "page_fetched",
    "price_m",
    "price_per_m2",
    "has_coord",
    "is_rent",
]


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("BronzeHistoricalIngestion")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

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
# INGEST
# ============================================================

summary = {
    "batch_id": BATCH_ID,
    "ingestion_mode": "historical_backfill",
    "started_at": datetime.now(
        timezone.utc
    ).isoformat(),
    "sources": {},
}

total_input = 0
total_output = 0


print("=" * 70)
print("BRONZE HISTORICAL INGESTION")
print("BATCH:", BATCH_ID)
print("=" * 70)


for source_name, config in SOURCES.items():

    file_name = config["file"]
    expected_rows = config["expected_rows"]

    input_path = (
        f"{INPUT_ROOT}/"
        f"{source_name}/"
        f"{file_name}"
    )

    output_path = (
        f"{BRONZE_ROOT}/"
        f"{source_name}/"
        f"batch_id={BATCH_ID}"
    )

    print()
    print("-" * 70)
    print("SOURCE:", source_name)
    print("INPUT :", input_path)
    print("OUTPUT:", output_path)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(input_path)
    )

    # --------------------------------------------------------
    # Validate schema
    # --------------------------------------------------------

    actual_columns = df.columns

    if actual_columns != EXPECTED_COLUMNS:
        raise RuntimeError(
            f"Schema mismatch tai {source_name}\n"
            f"Expected: {EXPECTED_COLUMNS}\n"
            f"Actual  : {actual_columns}"
        )

    # --------------------------------------------------------
    # Validate input count
    # --------------------------------------------------------

    input_count = df.count()

    print(
        f"INPUT ROWS: "
        f"{input_count:,}"
    )

    if input_count != expected_rows:
        raise RuntimeError(
            f"{source_name}: "
            f"expected {expected_rows:,} rows, "
            f"got {input_count:,}"
        )

    # --------------------------------------------------------
    # Bronze metadata
    # --------------------------------------------------------

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
            F.lit(BATCH_ID)
        )
        .withColumn(
            "_ingestion_mode",
            F.lit("historical_backfill")
        )
        .withColumn(
            "_ingested_at",
            F.current_timestamp()
        )
    )

    # --------------------------------------------------------
    # Write Bronze
    # --------------------------------------------------------

    (
        bronze_df.write
        .mode("overwrite")
        .parquet(output_path)
    )

    # --------------------------------------------------------
    # Verify read-back
    # --------------------------------------------------------

    verify_df = spark.read.parquet(
        output_path
    )

    output_count = verify_df.count()

    print(
        f"OUTPUT ROWS: "
        f"{output_count:,}"
    )

    if output_count != input_count:
        raise RuntimeError(
            f"{source_name}: "
            f"write verification failed. "
            f"Input={input_count}, "
            f"Output={output_count}"
        )

    print(
        f"[OK] {source_name}: "
        f"{output_count:,} rows"
    )

    total_input += input_count
    total_output += output_count

    summary["sources"][source_name] = {
        "file": file_name,
        "input_rows": input_count,
        "output_rows": output_count,
        "output_path": output_path,
        "status": "SUCCESS",
    }


# ============================================================
# FINAL VALIDATION
# ============================================================

EXPECTED_TOTAL = 30317

print()
print("=" * 70)
print("FINAL VALIDATION")
print("=" * 70)

print(
    "TOTAL INPUT :",
    f"{total_input:,}"
)

print(
    "TOTAL OUTPUT:",
    f"{total_output:,}"
)

if total_input != EXPECTED_TOTAL:
    raise RuntimeError(
        f"Expected total {EXPECTED_TOTAL:,}, "
        f"got {total_input:,}"
    )

if total_output != EXPECTED_TOTAL:
    raise RuntimeError(
        f"Bronze total should be "
        f"{EXPECTED_TOTAL:,}, "
        f"got {total_output:,}"
    )


summary["total_input_rows"] = total_input
summary["total_output_rows"] = total_output
summary["status"] = "SUCCESS"
summary["finished_at"] = datetime.now(
    timezone.utc
).isoformat()


# ============================================================
# SAVE VALIDATION SUMMARY
# ============================================================

summary_path = Path(
    f"{PROJECT_ROOT}/"
    "outputs/validation/"
    "bronze_ingestion_summary.json"
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
    "SUMMARY:",
    summary_path
)

print()
print("=" * 70)
print("BRONZE INGESTION SUCCESS")
print(
    f"{total_output:,} ROWS WRITTEN"
)
print("=" * 70)

spark.stop()
