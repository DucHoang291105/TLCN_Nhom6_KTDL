import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

BATCH_ID = "historical_backfill_v1"

EXPECTED = {
    "alonhadat": 1278,
    "chotot": 8152,
    "homedy": 566,
    "luachonnhadat": 883,
    "mogi": 19075,
    "muaban": 363,
}

ROOT = "s3a://lakehouse-bronze/real_estate/historical"

spark = (
    SparkSession.builder
    .appName("VerifyBronze")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

conf = spark.sparkContext._jsc.hadoopConfiguration()

conf.set("fs.s3a.endpoint", "http://minio:9000")
conf.set("fs.s3a.access.key", MINIO_USER)
conf.set("fs.s3a.secret.key", MINIO_PASSWORD)
conf.set("fs.s3a.path.style.access", "true")
conf.set("fs.s3a.connection.ssl.enabled", "false")
conf.set(
    "fs.s3a.impl",
    "org.apache.hadoop.fs.s3a.S3AFileSystem"
)
conf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

metadata_cols = [
    "_source_name",
    "_source_file",
    "_batch_id",
    "_ingestion_mode",
    "_ingested_at",
]

errors = []
total = 0

print("=" * 70)
print("VERIFY BRONZE")
print("=" * 70)

for source, expected_rows in EXPECTED.items():

    path = (
        f"{ROOT}/"
        f"{source}/"
        f"batch_id={BATCH_ID}"
    )

    print()
    print("SOURCE:", source)
    print("PATH  :", path)

    df = spark.read.parquet(path)

    rows = df.count()
    total += rows

    print("ROWS   :", rows)
    print("COLUMNS:", len(df.columns))

    if rows != expected_rows:
        errors.append(
            f"{source}: expected {expected_rows}, got {rows}"
        )

    for col in metadata_cols:

        if col not in df.columns:
            errors.append(
                f"{source}: missing {col}"
            )
            continue

        nulls = (
            df
            .filter(F.col(col).isNull())
            .count()
        )

        if nulls > 0:
            errors.append(
                f"{source}: {col} has {nulls} null(s)"
            )

    print("[OK]", source)

print()
print("=" * 70)
print("TOTAL:", total)
print("=" * 70)

if total != 30317:
    errors.append(
        f"Expected total 30317, got {total}"
    )

if errors:
    print("VERIFY FAILED")

    for err in errors:
        print("-", err)

    raise RuntimeError(
        f"{len(errors)} validation error(s)"
    )

print()
print("BRONZE VERIFY SUCCESS")
print("30,317 ROWS VERIFIED")

spark.stop()
