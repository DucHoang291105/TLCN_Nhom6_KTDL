import os
from pyspark.sql import SparkSession


MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

if not MINIO_USER or not MINIO_PASSWORD:
    raise RuntimeError("Thieu MINIO credentials")


spark = (
    SparkSession.builder
    .appName("TestIcebergMinIO")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config(
        "spark.sql.catalog.lakehouse",
        "org.apache.iceberg.spark.SparkCatalog"
    )
    .config(
        "spark.sql.catalog.lakehouse.type",
        "hadoop"
    )
    .config(
        "spark.sql.catalog.lakehouse.warehouse",
        "s3a://lakehouse-silver/iceberg"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ============================================================
# MINIO / S3A
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


print("=" * 60)
print("TEST ICEBERG + SPARK + MINIO")
print("=" * 60)


# ============================================================
# NAMESPACE
# ============================================================

spark.sql("""
CREATE NAMESPACE IF NOT EXISTS lakehouse.system_test
""")


# Xoa bang test cu de co the chay lai nhieu lan
spark.sql("""
DROP TABLE IF EXISTS lakehouse.system_test.connection_test
""")


# ============================================================
# CREATE ICEBERG TABLE
# ============================================================

spark.sql("""
CREATE TABLE lakehouse.system_test.connection_test (
    id BIGINT,
    name STRING,
    created_at TIMESTAMP
)
USING iceberg
""")


# ============================================================
# INSERT
# ============================================================

spark.sql("""
INSERT INTO lakehouse.system_test.connection_test
VALUES
    (1, 'Spark', current_timestamp()),
    (2, 'MinIO', current_timestamp()),
    (3, 'Iceberg', current_timestamp())
""")


# ============================================================
# READ
# ============================================================

print()
print("=== ICEBERG TABLE ===")

df = spark.sql("""
SELECT *
FROM lakehouse.system_test.connection_test
ORDER BY id
""")

df.show(truncate=False)

count = df.count()

print("ROW COUNT:", count)

if count != 3:
    raise RuntimeError(
        f"Expected 3 rows, got {count}"
    )


# ============================================================
# TABLE LIST
# ============================================================

print()
print("=== TABLES ===")

spark.sql("""
SHOW TABLES IN lakehouse.system_test
""").show(truncate=False)


print()
print("=" * 60)
print("ICEBERG TEST SUCCESS")
print("SPARK -> ICEBERG -> MINIO: OK")
print("=" * 60)

spark.stop()
