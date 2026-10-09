"""Shared Spark session configuration for MinIO and the Iceberg REST catalog."""

from __future__ import annotations

import os
from pathlib import Path
from pyspark.sql import DataFrame, SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CATALOG_NAME = os.getenv("ICEBERG_CATALOG", "lakehouse")
SILVER_NAMESPACE = os.getenv("ICEBERG_SILVER_NAMESPACE", "silver")
GOLD_NAMESPACE = os.getenv("ICEBERG_GOLD_NAMESPACE", "gold")
SMOKE_NAMESPACE = "smoke_test"

# One MinIO bucket per Medallion layer. The JDBC catalog behind the Iceberg REST
# fixture ignores namespace-level locations, so every table is created with an
# explicit location under its layer bucket instead of the catalog warehouse.
LAYER_TABLE_ROOTS = {
    SILVER_NAMESPACE: os.getenv("SILVER_TABLE_ROOT", "s3://lakehouse-silver/real_estate"),
    GOLD_NAMESPACE: os.getenv("GOLD_TABLE_ROOT", "s3://lakehouse-gold/real_estate"),
    SMOKE_NAMESPACE: os.getenv("SMOKE_TABLE_ROOT", "s3://lakehouse-silver/_smoke_test"),
}


def build_spark_session(app_name: str) -> SparkSession:
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")
    if not access_key or not secret_key:
        raise RuntimeError("Missing MINIO_ROOT_USER or MINIO_ROOT_PASSWORD")
    endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    rest_uri = os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181")
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse/")
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.executorEnv.PYTHONPATH", str(PROJECT_ROOT))
        .config("spark.executorEnv.AWS_REGION", "us-east-1")
        .config("spark.executorEnv.AWS_ACCESS_KEY_ID", access_key)
        .config("spark.executorEnv.AWS_SECRET_ACCESS_KEY", secret_key)
        .config("spark.driver.extraJavaOptions", "-Daws.region=us-east-1")
        .config("spark.executor.extraJavaOptions", "-Daws.region=us-east-1")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "rest")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", rest_uri)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", warehouse)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.endpoint", endpoint)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.client.region", "us-east-1")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.access-key-id", access_key)
        .config(f"spark.sql.catalog.{CATALOG_NAME}.s3.secret-access-key", secret_key)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    hadoop = spark.sparkContext._jsc.hadoopConfiguration()
    settings = {
        "fs.s3a.endpoint": endpoint, "fs.s3a.access.key": access_key,
        "fs.s3a.secret.key": secret_key, "fs.s3a.path.style.access": "true",
        "fs.s3a.connection.ssl.enabled": "false", "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    }
    for key, value in settings.items():
        hadoop.set(key, value)
    return spark


def ensure_silver_namespace(spark: SparkSession) -> str:
    namespace = f"{CATALOG_NAME}.{SILVER_NAMESPACE}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    return namespace


def ensure_gold_namespace(spark: SparkSession) -> str:
    namespace = f"{CATALOG_NAME}.{GOLD_NAMESPACE}"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    return namespace


def table_location(table: str) -> str:
    """Return the layer-bucket location for ``catalog.namespace.table``."""

    parts = table.split(".")
    if len(parts) != 3 or parts[0] != CATALOG_NAME or parts[1] not in LAYER_TABLE_ROOTS:
        raise ValueError(f"No layer bucket configured for table: {table}")
    return f"{LAYER_TABLE_ROOTS[parts[1]].rstrip('/')}/{parts[2]}"


def _current_location(spark: SparkSession, table: str) -> str | None:
    if not spark.catalog.tableExists(table):
        return None
    rows = spark.sql(f"DESCRIBE TABLE EXTENDED {table}").filter("col_name = 'Location'").collect()
    return rows[0]["data_type"].rstrip("/") if rows else None


def write_iceberg_table(frame: DataFrame, table: str) -> None:
    """Create or replace an Iceberg v2 table in its layer bucket.

    A replace keeps the old table location, so a table still stored elsewhere
    (for example the former ``s3://warehouse/`` root) is purged and recreated.
    """

    spark = frame.sparkSession
    location = table_location(table)
    current = _current_location(spark, table)
    if current is not None and current != location:
        spark.sql(f"DROP TABLE {table} PURGE")
    (
        frame.writeTo(table).using("iceberg")
        .tableProperty("format-version", "2")
        .tableProperty("location", location)
        .createOrReplace()
    )
