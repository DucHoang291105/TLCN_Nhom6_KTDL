"""Shared Spark session configuration for MinIO and the Iceberg REST catalog."""

from __future__ import annotations

import os
from pathlib import Path
from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CATALOG_NAME = os.getenv("ICEBERG_CATALOG", "lakehouse")
SILVER_NAMESPACE = os.getenv("ICEBERG_SILVER_NAMESPACE", "silver")


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
