import os
from pyspark.sql import SparkSession

MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

if not MINIO_USER or not MINIO_PASSWORD:
    raise RuntimeError("Khong tim thay MINIO_ROOT_USER / MINIO_ROOT_PASSWORD")

spark = (
    SparkSession.builder
    .appName("TestSparkMinIO")
    .getOrCreate()
)

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", MINIO_USER)
hadoop_conf.set("fs.s3a.secret.key", MINIO_PASSWORD)
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
)

data = [
    (1, "Spark"),
    (2, "MinIO"),
    (3, "Bronze")
]

df = spark.createDataFrame(data, ["id", "name"])

print("=== DATA GOC ===")
df.show()

output_path = "s3a://lakehouse-bronze/test_connection"

print("Dang ghi vao:", output_path)

df.write.mode("overwrite").parquet(output_path)

print("WRITE SUCCESS")

check_df = spark.read.parquet(output_path)

print("=== DOC LAI TU MINIO ===")
check_df.show()

count = check_df.count()

print("ROW COUNT:", count)

if count != 3:
    raise RuntimeError(f"Expected 3 rows, got {count}")

print("READ SUCCESS")
print("SPARK -> MINIO: OK")

spark.stop()
