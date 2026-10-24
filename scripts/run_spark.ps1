param(
    [Parameter(Mandatory=$true)]
    [string]$App,
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$AppArgs
)

$containerPath = "/opt/project/" + ($App -replace "\\", "/")

docker exec -e PYTHONPATH=/opt/project tlcn_spark_master `
    /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --conf spark.jars.ivy=/home/spark/.ivy2 `
    --conf spark.executorEnv.PYTHONPATH=/opt/project `
    --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0 `
    $containerPath @AppArgs
$jobExitCode = $LASTEXITCODE

# Worker cleanup is configured in Docker Compose (30s interval, 60s retention).
# Do not delete app-* here: a newly started Airflow/manual app may already be
# using one of those directories. Spark's own cleanup excludes active apps.

exit $jobExitCode
