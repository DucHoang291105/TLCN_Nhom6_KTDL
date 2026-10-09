param(
    [Parameter(Mandatory=$true)]
    [string]$App
)

$containerPath = "/opt/project/" + ($App -replace "\\", "/")

docker exec -e PYTHONPATH=/opt/project tlcn_spark_master `
    /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --conf spark.jars.ivy=/tmp/.ivy2 `
    --conf spark.executorEnv.PYTHONPATH=/opt/project `
    --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0 `
    $containerPath
$jobExitCode = $LASTEXITCODE

# Each application copies ~370 MB of dependency jars into the worker work dir
# and Spark Standalone never removes them; ~20 runs filled the Docker disk.
# Jobs run one at a time, so finished application dirs are safe to delete.
docker exec tlcn_spark_worker sh -c "rm -rf /opt/spark/work/app-*" | Out-Null

exit $jobExitCode
