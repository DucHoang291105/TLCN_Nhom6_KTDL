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

exit $LASTEXITCODE
