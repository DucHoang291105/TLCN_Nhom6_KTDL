param(
    [Parameter(Mandatory=$true)]
    [string]$App,
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$AppArgs
)

# Manual runs must obey the same SQLite catalog write window as Airflow.
$ErrorActionPreference = "Stop"
$trinoStates = @(docker ps -a --filter "name=^/tlcn_trino$" --format "{{.State}}")
if ($LASTEXITCODE -ne 0) { throw "Cannot check the catalog write window: Docker is unavailable" }
foreach ($state in $trinoStates) {
    if ($state.Trim() -notin @("created", "exited", "dead")) {
        throw "Trino is active: stop tlcn_trino before manual Spark jobs; restart query/dashboard after the pipeline finishes"
    }
}

$containerPath = "/opt/project/" + ($App -replace "\\", "/")

$previousPreference = $ErrorActionPreference
$ErrorActionPreference = "Continue"
try {
    # Windows PowerShell must not mistake Spark's INFO/WARN stderr for failure
    # when callers redirect logs. The Docker exit code decides job success.
    docker exec -e PYTHONPATH=/opt/project tlcn_spark_master `
        /opt/spark/bin/spark-submit `
        --master spark://spark-master:7077 `
        --conf spark.jars.ivy=/home/spark/.ivy2 `
        --conf spark.executorEnv.PYTHONPATH=/opt/project `
        --packages org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0 `
        $containerPath @AppArgs 2>&1 | ForEach-Object { "$_" }
    $jobExitCode = $LASTEXITCODE
} finally {
    $ErrorActionPreference = $previousPreference
}

# Worker cleanup is configured in Docker Compose (30s interval, 60s retention).
# Do not delete app-* here: a newly started Airflow/manual app may already be
# using one of those directories. Spark's own cleanup excludes active apps.

exit $jobExitCode
