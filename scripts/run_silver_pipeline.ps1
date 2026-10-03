$ErrorActionPreference = "Stop"

$jobs = @(
    "src\bronze\audit_bronze_state.py",
    "src\silver\iceberg_smoke_check.py",
    "src\silver\build_listing_core_spark.py",
    "src\silver\build_listing_history.py",
    "src\silver\build_location.py",
    "src\silver\verify_silver.py"
)

docker compose up -d minio minio-init iceberg-rest spark-master spark-worker
if ($LASTEXITCODE -ne 0) { throw "Cannot start core Docker services" }

foreach ($job in $jobs) {
    Write-Host "`n=== RUN $job ===" -ForegroundColor Cyan
    & "$PSScriptRoot\run_spark.ps1" $job
    if ($LASTEXITCODE -ne 0) { throw "Silver pipeline failed at $job" }
}

python -m pytest -q
if ($LASTEXITCODE -ne 0) { throw "Unit tests failed" }

Write-Host "`nSILVER PIPELINE PASS" -ForegroundColor Green
