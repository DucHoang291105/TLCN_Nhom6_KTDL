$ErrorActionPreference = "Stop"
. "$PSScriptRoot\pipeline_utils.ps1"

$jobs = @(
    "src\bronze\audit_bronze_state.py",
    "src\silver\iceberg_smoke_check.py",
    "src\silver\build_listing_core_spark.py",
    "src\silver\build_listing_history.py",
    "src\silver\build_location.py",
    "src\silver\build_listing_feature.py",
    "src\silver\verify_silver.py"
)

Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    Invoke-Native { docker compose up -d minio minio-init iceberg-rest spark-master spark-worker } "Cannot start core Docker services"
    Invoke-SparkJobs $jobs "Silver"
    Invoke-Native { python -m pytest -q } "Unit tests failed"
} finally {
    Pop-Location
}

Write-Host "`nSILVER PIPELINE PASS" -ForegroundColor Green
