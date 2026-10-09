$ErrorActionPreference = "Stop"
. "$PSScriptRoot\pipeline_utils.ps1"

# Requires a PASS Silver build (run_silver_pipeline.ps1).
$jobs = @(
    "src\gold\build_dimensions.py",
    "src\gold\build_fact_listing.py",
    "src\gold\build_price_benchmark.py",
    "src\gold\build_budget_tradeoff.py",
    "src\gold\build_area_substitution.py",
    "src\gold\build_data_quality.py",
    "src\gold\build_repricing.py",
    "src\gold\build_market_overview.py",
    "src\gold\verify_gold.py"
)

Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    Invoke-Native { docker compose up -d minio minio-init iceberg-rest spark-master spark-worker } "Cannot start core Docker services"
    Invoke-SparkJobs $jobs "Gold"
    Invoke-Native { python -m pytest -q } "Unit tests failed"
} finally {
    Pop-Location
}

Write-Host "`nGOLD PIPELINE PASS" -ForegroundColor Green
