$ErrorActionPreference = "Stop"
. "$PSScriptRoot\pipeline_utils.ps1"

# Rebuild Silver and Gold twice from the same Bronze input and compare, per
# table, row count, distinct grain keys and a content hash of all rows
# (build-time columns excluded). Evidence: docs/validation/deterministic_rebuild.json
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    foreach ($run in 1, 2) {
        Write-Host "`n##### REBUILD $run #####" -ForegroundColor Yellow
        & "$PSScriptRoot\run_silver_pipeline.ps1"
        & "$PSScriptRoot\run_gold_pipeline.ps1"
        Invoke-Native { & "$PSScriptRoot\run_spark.ps1" "src\common\table_fingerprints.py" "outputs/determinism/run$run.json" } "Fingerprint job failed (run $run)"
    }
    Invoke-Native { python -m src.common.compare_rebuild_counts outputs\determinism\run1.json outputs\determinism\run2.json } "Rebuilds are not deterministic"
} finally {
    Pop-Location
}

Write-Host "`nDETERMINISTIC REBUILD PASS" -ForegroundColor Green
