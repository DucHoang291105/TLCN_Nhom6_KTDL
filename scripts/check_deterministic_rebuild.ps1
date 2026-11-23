$ErrorActionPreference = "Stop"
. "$PSScriptRoot\pipeline_utils.ps1"

$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    foreach ($run in 1, 2) {
        Write-Host "`n##### REBUILD $run #####" -ForegroundColor Yellow
        & "$PSScriptRoot\run_silver_pipeline.ps1" -HistoryMode rebuild
        & "$PSScriptRoot\run_gold_pipeline.ps1"
        Invoke-Native { & "$PSScriptRoot\run_spark.ps1" "src\common\table_fingerprints.py" "outputs/determinism/run$run.json" } "Fingerprint job failed (run $run)"
    }
    Invoke-Native { python -m src.common.compare_rebuild_counts outputs\determinism\run1.json outputs\determinism\run2.json } "Rebuilds are not deterministic"
} finally {
    Pop-Location
}

Write-Host "`nDETERMINISTIC REBUILD PASS" -ForegroundColor Green
