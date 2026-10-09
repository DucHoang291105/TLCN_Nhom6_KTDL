$ErrorActionPreference = "Stop"
. "$PSScriptRoot\pipeline_utils.ps1"

# Rebuild Silver and Gold twice from the same Bronze input and compare the
# row count of every table. Evidence: docs/validation/deterministic_rebuild.json
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    foreach ($run in 1, 2) {
        Write-Host "`n##### REBUILD $run #####" -ForegroundColor Yellow
        & "$PSScriptRoot\run_silver_pipeline.ps1"
        & "$PSScriptRoot\run_gold_pipeline.ps1"
        $runDir = "outputs\determinism\run$run"
        New-Item -ItemType Directory -Force $runDir | Out-Null
        Copy-Item "docs\validation\silver_final_verification.json", "docs\validation\gold_verification.json" $runDir
    }
    Invoke-Native { python -m src.common.compare_rebuild_counts outputs\determinism\run1 outputs\determinism\run2 } "Rebuilds are not deterministic"
} finally {
    Pop-Location
}

Write-Host "`nDETERMINISTIC REBUILD PASS" -ForegroundColor Green
