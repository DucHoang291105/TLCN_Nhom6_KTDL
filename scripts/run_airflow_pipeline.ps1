param(
    [string]$BatchId = (Get-Date -Format "yyyyMMdd"),
    [string]$RunId = ("manual__" + (Get-Date -Format "yyyyMMdd_HHmmss"))
)
$ErrorActionPreference = "Stop"
. "$PSScriptRoot\pipeline_utils.ps1"
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    Invoke-Native { docker compose --profile orchestration up -d --build --wait --wait-timeout 180 } "Cannot start orchestration services"
    Invoke-Native { docker exec tlcn_airflow python -m src.orchestration.airflow_run trigger --batch-id $BatchId --run-id $RunId } "Cannot trigger Airflow run (wait for Airflow to be ready, then retry)"
    Write-Host "Run $RunId submitted: http://localhost:8082/dags/lakehouse_pipeline"
    Write-Host "Status: docker exec tlcn_airflow python -m src.orchestration.airflow_run status --run-id $RunId"
} finally {
    Pop-Location
}
