$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    docker compose --profile orchestration --profile query --profile dashboard ps
    Write-Output ""
    Write-Output "Service URLs"
    Write-Output "MinIO:            http://localhost:9001"
    Write-Output "Spark Master:     http://localhost:8080"
    Write-Output "Iceberg REST:     http://localhost:8181/v1/config"
    Write-Output "Trino:            http://localhost:8090"
    Write-Output "Airflow:          http://localhost:8082"
    Write-Output "Superset:         http://localhost:8088"
} finally {
    Pop-Location
}
