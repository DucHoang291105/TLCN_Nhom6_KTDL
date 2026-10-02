$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    docker compose --profile full ps
    Write-Output ""
    Write-Output "Service URLs"
    Write-Output "MinIO:            http://localhost:9001"
    Write-Output "Spark Master:     http://localhost:8080"
    Write-Output "Iceberg REST:     http://localhost:8181/v1/config"
    Write-Output "Trino:            http://localhost:8090"
    Write-Output "Airflow:          http://localhost:8082"
    Write-Output "Superset:         http://localhost:8088"
    Write-Output "FastAPI:          http://localhost:8000/docs"
    Write-Output "Web UI:           http://localhost:3000"
    Write-Output "HDFS NameNode:    http://localhost:9870"
    Write-Output "YARN:             http://localhost:8089"
} finally {
    Pop-Location
}
