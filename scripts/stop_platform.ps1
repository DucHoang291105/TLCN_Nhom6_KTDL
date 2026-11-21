param([switch]$All)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    if ($All) { docker compose -f docker-compose.yml -f docker-compose.experimental.yml --profile full stop } else { docker compose stop }
    if ($LASTEXITCODE -ne 0) { throw "Could not stop Docker services" }
} finally {
    Pop-Location
}
