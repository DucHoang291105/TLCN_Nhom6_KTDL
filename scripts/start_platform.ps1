param(
    [ValidateSet("core", "query", "orchestration", "dashboard")]
    [string]$Profile = "core"
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    if ($Profile -in @("query", "dashboard")) {
        $python = if (Test-Path .venv/Scripts/python.exe) { ".venv/Scripts/python.exe" } else { "python" }
        & $python -m src.orchestration.environment read
        if ($LASTEXITCODE -ne 0) { throw "Verified publication is required before starting readers" }
        return
    }
    $composeFiles = @("-f", "docker-compose.yml")
    if ($Profile -eq "core") {
        docker compose @composeFiles up -d
    } else {
        docker compose @composeFiles --profile $Profile up -d --build
    }
    if ($LASTEXITCODE -ne 0) { throw "Could not start profile: $Profile" }
    docker compose @composeFiles --profile $Profile ps
} finally {
    Pop-Location
}
