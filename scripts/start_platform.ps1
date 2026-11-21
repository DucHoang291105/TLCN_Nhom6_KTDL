param(
    [ValidateSet("core", "query", "application", "orchestration", "dashboard", "hadoop", "full")]
    [string]$Profile = "core",
    [switch]$AllowHighMemory
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
    if ($Profile -eq "full" -and -not $AllowHighMemory) {
        throw "Profile 'full' can exhaust a 16 GB machine. Start one profile at a time, or add -AllowHighMemory."
    }
    $composeFiles = @("-f", "docker-compose.yml")
    if ($Profile -in @("application", "hadoop", "full")) { $composeFiles += @("-f", "docker-compose.experimental.yml") }
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
