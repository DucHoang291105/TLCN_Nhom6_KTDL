param(
    [ValidateSet("core", "query", "application", "orchestration", "dashboard", "hadoop", "full")]
    [string]$Profile = "core",
    [switch]$AllowHighMemory
)

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    if ($Profile -eq "full" -and -not $AllowHighMemory) {
        throw "Profile 'full' can exhaust a 16 GB machine. Start one profile at a time, or add -AllowHighMemory."
    }
    if ($Profile -eq "core") {
        docker compose up -d
    } else {
        docker compose --profile $Profile up -d --build
    }
    if ($LASTEXITCODE -ne 0) { throw "Could not start profile: $Profile" }
    docker compose --profile $Profile ps
} finally {
    Pop-Location
}
