param(
    [Parameter(Mandatory=$true)][string]$BatchId,
    [string]$RunId = ("defense_" + (Get-Date -Format "yyyyMMdd_HHmmss"))
)
$ErrorActionPreference = "Stop"
. "$PSScriptRoot\pipeline_utils.ps1"
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    $python = if (Test-Path .venv/Scripts/python.exe) { ".venv/Scripts/python.exe" } else { "python" }
    Invoke-Native { & $python -m src.quality.defense_suite --batch-id $BatchId --run-id $RunId } "Defense acceptance suite failed"
} finally {
    Pop-Location
}
