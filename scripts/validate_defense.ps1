param(
    [Parameter(Mandatory=$true)][string]$BatchId,
    [string]$RunId = ("defense_" + (Get-Date -Format "yyyyMMdd_HHmmss")),
    [string]$AsOf = ""
)
$ErrorActionPreference = "Stop"
. "$PSScriptRoot\pipeline_utils.ps1"
Push-Location (Split-Path -Parent $PSScriptRoot)
try {
    $python = if (Test-Path .venv/Scripts/python.exe) { ".venv/Scripts/python.exe" } else { "python" }
    $freeze = if ($AsOf) { @("--as-of", $AsOf) } else { @() }
    Invoke-Native { & $python -m src.orchestration.keep_awake -- $python -m src.quality.defense_suite --batch-id $BatchId --run-id $RunId @freeze } "Defense acceptance suite failed"
} finally {
    Pop-Location
}
