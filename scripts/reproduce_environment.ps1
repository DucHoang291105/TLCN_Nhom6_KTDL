param(
    [ValidateSet('Prepare', 'Check', 'Dashboard')][string]$Phase = 'Check'
)
$ErrorActionPreference = 'Stop'
. "$PSScriptRoot\pipeline_utils.ps1"
$root = Split-Path -Parent $PSScriptRoot
Push-Location $root
$started = Get-Date
$result = 'FAIL'
try {
    if ($Phase -eq 'Prepare') {
        Invoke-Native { git lfs install } 'Git LFS setup failed'
        Invoke-Native { git lfs pull } 'LFS download failed'
        if (-not (Test-Path .venv/Scripts/python.exe)) {
            Invoke-Native { python -m venv .venv } 'Virtual environment creation failed'
        }
        Invoke-Native { & .venv/Scripts/python.exe -m pip install -r requirements-dev.txt } 'Dependencies failed'
        Invoke-Native { & .venv/Scripts/python.exe -m src.orchestration.init_environment } 'Environment setup failed'
    }
    if (-not (Test-Path .venv/Scripts/python.exe)) { throw 'Run Prepare first.' }
    Invoke-Native { & .venv/Scripts/python.exe -m src.quality.check_environment } 'Host preflight failed'
    if ($Phase -eq 'Dashboard') {
        # The operator must already have completed the DAG successfully.
        Invoke-Native { & .venv/Scripts/python.exe -m src.orchestration.environment read } 'Read window failed'
        Invoke-Native { & .venv/Scripts/python.exe -m src.dashboard.provision_superset } 'Dashboard provisioning failed'
        Invoke-Native { & .venv/Scripts/python.exe -m src.quality.verify_pipeline_dashboard --output outputs/reproduction/dashboard_check.json } 'Dashboard verification failed'
    }
    $result = 'PASS'
} finally {
    $finished = Get-Date
    New-Item -ItemType Directory -Force outputs/reproduction | Out-Null
    [ordered]@{
        phase = $Phase; status = $result
        started_at = $started.ToUniversalTime().ToString('o')
        finished_at = $finished.ToUniversalTime().ToString('o')
        elapsed_seconds = ($finished - $started).TotalSeconds
        scope = 'One phase only; does not certify a clean-machine end-to-end run'
        clean_machine_certified = $false
    } | ConvertTo-Json | Set-Content -Encoding UTF8 "outputs/reproduction/$Phase.json"
    Pop-Location
}
