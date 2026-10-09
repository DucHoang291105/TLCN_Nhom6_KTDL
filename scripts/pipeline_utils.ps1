# Shared helpers for the Silver/Gold pipeline scripts.
# Windows PowerShell 5.1 turns native stderr (Docker/Spark progress logs) into
# error records; with $ErrorActionPreference = "Stop" that aborts a healthy run
# whenever output is redirected. Native commands are therefore run with
# "Continue" and judged only by their exit code.

function Invoke-Native {
    param([Parameter(Mandatory = $true)][scriptblock]$Command, [string]$FailMessage)
    $previous = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $Command 2>&1 | ForEach-Object { "$_" }
        $code = $LASTEXITCODE
    } finally {
        $ErrorActionPreference = $previous
    }
    if ($code -ne 0) { throw "$FailMessage (exit code $code)" }
}

function Invoke-SparkJobs {
    param([string[]]$Jobs, [string]$PipelineName)
    foreach ($job in $Jobs) {
        Write-Host "`n=== RUN $job ===" -ForegroundColor Cyan
        Invoke-Native { & "$PSScriptRoot\run_spark.ps1" $job } "$PipelineName pipeline failed at $job"
    }
}
