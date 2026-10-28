"""Exercise the Windows runner without starting Spark or changing containers."""
import shutil
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.skipif(sys.platform != "win32" or not shutil.which("powershell"), reason="Windows PowerShell runner")
@pytest.mark.parametrize("mode,expected", [("success", 0), ("job_failure", 7), ("trino_active", 31)])
def test_spark_runner_stderr_exit_and_catalog_guard(tmp_path, mode, expected):
    harness = tmp_path / "probe.ps1"
    harness.write_text(r'''
param($Runner, $Python, $Mode, $Log)
$ErrorActionPreference = 'Stop'
$script:execCalled = $false
function docker {
    if ($args[0] -eq 'ps') {
        $global:LASTEXITCODE = 0
        if ($Mode -eq 'trino_active') { 'running' } else { 'exited' }
        return
    }
    $script:execCalled = $true
    $code = if ($Mode -eq 'job_failure') { 7 } else { 0 }
    & $Python -c "import sys; sys.stderr.write('Spark INFO probe'); sys.exit($code)"
}
try {
    & $Runner 'src/quality/probe.py' *> $Log
    exit $LASTEXITCODE
} catch {
    if ($Mode -eq 'trino_active' -and -not $script:execCalled -and $_.Exception.Message -like '*Trino is active*') { exit 31 }
    Write-Output $_.Exception.Message
    exit 99
}
''', encoding="utf-8")
    runner = Path(__file__).resolve().parents[2] / "scripts/run_spark.ps1"
    result = subprocess.run([
        "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(harness),
        str(runner), sys.executable, mode, str(tmp_path / "runner.log"),
    ], capture_output=True, text=True, timeout=30)
    assert result.returncode == expected, result.stdout + result.stderr
    if mode != "trino_active":
        assert "Spark INFO probe" in (tmp_path / "runner.log").read_text(encoding="utf-16")
