"""Run the complete local pipeline/operation acceptance suite without fake data."""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from src.common.atomic_io import write_json_atomic
from src.orchestration.pipeline import validate_snapshots
from src.quality.defense_evidence import ROOT, capture, read, verify


def execute(work, name, command):
    print(f"START {name}", flush=True)
    write_json_atomic(work / "progress.json", {"stage": name, "state": "running", "at": datetime.now(timezone.utc).isoformat()})
    environment = {**os.environ, "PATH": str(Path(sys.executable).parent) + os.pathsep + os.environ.get("PATH", ""),
                   "PYTHONPATH": str(ROOT), "PYTHONUTF8": "1"}
    with (work / f"{name}.log").open("wb") as log:
        result = subprocess.run(command, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, env=environment)
    if result.returncode:
        print((work / f"{name}.log").read_text(encoding="utf-8", errors="replace")[-5000:], flush=True)
        raise RuntimeError(f"{name} failed with exit code {result.returncode}; later stages were not run")
    print(f"PASS {name}", flush=True)


def airflow(action, run_id, *args):
    return ["docker", "exec", "tlcn_airflow", "python", "-m", "src.orchestration.airflow_run",
            action, "--run-id", run_id, *args]


def wait_run(work, run_id, output, expect_failure=False):
    deadline, previous = time.monotonic() + 10800, None
    while time.monotonic() < deadline:
        response = subprocess.run(airflow("status", run_id, "--concise"), cwd=ROOT,
                                  capture_output=True, text=True, check=True)
        status = json.loads(response.stdout)
        if status != previous:
            print(json.dumps(status), flush=True)
            write_json_atomic(work / "progress.json", {"stage": output, **status})
            previous = status
        if status["state"] in {"success", "failed"}:
            extra = ["--expect-failure"] if expect_failure else []
            execute(work, output, airflow("export", run_id, "--output", output + ".json", "--concise", *extra))
            if status["state"] != ("failed" if expect_failure else "success"):
                raise RuntimeError(f"Unexpected Airflow state: {status}")
            return
        time.sleep(15)
    raise TimeoutError(f"Airflow run {run_id} exceeded the verification timeout")


def suite(batch, run_id, attach=False):
    if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
        raise ValueError("Run ID must contain only letters, digits, _ or -")
    work = ROOT / "outputs/defense" / run_id
    work.mkdir(parents=True, exist_ok=True)
    if (work / "baseline.json").exists():
        raise ValueError("Use a new run ID; previous release evidence must not be overwritten")
    shutil.copytree(ROOT / "docs/validation", work / "before", dirs_exist_ok=True)
    validate_snapshots(batch)
    baseline = capture(ROOT, batch, run_id)
    if attach:
        existing = subprocess.run(airflow("status", run_id), capture_output=True, text=True, check=True, cwd=ROOT)
        run = json.loads(existing.stdout)
        if run["batch_id"] != batch:
            raise ValueError("Attached DAG run uses a different batch")
        baseline["attached_run"] = True
        baseline["code_captured_at"] = baseline["started_at"]
        baseline["started_at"] = run["start_date"].replace("Z", "+00:00")
    write_json_atomic(work / "baseline.json", baseline)
    execute(work, "unit-tests", [sys.executable, "-m", "pytest", "-q", f"--junitxml={work / 'unit-tests.xml'}"])
    execute(work, "compile", [sys.executable, "-m", "compileall", "-q", "src", "dags", "tests"])
    execute(work, "compose-config", ["docker", "compose", "config", "--quiet"])
    failure = run_id + "_failure_gate"
    if not attach:
        execute(work, "write-window", [sys.executable, "-m", "src.orchestration.environment", "write"])
        # Fault injection runs first so the latest normal DAG finishes green.
        execute(work, "failure-trigger", airflow("trigger", failure, "--batch-id", "20000101"))
        wait_run(work, failure, "airflow_failure_gate", expect_failure=True)
        execute(work, "trigger", airflow("trigger", run_id, "--batch-id", batch, "--history-mode", "incremental"))
    wait_run(work, run_id, "integration_airflow_run")
    if attach:
        execute(work, "failure-trigger", airflow("trigger", failure, "--batch-id", "20000101"))
        wait_run(work, failure, "airflow_failure_gate", expect_failure=True)
    # Also exercise current manual-ingest preflight after an attached run.
    ps = ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File"]
    execute(work, "bronze-preflight", [*ps, "scripts/run_spark.ps1", "src/bronze/ingest_snapshots_bronze.py", "--skip-existing"])
    execute(work, "two-rebuilds", [*ps, "scripts/check_deterministic_rebuild.ps1"])
    for number in (1, 2):
        shutil.copy2(ROOT / f"outputs/determinism/run{number}.json", work / f"run{number}.json")
    for job in ("verify_history_incremental", "verify_lifecycle_fixtures", "demo_iceberg_features"):
        execute(work, job, [*ps, "scripts/run_spark.ps1", f"src/quality/{job}.py"])
    execute(work, "read-window", [sys.executable, "-m", "src.orchestration.environment", "read"])
    archive = f"/app/superset_home/before_{run_id}.zip"
    execute(work, "dashboard-backup", ["docker", "exec", "tlcn_superset", "superset", "export-dashboards", "-f", archive])
    execute(work, "dashboard-backup-copy", ["docker", "cp", f"tlcn_superset:{archive}", str(work / "superset_before.zip")])
    execute(work, "dashboard", [sys.executable, "-m", "src.dashboard.provision_superset"])
    execute(work, "dashboard-integration", [sys.executable, "-m", "src.quality.verify_pipeline_dashboard"])
    guard = subprocess.run([*ps, "scripts/run_spark.ps1", "src/quality/__guard_probe__.py"],
                           cwd=ROOT, capture_output=True, text=True, timeout=30)
    blocked = guard.returncode != 0 and "Trino is active" in guard.stdout + guard.stderr
    write_json_atomic(work / "manual_writer_guard.json", {"status": "PASS" if blocked else "FAIL", "exit_code": guard.returncode})
    payload = verify(ROOT, work)
    write_json_atomic(work / "progress.json", {"stage": "complete", "state": payload["status"]})
    print(f"DEFENSE SUITE {payload['status']}: {work}", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--run-id", default="defense_" + datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).strftime("%Y%m%d_%H%M%S"))
    parser.add_argument("--attach-run", action="store_true", help="Continue testing a known DAG run; export its actual start time")
    args = parser.parse_args()
    suite(args.batch_id, args.run_id, args.attach_run)


if __name__ == "__main__":
    main()
