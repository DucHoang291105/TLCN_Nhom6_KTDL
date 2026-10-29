"""Switch the local SQLite catalog between a Spark write window and query UI."""
from __future__ import annotations

import argparse
import json
import subprocess
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ACTIVE_RUNS = """
import json
from src.orchestration.airflow_run import session
c = session()
offset = 0
active = []
while True:
    r = c.get('http://localhost:8080/api/v2/dags/lakehouse_pipeline/dagRuns',
              params={'limit': 100, 'offset': offset}, timeout=30)
    r.raise_for_status()
    data = r.json()
    rows = data['dag_runs']
    active.extend(d['dag_run_id'] for d in rows if d['state'] in ('running', 'queued'))
    offset += len(rows)
    if not rows or offset >= data['total_entries']:
        break
print(json.dumps(active))
"""


def state(name):
    result = subprocess.run(["docker", "ps", "-a", "--filter", f"name=^/{name}$", "--format", "{{.State}}"],
                            check=True, capture_output=True, text=True, cwd=ROOT)
    return result.stdout.strip() or "absent"


def active_work():
    apps, runs = [], []
    spark_state, airflow_state = state("tlcn_spark_master"), state("tlcn_airflow")
    allowed = {"absent", "created", "exited", "dead", "running"}
    if spark_state not in allowed or airflow_state not in allowed:
        raise RuntimeError("Spark/Airflow is paused or restarting; cannot certify an idle writer")
    if spark_state == "running":
        with urllib.request.urlopen("http://localhost:8080/json", timeout=15) as response:
            apps = [app["id"] for app in json.load(response)["activeapps"]]
    if airflow_state == "running":
        result = subprocess.run(["docker", "exec", "tlcn_airflow", "python", "-c", ACTIVE_RUNS],
                                check=True, capture_output=True, text=True, cwd=ROOT)
        runs = json.loads(result.stdout)
    return {"spark_apps": apps, "airflow_runs": runs}


def assert_idle(work):
    if work["spark_apps"] or work["airflow_runs"]:
        raise RuntimeError(f"Pipeline still active; do not switch the catalog window: {work}")


def switch(mode):
    assert_idle(active_work())
    if mode == "check":
        print("No active Spark application or Airflow run")
        return
    if mode == "write":
        for name in ("tlcn_superset", "tlcn_trino"):
            if state(name) not in {"absent", "created", "exited", "dead"}:
                subprocess.run(["docker", "stop", name], check=True, cwd=ROOT)
        command = ["docker", "compose", "--profile", "orchestration", "up", "-d", "--wait",
                   "--wait-timeout", "180", "minio", "iceberg-rest", "postgres", "spark-master", "spark-worker", "airflow"]
    else:
        command = ["docker", "compose", "--profile", "query", "--profile", "dashboard", "up", "-d", "--wait",
                   "--wait-timeout", "180", "trino", "superset"]
    subprocess.run(command, check=True, cwd=ROOT)
    print(f"Local catalog window: {mode}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("write", "read", "check"))
    switch(parser.parse_args().mode)


if __name__ == "__main__":
    main()
