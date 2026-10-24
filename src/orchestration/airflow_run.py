"""Local-only Airflow API client; credentials stay in the container's persisted volume."""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import requests


def session():
    from airflow.configuration import conf
    password_file = conf.get("core", "simple_auth_manager_passwords_file", fallback="") or "/opt/airflow/simple_auth_manager_passwords.json.generated"
    credentials = json.loads(Path(password_file).read_text())
    response = requests.post("http://localhost:8080/auth/token", json={"username": "admin", "password": credentials["admin"]}, timeout=30)
    response.raise_for_status()
    client = requests.Session()
    client.headers["Authorization"] = "Bearer " + response.json()["access_token"]
    return client


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=("trigger", "status", "export"))
    parser.add_argument("--batch-id", default="20261016")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--output", default="airflow_task_states.json")
    parser.add_argument("--concise", action="store_true")
    parser.add_argument("--expect-failure", action="store_true")
    args = parser.parse_args()
    client = session()
    root = "http://localhost:8080/api/v2/dags/lakehouse_pipeline"
    if args.action == "trigger":
        # Register the manual run before enabling the scheduler; one active run at a time.
        now = datetime.now(timezone.utc).isoformat()
        response = client.post(root + "/dagRuns", json={"dag_run_id": args.run_id, "logical_date": now,
            "conf": {"batch_id": args.batch_id}}, timeout=30)
        response.raise_for_status()
        response = client.patch(root, json={"is_paused": False}, timeout=30)
        response.raise_for_status()
        print(json.dumps({"run_id": args.run_id, "batch_id": args.batch_id, "state": "queued"}))
        return
    from urllib.parse import quote
    response = client.get(root + "/dagRuns/" + quote(args.run_id, safe=""), timeout=30)
    response.raise_for_status()
    run = response.json()
    response = client.get(root + "/dagRuns/" + quote(args.run_id, safe="") + "/taskInstances?limit=100", timeout=30)
    response.raise_for_status()
    tasks = response.json()["task_instances"]
    for task in tasks:
        if task.get("duration") is None and task.get("start_date") and task.get("end_date"):
            start = datetime.fromisoformat(task["start_date"].replace("Z", "+00:00"))
            end = datetime.fromisoformat(task["end_date"].replace("Z", "+00:00"))
            task["duration"] = (end - start).total_seconds()
    payload = {"dag_id": "lakehouse_pipeline", "run_id": args.run_id, "state": run["state"],
        "batch_id": run.get("conf", {}).get("batch_id"), "start_date": run.get("start_date"), "end_date": run.get("end_date"),
        "schedule": "0 22 * * 2,5", "timezone": "Asia/Ho_Chi_Minh",
        "tasks": [{k: t.get(k) for k in ("task_id", "state", "start_date", "end_date", "duration", "try_number")} for t in tasks]}
    if args.expect_failure:
        checks = {"run_failed": run["state"] == "failed",
                  "first_task_failed": any(t["task_id"] == "acquisition.prepare_guland" and t["state"] == "failed" for t in tasks),
                  "all_downstream_blocked": all(t["state"] == "upstream_failed" for t in tasks if t["task_id"] != "acquisition.prepare_guland")}
        payload.update({"expected_failure_probe": True, "checks": checks, "verification_status": "PASS" if all(checks.values()) else "FAIL"})
        if not all(checks.values()):
            raise RuntimeError("Failure-gate probe did not block all downstream tasks")
    if args.action == "export":
        if Path(args.output).name != args.output:
            raise ValueError("Output must be a filename in docs/validation")
        out = Path("/opt/project/docs/validation") / args.output
        out.write_text(json.dumps(payload, indent=2) + "\n")
    if args.concise:
        from collections import Counter
        print(json.dumps({"run_id": args.run_id, "state": run["state"], "tasks": dict(Counter(t["state"] for t in tasks))}))
    else:
        print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
