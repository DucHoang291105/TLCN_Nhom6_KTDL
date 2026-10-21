"""Run Spark in the existing Docker cluster and propagate failures to Airflow."""
from __future__ import annotations
import csv
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCES = ("batdongsan", "guland", "nhadatvui")
PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0"


def validate_batch(batch):
    if len(batch) != 8 or datetime.strptime(batch, "%Y%m%d").strftime("%Y%m%d") != batch:
        raise ValueError("Batch must be an actual date in YYYYMMDD")
    return batch


def snapshot_path(source, batch):
    if source not in SOURCES:
        raise ValueError(f"Unknown source {source}")
    return PROJECT_ROOT / "data/incoming/snapshots" / source / validate_batch(batch) / f"{source}_raw.csv"


def prepare_web_source(source, batch):
    """Reuse existing exports; crawl only a missing current-day web batch."""
    from zoneinfo import ZoneInfo
    path = snapshot_path(source, batch)
    if path.is_file():
        summary_path = path.parent / "crawl_summary.json"
        if summary_path.exists() and json.loads(summary_path.read_text(encoding="utf-8")).get("failed_pages"):
            raise RuntimeError(f"Existing {source}/{batch} crawl is incomplete; inspect its summary")
        print(f"REUSE existing {source}/{batch}; no crawl requested")
        return
    if source not in ("guland", "nhadatvui"):
        raise ValueError("Batdongsan is supplied through the Chrome extension CSV export")
    today = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).strftime("%Y%m%d")
    if batch != today:
        raise ValueError("Cannot recreate a historical crawl by changing its batch date")
    env = {**os.environ, "CRAWL_BATCH_ID": batch, "CRAWL_PAGES": os.getenv(f"{source.upper()}_CRAWL_PAGES", "200")}
    subprocess.run([sys.executable, str(PROJECT_ROOT / f"src/ingestion/{source}/{source}_raw_crawler.py")], env=env, check=True)
    summary = json.loads((path.parent / "crawl_summary.json").read_text(encoding="utf-8"))
    if not path.exists() or not summary.get("records_written") or summary.get("failed_pages"):
        raise RuntimeError(f"Incomplete {source} crawl; inspect crawl_summary.json before retrying")


def require_bronze_pass():
    audit = json.loads((PROJECT_ROOT / "docs/validation/bronze_state_audit.json").read_text(encoding="utf-8"))
    if audit.get("status") != "PASS":
        raise RuntimeError("Bronze audit is not PASS")


def validate_snapshots(batch):
    csv.field_size_limit(2**31 - 1)
    entries = []
    for source in SOURCES:
        if not snapshot_path(source, batch).is_file():
            raise FileNotFoundError(f"Missing {source}/{batch}; export the manual CSV before continuing")
        for folder in sorted(snapshot_path(source, batch).parent.parent.iterdir()):
            if not folder.is_dir() or not folder.name.isdigit() or len(folder.name) != 8:
                continue
            path = snapshot_path(source, folder.name)
            with path.open(encoding="utf-8-sig", newline="") as handle:
                if handle.readline().startswith("version https://git-lfs.github.com/spec/v1"):
                    raise RuntimeError(f"LFS pointer only: run git lfs pull ({path})")
                handle.seek(0)
                reader = csv.DictReader(handle)
                if "listing_id" not in (reader.fieldnames or []):
                    raise ValueError(f"Missing listing_id column: {path}")
                n = 0
                max_page = None
                for row in reader:
                    if None in row:
                        raise ValueError(f"Malformed CSV record in {path} near row {n+2}")
                    n += 1
                    page = row.get("_source_page") or row.get("source_page") or row.get("page_number_attr")
                    try:
                        page = int(page)
                        max_page = max(max_page or 0, page)
                    except (TypeError, ValueError):
                        pass
            if n == 0:
                raise ValueError(f"Empty snapshot: {path}")
            checksum = hashlib.sha256()
            with path.open("rb") as handle:
                for block in iter(lambda: handle.read(1024 * 1024), b""):
                    checksum.update(block)
            entries.append({"source": source, "batch_id": folder.name, "rows": n, "columns": len(reader.fieldnames),
                            "max_source_page": max_page, "sha256": checksum.hexdigest(), "size_bytes": path.stat().st_size})
    payload = {"generated_at": datetime.now(timezone.utc).isoformat(), "expected_batch_id": batch,
               "snapshots": entries, "snapshot_rows": sum(r["rows"] for r in entries), "status": "PASS"}
    out = PROJECT_ROOT / "docs/validation/snapshot_manifest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Snapshot manifest PASS: {len(entries)} files, {payload['snapshot_rows']:,} rows")


def spark_command(app, args=()):
    relative = Path(app)
    if relative.is_absolute() or ".." in relative.parts or not app.startswith("src/") or not app.endswith(".py"):
        raise ValueError("Spark app must be a relative src/*.py path")
    return ["/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "--conf", "spark.jars.ivy=/tmp/.ivy2",
            "--conf", "spark.executorEnv.PYTHONPATH=/opt/project", "--packages", PACKAGES, "/opt/project/" + app, *args]


def run_spark(app, args=()):
    import docker
    client = docker.from_env(timeout=3000)
    try:
        container = client.containers.get("tlcn_spark_master")
        execution = client.api.exec_create(container.id, spark_command(app, args), environment={"PYTHONPATH": "/opt/project"})
        for chunk in client.api.exec_start(execution["Id"], stream=True):
            print(chunk.decode("utf-8", errors="replace"), end="", flush=True)
        code = client.api.exec_inspect(execution["Id"])["ExitCode"]
        if code != 0:
            raise RuntimeError(f"Spark job {app} failed (exit code {code})")
        if app.endswith("audit_bronze_state.py"):
            require_bronze_pass()
    finally:
        client.close()


def save_run_evidence(run_id):
    results = {}
    for name in ("snapshot_manifest", "bronze_state_audit", "silver_final_verification", "gold_verification", "gold_listing_lifecycle_summary"):
        payload = json.loads((PROJECT_ROOT / f"docs/validation/{name}.json").read_text(encoding="utf-8"))
        if payload.get("status") != "PASS":
            raise RuntimeError(f"Cannot publish successful run: {name} is not PASS")
        results[name] = {"status": payload["status"], "generated_at": payload.get("generated_at"), "checks": len(payload.get("checks", {}))}
    evidence = {"generated_at": datetime.now(timezone.utc).isoformat(), "dag_id": "lakehouse_pipeline", "run_id": run_id,
                "checks": results, "status": "PASS"}
    (PROJECT_ROOT / "docs/validation/airflow_pipeline_run.json").write_text(json.dumps(evidence, indent=2) + "\n", encoding="utf-8")
