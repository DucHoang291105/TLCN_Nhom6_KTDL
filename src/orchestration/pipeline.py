"""Run Spark in the existing Docker cluster and propagate failures to Airflow."""
from __future__ import annotations
import json
import csv
import hashlib
import os
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCES = ("batdongsan", "guland", "nhadatvui")
PACKAGES = "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.apache.iceberg:iceberg-aws-bundle:1.11.0"

# Spark stages of lakehouse_pipeline, in order (the DAG builds its tasks from
# this). The DAG also has ACQUISITION_TASKS before them and publish_evidence
# after them; acceptance checks derive the expected task count from here.
STAGES = {
    "bronze": [("ingest_snapshots", "src/bronze/ingest_snapshots_bronze.py"), ("audit_bronze", "src/bronze/audit_bronze_state.py")],
    "silver": [("iceberg_smoke", "src/silver/iceberg_smoke_check.py"), ("listing_core", "src/silver/build_listing_core_spark.py"),
               ("listing_history", "src/silver/build_listing_history.py"), ("location", "src/silver/build_location.py"),
               ("feature", "src/silver/build_listing_feature.py"), ("verify_silver", "src/silver/verify_silver.py")],
    "gold": [("dimensions", "src/gold/build_dimensions.py"), ("fact_listing", "src/gold/build_fact_listing.py"),
             ("price_benchmark", "src/gold/build_price_benchmark.py"), ("price_model", "src/gold/build_price_model.py"),
             ("budget_tradeoff", "src/gold/build_budget_tradeoff.py"),
             ("area_substitution", "src/gold/build_area_substitution.py"),
             ("commute_choice", "src/gold/build_commute_choice.py"),
             ("data_quality", "src/gold/build_data_quality.py"),
             ("repricing", "src/gold/build_repricing.py"), ("market_overview", "src/gold/build_market_overview.py"),
             ("listing_lifecycle", "src/gold/build_listing_lifecycle.py"),
             ("bq_evidence", "src/gold/build_bq_evidence.py"), ("verify_gold", "src/gold/verify_gold.py")],
}
ACQUISITION_TASKS = ("prepare_guland", "prepare_nhadatvui", "wait_batdongsan_csv", "validate_snapshot_manifest")
DAG_TASK_COUNT = len(ACQUISITION_TASKS) + sum(len(jobs) for jobs in STAGES.values()) + 1


def failure_gate_tasks_valid(tasks):
    """A failed Guland probe blocks processing; the other crawler may finish."""
    expected = {f'acquisition.{t}' for t in ACQUISITION_TASKS} | {'publish_evidence'}
    expected.update(f'{group}.{name}' for group,jobs in STAGES.items() for name,_ in jobs)
    states = {t['task_id']:t['state'] for t in tasks}
    if len(states) != len(tasks) or set(states) != expected:
        return False
    guland, ndv = 'acquisition.prepare_guland', 'acquisition.prepare_nhadatvui'
    return (states[guland] == 'failed' and states[ndv] in {'success','failed','upstream_failed'}
            and all(state == 'upstream_failed' for name,state in states.items() if name not in {guland,ndv}))


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
        if summary_path.exists():
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            if summary.get("failed_pages") or summary.get("status", "PASS") != "PASS":
                raise RuntimeError(f"Existing {source}/{batch} crawl is incomplete; inspect its summary")
            if summary.get('csv_sha256') and csv_checksum(path) != summary['csv_sha256']:
                raise RuntimeError(f'Existing {source}/{batch} CSV changed after publication')
            if summary.get('crawl_contract_version') == 1:
                validate_crawl_output(path.parent, source, batch, summary.get('requested_pages'))
        print(f"REUSE existing {source}/{batch}; no crawl requested")
        return
    if source not in ("guland", "nhadatvui"):
        raise ValueError("Batdongsan is supplied through the Chrome extension CSV export")
    today = datetime.now(ZoneInfo("Asia/Ho_Chi_Minh")).strftime("%Y%m%d")
    if batch != today:
        raise ValueError("Cannot recreate a historical crawl by changing its batch date")
    from src.common.atomic_io import write_json_atomic
    from src.ingestion.crawl_runtime import integer_setting
    pages = integer_setting(f'{source.upper()}_CRAWL_PAGES', 20 if source == 'guland' else 200, maximum=5000)
    budget = integer_setting('CRAWL_RUN_TIMEOUT_SECONDS', 2100)
    # A sibling directory stays on the same bind mount in Docker. Its name
    # cannot be mistaken for a YYYYMMDD batch by Bronze/manifest discovery.
    staging = path.parent.parent / f'.{batch}.crawl-{uuid.uuid4().hex}'
    staging.mkdir(parents=True)
    env = {**os.environ, 'CRAWL_BATCH_ID':batch, 'CRAWL_PAGES':str(pages),
           'CRAWL_START_PAGE':'1', 'CRAWL_OUTPUT_DIR':str(staging),
           'CRAWL_RUN_TIMEOUT_SECONDS':str(budget), 'PYTHONUTF8':'1', 'PYTHONUNBUFFERED':'1'}
    try:
        subprocess.run([sys.executable, str(PROJECT_ROOT / f'src/ingestion/{source}/{source}_raw_crawler.py')],
                       env=env, check=True, timeout=budget + 15)
        summary = validate_crawl_output(staging, source, batch, pages)
        for name, filename in (('output_csv',f'{source}_raw.csv'),('output_jsonl',f'{source}_raw.jsonl'),
                               ('pages_dir','pages'),('log_file','crawl.log')):
            if name in summary:
                summary[name] = str(path.parent / filename)
        summary['publication'] = 'validated staging directory renamed into the canonical batch'
        write_json_atomic(staging / 'crawl_summary.json', summary)
        if path.parent.exists():
            raise RuntimeError(f'Batch directory appeared during crawl; preserve it: {path.parent}')
        staging.rename(path.parent)
        print(f'PUBLISH {source}/{batch}: {summary["records_written"]} records')
    except Exception as exc:
        marker = staging / 'crawl_summary.json'
        if marker.exists():
            try:
                failed = json.loads(marker.read_text(encoding='utf-8'))
                failed['worker_status'] = failed.get('status')
                failed['status'] = 'FAIL'
                failed['publication_error'] = str(exc)
                write_json_atomic(marker, failed)
            except (OSError, ValueError):
                pass
        raise RuntimeError(f'Incomplete {source} crawl; unpublished files retained at {staging}: {exc}') from exc


def csv_checksum(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def validate_crawl_output(folder, source, batch, requested_pages):
    if type(requested_pages) is not int or not 1 <= requested_pages <= 5000:
        raise ValueError('Invalid requested page count in crawl contract')
    folder = Path(folder)
    summary = json.loads((folder/'crawl_summary.json').read_text(encoding='utf-8'))
    pages = summary.get('completed_pages', [])
    start = summary.get('start_page', summary.get('started_page'))
    if (summary.get('status') != 'PASS' or summary.get('failed_pages') or
        summary.get('source') != source or summary.get('run_id') != batch or start != 1 or
        summary.get('requested_pages') != requested_pages or not pages or
        any(type(p) is not int for p in pages) or pages != list(range(1,len(pages)+1)) or
        len(pages) > requested_pages or (len(pages) < requested_pages and summary.get('end_of_listing') is not True)):
        raise ValueError('Crawl summary does not certify the requested page range')
    path = folder/f'{source}_raw.csv'
    with path.open(encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        if not {'listing_id','title','_scraped_at'} <= set(reader.fieldnames or []):
            raise ValueError('Crawl CSV does not match the source contract')
        rows = 0
        for row in reader:
            if not (row.get('listing_id') or '').strip() or not (row.get('_scraped_at') or '').strip():
                raise ValueError('Crawl CSV contains an incomplete row')
            rows += 1
    if rows <= 0 or rows != summary.get('records_written'):
        raise ValueError('CSV row count does not match the successful crawl summary')
    summary['csv_sha256'] = csv_checksum(path)
    return summary


def require_bronze_pass():
    audit = json.loads((PROJECT_ROOT / "docs/validation/bronze_state_audit.json").read_text(encoding="utf-8"))
    if audit.get("status") != "PASS":
        raise RuntimeError("Bronze audit is not PASS")


def validate_snapshots(batch, force_rescan=False):
    from src.orchestration.snapshot_manifest import build_manifest
    return build_manifest(PROJECT_ROOT, SOURCES, batch, force_rescan=force_rescan,
                          workers=int(os.getenv("SNAPSHOT_HASH_WORKERS", "4")))


def spark_command(app, args=()):
    relative = Path(app)
    if relative.is_absolute() or ".." in relative.parts or not app.startswith("src/") or not app.endswith(".py"):
        raise ValueError("Spark app must be a relative src/*.py path")
    return ["/opt/spark/bin/spark-submit", "--master", "spark://spark-master:7077", "--conf", "spark.jars.ivy=/home/spark/.ivy2",
            "--conf", "spark.executorEnv.PYTHONPATH=/opt/project", "--packages", PACKAGES, "/opt/project/" + app, *args]


def require_catalog_write_window(client):
    """The local SQLite catalog requires a separate Spark write window."""
    for service in client.containers.list(all=True, filters={"name": "^/tlcn_trino$"}):
        if service.status not in {"created", "exited", "dead"}:
            raise RuntimeError("Trino is active: stop tlcn_trino before the Spark pipeline writes to the local SQLite Iceberg catalog; restart the query profile after the DAG finishes")


def run_spark(app, args=()):
    import docker
    client = docker.from_env(timeout=3000)
    try:
        require_catalog_write_window(client)
        container = client.containers.get("tlcn_spark_master")
        owner = 'manual'
        try:
            from airflow.sdk import get_current_context
            owner = get_current_context()['run_id']
        except (ImportError, RuntimeError):
            pass
        execution = client.api.exec_create(container.id,
            ['python3', '-m', 'src.orchestration.publication', app, *args],
            environment={"PYTHONPATH": "/opt/project", "LAKEHOUSE_RUN_ID": owner})
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
    from src.common.atomic_io import write_json_atomic
    from src.orchestration.publication import catalog_lock, require_published
    with catalog_lock(PROJECT_ROOT):
        publication = require_published(PROJECT_ROOT)
        if publication.get('run_id') != run_id:
            raise RuntimeError('The published tables belong to another pipeline run')
    results = {}
    for name in ("snapshot_manifest", "bronze_state_audit", "silver_final_verification", "gold_verification", "gold_listing_lifecycle_summary"):
        payload = json.loads((PROJECT_ROOT / f"docs/validation/{name}.json").read_text(encoding="utf-8"))
        if payload.get("status") != "PASS":
            raise RuntimeError(f"Cannot publish successful run: {name} is not PASS")
        results[name] = {"status": payload["status"], "generated_at": payload.get("generated_at"), "checks": len(payload.get("checks", {}))}
    evidence = {"generated_at": datetime.now(timezone.utc).isoformat(), "dag_id": "lakehouse_pipeline", "run_id": run_id,
                "checks": results, "status": "PASS"}
    write_json_atomic(PROJECT_ROOT / "docs/validation/airflow_pipeline_run.json", evidence)
