"""Tuesday/Friday lakehouse pipeline; Batdongsan arrives as a Chrome CSV export."""
import sys
from datetime import timedelta
import pendulum
from airflow.sdk import DAG, Param, TaskGroup, get_current_context
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor

if "/opt/project" not in sys.path:
    sys.path.insert(0, "/opt/project")
from src.orchestration.pipeline import prepare_web_source, run_spark, save_run_evidence, snapshot_path, validate_snapshots


def batch_id():
    context = get_current_context()
    requested = context["params"].get("batch_id")
    logical_date = context.get("logical_date") or pendulum.now("Asia/Ho_Chi_Minh")
    return requested or pendulum.instance(logical_date).in_timezone("Asia/Ho_Chi_Minh").strftime("%Y%m%d")


def prepare(source):
    prepare_web_source(source, batch_id())


def manual_export_ready():
    return snapshot_path("batdongsan", batch_id()).is_file()


def manifest():
    validate_snapshots(batch_id())


def publish():
    save_run_evidence(get_current_context()["run_id"])


def run_history():
    run_spark("src/silver/build_listing_history.py", ("--mode", get_current_context()["params"]["history_mode"]))


with DAG(
    dag_id="lakehouse_pipeline", description="CSV → Bronze → Silver → verify → Gold/lifecycle → verify",
    schedule="0 22 * * 2,5", start_date=pendulum.datetime(2026, 10, 16, tz="Asia/Ho_Chi_Minh"),
    catchup=False, max_active_runs=1, max_active_tasks=1,
    default_args={"owner": "group6", "retries": 0, "execution_timeout": timedelta(minutes=45)},
    params={"batch_id": Param("", type="string", pattern="^([0-9]{8})?$", description="YYYYMMDD; empty uses run date in Vietnam"),
            "history_mode": Param("incremental", enum=["incremental", "rebuild"], type="string")},
    tags=["lakehouse", "pipeline"],
) as dag:
    with TaskGroup("acquisition") as acquisition:
        guland = PythonOperator(task_id="prepare_guland", python_callable=prepare, op_kwargs={"source": "guland"})
        nhadatvui = PythonOperator(task_id="prepare_nhadatvui", python_callable=prepare, op_kwargs={"source": "nhadatvui"})
        bds = PythonSensor(task_id="wait_batdongsan_csv", python_callable=manual_export_ready,
                           mode="reschedule", poke_interval=60, timeout=7200)
        ready = PythonOperator(task_id="validate_snapshot_manifest", python_callable=manifest)
        guland >> nhadatvui >> bds >> ready
    previous = acquisition
    stages = {
        "bronze": [("ingest_snapshots", "src/bronze/ingest_snapshots_bronze.py"), ("audit_bronze", "src/bronze/audit_bronze_state.py")],
        "silver": [("iceberg_smoke", "src/silver/iceberg_smoke_check.py"), ("listing_core", "src/silver/build_listing_core_spark.py"),
                   ("listing_history", "src/silver/build_listing_history.py"), ("location", "src/silver/build_location.py"),
                   ("feature", "src/silver/build_listing_feature.py"), ("verify_silver", "src/silver/verify_silver.py")],
        "gold": [("dimensions", "src/gold/build_dimensions.py"), ("fact_listing", "src/gold/build_fact_listing.py"),
                 ("price_benchmark", "src/gold/build_price_benchmark.py"), ("price_model", "src/gold/build_price_model.py"),
                 ("budget_tradeoff", "src/gold/build_budget_tradeoff.py"),
                 ("area_substitution", "src/gold/build_area_substitution.py"), ("data_quality", "src/gold/build_data_quality.py"),
                 ("repricing", "src/gold/build_repricing.py"), ("market_overview", "src/gold/build_market_overview.py"),
                 ("listing_lifecycle", "src/gold/build_listing_lifecycle.py"), ("verify_gold", "src/gold/verify_gold.py")],
    }
    for group, jobs in stages.items():
        with TaskGroup(group) as tasks:
            last = None
            for task_id, app in jobs:
                kwargs = {"app": app}
                if task_id == "ingest_snapshots":
                    kwargs["args"] = ["--skip-existing"]
                task = PythonOperator(task_id=task_id, python_callable=run_history if task_id == "listing_history" else run_spark,
                                      op_kwargs={} if task_id == "listing_history" else kwargs)
                if last is not None:
                    last >> task
                last = task
        previous >> tasks
        previous = tasks
    previous >> PythonOperator(task_id="publish_evidence", python_callable=publish)
