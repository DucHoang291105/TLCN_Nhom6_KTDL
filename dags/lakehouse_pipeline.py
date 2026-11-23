import sys
from datetime import timedelta
import pendulum
from airflow.sdk import DAG, Param, TaskGroup, get_current_context
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor

if "/opt/project" not in sys.path:
    sys.path.insert(0, "/opt/project")
from src.orchestration.pipeline import STAGES, prepare_web_source, run_spark, save_run_evidence, snapshot_path, validate_snapshots


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


        [guland, nhadatvui] >> bds >> ready
    previous = acquisition
    for group, jobs in STAGES.items():
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
