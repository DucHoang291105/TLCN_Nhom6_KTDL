"""End-to-end smoke test for Spark -> Iceberg REST -> MinIO."""
from __future__ import annotations
import json, sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path: sys.path.insert(0, str(PROJECT_ROOT))
from src.common.spark_session import CATALOG_NAME, SMOKE_NAMESPACE, build_spark_session, table_location, write_iceberg_table

def main() -> None:
    spark = build_spark_session("IcebergSmokeTest")
    namespace = f"{CATALOG_NAME}.{SMOKE_NAMESPACE}"; table = f"{namespace}.silver_io_test"
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    data = spark.createDataFrame([(1, "Silver", datetime(2026, 10, 3, 16, 0, 0)), (2, "Iceberg", datetime(2026, 10, 3, 16, 1, 0))], ["id", "label", "observed_at"])
    write_iceberg_table(data, table)
    read_back = spark.table(table).orderBy("id")
    count = read_back.count()
    if count != 2 or read_back.columns != data.columns:
        raise RuntimeError("Iceberg smoke-test read-back mismatch")
    summary = {"generated_at":datetime.now(timezone.utc).isoformat(),"table":table,"location":table_location(table),"row_count":count,"columns":read_back.columns,"spark_version":spark.version,"status":"PASS"}
    for out in (PROJECT_ROOT/"outputs/validation",PROJECT_ROOT/"docs/validation"):
        out.mkdir(parents=True,exist_ok=True); (out/"iceberg_smoke_test.json").write_text(json.dumps(summary,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(summary,indent=2)); spark.sql(f"DROP TABLE IF EXISTS {table} PURGE"); spark.sql(f"DROP NAMESPACE IF EXISTS {namespace}"); spark.stop()

if __name__ == "__main__": main()


