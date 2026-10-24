"""Execute auditable time travel, schema evolution and partition demos in sandbox."""
from __future__ import annotations
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from src.common.spark_session import build_spark_session, CATALOG_NAME, table_location
from src.silver.history import snapshot_id


def main():
    spark = build_spark_session("DemoIcebergFeatures")
    statements, results, checks = [], {}, {}
    try:
        ns = f"{CATALOG_NAME}.smoke_test"
        table = f"{ns}.features_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")

        def execute(sql, label=None):
            statements.append(sql + ";")
            rows = spark.sql(sql).collect()
            if label:
                results[label] = [r.asDict(recursive=True) for r in rows]
            return rows

        execute(f"CREATE TABLE {table} (id BIGINT, price BIGINT, snapshot_date DATE) USING iceberg PARTITIONED BY (snapshot_date) LOCATION '{table_location(table)}' TBLPROPERTIES ('format-version'='2')")
        execute(f"INSERT INTO {table} VALUES (1, 100, DATE '2026-10-16'), (2, 200, DATE '2026-10-20')")
        original = snapshot_id(spark, table)
        execute(f"MERGE INTO {table} t USING (SELECT CAST(1 AS BIGINT) id, CAST(120 AS BIGINT) price, DATE '2026-10-16' snapshot_date UNION ALL SELECT CAST(3 AS BIGINT), CAST(300 AS BIGINT), DATE '2026-10-23') s ON t.id=s.id WHEN MATCHED THEN UPDATE SET price=s.price WHEN NOT MATCHED THEN INSERT *")
        current = execute(f"SELECT * FROM {table} ORDER BY id", "current_after_merge")
        old = execute(f"SELECT * FROM {table} VERSION AS OF {original} ORDER BY id", "as_of_original_snapshot")
        checks["time_travel_old_price_and_row_count"] = len(old) == 2 and old[0].price == 100
        checks["merge_update_and_insert"] = len(current) == 3 and current[0].price == 120
        files_before = {r.file_path for r in execute(f"SELECT file_path FROM {table}.files")}
        execute(f"ALTER TABLE {table} ADD COLUMNS (quality_note STRING)")
        files_after = {r.file_path for r in execute(f"SELECT file_path FROM {table}.files")}
        evolved = execute(f"SELECT id, quality_note FROM {table} ORDER BY id", "after_add_column")
        checks["schema_add_preserves_rows_as_null"] = len(evolved) == 3 and all(r.quality_note is None for r in evolved)
        checks["schema_add_is_metadata_only"] = files_before == files_after
        execute(f"ALTER TABLE {table} RENAME COLUMN quality_note TO review_note")
        checks["schema_rename_preserves_rows"] = "review_note" in spark.table(table).columns and spark.table(table).count() == 3
        execute(f"SELECT snapshot_id, operation, summary FROM {table}.snapshots ORDER BY committed_at", "snapshots")
        partitions = execute(f"SELECT partition, record_count FROM {table}.partitions ORDER BY partition", "partitions")
        checks["snapshot_date_partition"] = len(partitions) == 3 and sum(r.record_count for r in partitions) == 3
        execute(f"DESCRIBE TABLE {table}", "schema")
        payload = {"generated_at": datetime.now(timezone.utc).isoformat(), "demo_table": table,
                   "original_snapshot_id": original, "checks": checks, "outputs": results,
                   "status": "PASS" if all(checks.values()) else "FAIL"}
        sql_path = ROOT / "docs/demo/iceberg_features.sql"
        sql_path.parent.mkdir(parents=True, exist_ok=True)
        sql_path.write_text("-- Executed Spark SQL, isolated sandbox; use a new table name to rerun.\n\n" + "\n\n".join(statements) + "\n", encoding="utf-8")
        for folder in ("outputs/validation", "docs/validation"):
            out = ROOT / folder / "iceberg_features.json"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
        print(json.dumps(payload, indent=2, default=str))
        if payload["status"] != "PASS":
            raise RuntimeError("Iceberg demonstration failed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
