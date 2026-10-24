"""SCD2 history and recoverable, per-listing Iceberg MERGE.

Only affected source_ids are recomputed, including their older observations.
This handles late batches, corrections, deletions and A->B->A transitions.
The state table is committed AFTER history; a snapshot mismatch triggers a
full reconciliation on retry rather than trusting a partially advanced cursor.
"""
from __future__ import annotations

import re
import time
import uuid
from src.common.utils import CANONICAL_COLUMNS

BASE_COLUMNS = CANONICAL_COLUMNS + ["batch_id", "snapshot_date", "bronze_ingested_at", "record_hash", "dq_status", "dq_reasons", "completeness_score"]
HISTORY_COLUMNS = BASE_COLUMNS + ["version_number", "valid_from", "valid_to", "is_current_version"]
KEYS = ["source_id", "version_number"]
STATE_COLUMNS = ["source_id", "observation_signature"]
SNAPSHOT_PROPERTY = "pipeline.history-snapshot-id"


def identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}", value):
        raise ValueError(f"Invalid SQL identifier: {value}")
    return ".".join(f"`{part}`" for part in value.split("."))


def merge_sql(target, view, columns, keys):
    target, view = identifier(target), identifier(view)
    quoted = {c: identifier(c) for c in columns}
    on = " AND ".join(f"t.{quoted[c]} = s.{quoted[c]}" for c in keys)
    same = " AND ".join(f"t.{quoted[c]} <=> s.{quoted[c]}" for c in columns)
    update = ", ".join(f"t.{quoted[c]} = s.{quoted[c]}" for c in columns if c not in keys)
    names = ", ".join(quoted.values())
    values = ", ".join(f"s.{quoted[c]}" for c in columns)
    return f"""MERGE INTO {target} t USING {view} s ON {on}
WHEN MATCHED AND s._delete THEN DELETE
WHEN MATCHED AND NOT s._delete AND NOT ({same}) THEN UPDATE SET {update}
WHEN NOT MATCHED AND NOT s._delete THEN INSERT ({names}) VALUES ({values})"""


def history_from(observations):
    from pyspark.sql import Window, functions as F
    observed = F.coalesce(F.col("snapshot_date").cast("timestamp"), F.col("scraped_at"), F.col("bronze_ingested_at"))
    order = Window.partitionBy("source_id").orderBy(observed, F.col("scraped_at").asc_nulls_last(), "batch_id", "record_hash")
    changed = observations.withColumn("_previous_hash", F.lag("record_hash").over(order)).filter(
        F.col("_previous_hash").isNull() | (F.col("_previous_hash") != F.col("record_hash")))
    result = changed.withColumn("version_number", F.row_number().over(order)).withColumn("valid_from", observed)
    versions = Window.partitionBy("source_id").orderBy("version_number")
    return result.withColumn("valid_to", F.lead("valid_from").over(versions)).withColumn(
        "is_current_version", F.col("valid_to").isNull()).select(*HISTORY_COLUMNS)


def observation_signatures(observations):
    from pyspark.sql import functions as F
    # Include ALL history inputs, not only record_hash: metadata-only corrections
    # may change the row selected at a version boundary without a business change.
    row_hash = F.sha2(F.to_json(F.struct(*[F.col(c) for c in BASE_COLUMNS]), {"ignoreNullFields": "false"}), 256)
    return observations.select("source_id", row_hash.alias("_row_hash")).groupBy("source_id").agg(
        F.sha2(F.to_json(F.sort_array(F.collect_list("_row_hash"))), 256).alias("observation_signature"))


def snapshot_id(spark, table):
    if not spark.catalog.tableExists(table):
        return None
    iceberg = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    snap = iceberg.currentSnapshot()
    return str(snap.snapshotId()) if snap is not None else "empty"


def snapshot_marker(spark, state):
    if not spark.catalog.tableExists(state):
        return None
    rows = spark.sql(f"SHOW TBLPROPERTIES {identifier(state)}").collect()
    return {r["key"]: r["value"] for r in rows}.get(SNAPSHOT_PROPERTY)


def mark_state(spark, target, state):
    value = snapshot_id(spark, target)
    spark.sql(f"ALTER TABLE {identifier(state)} SET TBLPROPERTIES ('{SNAPSHOT_PROPERTY}'='{value}')")


def staged_changes(desired, existing, keys):
    from pyspark.sql import functions as F
    removed = existing.join(desired.select(*keys), keys, "left_anti")
    return desired.withColumn("_delete", F.lit(False)).unionByName(removed.withColumn("_delete", F.lit(True)))


def execute_merge(spark, target, stage, columns, keys):
    view = "history_stage_" + uuid.uuid4().hex
    stage.createOrReplaceTempView(view)
    try:
        spark.sql(merge_sql(target, view, columns, keys))
    finally:
        spark.catalog.dropTempView(view)


def sync_history(spark, observations, target, state, mode="incremental"):
    from pyspark.sql import functions as F
    from pyspark.storagelevel import StorageLevel
    from src.common.spark_session import table_location, write_iceberg_table
    if mode not in ("incremental", "rebuild"):
        raise ValueError(f"Unknown history mode {mode}")
    identifier(target); identifier(state)
    if target == state:
        raise ValueError("History and state tables must differ")
    started = time.perf_counter()
    # Do not advance any state when input grain is invalid.
    if observations.filter("source_id IS NULL OR record_hash IS NULL").limit(1).count():
        raise ValueError("History requires non-null source_id and record_hash")
    if observations.filter("coalesce(cast(snapshot_date AS timestamp), scraped_at, bronze_ingested_at) IS NULL").limit(1).count():
        raise ValueError("History requires an observation time")
    if observations.groupBy("source_id", "batch_id").count().filter("count>1").limit(1).count():
        raise ValueError("Duplicate observation grain (source_id, batch_id)")
    signatures = observation_signatures(observations).persist(StorageLevel.MEMORY_AND_DISK)
    input_ids = signatures.count()
    before = snapshot_id(spark, target)
    affected = desired = stage = None
    try:
        if mode == "rebuild":
            desired = history_from(observations).persist(StorageLevel.MEMORY_AND_DISK)
            write_iceberg_table(desired, target, partition_by=("snapshot_date",))
            write_iceberg_table(signatures, state)
            mark_state(spark, target, state)
            affected_count, operation = input_ids, "FULL_REBUILD"
            staged_rows = desired.count()
        else:
            existed = spark.catalog.tableExists(target)
            trusted = existed and spark.catalog.tableExists(state) and snapshot_marker(spark, state) == before
            if existed:
                actual_location = spark.sql(f"DESCRIBE TABLE EXTENDED {identifier(target)}").filter("col_name='Location'").first()["data_type"].rstrip("/")
                if actual_location != table_location(target):
                    raise ValueError("Unexpected history location; no data will be purged automatically")
                iceberg = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, target)
                partition_text = str(iceberg.spec())
                if "identity(" not in partition_text or "snapshot_date" not in partition_text:
                    spark.sql(f"ALTER TABLE {identifier(target)} ADD PARTITION FIELD snapshot_date")
            else:
                write_iceberg_table(history_from(observations.limit(0)), target, partition_by=("snapshot_date",))
            existing = spark.table(target)
            if not spark.catalog.tableExists(state):
                write_iceberg_table(signatures.limit(0), state)
            old_state = spark.table(state)
            if trusted:
                joined = signatures.alias("n").join(old_state.alias("p"), "source_id", "full")
                affected = joined.filter(~F.col("n.observation_signature").eqNullSafe(F.col("p.observation_signature"))).select("source_id")
            else:
                affected = signatures.select("source_id").union(existing.select("source_id")).union(old_state.select("source_id")).distinct()
            affected = affected.persist(StorageLevel.MEMORY_AND_DISK)
            affected_count = affected.count()
            operation = "MERGE_AFFECTED_IDS" if trusted else "MERGE_RECONCILE"
            staged_rows = 0
            if affected_count:
                desired = history_from(observations.join(affected, "source_id")).persist(StorageLevel.MEMORY_AND_DISK)
                stage = staged_changes(desired, existing.join(affected, "source_id").select(*HISTORY_COLUMNS), KEYS).persist(StorageLevel.MEMORY_AND_DISK)
                staged_rows = stage.count()
                execute_merge(spark, target, stage, HISTORY_COLUMNS, KEYS)
                # A crash before either of these steps leaves an old marker; retry
                # reconciles safely. Never advance state before the history commit.
                wanted_state = signatures.join(affected, "source_id").select(*STATE_COLUMNS)
                prior_state = old_state.join(affected, "source_id").select(*STATE_COLUMNS)
                execute_merge(spark, state, staged_changes(wanted_state, prior_state, ["source_id"]), STATE_COLUMNS, ["source_id"])
                mark_state(spark, target, state)
            else:
                operation = "NO_CHANGE"
                if not trusted:
                    mark_state(spark, target, state)
        elapsed = time.perf_counter() - started
        return {"mode": mode, "operation": operation, "input_source_ids": input_ids,
            "affected_source_ids": affected_count, "staged_history_rows": staged_rows,
            "history_rows": spark.table(target).count(), "history_table": target, "state_table": state,
            "snapshot_before": before, "snapshot_after": snapshot_id(spark, target),
            "elapsed_seconds": round(elapsed, 4), "partition_by": ["snapshot_date"], "status": "PASS"}
    finally:
        for frame in (stage, desired, affected, signatures):
            if frame is not None:
                frame.unpersist()
