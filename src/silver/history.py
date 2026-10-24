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
import hashlib
import json
import base64
from pathlib import Path
from src.common.utils import CANONICAL_COLUMNS
from src.common.writer_lock import history_writer_lock

BASE_COLUMNS = CANONICAL_COLUMNS + ["batch_id", "snapshot_date", "bronze_ingested_at", "record_hash", "dq_status", "dq_reasons", "completeness_score"]
HISTORY_COLUMNS = BASE_COLUMNS + ["version_number", "valid_from", "valid_to", "is_current_version"]
KEYS = ["source_id", "version_number"]
STATE_COLUMNS = ["source_id", "observation_signature"]
SNAPSHOT_PROPERTY = "pipeline.history-snapshot-id"
CACHE_PROPERTY = "pipeline.history-input-cache-v1"
LOGIC_DIGEST = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


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


def observation_state(observations):
    from pyspark.sql import functions as F
    # Include ALL history inputs, not only record_hash: metadata-only corrections
    # may change the row selected at a version boundary without a business change.
    row_hash = F.sha2(F.to_json(F.struct(*[F.col(c) for c in BASE_COLUMNS]), {"ignoreNullFields": "false"}), 256)
    invalid_key = F.col("source_id").isNull() | (F.trim("source_id") == "") | F.col("record_hash").isNull() | (F.trim("record_hash") == "") | F.col("batch_id").isNull() | (F.trim("batch_id") == "")
    invalid_time = F.coalesce(F.col("snapshot_date").cast("timestamp"), F.col("scraped_at"), F.col("bronze_ingested_at")).isNull()
    return observations.select("source_id", "batch_id", row_hash.alias("_row_hash"),
        invalid_key.cast("int").alias("_invalid_key"), invalid_time.cast("int").alias("_invalid_time")).groupBy("source_id").agg(
        F.sha2(F.to_json(F.sort_array(F.collect_list("_row_hash"))), 256).alias("observation_signature"),
        F.count("*").alias("_rows"), F.size(F.collect_set("batch_id")).alias("_batches"),
        F.sum("_invalid_key").alias("_invalid_key"), F.sum("_invalid_time").alias("_invalid_time"))


def observation_signatures(observations):
    return observation_state(observations).select(*STATE_COLUMNS)


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
    columns = desired.columns
    joined = desired.withColumn("_desired_present", F.lit(True)).alias("n").join(existing.alias("p"),
        [F.col(f"n.{c}") == F.col(f"p.{c}") for c in keys], "full")
    different = ~F.struct(*[F.col(f"n.{c}") for c in columns]).eqNullSafe(F.struct(*[F.col(f"p.{c}") for c in columns]))
    present = F.col("n._desired_present").isNotNull()
    return joined.filter(different).select(*[
        (F.coalesce(F.col(f"n.{c}"), F.col(f"p.{c}")) if c in keys else
         F.when(present, F.col(f"n.{c}")).otherwise(F.col(f"p.{c}"))).alias(c) for c in columns],
         (~present).alias("_delete"))


def execute_merge(spark, target, stage, columns, keys):
    view = "history_stage_" + uuid.uuid4().hex
    stage.createOrReplaceTempView(view)
    try:
        spark.sql(merge_sql(target, view, columns, keys))
    finally:
        spark.catalog.dropTempView(view)


def _sync_history(spark, observations, target, state, mode="incremental"):
    from pyspark.sql import functions as F
    from pyspark.storagelevel import StorageLevel
    from src.common.spark_session import table_location, write_iceberg_table
    if mode not in ("incremental", "rebuild"):
        raise ValueError(f"Unknown history mode {mode}")
    identifier(target); identifier(state)
    if target == state:
        raise ValueError("History and state tables must differ")
    started = time.perf_counter()
    # One fused scan/grouping validates grain and computes all signatures.
    summary = observation_state(observations).persist(StorageLevel.MEMORY_AND_DISK)
    affected = desired = stage = None
    try:
        stats = summary.agg(F.count("*").alias("ids"), F.sum("_rows").alias("rows"),
            F.sum("_invalid_key").alias("invalid_keys"), F.sum("_invalid_time").alias("invalid_times"),
            F.sum(F.when(F.col("_rows") != F.col("_batches"), 1).otherwise(0)).alias("duplicates")).first()
        if stats.invalid_keys:
            raise ValueError("History requires non-null source_id, record_hash and batch_id")
        if stats.invalid_times:
            raise ValueError("History requires an observation time")
        if stats.duplicates:
            raise ValueError("Duplicate observation grain (source_id, batch_id)")
        input_ids = stats.ids
        signatures = summary.select(*STATE_COLUMNS)
        before = snapshot_id(spark, target)
        for table in (target, state):
            if spark.catalog.tableExists(table):
                loaded = iceberg_table(spark, table)
                actual = loaded.location().rstrip("/")
                if actual != table_location(table):
                    raise ValueError(f"Unexpected history/state location: {table}; no automatic purge allowed")
                expected_columns = HISTORY_COLUMNS if table == target else STATE_COLUMNS
                if mode == "incremental" and [field.name() for field in loaded.schema().columns()] != expected_columns:
                    raise ValueError(f"Unexpected history/state schema: {table}; inspect before a reviewed rebuild")
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
                if staged_rows:
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
            "affected_source_ids": affected_count, "observation_rows": stats.rows or 0, "staged_history_rows": staged_rows,
            "history_rows": spark.table(target).count(), "history_table": target, "state_table": state,
            "snapshot_before": before, "snapshot_after": snapshot_id(spark, target),
            "elapsed_seconds": round(elapsed, 4), "partition_by": ["snapshot_date"], "status": "PASS"}
    finally:
        for frame in (stage, desired, affected, summary):
            if frame is not None:
                frame.unpersist()


def iceberg_table(spark, table):
    identifier(table)
    result = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, table)
    result.refresh()
    return result


def metadata_token(table):
    return str(table.operations().current().metadataFileLocation())


def table_snapshot(table):
    current = table.currentSnapshot()
    return str(current.snapshotId()) if current is not None else "empty"


def metadata_count(table):
    current = table.currentSnapshot()
    if current is None:
        return 0
    props = current.summary()
    if any(int(props.get(k) or "0") for k in ("total-position-deletes", "total-equality-deletes")):
        return None
    return int(props.get("total-records")) if props.get("total-records") is not None else None


def cache_hit(cached, expected, counts):
    if not isinstance(cached, dict) or cached.get("guard") != expected:
        return None
    result = cached.get("result", {})
    if not isinstance(result, dict):
        return None
    names = ("observation_rows", "input_source_ids", "history_rows")
    if any(type(result.get(k)) is not int or result[k] < 0 or counts.get(k) != result[k] for k in names):
        return None
    return {k: result[k] for k in names}


def sync_history(spark, observations, target, state, mode="incremental"):
    with history_writer_lock(target):
        return _sync_history(spark, observations, target, state, mode)


def sync_history_table(spark, input_table, target, state, mode="incremental", force_verify=False):
    """Fast no-op only for identical input/history metadata AND state data snapshot.

    No wall-clock/mtime watermark is trusted. If any guard differs, fall back to
    full input validation/signatures, including late corrections and deletions.
    """
    from src.common.spark_session import table_location
    for table in (input_table, target, state):
        identifier(table)
    if len({input_table, target, state}) != 3:
        raise ValueError("Input, history and state tables must differ")
    if mode not in ("incremental", "rebuild"):
        raise ValueError("Unknown history mode")
    started = time.perf_counter()
    with history_writer_lock(target):
        source = iceberg_table(spark, input_table)
        input_token = metadata_token(source)
        timezone = spark.conf.get("spark.sql.session.timeZone")
        if mode == "incremental" and not force_verify and all(spark.catalog.tableExists(t) for t in (target, state)):
            history, ledger = iceberg_table(spark, target), iceberg_table(spark, state)
            if history.location().rstrip("/") != table_location(target) or ledger.location().rstrip("/") != table_location(state):
                raise ValueError("Unexpected history/state location; no automatic purge allowed")
            expected = {"input_table": input_table, "input_metadata": input_token, "history_metadata": metadata_token(history),
                        "state_snapshot": table_snapshot(ledger), "state_schema": str(ledger.schema()),
                        "logic": LOGIC_DIGEST, "timezone": timezone}
            try:
                encoded = ledger.properties().get(CACHE_PROPERTY)
                cached = json.loads(base64.urlsafe_b64decode(encoded).decode()) if encoded else None
            except (ValueError, TypeError):
                cached = None
            counts = {"observation_rows": metadata_count(source), "input_source_ids": metadata_count(ledger), "history_rows": metadata_count(history)}
            result = cache_hit(cached, expected, counts)
            if result is not None and ledger.properties().get(SNAPSHOT_PROPERTY) == table_snapshot(history):
                return {**result, "mode": mode, "operation": "UNCHANGED_INPUT_SNAPSHOT", "affected_source_ids": 0,
                    "staged_history_rows": 0, "history_table": target, "state_table": state,
                    "snapshot_before": table_snapshot(history), "snapshot_after": table_snapshot(history),
                    "elapsed_seconds": round(time.perf_counter()-started, 4), "partition_by": ["snapshot_date"],
                    "status": "PASS", "input_metadata": input_token, "input_validation": "UNCHANGED_VERIFIED_SNAPSHOT"}
        current = source.currentSnapshot()
        observations = (spark.read.option("snapshot-id", str(current.snapshotId())).table(input_table)
                        if current is not None else spark.table(input_table))
        result = _sync_history(spark, observations, target, state, mode)
        history, ledger = iceberg_table(spark, target), iceberg_table(spark, state)
        guard = {"input_table": input_table, "input_metadata": input_token, "history_metadata": metadata_token(history),
                 "state_snapshot": table_snapshot(ledger), "state_schema": str(ledger.schema()),
                 "logic": LOGIC_DIGEST, "timezone": timezone}
        cache = json.dumps({"guard": guard, "result": {k: result[k] for k in ("observation_rows", "input_source_ids", "history_rows")}}, separators=(",", ":"))
        encoded = base64.urlsafe_b64encode(cache.encode()).decode()
        spark.sql(f"ALTER TABLE {identifier(state)} SET TBLPROPERTIES ('{CACHE_PROPERTY}'='{encoded}')")
        result.update(input_metadata=input_token, input_validation="FULL_INPUT_SCAN",
                      elapsed_seconds=round(time.perf_counter()-started, 4))
        return result
