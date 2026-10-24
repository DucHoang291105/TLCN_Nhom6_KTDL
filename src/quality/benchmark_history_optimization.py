"""Alternating controlled benchmark of frozen history versus optimized implementation."""
from __future__ import annotations
import argparse
import hashlib
import importlib.util
import json
import statistics
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from src.common.atomic_io import write_json_atomic
from src.common.spark_session import build_spark_session, write_iceberg_table, CATALOG_NAME
from src.silver.history import sync_history, sync_history_table, snapshot_id, SNAPSHOT_PROPERTY
from src.quality.verify_history_incremental import compare, fixture_checks


def measurements(values):
    return {"seconds": values, "median_seconds": round(statistics.median(values), 4),
            "min_seconds": min(values), "max_seconds": max(values)}


def cache_fixtures(spark, ns, reference):
    from src.silver.build_listing_core_spark import OBSERVATION_SCHEMA
    source, target, state = (f"{ns}.history_cache_{name}" for name in ("input", "output", "state"))
    def row(sid, day, value):
        result = {c: None for c in OBSERVATION_SCHEMA.fieldNames()}
        result.update(source_id=sid, source="fixture", batch_id=f"202610{day:02d}", snapshot_date=date(2026, 10, day),
                      record_hash=value, bronze_ingested_at=datetime(2026, 10, 24), completeness_score=0.5, dq_reasons=[])
        return result
    rows = [row("x", 16, "A"), row("y", 16, "Q")]
    frame = spark.createDataFrame(rows, OBSERVATION_SCHEMA)
    write_iceberg_table(frame, source)
    sync_history_table(spark, source, target, state, "rebuild")
    checks = {}
    before = snapshot_id(spark, target)
    fast = sync_history_table(spark, source, target, state)
    checks["unchanged_snapshot_fast_no_write"] = fast["operation"] == "UNCHANGED_INPUT_SNAPSHOT" and snapshot_id(spark, target) == before
    verified = sync_history_table(spark, source, target, state, force_verify=True)
    checks["force_verify_bypasses_cache"] = verified["input_validation"] == "FULL_INPUT_SCAN"
    rows.append(row("x", 20, "B"))
    frame = spark.createDataFrame(rows, OBSERVATION_SCHEMA)
    write_iceberg_table(frame, source)
    changed = sync_history_table(spark, source, target, state)
    checks["new_input_snapshot_invalidates_cache"] = changed["input_validation"] == "FULL_INPUT_SCAN" and compare(spark.table(target), reference.history_from(frame))["equal"]
    spark.sql(f"UPDATE {state} SET observation_signature='tampered' WHERE source_id='x'")
    repaired = sync_history_table(spark, source, target, state)
    checks["state_data_change_invalidates_cache"] = repaired["input_validation"] == "FULL_INPUT_SCAN" and repaired["affected_source_ids"] == 1
    spark.sql(f"UPDATE {target} SET completeness_score=0.0 WHERE source_id='x'")
    repaired = sync_history_table(spark, source, target, state)
    checks["history_data_change_forces_reconcile"] = repaired["operation"] == "MERGE_RECONCILE" and compare(spark.table(target), reference.history_from(frame))["equal"]
    spark.sql(f"ALTER TABLE {state} SET TBLPROPERTIES ('{SNAPSHOT_PROPERTY}'='stale-checkpoint')")
    repaired = sync_history_table(spark, source, target, state)
    checks["checkpoint_property_change_invalidates_cache"] = repaired["operation"] == "MERGE_RECONCILE"
    spark.sql(f"ALTER TABLE {source} ADD COLUMNS (unused_reference STRING)")
    evolved = sync_history_table(spark, source, target, state)
    checks["input_schema_metadata_change_invalidates_cache"] = evolved["input_validation"] == "FULL_INPUT_SCAN"
    before = snapshot_id(spark, target)
    write_iceberg_table(frame.unionByName(frame.limit(1)), source)
    try:
        sync_history_table(spark, source, target, state)
    except ValueError as error:
        checks["duplicate_input_rejected_before_commit"] = "Duplicate observation grain" in str(error) and snapshot_id(spark, target) == before
    else:
        checks["duplicate_input_rejected_before_commit"] = False
    write_iceberg_table(frame, source)
    sync_history_table(spark, source, target, state)
    spark.sql(f"ALTER TABLE {target} ADD COLUMNS (unexpected_column STRING)")
    before = snapshot_id(spark, target)
    try:
        sync_history_table(spark, source, target, state)
    except ValueError as error:
        checks["unexpected_target_schema_fails_closed"] = "Unexpected history/state schema" in str(error) and snapshot_id(spark, target) == before
    else:
        checks["unexpected_target_schema_fails_closed"] = False
    spark.sql(f"ALTER TABLE {target} DROP COLUMN unexpected_column")
    sync_history_table(spark, source, target, state)
    checks["valid_input_recovers_after_failure"] = compare(spark.table(target), reference.history_from(frame))["equal"]
    assert all(checks.values()), checks
    return checks


def main():
    from pyspark.sql import functions as F
    from pyspark.storagelevel import StorageLevel
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    if not 2 <= args.repetitions <= 5:
        raise ValueError("Use 2-5 repetitions")
    spec = importlib.util.spec_from_file_location("frozen_history", ROOT / "tests/reference/history_a6909ae.py")
    reference = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reference)
    assert hashlib.sha256(Path(reference.__file__).read_bytes()).hexdigest() == "798273d98a09adcfd46d1c5a6e1466af3845cc605f556aac61b4654d5a5bb447"
    spark = build_spark_session("BenchmarkPersonAHistoryOptimization")
    try:
        ns = f"{CATALOG_NAME}.smoke_test"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
        input_table = f"{CATALOG_NAME}.silver.listing_observation"
        observations = spark.table(input_table).persist(StorageLevel.MEMORY_AND_DISK)
        n = observations.count()
        baseline = observations.filter(~F.col("source").isin("batdongsan", "guland", "nhadatvui") | (F.col("batch_id") <= "20261016"))
        tables = {name: f"{ns}.history_optimization_{name}" for name in ("before", "after", "full")}
        timings = {key: [] for key in ("before_merge", "after_merge", "before_no_change", "after_no_change", "full_rebuild", "snapshot_fast_path")}
        comparisons, samples = [], []
        for iteration in range(args.repetitions):
            for name in ("before", "after"):
                reference.sync_history(spark, baseline, tables[name], tables[name]+"_state", "rebuild")
            order = ("before", "after") if iteration % 2 == 0 else ("after", "before")
            sample = {"iteration": iteration+1, "order": list(order)}
            for name in order:
                function = reference.sync_history if name == "before" else sync_history
                sample[name] = function(spark, observations, tables[name], tables[name]+"_state")
                timings[name+"_merge"].append(sample[name]["elapsed_seconds"])
                retry = function(spark, observations, tables[name], tables[name]+"_state")
                assert retry["operation"] == "NO_CHANGE" and retry["snapshot_before"] == retry["snapshot_after"]
                timings[name+"_no_change"].append(retry["elapsed_seconds"])
            comparison = compare(spark.table(tables["before"]), spark.table(tables["after"]))
            assert comparison["equal"]
            comparisons.append(comparison)
            samples.append(sample)
            print("Controlled benchmark iteration: " + json.dumps(sample), flush=True)
            full = sync_history(spark, observations, tables["full"], tables["full"]+"_state", "rebuild")
            timings["full_rebuild"].append(full["elapsed_seconds"])
        comparison_full = compare(spark.table(tables["after"]), spark.table(tables["full"]))
        assert comparison_full["equal"]
        sync_history_table(spark, input_table, tables["after"], tables["after"]+"_state")
        for _ in range(args.repetitions):
            fast = sync_history_table(spark, input_table, tables["after"], tables["after"]+"_state")
            assert fast["operation"] == "UNCHANGED_INPUT_SNAPSHOT" and fast["snapshot_before"] == fast["snapshot_after"]
            timings["snapshot_fast_path"].append(fast["elapsed_seconds"])
        fixtures = fixture_checks(spark, ns)
        guarded = cache_fixtures(spark, ns, reference)
        observations.unpersist()
        payload = {"generated_at": datetime.now(timezone.utc).isoformat(), "baseline_commit": "a6909aed578f86cd5c7181e861ed55688c237012",
                   "observation_rows": n, "samples": samples, "timings": {k: measurements(v) for k,v in timings.items()},
                   "comparisons": comparisons, "comparison_full_rebuild": comparison_full, "scd2_fixtures": fixtures,
                   "cache_fixtures": guarded, "status": "PASS",
                   "timing_scope": "One Docker Spark app, identical cached observation input, 3 controlled repetitions by default; AB/BA alternating order. Seeding and exact comparisons excluded from timers. Snapshot fast path is a separate unchanged-input case, not new-batch MERGE."}
        write_json_atomic(ROOT / "docs/validation/person_a_history_optimization.json", payload)
        print(json.dumps(payload, indent=2), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
