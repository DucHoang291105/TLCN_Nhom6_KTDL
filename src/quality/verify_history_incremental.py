"""Execute real Iceberg MERGE vs rebuild, retry and adversarial SCD2 checks."""
from __future__ import annotations
import argparse
import hashlib
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from src.common.spark_session import build_spark_session, ensure_silver_namespace, CATALOG_NAME
from src.silver.history import HISTORY_COLUMNS, SNAPSHOT_PROPERTY, history_from, sync_history, snapshot_id


def fingerprint(frame):
    from pyspark.sql import functions as F
    digest = hashlib.sha256()
    rows = 0
    # Multiset hash: include duplicate row hashes, globally sorted, no volatile
    # columns excluded. JSON column order and session timezone are fixed.
    hashes = frame.select(F.sha2(F.to_json(F.struct(*[F.col(c) for c in HISTORY_COLUMNS]),
                         {"ignoreNullFields": "false"}), 256).alias("h")).orderBy("h")
    for row in hashes.toLocalIterator():
        digest.update(bytes.fromhex(row.h))
        rows += 1
    return {"rows": rows, "sha256_sorted_row_hashes": digest.hexdigest()}


def compare(left, right):
    a, b = fingerprint(left), fingerprint(right)
    left_only = left.select(*HISTORY_COLUMNS).exceptAll(right.select(*HISTORY_COLUMNS)).count()
    right_only = right.select(*HISTORY_COLUMNS).exceptAll(left.select(*HISTORY_COLUMNS)).count()
    return {"merge": a, "rebuild": b, "merge_only_rows": left_only, "rebuild_only_rows": right_only,
            "equal": a == b and left_only == right_only == 0}


def fixture_checks(spark, ns):
    from pyspark.sql import functions as F
    from src.silver.build_listing_core_spark import OBSERVATION_SCHEMA
    target, state = f"{ns}.history_scd2_fixture", f"{ns}.history_scd2_fixture_state"

    def row(listing, batch, value, **updates):
        values = {name: None for name in OBSERVATION_SCHEMA.fieldNames()}
        values.update(source="fixture", source_group="crawl", source_id=listing, ad_id=listing,
                      batch_id=batch, snapshot_date=datetime.strptime(batch, "%Y%m%d").date(),
                      scraped_at=datetime.strptime(batch, "%Y%m%d"), bronze_ingested_at=datetime(2026, 10, 24),
                      record_hash=value, dq_status="PASS", dq_reasons=[], completeness_score=0.5)
        values.update(updates)
        return values

    checks = {}
    def exercise(name, rows, expected, mode="incremental"):
        frame = spark.createDataFrame(rows, OBSERVATION_SCHEMA)
        result = sync_history(spark, frame, target, state, mode)
        actual = spark.table(target).orderBy("source_id", "version_number").collect()
        timeline = [(r.source_id, r.record_hash, r.snapshot_date.isoformat()) for r in actual]
        assert timeline == expected, (name, timeline, expected)
        for listing in {r.source_id for r in actual}:
            versions = [r for r in actual if r.source_id == listing]
            for i, current in enumerate(versions):
                following = versions[i+1] if i+1 < len(versions) else None
                assert current.version_number == i + 1
                assert current.valid_from.date() == current.snapshot_date
                assert current.valid_to == (following.valid_from if following else None)
                assert current.is_current_version == (following is None)
        assert compare(spark.table(target), history_from(frame))["equal"]
        checks[name] = {"status": "PASS", "operation": result["operation"], "rows": len(actual)}
        print(f"SCD2 fixture PASS: {name}", flush=True)
        return frame, result

    initial = [row("x", "20261016", "A"), row("x", "20261020", "A"), row("y", "20261016", "Q")]
    exercise("collapse_consecutive_hashes", initial, [("x", "A", "2026-10-16"), ("y", "Q", "2026-10-16")], "rebuild")
    changed = initial + [row("x", "20261023", "B"), row("x", "20261024", "A")]
    expected = [("x", "A", "2026-10-16"), ("x", "B", "2026-10-23"), ("x", "A", "2026-10-24"), ("y", "Q", "2026-10-16")]
    frame, result = exercise("a_b_a_is_three_versions", changed, expected)
    exercise("late_arrival_resequences_intervals", changed + [row("x", "20261018", "C")],
             [("x", "A", "2026-10-16"), ("x", "C", "2026-10-18"), ("x", "A", "2026-10-20")] + expected[1:])
    corrected = initial + [row("x", "20261023", "A"), row("x", "20261024", "A")]
    exercise("correction_deletes_obsolete_versions", corrected, [("x", "A", "2026-10-16"), ("y", "Q", "2026-10-16")])
    metadata_fix = [dict(r, completeness_score=0.9) if r["source_id"] == "x" else r for r in corrected]
    frame, result = exercise("metadata_only_correction", metadata_fix, [("x", "A", "2026-10-16"), ("y", "Q", "2026-10-16")])
    assert result["affected_source_ids"] == 1
    assert spark.table(target).filter("source_id='x'").first().completeness_score == 0.9
    before = snapshot_id(spark, target)
    result = sync_history(spark, frame, target, state)
    assert result["operation"] == "NO_CHANGE" and snapshot_id(spark, target) == before
    checks["idempotent_no_new_snapshot"] = {"status": "PASS"}
    spark.sql(f"ALTER TABLE {state} SET TBLPROPERTIES ('{SNAPSHOT_PROPERTY}'='stale-simulated-crash')")
    result = sync_history(spark, frame, target, state)
    assert result["operation"] == "MERGE_RECONCILE"
    assert compare(spark.table(target), history_from(frame))["equal"]
    checks["stale_checkpoint_recovers"] = {"status": "PASS"}
    before = snapshot_id(spark, target)
    try:
        sync_history(spark, frame.unionByName(frame.limit(1)), target, state)
    except ValueError as error:
        assert "Duplicate observation grain" in str(error)
        assert snapshot_id(spark, target) == before
    else:
        raise AssertionError("Duplicate input was accepted")
    checks["invalid_input_does_not_commit"] = {"status": "PASS"}
    invalid_time = spark.createDataFrame([row("z", "20261023", "Z", snapshot_date=None, scraped_at=None, bronze_ingested_at=None)], OBSERVATION_SCHEMA)
    try:
        sync_history(spark, invalid_time, target, state)
    except ValueError as error:
        assert "observation time" in str(error)
        assert snapshot_id(spark, target) == before
    else:
        raise AssertionError("Undated observation was accepted")
    checks["missing_observation_time_rejected"] = {"status": "PASS"}
    exercise("removed_source_id_deleted", [r for r in metadata_fix if r["source_id"] != "y"], [("x", "A", "2026-10-16")])
    exercise("empty_input_deletes_all_versions", [], [])
    return checks


def main():
    from pyspark.sql import functions as F
    from pyspark.storagelevel import StorageLevel
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-batch", default="20261016")
    args = parser.parse_args()
    datetime.strptime(args.baseline_batch, "%Y%m%d")
    spark = build_spark_session("VerifyIncrementalHistory")
    try:
        silver = ensure_silver_namespace(spark)
        ns = f"{CATALOG_NAME}.smoke_test"
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")
        observations = spark.table(f"{silver}.listing_observation").persist(StorageLevel.MEMORY_AND_DISK)
        input_rows = observations.count()
        baseline = observations.filter(~F.col("source").isin("batdongsan", "guland", "nhadatvui") |
                                       (F.col("batch_id") <= args.baseline_batch))
        merge_table, reference = f"{ns}.history_merge_benchmark", f"{ns}.history_rebuild_benchmark"
        seed = sync_history(spark, baseline, merge_table, merge_table + "_state", "rebuild")
        print("Baseline history built", flush=True)
        merged = sync_history(spark, observations, merge_table, merge_table + "_state")
        print("Incremental MERGE completed: " + json.dumps(merged), flush=True)
        rebuilt = sync_history(spark, observations, reference, reference + "_state", "rebuild")
        print("Full rebuild completed: " + json.dumps(rebuilt), flush=True)
        comparison = compare(spark.table(merge_table), spark.table(reference))
        retry = sync_history(spark, observations, merge_table, merge_table + "_state")
        checks = {"identical_content": comparison["equal"],
                  "merge_used_for_changed_ids": merged["operation"] == "MERGE_AFFECTED_IDS" and merged["affected_source_ids"] > 0,
                  "retry_no_write": retry["operation"] == "NO_CHANGE" and retry["snapshot_before"] == retry["snapshot_after"]}
        fixtures = fixture_checks(spark, ns)
        production = sync_history(spark, observations, f"{silver}.listing_history", f"{silver}.listing_history_state")
        checks["production_equals_full_rebuild"] = compare(spark.table(f"{silver}.listing_history"), spark.table(reference))["equal"]
        observations.unpersist()
        payload = {"generated_at": datetime.now(timezone.utc).isoformat(), "baseline_batch": args.baseline_batch,
                   "observation_rows": input_rows, "seed": seed, "incremental": merged, "full_rebuild": rebuilt,
                   "comparison": comparison, "retry": retry, "production": production, "checks": checks,
                   "fixture_checks": fixtures, "fixture_check_count": len(fixtures),
                   "timing_scope": "Wall time inside sync_history, cached same observation input; includes validation, signatures and history/state writes. One local run, not a performance SLA.",
                   "status": "PASS" if all(checks.values()) else "FAIL"}
        for folder in ("outputs/validation", "docs/validation"):
            out = ROOT / folder / "history_incremental_verification.json"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(payload, indent=2))
        if payload["status"] != "PASS":
            raise RuntimeError("Incremental verification failed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
