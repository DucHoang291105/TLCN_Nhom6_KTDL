"""A-owned lifecycle benchmark; definition and all non-build columns must match."""
import hashlib
import importlib.util
import json
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from src.common.atomic_io import write_json_atomic
from src.common.spark_session import build_spark_session, write_iceberg_table, CATALOG_NAME
from src.gold.build_listing_lifecycle import calendar_from_audit, build_lifecycle_frame


def fingerprint(frame):
    from pyspark.sql import functions as F
    digest = hashlib.sha256()
    columns = sorted(c for c in frame.columns if c != "lifecycle_built_at")
    hashes = frame.select(F.sha2(F.to_json(F.struct(*columns), {"ignoreNullFields": "false"}), 256).alias("h")).orderBy("h")
    for row in hashes.toLocalIterator():
        digest.update(bytes.fromhex(row.h))
    return digest.hexdigest()


def main():
    from pyspark.storagelevel import StorageLevel
    spec = importlib.util.spec_from_file_location("reference_lifecycle", ROOT / "tests/reference/lifecycle_a6909ae.py")
    reference = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reference)
    spark = build_spark_session("BenchmarkPersonALifecycle")
    try:
        silver, gold, ns = (f"{CATALOG_NAME}.{name}" for name in ("silver", "gold", "smoke_test"))
        observations = spark.table(f"{silver}.listing_observation").persist(StorageLevel.MEMORY_AND_DISK)
        n = observations.count()
        audit = json.loads((ROOT / "docs/validation/bronze_state_audit.json").read_text())
        calendar = spark.createDataFrame(calendar_from_audit(audit), "source string, batch_id string")
        sources = spark.table(f"{gold}.dim_source")
        timings = {"before": [], "after": []}
        checks = []
        for iteration in range(3):
            for name in (("before", "after") if iteration % 2 == 0 else ("after", "before")):
                function = reference.build_lifecycle_frame if name == "before" else build_lifecycle_frame
                started = time.perf_counter()
                write_iceberg_table(function(observations, calendar, sources), f"{ns}.lifecycle_optimization_{name}")
                timings[name].append(round(time.perf_counter()-started, 4))
            a, b = (spark.table(f"{ns}.lifecycle_optimization_{name}").drop("lifecycle_built_at") for name in ("before", "after"))
            check = {"iteration": iteration+1, "schema_equal": a.schema == b.schema,
                     "rows_before": a.count(), "rows_after": b.count(), "hash_before": fingerprint(a), "hash_after": fingerprint(b),
                     "before_only": a.exceptAll(b).count(), "after_only": b.exceptAll(a).count()}
            assert check["schema_equal"] and check["hash_before"] == check["hash_after"] and check["before_only"] == check["after_only"] == 0
            checks.append(check)
            print(json.dumps(check), flush=True)
        observations.unpersist()
        payload = {"generated_at": datetime.now(timezone.utc).isoformat(), "input_rows": n,
                   "baseline_commit": "a6909aed578f86cd5c7181e861ed55688c237012", "comparisons": checks,
                   "timings": {name: {"seconds": values, "median_seconds": round(statistics.median(values), 4)} for name,values in timings.items()},
                   "timing_scope": "Same cached input, one Spark app, alternating AB/BA, three repetitions. Lifecycle calculation plus sandbox Iceberg write; exact comparisons excluded. Only lifecycle_built_at excluded from hashes.", "status": "PASS"}
        write_json_atomic(ROOT / "docs/validation/person_a_lifecycle_optimization.json", payload)
        print(json.dumps(payload, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
