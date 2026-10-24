"""Controlled manifest benchmark: frozen scanner versus SHA256-verified profile cache."""
import argparse
import importlib.util
import json
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path
from src.common.atomic_io import write_json_atomic
from src.orchestration import pipeline
from src.orchestration.snapshot_manifest import build_manifest

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch", default="20261023")
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location("reference_pipeline", ROOT / "tests/reference/pipeline_a6909ae.py")
    reference = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reference)
    started = time.perf_counter()
    reference.validate_snapshots(args.batch)
    baseline_seconds = time.perf_counter()-started
    baseline = json.loads((ROOT / "docs/validation/snapshot_manifest.json").read_text())
    first = build_manifest(ROOT, pipeline.SOURCES, args.batch)
    fields = ("source", "batch_id", "rows", "columns", "max_source_page", "sha256", "size_bytes")
    project = lambda payload: [{k: r[k] for k in fields} for r in payload["snapshots"]]
    checks = {"cold_parse_equals_reference": project(first) == project(baseline)}
    timings = {}
    for workers in (1, 2, 4):
        values = []
        for iteration in range(3):
            started = time.perf_counter()
            result = build_manifest(ROOT, pipeline.SOURCES, args.batch, workers=workers)
            values.append(round(time.perf_counter()-started, 4))
            assert project(result) == project(baseline)
            assert result["verification"]["csv_profiles_parsed"] == 0
            assert result["verification"]["sha256_files_checked"] == len(result["snapshots"])
        timings[str(workers)] = {"seconds": values, "median_seconds": round(statistics.median(values), 4)}
    checks["all_cached_runs_equal_reference_and_rehash_all_files"] = True
    payload = {"generated_at": datetime.now(timezone.utc).isoformat(), "baseline_commit": "a6909aed578f86cd5c7181e861ed55688c237012",
               "baseline_seconds": round(baseline_seconds, 4), "cold_optimized_seconds": first["verification"]["elapsed_seconds"],
               "cached_by_hash_workers": timings, "snapshot_files": len(baseline["snapshots"]),
               "snapshot_rows": baseline["snapshot_rows"], "total_bytes_rehashed_per_run": sum(r["size_bytes"] for r in baseline["snapshots"]),
               "checks": checks, "status": "PASS" if all(checks.values()) else "FAIL",
               "timing_scope": "Same Airflow container, same complete CSV files; baseline full CSV scan once, cached paths three repetitions each. Every cached run still reads and SHA256-checks every byte."}
    write_json_atomic(ROOT / "docs/validation/person_a_manifest_benchmark.json", payload)
    print(json.dumps(payload, indent=2), flush=True)
    if payload["status"] != "PASS":
        raise RuntimeError("Manifest benchmark failed")


if __name__ == "__main__":
    main()
