"""Bind defense verification to the exact code, source bytes and current run."""
from __future__ import annotations

import ast
import hashlib
import json
import re
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from xml.etree import ElementTree

from src.common.atomic_io import write_json_atomic
from src.common.table_fingerprints import GRAINS, MULTISET_TABLES
from src.orchestration.pipeline import SOURCES, validate_batch
from src.orchestration.snapshot_manifest import file_checksum

ROOT = Path(__file__).resolve().parents[2]
ARTIFACTS = (
    "snapshot_manifest", "bronze_state_audit", "integration_airflow_run", "airflow_failure_gate",
    "silver_final_verification", "gold_verification", "deterministic_rebuild",
    "history_incremental_verification", "lifecycle_fixture_tests", "iceberg_features",
    "superset_dashboard_check", "pipeline_dashboard_integration",
)


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def all_pass(checks):
    return bool(checks) and all(value is True for value in checks.values())


def code_state(root):
    """Hash tracked and not-ignored code/config; omit secrets and runtime outputs."""
    raw = subprocess.check_output(["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"], cwd=root)
    paths = {p.decode("utf-8") for p in raw.split(b"\0") if p}
    selected = {p for p in paths if p.startswith(("src/", "scripts/", "tests/", "dags/", "config/", "docker/", ".github/"))
                or p in {"docker-compose.yml", ".gitignore", ".gitattributes", "requirements.txt", "requirements-dev.txt"}
                or p in {"docs/operations/release_validation.md", "docs/architecture.md"}}
    files = {}
    for name in sorted(selected):
        path = Path(root) / name
        if path.is_file() and path.suffix not in {".pyc", ".pyo"}:
            files[name] = hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
        elif not path.exists():
            files[name] = "DELETED"
    digest = hashlib.sha256(json.dumps(files, sort_keys=True).encode()).hexdigest()
    return {"sha256": digest, "files": files}


def inputs(root):
    root = Path(root)
    module = ast.parse((root / "src/bronze/ingest_bronze.py").read_text(encoding="utf-8-sig"))
    historical = next(ast.literal_eval(node.value) for node in module.body if isinstance(node, ast.Assign)
                      and any(isinstance(t, ast.Name) and t.id == "SOURCES" for t in node.targets))
    paths = [root / "data/incoming/historical" / source / spec["file"] for source, spec in historical.items()]
    by_source = {}
    for source in SOURCES:
        folder = root / "data/incoming/snapshots" / source
        batches = sorted(p.name for p in folder.iterdir() if p.is_dir() and re.fullmatch(r"\d{8}", p.name))
        for batch in batches:
            validate_batch(batch)
            paths.append(folder / batch / f"{source}_raw.csv")
        by_source[source] = batches
    common = set.intersection(*(set(batches) for batches in by_source.values()))
    if not common:
        raise ValueError("No complete batch shared by the three crawl sources")
    hashes = {p.relative_to(root).as_posix(): file_checksum(p)[0] for p in paths}
    return {"sha256": hashes, "batches_by_source": by_source, "latest_complete_batch": max(common),
            "latest_by_source": {source: max(batches) for source, batches in by_source.items()},
            "latest_observed_batch": max(max(batches) for batches in by_source.values())}


def capture(root, batch, run_id):
    validate_batch(batch)
    data = inputs(root)
    if batch != data["latest_complete_batch"]:
        raise ValueError(f"Use latest complete batch {data['latest_complete_batch']}; newer partial batches remain listed separately")
    return {"started_at": datetime.now(timezone.utc).isoformat(), "run_id": run_id,
            "batch_id": batch, "code": code_state(root), "inputs": data}


def crawl_inventory(root, data):
    known = read(Path(root) / "config/source_gaps.json")["known_gaps"]
    lookup = {(row["source"], row["batch_id"]): row for row in known}
    missing = []
    end = datetime.strptime(data["latest_observed_batch"], "%Y%m%d")
    for source, batches in data["batches_by_source"].items():
        day = datetime.strptime(min(batches), "%Y%m%d")
        while day <= end:
            batch = day.strftime("%Y%m%d")
            if day.weekday() in (1, 4) and batch not in batches:
                missing.append({"source": source, "batch_id": batch, "declared": (source, batch) in lookup,
                                "reason": lookup.get((source, batch), {}).get("reason")})
            day += timedelta(days=1)
    return {"latest_by_source": data["latest_by_source"], "batches_by_source": data["batches_by_source"],
            "missing_scheduled_batches": missing, "all_gaps_declared": all(row["declared"] for row in missing),
            "known_gaps": known}


def bronze_matches_manifest(audit, manifest):
    rows = [r for r in audit["batches"] if r["branch"] == "snapshots"]
    stored = {(r["source"], r["batch_id"]): r["row_count"] for r in rows}
    expected = {(r["source"], r["batch_id"]): r["rows"] for r in manifest["snapshots"]}
    return bool(expected) and len(rows) == len(stored) == len(expected) and stored == expected


def junit_pass(path):
    tree = ElementTree.parse(path).getroot()
    suites = [tree] if tree.tag == "testsuite" else list(tree.iter("testsuite"))
    return bool(suites) and sum(int(s.get("tests", "0")) for s in suites) > 0 and all(
        int(s.get("failures", "0")) == int(s.get("errors", "0")) == int(s.get("skipped", "0")) == 0 for s in suites)


def fingerprint_checks(first, second):
    a, b = first.get("tables", {}), second.get("tables", {})
    coverage = set(a) == set(b) == set(GRAINS)
    version = first.get("fingerprint_version") == second.get("fingerprint_version") == "sha256_multiset_v2"
    checks = {"all_production_tables_compared": coverage, "full_sha256_version": version}
    for name in GRAINS:
        left, right = a.get(name, {}), b.get(name, {})
        checks[f"rebuild:{name}"] = bool(
            left.get("schema") and left.get("schema") == right.get("schema")
            and left.get("grain") == right.get("grain") == GRAINS[name]
            and type(left.get("rows")) is int and left["rows"] == right.get("rows")
            and left.get("distinct_keys") == right.get("distinct_keys")
            and (name in MULTISET_TABLES or left.get("distinct_keys") == left["rows"])
            and left.get("hash_method") == right.get("hash_method") == "sha256_of_sorted_full_row_sha256_multiset_v2"
            and re.fullmatch(r"[0-9a-f]{64}", left.get("content_hash", ""))
            and left["content_hash"] == right.get("content_hash"))
    return checks


def verify(root, work):
    root, work = Path(root), Path(work)
    baseline = read(work / "baseline.json")
    docs = {name: read(root / "docs/validation" / f"{name}.json") for name in ARTIFACTS}
    current_inputs = inputs(root)
    inventory = crawl_inventory(root, current_inputs)
    run = docs["integration_airflow_run"]
    silver, gold = docs["silver_final_verification"], docs["gold_verification"]
    failure, history = docs["airflow_failure_gate"], docs["history_incremental_verification"]
    chart, dashboard = docs["superset_dashboard_check"], docs["pipeline_dashboard_integration"]
    first, second = read(work / "run1.json"), read(work / "run2.json")
    checks = {
        "code_unchanged": code_state(root) == baseline["code"],
        "all_input_bytes_unchanged": current_inputs == baseline["inputs"],
        "latest_complete_batch_used": run["batch_id"] == baseline["batch_id"] == current_inputs["latest_complete_batch"],
        "latest_observed_batch_complete": current_inputs["latest_complete_batch"] == current_inputs["latest_observed_batch"],
        "all_calendar_gaps_declared": inventory["all_gaps_declared"],
        "correct_airflow_run": run["run_id"] == baseline["run_id"],
        "airflow_24_success": run["state"] == "success" and len(run["tasks"]) == 24 and all(t["state"] == "success" for t in run["tasks"]),
        "failure_blocks_downstream": failure.get("verification_status") == "PASS" and all_pass(failure.get("checks", {}))
            and len(failure["tasks"]) == 24 and sum(t["state"] == "upstream_failed" for t in failure["tasks"]) == 23,
        "silver_quality": silver["status"] == "PASS" and len(silver["checks"]) >= 30 and all_pass(silver["checks"]),
        "gold_quality": gold["status"] == "PASS" and len(gold["checks"]) >= 108 and all_pass(gold["checks"]),
        "bronze_audit": docs["bronze_state_audit"]["status"] == "PASS",
        "bronze_matches_all_snapshot_inputs": bronze_matches_manifest(docs["bronze_state_audit"], docs["snapshot_manifest"]),
        "manifest_hashes": docs["snapshot_manifest"]["status"] == "PASS"
            and docs["snapshot_manifest"]["verification"]["sha256_files_checked"] == len(docs["snapshot_manifest"]["snapshots"]),
        "history_merge_and_retry": history["status"] == "PASS" and all_pass(history["checks"]),
        "history_fixtures": len(history["fixture_checks"]) >= 11 and all(v.get("status") == "PASS" for v in history["fixture_checks"].values()),
        "iceberg_demo": docs["iceberg_features"]["status"] == "PASS" and len(docs["iceberg_features"]["checks"]) >= 6 and all_pass(docs["iceberg_features"]["checks"]),
        "lifecycle_fixtures": docs["lifecycle_fixture_tests"]["status"] == "PASS" and len(docs["lifecycle_fixture_tests"]["checks"]) >= 12 and all_pass(docs["lifecycle_fixture_tests"]["checks"]),
        "dashboard_live": chart["status"] == dashboard["status"] == "PASS" and len(chart["chart_checks"]) == 29
            and all(c["match"] is True for c in chart["chart_checks"]) and all_pass(dashboard["checks"])
            and dashboard["run_id"] == baseline["run_id"],
        "unit_tests": junit_pass(work / "unit-tests.xml"),
        "manual_write_guard": read(work / "manual_writer_guard.json")["status"] == "PASS",
    }
    checks.update(fingerprint_checks(first, second))
    started = datetime.fromisoformat(baseline["started_at"])
    evidence = {}
    for name, data in docs.items():
        path = root / "docs/validation" / f"{name}.json"
        timestamp = data.get("generated_at") or datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat()
        checks[f"fresh:{name}"] = datetime.fromisoformat(timestamp.replace("Z", "+00:00")) >= started
        evidence[name] = {"sha256": file_checksum(path)[0], "timestamp": timestamp}
    for label, data in (("first", first), ("second", second)):
        checks[f"fresh_fingerprint:{label}"] = datetime.fromisoformat(data["generated_at"]) >= started
    payload = {"generated_at": datetime.now(timezone.utc).isoformat(), "scope": "Person A: pipeline and operation across the three-week plan",
               "run_id": baseline["run_id"], "batch_id": baseline["batch_id"], "code": baseline["code"],
               "inputs": current_inputs, "checks": checks, "evidence": evidence,
               "status": "PASS" if all_pass(checks) else "FAIL",
               "data_note": "Use the actual per-source calendars; absent batches are not fabricated. latest_complete_batch is the DAG preflight date, latest_observed_batch includes partial newer dates.",
               "operation_mode": "Sequential Spark writing then Trino/Superset reading on the local SQLite catalog",
               "business_limits": dashboard["manual_labeling"],
               "silver_counts": silver["table_counts"], "gold_counts": gold["table_counts"]}
    write_json_atomic(root / "docs/validation/defense_verification.json", payload)
    write_json_atomic(root / "docs/validation/crawl_inventory.json", inventory)
    print(json.dumps({"status": payload["status"], "checks": len(checks), "failures": [k for k,v in checks.items() if not v]}, indent=2))
    if payload["status"] != "PASS":
        raise RuntimeError("Defense verification failed; do not tag this candidate")
    return payload
