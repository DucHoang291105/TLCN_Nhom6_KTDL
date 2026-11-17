"""Read-only CSV coverage audit; missing from a crawl is not evidence of sale.

This does not ingest data, fetch URLs, change gap declarations or replace the
production snapshot manifest. A cutoff is inclusive, not a claim that its
scheduled 22:00 crawl has already taken place.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.common.atomic_io import write_json_atomic
from src.orchestration.pipeline import SOURCES, validate_batch
from src.orchestration.snapshot_manifest import file_checksum

ROOT = Path(__file__).resolve().parents[2]
PAGE_FIELDS = ("_source_page", "source_page", "page_number_attr")


def positive_page(row):
    for field in PAGE_FIELDS:
        try:
            value = int(row.get(field, ""))
            if value > 0:
                return value
        except (TypeError, ValueError):
            pass
    return None


def scan_csv(path):
    """Verify bytes before/after parsing; retain one minimum page per raw ID."""
    digest, size = file_checksum(path)
    rows, missing_ids, missing_pages = 0, 0, 0
    ids, pages = {}, Counter()
    csv.field_size_limit(2**31 - 1)
    with path.open(encoding="utf-8-sig", newline="") as handle:
        first = handle.readline()
        if first.startswith("version https://git-lfs.github.com/spec/v1"):
            raise ValueError("LFS pointer: run git lfs pull")
        handle.seek(0)
        reader = csv.DictReader(handle)
        columns = reader.fieldnames or []
        if "listing_id" not in columns or len(set(columns)) != len(columns) or any(not c.strip() for c in columns):
            raise ValueError("Missing listing_id or duplicate/empty CSV header")
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"Malformed CSV near record {rows + 1}")
            rows += 1
            page = positive_page(row)
            if page is None:
                missing_pages += 1
            else:
                pages[page] += 1
            key = row["listing_id"].strip()
            if not key:
                missing_ids += 1
                continue
            if key not in ids or (page is not None and (ids[key] is None or page < ids[key])):
                ids[key] = page
    if file_checksum(path) != (digest, size):
        raise ValueError("CSV changed during audit; freeze input and retry")
    if not rows:
        raise ValueError("Empty snapshot")
    return ({"rows": rows, "unique_ids": len(ids), "missing_id_rows": missing_ids,
             "duplicate_id_rows": rows - missing_ids - len(ids), "missing_page_rows": missing_pages,
             "observed_pages": len(pages), "min_page": min(pages, default=None),
             "max_page": max(pages, default=None), "sha256": digest, "size_bytes": size}, ids)


def coverage(records):
    """records: ordered (metadata, ID -> observed page) pairs for ONE source."""
    if not records:
        return {"status": "NO_DATA"}
    all_ids = set().union(*(set(ids) for _, ids in records))
    latest_meta, latest = records[-1]
    result = {"status": "MEASURED", "latest_batch": latest_meta["batch_id"],
              "batch_count": len(records), "unique_ids_ever": len(all_ids),
              "latest_unique_ids": len(latest), "absent_two_batches": None,
              "absent_two_batches_share": None, "consecutive_pairs": []}
    for (old_meta, old), (new_meta, new) in zip(records, records[1:]):
        overlap = len(old.keys() & new.keys())
        result["consecutive_pairs"].append({"old_batch": old_meta["batch_id"], "new_batch": new_meta["batch_id"],
            "old_unique_ids": len(old), "new_unique_ids": len(new), "retained_ids": overlap,
            "retained_share_of_old": overlap / len(old) if old else None,
            "not_observed_from_old": len(old) - overlap})
    if len(records) < 2:
        return result
    absent = all_ids - set(latest) - set(records[-2][1])
    result.update(absent_two_batches=len(absent),
                  absent_two_batches_share=len(absent) / len(all_ids) if all_ids else None)
    # Compare with an actual earlier deeper batch; page rank changes over time.
    # This diagnoses sampling exposure, not which individual URL is still live.
    cap = latest_meta["max_page"]
    candidates = [(meta, ids) for meta, ids in records[:-2]
                  if cap is not None and meta["max_page"] is not None and meta["max_page"] > cap]
    if candidates:
        meta, ids = max(candidates, key=lambda item: item[0]["max_page"])
        cohorts = {}
        for name, cohort in {
            "within_latest_depth": {key for key, page in ids.items() if page is not None and page <= cap},
            "beyond_latest_depth": {key for key, page in ids.items() if page is not None and page > cap},
            "unknown_page": {key for key, page in ids.items() if page is None},
        }.items():
            n = len(cohort & absent)
            cohorts[name] = {"ids": len(cohort), "absent_two_batches": n,
                             "absent_share": n / len(cohort) if cohort else None}
        result["depth_cohorts"] = {"reference_batch": meta["batch_id"], "reference_max_page": meta["max_page"],
                                    "latest_max_page": cap, "cohorts": cohorts}
    return result


def audit(root, as_of, baseline):
    validate_batch(as_of)
    validate_batch(baseline)
    if baseline > as_of:
        raise ValueError("Baseline must not be after as-of")
    root = Path(root)
    manifest_path = root / "docs/validation/snapshot_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.exists() else {}
    pinned = {(r["source"], r["batch_id"]): r for r in manifest.get("snapshots", [])}
    gaps = json.loads((root / "config/source_gaps.json").read_text(encoding="utf-8"))["known_gaps"]
    declared = {(r["source"], r["batch_id"]) for r in gaps if r.get("reason", "").strip()}
    snapshots, errors, extras, excluded, sources, missing, present = [], [], [], [], {}, [], set()
    for source in SOURCES:
        directory = root / "data/incoming/snapshots" / source
        records = []
        candidates = sorted(directory.rglob("*.csv")) if directory.exists() else []
        canonical = set()
        for folder in sorted(directory.iterdir()) if directory.exists() else []:
            if not folder.is_dir() or not re.fullmatch(r"[0-9]{8}", folder.name):
                continue
            path = folder / f"{source}_raw.csv"
            canonical.add(path)
            try:
                validate_batch(folder.name)
                if folder.name > as_of:
                    excluded.append(path.relative_to(root).as_posix())
                    continue
                meta, ids = scan_csv(path)
                meta.update(source=source, batch_id=folder.name, path=path.relative_to(root).as_posix())
                present.add((source, folder.name))
                old = pinned.get((source, folder.name))
                meta["matches_pipeline_manifest"] = meta["sha256"] == old["sha256"] if old else None
                summary_path = folder / "crawl_summary.json"
                if summary_path.exists():
                    summary = json.loads(summary_path.read_text(encoding="utf-8"))
                    meta["crawler_report"] = {k: summary.get(k) for k in
                        ("status", "requested_pages", "records_written", "stop_reason")}
                    if summary.get("failed_pages") or summary.get("status", "PASS") != "PASS":
                        errors.append({"path": meta["path"], "error": "Crawler reports incomplete pages"})
                snapshots.append(meta)
                records.append((meta, ids))
                if old and not meta["matches_pipeline_manifest"]:
                    errors.append({"path": meta["path"], "error": "Previously ingested snapshot bytes changed"})
            except (ValueError, OSError, RuntimeError, csv.Error) as exc:
                errors.append({"path": path.relative_to(root).as_posix(), "error": str(exc)})
        extras.extend(p.relative_to(root).as_posix() for p in candidates if p not in canonical)
        batches = [meta["batch_id"] for meta, _ in records]
        sources[source] = {"baseline": coverage([r for r in records if r[0]["batch_id"] <= baseline]),
                           "local": coverage(records), "batches": batches}
        if not records:
            errors.append({"source": source, "error": "No valid canonical snapshots"})
            continue
        day = datetime.strptime(min(batches), "%Y%m%d")
        while day.strftime("%Y%m%d") <= as_of:
            batch = day.strftime("%Y%m%d")
            if day.weekday() in (1, 4) and batch not in batches:
                missing.append({"source": source, "batch_id": batch, "declared": (source, batch) in declared})
            day += timedelta(days=1)
    for source, batch in pinned:
        if batch <= as_of and (source, batch) not in present:
            errors.append({"source": source, "batch_id": batch, "error": "Pinned snapshot missing or unreadable"})
    common = set.intersection(*(set(v["batches"]) for v in sources.values()))
    checks = {"files_valid_and_pinned_bytes_unchanged": not errors,
              "no_ignored_or_extra_csv": not extras,
              "all_calendar_gaps_declared": all(r["declared"] for r in missing),
              "requested_batch_complete": all(as_of in v["batches"] for v in sources.values())}
    return {"generated_at": datetime.now(timezone.utc).isoformat(), "as_of": as_of, "baseline_cutoff": baseline,
            "scope": "Local raw CSV diagnostic, not Silver/Gold acceptance or confirmed website availability",
            "status": "PASS" if all(checks.values()) else "NEEDS_INPUT_REVIEW", "checks": checks,
            "latest_complete_local_batch": max(common) if common else None,
            "snapshot_files": len(snapshots), "snapshot_rows": sum(r["rows"] for r in snapshots),
            "snapshots": snapshots, "sources": sources, "missing_scheduled_batches": missing,
            "extra_csv_not_consumed_by_pipeline": extras, "excluded_after_cutoff": excluded, "errors": errors,
            "limits": ["Raw ID counts precede Silver DQ/dedup; do not equate them to Gold counts.",
                       "Page numbers describe exported records, not every attempted or successful HTTP request.",
                       "Depth cohorts show association, not a causal estimate or confirmation of sold/deleted listings.",
                       "Cutoff is inclusive; missing current-day input may still be awaiting its 22:00 crawl."]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", required=True, help="Inclusive YYYYMMDD")
    parser.add_argument("--baseline", default="20261027")
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path, default=ROOT / "docs/validation/crawl_coverage.json")
    parser.add_argument("--strict", action="store_true", help="Exit 2 if any input readiness check fails")
    args = parser.parse_args()
    result = audit(args.root, args.as_of, args.baseline)
    write_json_atomic(args.output, result)
    print(json.dumps({k: result[k] for k in ("status", "checks", "snapshot_files", "snapshot_rows",
                                            "latest_complete_local_batch")}, indent=2))
    if args.strict and result["status"] != "PASS":
        raise SystemExit(2)


if __name__ == "__main__":
    main()
