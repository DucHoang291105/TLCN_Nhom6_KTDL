"""SHA256 on every run; reuse CSV profiling only for exactly unchanged bytes."""
from __future__ import annotations
import csv
import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from src.common.atomic_io import write_json_atomic

PARSER_VERSION = "csv_contract_v2"


def file_checksum(path):
    digest = hashlib.sha256()
    before = path.stat()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    after = path.stat()
    if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
        raise ValueError(f"Snapshot changed while hashing: {path}")
    return digest.hexdigest(), after.st_size


def profile_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        if handle.readline().startswith("version https://git-lfs.github.com/spec/v1"):
            raise RuntimeError(f"LFS pointer only: run git lfs pull ({path})")
        handle.seek(0)
        reader = csv.DictReader(handle)
        columns = reader.fieldnames or []
        if "listing_id" not in columns:
            raise ValueError(f"Missing listing_id column: {path}")
        if len(set(columns)) != len(columns) or any(not c.strip() for c in columns):
            raise ValueError(f"Duplicate or empty CSV header: {path}")
        n, max_page = 0, None
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise ValueError(f"Malformed CSV record in {path} near row {n+2}")
            n += 1
            value = row.get("_source_page") or row.get("source_page") or row.get("page_number_attr")
            try:
                max_page = max(max_page or 0, int(value))
            except (TypeError, ValueError):
                pass
        if not n:
            raise ValueError(f"Empty snapshot: {path}")
        return {"rows": n, "columns": len(columns), "max_source_page": max_page}


def reusable_profile(entry):
    return (entry.get("parser_version") == PARSER_VERSION and
            type(entry.get("rows")) is int and entry["rows"] > 0 and
            type(entry.get("columns")) is int and entry["columns"] > 0 and
            "max_source_page" in entry)


def build_manifest(root, sources, batch, force_rescan=False, workers=4, output=None):
    from src.orchestration.pipeline import validate_batch
    validate_batch(batch)
    if not 1 <= workers <= 8:
        raise ValueError("Manifest workers must be between 1 and 8")
    started = time.perf_counter()
    root = Path(root)
    out = Path(output) if output else root / "docs/validation/snapshot_manifest.json"
    previous = json.loads(out.read_text(encoding="utf-8")) if out.exists() else {}
    known = {(r["source"], r["batch_id"]): r for r in previous.get("snapshots", [])}
    paths = []
    for source in sources:
        source_dir = root / "data/incoming/snapshots" / source
        if not (source_dir / batch / f"{source}_raw.csv").is_file():
            raise FileNotFoundError(f"Missing {source}/{batch}; export CSV before continuing")
        for folder in sorted(source_dir.iterdir()):
            if folder.is_dir() and folder.name.isdigit() and len(folder.name) == 8:
                validate_batch(folder.name)
                paths.append((source, folder.name, folder / f"{source}_raw.csv"))
    csv.field_size_limit(2**31 - 1)
    missing = set(known) - {(source, folder) for source, folder, path in paths}
    if missing:
        raise ValueError(f"Previously recorded snapshots missing: {sorted(missing)}")

    def inspect(item):
        source, folder, path = item
        digest, size = file_checksum(path)
        old = known.get((source, folder), {})
        if old.get("sha256", digest) != digest:
            raise ValueError(f"Previously recorded snapshot changed: {source}/{folder}; inspect and explicitly reload Bronze")
        cached = (not force_rescan and reusable_profile(old) and
                  old.get("sha256") == digest and old.get("size_bytes") == size)
        profile = {k: old[k] for k in ("rows", "columns", "max_source_page")} if cached else profile_csv(path)
        if not cached and file_checksum(path) != (digest, size):
            raise ValueError(f"Snapshot changed while parsing: {path}")
        return ({"source": source, "batch_id": folder, **profile, "sha256": digest,
                 "size_bytes": size, "parser_version": PARSER_VERSION}, cached)

    # map preserves source/batch order. Any failure leaves the previous manifest intact.
    with ThreadPoolExecutor(max_workers=workers) as pool:
        inspected = list(pool.map(inspect, paths))
    entries = [entry for entry, cached in inspected]
    payload = {"generated_at": datetime.now(timezone.utc).isoformat(), "expected_batch_id": batch,
               "snapshots": entries, "snapshot_rows": sum(r["rows"] for r in entries), "status": "PASS",
               "verification": {"sha256_files_checked": len(entries), "csv_profiles_reused": sum(c for _, c in inspected),
                                "csv_profiles_parsed": sum(not c for _, c in inspected), "force_rescan": force_rescan,
                                "workers": workers, "elapsed_seconds": round(time.perf_counter()-started, 4)}}
    write_json_atomic(out, payload)
    print(f"Snapshot manifest PASS: {len(entries)} files, {payload['snapshot_rows']:,} rows; {payload['verification']}")
    return payload
