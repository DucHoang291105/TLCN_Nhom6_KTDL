"""SHA256 on every run; reuse CSV profiling only for exactly unchanged bytes."""
from __future__ import annotations
import csv
import hashlib
import io
import json
import time
from itertools import chain
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from src.common.atomic_io import write_json_atomic

PARSER_VERSION = "csv_contract_v2"


def _file_version(stat):
    return stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns


def file_checksum(path):
    digest = hashlib.sha256()
    before = path.stat()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    after = path.stat()
    if _file_version(before) != _file_version(after):
        raise ValueError(f"Snapshot changed while hashing: {path}")
    return digest.hexdigest(), after.st_size


def _profile_stream(handle, path):
    first_line = handle.readline()
    if first_line.startswith("version https://git-lfs.github.com/spec/v1"):
        raise RuntimeError(f"LFS pointer only: run git lfs pull ({path})")
    reader = csv.DictReader(chain((first_line,), handle))
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


def profile_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return _profile_stream(handle, path)


class _HashingReader(io.RawIOBase):
    """Hash the exact bytes consumed by the buffered UTF-8 CSV reader."""

    def __init__(self, raw, digest):
        super().__init__()
        self.raw, self.digest = raw, digest

    def readable(self):
        return True

    def readinto(self, buffer):
        size = self.raw.readinto(buffer)
        if size:
            self.digest.update(memoryview(buffer)[:size])
        return size


class _CSVBuffer(io.BufferedReader):
    def read1(self, size=-1):
        # TextIOWrapper requests small read1 chunks; fill the larger buffer so
        # SHA256 and the underlying file are handled in blocks rather than lines.
        return self.read(size)


def profile_csv_with_checksum(path):
    """Profile new/invalidated CSVs and SHA256 them in one bounded-memory pass."""
    before = path.stat()
    digest = hashlib.sha256()
    with path.open("rb", buffering=0) as raw:
        with io.TextIOWrapper(_CSVBuffer(_HashingReader(raw, digest), buffer_size=1024 * 1024),
                              encoding="utf-8-sig", newline="") as handle:
            profile = _profile_stream(handle, path)
    after = path.stat()
    if _file_version(before) != _file_version(after):
        raise ValueError(f"Snapshot changed while parsing: {path}")
    return profile, digest.hexdigest(), after.st_size


def reusable_profile(entry):
    return (entry.get("parser_version") == PARSER_VERSION and
            type(entry.get("rows")) is int and entry["rows"] > 0 and
            type(entry.get("columns")) is int and entry["columns"] > 0 and
            "max_source_page" in entry)


def confirm_inventory(root, sources, manifest):
    """Check the complete pinned input set before any Bronze writes and at end."""
    from src.orchestration.pipeline import validate_batch
    if manifest.get("status") != "PASS":
        raise RuntimeError("Snapshot manifest is not PASS")
    rows = manifest.get("snapshots", [])
    expected = {(row["source"], row["batch_id"]): row["sha256"] for row in rows}
    if not expected or len(rows) != len(expected):
        raise RuntimeError("Empty or duplicate snapshot manifest entries")
    paths = {}
    for source in sources:
        directory = Path(root) / "data/incoming/snapshots" / source
        for folder in directory.iterdir():
            if folder.is_dir() and folder.name.isdigit() and len(folder.name) == 8:
                validate_batch(folder.name)
                paths[(source, folder.name)] = folder / f"{source}_raw.csv"
    if set(paths) != set(expected):
        raise RuntimeError("Snapshot inventory changed after preflight; freeze inputs and rebuild the manifest before Bronze writes")
    def check(item):
        key, path = item
        if file_checksum(path)[0] != expected[key]:
            raise RuntimeError(f"Snapshot bytes changed after preflight: {key}; inspect before reloading Bronze")
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(check, paths.items()))
    return len(paths)


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
        old = known.get((source, folder), {})
        cached = not force_rescan and reusable_profile(old) and bool(old.get("sha256"))
        if cached:
            digest, size = file_checksum(path)
            if old["sha256"] != digest:
                raise ValueError(f"Previously recorded snapshot changed: {source}/{folder}; inspect and explicitly reload Bronze")
            cached = old.get("size_bytes") == size
        if cached:
            profile = {k: old[k] for k in ("rows", "columns", "max_source_page")}
        else:
            profile, digest, size = profile_csv_with_checksum(path)
        if old.get("sha256", digest) != digest:
            raise ValueError(f"Previously recorded snapshot changed: {source}/{folder}; inspect and explicitly reload Bronze")
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
