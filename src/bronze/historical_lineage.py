"""Verify historical CSV bytes and the complete raw-row multiset in Bronze."""
from __future__ import annotations

import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from src.bronze.historical_contract import BATCH_ID, EXPECTED_COLUMNS, SOURCES
from src.common.atomic_io import write_json_atomic
from src.orchestration.snapshot_manifest import file_checksum

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = 'docs/validation/historical_input_manifest.json'


def inspect_input(path, source):
    before = file_checksum(path)
    csv.field_size_limit(2**31 - 1)
    with Path(path).open(encoding='utf-8-sig', newline='') as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != EXPECTED_COLUMNS:
            raise ValueError(f'Historical schema mismatch: {source}')
        count = 0
        for row in reader:
            if None in row or any(v is None for v in row.values()):
                raise ValueError(f'Malformed historical CSV: {source}, row {count + 1}')
            count += 1
    if count != SOURCES[source]['expected_rows'] or before != file_checksum(path):
        raise ValueError(f'Historical row count changed or input mutated during read: {source}')
    return {'sha256': before[0], 'size_bytes': before[1], 'rows': count, 'schema': EXPECTED_COLUMNS}


def validate_pinned(source, metadata, pinned):
    previous = pinned.get('sources', {}).get(source)
    if previous and any(previous.get(k) != metadata[k] for k in ('sha256', 'size_bytes', 'rows', 'schema')):
        raise ValueError(f'Immutable historical input changed: {source}; use a new explicit batch version')


def raw_csv(spark, path):
    return (spark.read.option('header', 'true').option('inferSchema', 'false')
            .option('encoding', 'UTF-8').option('multiLine', 'true').option('quote', '"').option('escape', '"')
            .csv(str(path)))


def compare_rows(expected, actual):
    """Compare duplicate multiplicities and NULL values, not only row counts."""
    from pyspark.sql import functions as F
    actual = actual.select(*EXPECTED_COLUMNS)
    expected = expected.select(*EXPECTED_COLUMNS)
    if expected.exceptAll(actual).limit(1).count() or actual.exceptAll(expected).limit(1).count():
        raise RuntimeError('Historical Bronze content differs from CSV; do not bootstrap a hash over stale Bronze')
    rows = (actual.select(F.sha2(F.to_json(F.struct(*EXPECTED_COLUMNS), {'ignoreNullFields': 'false'}), 256).alias('h'))
            .groupBy('h').count().orderBy('h').collect())
    return hashlib.sha256(''.join(f'{r.h}:{r["count"]}\n' for r in rows).encode()).hexdigest()


def audit_historical(spark, root=ROOT):
    root = Path(root)
    target = root / MANIFEST
    pinned = json.loads(target.read_text(encoding='utf-8')) if target.exists() else {}
    records = {}
    for source, spec in SOURCES.items():
        path = root / 'data/incoming/historical' / source / spec['file']
        metadata = inspect_input(path, source)
        validate_pinned(source, metadata, pinned)
        bronze = f's3a://lakehouse-bronze/real_estate/historical/{source}/batch_id={BATCH_ID}'
        fingerprint = compare_rows(raw_csv(spark, path), spark.read.parquet(bronze))
        if inspect_input(path, source) != metadata:
            raise RuntimeError(f'Input changed during Bronze comparison: {source}')
        records[source] = {**metadata, 'path': path.relative_to(root).as_posix(), 'bronze_path': bronze,
                           'raw_row_multiset_sha256': fingerprint, 'content_matches_bronze': True}
    result = {'status': 'PASS', 'generated_at': datetime.now(timezone.utc).isoformat(), 'batch_id': BATCH_ID,
              'bootstrap_policy': 'Full two-way exceptAll comparison before pinning any existing Bronze input',
              'sources': records, 'total_rows': sum(r['rows'] for r in records.values())}
    write_json_atomic(target, result)
    return result
