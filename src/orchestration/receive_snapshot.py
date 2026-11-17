"""Validate a dated CSV export; publish only with --apply, without overwriting."""
from __future__ import annotations

import argparse
import os
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from src.orchestration.pipeline import PROJECT_ROOT, SOURCES, validate_batch
from src.orchestration.snapshot_manifest import file_checksum
from src.quality.crawl_coverage import scan_csv


def receive(root, source, batch, exported_file, apply=False, today=None):
    if source not in SOURCES:
        raise ValueError('Unknown crawl source')
    validate_batch(batch)
    today = today or datetime.now(ZoneInfo('Asia/Ho_Chi_Minh')).strftime('%Y%m%d')
    if batch > today:
        raise ValueError('Cannot receive a future snapshot')
    exported_file = Path(exported_file)
    profile, ids = scan_csv(exported_file)
    if not ids:
        raise ValueError('No non-empty listing IDs')
    target = Path(root) / 'data/incoming/snapshots' / source / batch / f'{source}_raw.csv'
    result = {'source': source, 'batch_id': batch, 'target': str(target), **profile}
    if target.exists():
        if target.is_symlink() or file_checksum(target) != (profile['sha256'], profile['size_bytes']):
            raise ValueError('Existing snapshot differs; refusing overwrite')
        return {**result, 'action': 'REUSE_IDENTICAL'}
    if not apply:
        return {**result, 'action': 'VALIDATED_NOT_COPIED'}
    target.parent.mkdir(parents=True, exist_ok=True)
    staged = None
    try:
        with tempfile.NamedTemporaryFile(dir=target.parent, prefix='.receive-', suffix='.tmp', delete=False) as handle:
            staged = Path(handle.name)
            with exported_file.open('rb') as incoming:
                shutil.copyfileobj(incoming, handle)
            handle.flush()
            os.fsync(handle.fileno())
        if file_checksum(staged) != (profile['sha256'], profile['size_bytes']):
            raise ValueError('Input changed during copy')
        # Same-directory hard link publishes complete bytes atomically, fails
        # if another process created the target; unlike replace it cannot overwrite.
        os.link(staged, target)
    finally:
        if staged is not None:
            staged.unlink(missing_ok=True)
    return {**result, 'action': 'COPIED'}


def main():
    import json
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', choices=SOURCES, required=True)
    parser.add_argument('--batch-id', required=True, help='Actual collection date, not date received')
    parser.add_argument('--file', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    print(json.dumps(receive(PROJECT_ROOT, args.source, args.batch_id, args.file, args.apply), indent=2))


if __name__ == '__main__':
    main()
