from __future__ import annotations
import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from statistics import median

from src.common.atomic_io import write_json_atomic
from src.orchestration.publication import ROOT, identity


def benchmark():
    records = []
    for workers in (1, 4, 1, 4):
        started = time.perf_counter()
        digest = identity(ROOT, workers=workers)
        record = {'workers': workers, 'elapsed_seconds': round(time.perf_counter() - started, 4), 'sha256': digest}
        records.append(record)
        print(json.dumps(record), flush=True)
    checks = {'same_full_byte_hash_for_all_samples': len({r['sha256'] for r in records}) == 1,
              'four_samples_completed': len(records) == 4}
    serial = median(r['elapsed_seconds'] for r in records if r['workers'] == 1)
    parallel = median(r['elapsed_seconds'] for r in records if r['workers'] == 4)
    result = {'status': 'PASS' if all(checks.values()) else 'FAIL', 'checks': checks, 'records': records,
              'generated_at': datetime.now(timezone.utc).isoformat(), 'serial_median_seconds': serial,
              'parallel_median_seconds': parallel, 'speedup': serial / parallel if parallel else None,
              'scope': 'Identity checksum primitive on the same source/input; not a whole-pipeline speed claim. No metadata-only cache.'}
    write_json_atomic(ROOT / 'docs/validation/identity_benchmark.json', result)
    if result['status'] != 'PASS':
        raise RuntimeError('Source/input changed during benchmark or hash ordering is inconsistent')


def main():
    parser = argparse.ArgumentParser(description='Measure full-byte identity hashing with one/four workers on the same inputs.')
    parser.add_argument('--inside', action='store_true')
    if parser.parse_args().inside:
        benchmark()
    else:
        result = subprocess.run(['docker', 'exec', '-e', 'PYTHONPATH=/opt/project', 'tlcn_spark_master',
                                 'python3', '-m', 'src.quality.benchmark_publication', '--inside'], cwd=ROOT, check=False)
        sys.exit(result.returncode)


if __name__ == '__main__':
    main()
