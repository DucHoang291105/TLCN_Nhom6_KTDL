"""Verify identical source/input fingerprints on Windows, Spark and Airflow."""
from __future__ import annotations
import json
import subprocess
from datetime import datetime, timezone

from src.common.atomic_io import write_json_atomic
from src.orchestration.publication import ROOT, identity


def main():
    fingerprints = {'host': identity(ROOT)}
    code = 'from src.orchestration.publication import identity; print(identity())'
    for label, container, python in (('spark', 'tlcn_spark_master', 'python3'), ('airflow', 'tlcn_airflow', 'python')):
        result = subprocess.run(['docker', 'exec', '-e', 'PYTHONPATH=/opt/project', container, python, '-c', code],
                                cwd=ROOT, capture_output=True, text=True, check=True, timeout=90)
        fingerprints[label] = result.stdout.strip()
    checks = {'same_complete_tree_in_all_runtimes': len(set(fingerprints.values())) == 1,
              'sha256_format': all(len(h) == 64 and all(c in '0123456789abcdef' for c in h) for h in fingerprints.values())}
    report = {'status': 'PASS' if all(checks.values()) else 'FAIL', 'checks': checks,
              'generated_at': datetime.now(timezone.utc).isoformat(), 'fingerprints': fingerprints}
    write_json_atomic(ROOT / 'docs/validation/runtime_identity_check.json', report)
    print(json.dumps(report, indent=2))
    if report['status'] != 'PASS':
        raise SystemExit(2)


if __name__ == '__main__':
    main()
