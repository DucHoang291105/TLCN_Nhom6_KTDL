"""Read-only host preflight for a new clone; does not certify a full DAG run."""
from __future__ import annotations

import argparse
import base64
import json
import platform
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter

from src.common.atomic_io import write_json_atomic

ROOT = Path(__file__).resolve().parents[2]


def credentials_valid(path):
    if not path.is_file():
        return False
    values = dict(line.split('=', 1) for line in path.read_text(encoding='utf-8-sig').splitlines()
                  if '=' in line and not line.lstrip().startswith('#'))
    required = ('MINIO_ROOT_PASSWORD', 'POSTGRES_PASSWORD', 'AIRFLOW_JWT_SECRET',
                'AIRFLOW_FERNET_KEY', 'SUPERSET_SECRET_KEY', 'SUPERSET_ADMIN_PASSWORD')
    if any(not values.get(key) or values[key].startswith(('replace-', 'change-me')) for key in required):
        return False
    try:
        return len(base64.urlsafe_b64decode(values['AIRFLOW_FERNET_KEY'])) == 32
    except (ValueError, TypeError):
        return False


def inspect(root=ROOT):
    root = Path(root)
    started = perf_counter()
    checks = {'python_3_10_or_newer': sys.version_info >= (3, 10),
              'local_env_configured': credentials_valid(root / '.env')}
    commands = {'git': ['git', '--version'], 'git_lfs': ['git', 'lfs', 'version'],
                'docker_daemon': ['docker', 'info', '--format', '{{.ServerVersion}}'],
                'compose_config': ['docker', 'compose', 'config', '--quiet']}
    versions = {}
    for name, command in commands.items():
        try:
            result = subprocess.run(command, cwd=root, capture_output=True, text=True, timeout=30, check=False)
            checks[name] = result.returncode == 0
            # Do not collect compose output, environment variables or daemon config.
            if name != 'compose_config' and checks[name]:
                versions[name] = result.stdout.strip()[:200]
        except (OSError, subprocess.TimeoutExpired):
            checks[name] = False
    pointers = []
    incoming = root / 'data/incoming'
    files = sorted(incoming.rglob('*.csv'))
    for path in files:
        with path.open('rb') as handle:
            if handle.read(128).startswith(b'version https://git-lfs.github.com/spec/v1'):
                pointers.append(path.relative_to(root).as_posix())
    checks['input_csv_present'] = bool(files)
    checks['no_lfs_pointer_csv'] = not pointers
    return {'generated_at': datetime.now(timezone.utc).isoformat(),
            'scope': 'Host preflight only; not a clean-machine end-to-end certificate',
            'status': 'PASS' if all(checks.values()) else 'FAIL', 'checks': checks,
            'python': platform.python_version(), 'platform': platform.system(),
            'versions': versions, 'csv_files_seen': len(files), 'lfs_pointers': pointers,
            'elapsed_seconds': round(perf_counter() - started, 3), 'clean_machine_certified': False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / 'outputs/reproduction/preflight.json')
    args = parser.parse_args()
    result = inspect()
    write_json_atomic(args.output, result)
    print(json.dumps(result, indent=2))
    if result['status'] != 'PASS':
        raise SystemExit(2)


if __name__ == '__main__':
    main()
