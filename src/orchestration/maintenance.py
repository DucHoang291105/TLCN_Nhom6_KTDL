from __future__ import annotations

import argparse
import json
import re
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

from src.common.atomic_io import file_sha256, write_json_atomic

ROOT = Path(__file__).resolve().parents[2]
HELPER = 'apache/spark:3.5.9-scala2.12-java17-python3-r-ubuntu'
VOLUMES = ('minio_data', 'iceberg_catalog', 'postgres_data', 'superset_home', 'airflow_home')


def docker(*args):
    return subprocess.run(['docker', *args], cwd=ROOT, check=True, capture_output=True, text=True).stdout.strip()


def sha256(path):
    return file_sha256(path)


def assert_offline(volumes):
    for volume in volumes:
        active = docker('ps', '--filter', f'volume={volume}', '--format', '{{.Names}}')
        if active:
            raise RuntimeError(f'Coherent offline backup requires stopped services: {volume}: {active}')


def backup(destination):
    destination = Path(destination).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    if (destination / 'manifest.json').exists():
        raise ValueError('Use a new backup directory; never overwrite a backup manifest')
    volumes = [f'tlcn_bds_lakehouse_{suffix}' for suffix in VOLUMES]
    assert_offline(volumes)
    records = []
    for volume in volumes:
        docker('volume', 'inspect', volume)
        assert_offline(volumes)
        filename = volume + '.tar.gz'
        docker('run', '--rm', '--user', '0:0', '--entrypoint', '/bin/sh',
               '-v', f'{volume}:/source:ro', '-v', f'{destination}:/backup', HELPER,
               '-c', f'tar -czpf /backup/{filename} -C /source .')
        archive = destination / filename
        records.append({'volume': volume, 'archive': filename, 'sha256': sha256(archive), 'bytes': archive.stat().st_size})
        print(f'BACKED UP {volume}', flush=True)
    assert_offline(volumes)
    manifest = {'status': 'PASS', 'created_at': datetime.now(timezone.utc).isoformat(),
                'scope': 'Coherent offline MinIO + SQLite catalog + PostgreSQL + Superset + Airflow',
                'helper_image_id': docker('image', 'inspect', HELPER, '--format', '{{.Id}}'), 'volumes': records}
    write_json_atomic(destination / 'manifest.json', manifest)
    return manifest


def restore_check(destination):
    destination = Path(destination).resolve()
    manifest = json.loads((destination / 'manifest.json').read_text(encoding='utf-8'))
    assert_offline([r['volume'] for r in manifest['volumes']])
    restored = []
    for record in manifest['volumes']:
        if (not re.fullmatch(r'tlcn_bds_lakehouse_[a-z_]+\.tar\.gz', record['archive'])
                or record['volume'] not in {f'tlcn_bds_lakehouse_{s}' for s in VOLUMES}):
            raise ValueError('Unexpected backup volume or archive name')
        archive = destination / record['archive']
        if archive.parent != destination or sha256(archive) != record['sha256']:
            raise ValueError('Backup archive path/hash mismatch')
        target = 'tlcn_restore_check_' + uuid.uuid4().hex
        docker('volume', 'create', target)
        docker('run', '--rm', '--user', '0:0', '--entrypoint', '/bin/sh',
               '-v', f'{target}:/restore', '-v', f'{destination}:/backup:ro', HELPER,
               '-c', f'tar -xzpf /backup/{record["archive"]} -C /restore')

        docker('run', '--rm', '--user', '0:0', '--entrypoint', '/bin/sh',
               '-v', f'{target}:/restore:ro', '-v', f'{destination}:/backup:ro', HELPER,
               '-c', f'tar -dzpf /backup/{record["archive"]} -C /restore')
        restored.append({'source_volume': record['volume'], 'isolated_volume': target, 'archive_sha256': record['sha256']})
        print(f'RESTORE VERIFIED {record["volume"]}', flush=True)
    result = {'status': 'PASS', 'checked_at': datetime.now(timezone.utc).isoformat(), 'restored': restored,
              'limit': 'Byte/permission recovery in new local volumes; production volumes unchanged'}
    write_json_atomic(destination / 'restore_check.json', result)
    return result


def main():
    parser = argparse.ArgumentParser(description='Offline, checksummed backups and isolated restore checks for local Docker volumes.\n\nArchives contain private service metadata; keep them outside Git. No command in\nthis module restores over a production volume or removes a volume.\n')
    parser.add_argument('action', choices=('backup', 'restore-check'))
    parser.add_argument('--directory', type=Path, required=True)
    args = parser.parse_args()
    result = backup(args.directory) if args.action == 'backup' else restore_check(args.directory)
    print(json.dumps({'status': result['status'], 'directory': str(args.directory)}))


if __name__ == '__main__':
    main()
