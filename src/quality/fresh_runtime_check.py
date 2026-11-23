from __future__ import annotations
import json
import secrets
import subprocess
import time
import uuid
from datetime import datetime, timezone

from src.common.atomic_io import write_json_atomic
from src.orchestration.maintenance import ROOT, docker


def main():
    image = json.loads((ROOT / 'config/runtime_images.json').read_text())['external_images']['minio']
    token = uuid.uuid4().hex[:12]
    name, volume, network = (f'tlcn_fresh_{token}_{part}' for part in ('minio', 'data', 'network'))
    user, password = 'freshcheck', secrets.token_urlsafe(32)
    result = {'status': 'FAIL', 'image': image, 'container': name, 'volume': volume, 'network': network,
              'scope': 'New local MinIO volume + registry pull + four-bucket authenticated roundtrip; not second-machine full DAG'}
    try:
        docker('pull', image)
        docker('network', 'create', network)
        docker('volume', 'create', volume)
        docker('run', '-d', '--name', name, '--network', network, '--user', '0:0',
               '-e', f'MINIO_ROOT_USER={user}', '-e', f'MINIO_ROOT_PASSWORD={password}',
               '-v', f'{volume}:/data', image, 'server', '/data', '--console-address', ':9001')
        deadline = time.monotonic() + 90
        while True:
            probe = subprocess.run(['docker', 'exec', name, 'mc', 'ready', 'local'], capture_output=True, check=False)
            if probe.returncode == 0:
                break
            if time.monotonic() > deadline:
                raise TimeoutError('Fresh MinIO did not become ready')
            time.sleep(2)
        script = '''mc alias set probe http://localhost:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
for bucket in lakehouse-bronze lakehouse-silver lakehouse-gold warehouse; do
  mc mb --ignore-existing probe/$bucket >/dev/null
  printf verified-roundtrip | mc pipe probe/$bucket/_fresh_check >/dev/null
  test "$(mc cat probe/$bucket/_fresh_check)" = verified-roundtrip
done'''
        docker('exec', name, '/bin/sh', '-ec', script)
        docker('restart', name)
        time.sleep(3)

        read_only = '''for bucket in lakehouse-bronze lakehouse-silver lakehouse-gold warehouse; do
  test "$(mc cat probe/$bucket/_fresh_check)" = verified-roundtrip
done'''
        docker('exec', name, '/bin/sh', '-ec', read_only)
        result.update(status='PASS', checks={'registry_pull': True, 'fresh_volume': True,
                      'authenticated_four_bucket_roundtrip': True, 'restart_persistence': True},
                      version=docker('exec', name, 'minio', '--version').splitlines()[0])
    finally:

        subprocess.run(['docker', 'stop', name], capture_output=True, check=False)
        result['generated_at'] = datetime.now(timezone.utc).isoformat()
        write_json_atomic(ROOT / 'docs/validation/fresh_runtime_check.json', result)
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
