"""Create a local .env once, with independent randomly generated credentials."""
from __future__ import annotations

import base64
import secrets
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def initialize(root=ROOT):
    root = Path(root)
    target = root / '.env'
    if target.exists():
        return False
    values = {name: secrets.token_urlsafe(32) for name in (
        'MINIO_ROOT_PASSWORD', 'POSTGRES_PASSWORD', 'AIRFLOW_JWT_SECRET',
        'SUPERSET_SECRET_KEY', 'SUPERSET_ADMIN_PASSWORD')}
    values['AIRFLOW_FERNET_KEY'] = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode('ascii')
    lines = (root / '.env.example').read_text(encoding='utf-8').splitlines()
    found = {line.split('=', 1)[0] for line in lines if '=' in line}
    if not values.keys() <= found:
        raise ValueError('Environment template is missing required credential settings')
    output = '\n'.join(f'{key}={values[key]}' if (key := line.split('=', 1)[0]) in values else line
                       for line in lines) + '\n'
    # Exclusive creation also protects an .env created by another process.
    with target.open('x', encoding='utf-8', newline='\n') as handle:
        handle.write(output)
    return True


if __name__ == '__main__':
    print('Created .env; credentials remain local.' if initialize() else 'Existing .env preserved; no changes.')
