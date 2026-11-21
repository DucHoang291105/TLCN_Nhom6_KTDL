import base64
from pathlib import Path

import pytest

from src.orchestration.init_environment import initialize


def test_existing_environment_preserved_byte_for_byte(tmp_path):
    path = tmp_path / '.env'
    path.write_bytes(b'USER_OWNED=value\r\n')
    assert initialize(tmp_path) is False
    assert path.read_bytes() == b'USER_OWNED=value\r\n'


def test_credentials_are_independent_and_fernet_key_is_valid(tmp_path):
    template = Path(__file__).resolve().parents[2] / '.env.example'
    (tmp_path / '.env.example').write_bytes(template.read_bytes())
    assert initialize(tmp_path)
    values = dict(line.split('=', 1) for line in (tmp_path / '.env').read_text().splitlines() if '=' in line)
    secrets = [values[k] for k in ('MINIO_ROOT_PASSWORD', 'POSTGRES_PASSWORD', 'AIRFLOW_JWT_SECRET',
                                 'AIRFLOW_FERNET_KEY', 'SUPERSET_SECRET_KEY', 'SUPERSET_ADMIN_PASSWORD')]
    assert len(set(secrets)) == len(secrets)
    assert all(len(value) >= 40 for value in secrets)
    assert len(base64.urlsafe_b64decode(values['AIRFLOW_FERNET_KEY'])) == 32
    assert values['GULAND_CRAWL_PAGES'] == '20'
    assert values['POSTGRES_USER'] == 'postgres'


def test_incomplete_template_does_not_write_partial_env(tmp_path):
    (tmp_path / '.env.example').write_text('GULAND_CRAWL_PAGES=200\n')
    with pytest.raises(ValueError, match='missing'):
        initialize(tmp_path)
    assert not (tmp_path / '.env').exists()
