from src.quality.check_environment import credentials_valid
from src.orchestration.init_environment import initialize
from pathlib import Path


def test_missing_and_placeholder_credentials_are_rejected(tmp_path):
    path = tmp_path / '.env'
    assert not credentials_valid(path)
    path.write_text((Path(__file__).resolve().parents[2] / '.env.example').read_text())
    assert not credentials_valid(path)


def test_generated_env_passes_without_exposing_values(tmp_path):
    template = Path(__file__).resolve().parents[2] / '.env.example'
    (tmp_path / '.env.example').write_bytes(template.read_bytes())
    initialize(tmp_path)
    assert credentials_valid(tmp_path / '.env')
    path = tmp_path / '.env'
    text = path.read_text()
    path.write_text('\n'.join('AIRFLOW_FERNET_KEY=invalid' if line.startswith('AIRFLOW_FERNET_KEY=') else line
                              for line in text.splitlines()))
    assert not credentials_valid(path)
