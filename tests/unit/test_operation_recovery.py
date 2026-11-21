from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from src.orchestration import environment, airflow_run


@pytest.mark.parametrize('method,retries', [('GET', 1), ('HEAD', 1), ('POST', 0), ('PATCH', 0), ('DELETE', 0)])
def test_auth_retry_is_bounded_and_never_resubmits_mutations(monkeypatch, method, retries):
    first = SimpleNamespace(status_code=403, close=Mock())
    second = SimpleNamespace(status_code=403, close=Mock())
    request = Mock(side_effect=[first, second])
    monkeypatch.setattr(airflow_run.requests.Session, 'request', request)
    monkeypatch.setattr(airflow_run.time, 'sleep', Mock())
    result = airflow_run.ReadRetrySession().request(method, 'http://localhost/read')
    assert request.call_count == 1 + retries
    assert result.status_code == 403  # Real permission errors remain denied.


def test_write_window_drains_readers_before_reopening_catalog(monkeypatch, tmp_path):
    monkeypatch.setattr(environment, "ROOT", tmp_path)
    commands = []
    monkeypatch.setattr(environment, 'active_work', lambda: {'spark_apps': [], 'airflow_runs': []})
    monkeypatch.setattr(environment, 'state', lambda _: 'running')
    monkeypatch.setattr(environment.subprocess, 'run', lambda cmd, **kw: commands.append(cmd))
    environment.switch('write')
    assert commands[:3] == [['docker', 'stop', 'tlcn_superset'], ['docker', 'stop', 'tlcn_trino'],
                            ['docker', 'restart', 'tlcn_iceberg_rest']]
    assert '--wait' in commands[3]


def test_busy_writer_prevents_catalog_restart(monkeypatch, tmp_path):
    monkeypatch.setattr(environment, 'ROOT', tmp_path)
    monkeypatch.setattr(environment, 'active_work', lambda: {'spark_apps': ['app'], 'airflow_runs': []})
    run = Mock()
    monkeypatch.setattr(environment.subprocess, 'run', run)
    with pytest.raises(RuntimeError, match='Pipeline still active'):
        environment.switch('write')
    run.assert_not_called()
