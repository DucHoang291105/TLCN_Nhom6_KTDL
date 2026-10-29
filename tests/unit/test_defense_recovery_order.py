import pytest
from src.quality import defense_suite


def setup_suite(tmp_path, monkeypatch, fail_probe=False):
    (tmp_path / "docs/validation").mkdir(parents=True)
    monkeypatch.setattr(defense_suite, "ROOT", tmp_path)
    monkeypatch.setattr(defense_suite, "validate_snapshots", lambda _: None)
    monkeypatch.setattr(defense_suite, "capture", lambda *args: {"run_id": "test"})
    events = []
    monkeypatch.setattr(defense_suite, "execute", lambda work, name, command: events.append(name))
    def wait(work, run, output, expect_failure=False):
        events.append(output)
        if expect_failure and fail_probe:
            raise RuntimeError("Fault probe did not block downstream")
        if not expect_failure:
            raise StopIteration("Normal DAG is now the latest successful run")
    monkeypatch.setattr(defense_suite, "wait_run", wait)
    return events


def test_release_recovers_normal_dag_after_intentional_failure(tmp_path, monkeypatch):
    events = setup_suite(tmp_path, monkeypatch)
    with pytest.raises(StopIteration):
        defense_suite.suite("20261027", "test")
    assert events.index("airflow_failure_gate") < events.index("trigger") < events.index("integration_airflow_run")


def test_bad_failure_probe_stops_release_before_normal_dag(tmp_path, monkeypatch):
    events = setup_suite(tmp_path, monkeypatch, fail_probe=True)
    with pytest.raises(RuntimeError, match="did not block"):
        defense_suite.suite("20261027", "test")
    assert "trigger" not in events
