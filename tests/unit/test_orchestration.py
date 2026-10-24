import json
import sys
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from src.orchestration import pipeline
from src.gold.build_listing_lifecycle import calendar_from_audit


@pytest.mark.parametrize("value", ["20261016", "20260228"])
def test_valid_batch(value):
    assert pipeline.validate_batch(value) == value


@pytest.mark.parametrize("value", ["20261016_120000", "20260230", "../../etc", "2026101"])
def test_invalid_batch(value):
    with pytest.raises(ValueError):
        pipeline.validate_batch(value)


def test_existing_csv_is_reused_without_crawl(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline, "PROJECT_ROOT", tmp_path)
    path = pipeline.snapshot_path("guland", "20261016")
    path.parent.mkdir(parents=True)
    path.write_text("listing_id\n1\n")
    run = Mock(side_effect=AssertionError("Must not crawl existing batch"))
    monkeypatch.setattr(pipeline.subprocess, "run", run)
    pipeline.prepare_web_source("guland", "20261016")
    run.assert_not_called()


def test_historical_missing_batch_not_fabricated(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline, "PROJECT_ROOT", tmp_path)
    with pytest.raises(ValueError, match="historical"):
        pipeline.prepare_web_source("guland", "20200101")


@pytest.mark.parametrize("code", [1, 137, None])
def test_spark_failure_reaches_airflow(monkeypatch, code):
    client = Mock()
    client.containers.list.return_value = []
    client.api.exec_create.return_value = {"Id": "exec1"}
    client.api.exec_start.return_value = [b"job failed\n"]
    client.api.exec_inspect.return_value = {"ExitCode": code}
    monkeypatch.setitem(sys.modules, "docker", SimpleNamespace(from_env=lambda **kwargs: client))
    with pytest.raises(RuntimeError, match="exit code"):
        pipeline.run_spark("src/gold/verify_gold.py")
    client.close.assert_called_once()


def test_spark_success(monkeypatch):
    client = Mock()
    client.containers.list.return_value = []
    client.api.exec_create.return_value = {"Id": "exec1"}
    client.api.exec_start.return_value = []
    client.api.exec_inspect.return_value = {"ExitCode": 0}
    monkeypatch.setitem(sys.modules, "docker", SimpleNamespace(from_env=lambda **kwargs: client))
    pipeline.run_spark("src/gold/build_listing_lifecycle.py")


def test_spark_dependencies_use_the_persistent_named_volume():
    command = pipeline.spark_command("src/silver/build_listing_history.py")
    assert "spark.jars.ivy=/home/spark/.ivy2" in command
    assert "spark.jars.ivy=/tmp/.ivy2" not in command


def test_bronze_warn_stops_pipeline(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline, "PROJECT_ROOT", tmp_path)
    out = tmp_path / "docs/validation/bronze_state_audit.json"
    out.parent.mkdir(parents=True)
    out.write_text(json.dumps({"status": "WARN"}))
    with pytest.raises(RuntimeError, match="Bronze audit"):
        pipeline.require_bronze_pass()


def test_calendar_rejects_failed_audit():
    with pytest.raises(ValueError, match="PASS"):
        calendar_from_audit({"status": "WARN", "batches": []})


def test_incomplete_existing_crawl_is_not_silently_reused(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline, "PROJECT_ROOT", tmp_path)
    path = pipeline.snapshot_path("guland", "20261016")
    path.parent.mkdir(parents=True)
    path.write_text("listing_id\n1\n")
    (path.parent / "crawl_summary.json").write_text(json.dumps({"failed_pages": [2]}))
    with pytest.raises(RuntimeError, match="incomplete"):
        pipeline.prepare_web_source("guland", "20261016")


def test_calendar_uses_only_crawl_and_deduplicates():
    batches = [{"branch": "snapshots", "source": s, "batch_id": "20261016", "status": "READABLE"}
               for s in pipeline.SOURCES]
    batches += [batches[0], {"branch": "historical", "source": "mogi", "batch_id": "initial", "status": "READABLE"}]
    assert len(calendar_from_audit({"status": "PASS", "batches": batches})) == 3


def test_snapshot_manifest_rejects_changed_existing_csv(tmp_path, monkeypatch):
    monkeypatch.setattr(pipeline, "PROJECT_ROOT", tmp_path)
    for source in pipeline.SOURCES:
        path = pipeline.snapshot_path(source, "20261023")
        path.parent.mkdir(parents=True)
        path.write_text("listing_id\n1\n", encoding="utf-8")
    pipeline.validate_snapshots("20261023")
    manifest = tmp_path / "docs/validation/snapshot_manifest.json"
    before = manifest.read_bytes()
    pipeline.snapshot_path("batdongsan", "20261023").write_text("listing_id\n2\n", encoding="utf-8")
    with pytest.raises(ValueError, match="snapshot changed"):
        pipeline.validate_snapshots("20261023")
    assert manifest.read_bytes() == before
