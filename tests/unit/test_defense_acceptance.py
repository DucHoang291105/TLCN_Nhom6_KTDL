import copy
import json
import subprocess
from types import SimpleNamespace

import pytest

from src.common.table_fingerprints import GRAINS
from src.orchestration import environment
from src.quality.defense_evidence import all_pass, bronze_matches_manifest, crawl_inventory, fingerprint_checks, junit_pass


def fingerprints():
    return {"fingerprint_version": "sha256_multiset_v2", "tables": {
        table: {"schema": [{"name": "id", "type": "string", "nullable": True}], "grain": grain,
                "rows": 2, "distinct_keys": 2, "content_hash": "a"*64,
                "hash_method": "sha256_of_sorted_full_row_sha256_multiset_v2"}
        for table, grain in GRAINS.items()}}


def test_full_table_fingerprints_pass_and_quarantine_may_repeat():
    first, second = fingerprints(), fingerprints()
    for item in (first, second):
        item["tables"]["silver.listing_dq_quarantine"]["distinct_keys"] = 1
    assert all_pass(fingerprint_checks(first, second))


@pytest.mark.parametrize("case", ["missing_table", "extra_table", "missing_schema", "changed_schema", "bad_hash", "changed_hash", "duplicate_grain", "legacy_hash"])
def test_release_cannot_accept_partial_or_invalid_fingerprints(case):
    first, second = fingerprints(), fingerprints()
    table = "gold.fact_listing_price_model"
    row = second["tables"][table]
    if case == "missing_table": second["tables"].pop(table)
    elif case == "extra_table": second["tables"]["gold.unverified"] = copy.deepcopy(row)
    elif case == "missing_schema": row.pop("schema")
    elif case == "changed_schema": row["schema"][0]["type"] = "double"
    elif case == "bad_hash": row["content_hash"] = "a"
    elif case == "changed_hash": row["content_hash"] = "b"*64
    elif case == "duplicate_grain": row["distinct_keys"] = 1
    else: second["fingerprint_version"] = "legacy"
    assert not all_pass(fingerprint_checks(first, second))


@pytest.mark.parametrize("xml", ['<testsuites/>', '<testsuite tests="0"/>', '<testsuite tests="2" failures="1"/>',
                                  '<testsuite tests="2" errors="1"/>', '<testsuite tests="2" skipped="1"/>'])
def test_empty_failed_or_skipped_local_tests_are_not_release_evidence(tmp_path, xml):
    path = tmp_path / "junit.xml"
    path.write_text(xml)
    assert not junit_pass(path)


def test_junit_accepts_successful_nested_suite(tmp_path):
    path = tmp_path / "junit.xml"
    path.write_text('<testsuites><testsuite tests="3" failures="0" errors="0" skipped="0"/></testsuites>')
    assert junit_pass(path)


@pytest.mark.parametrize("work", [{"spark_apps": ["app1"], "airflow_runs": []},
                                  {"spark_apps": [], "airflow_runs": ["run1"]}])
def test_query_window_blocks_even_when_only_airflow_is_between_spark_jobs(work):
    with pytest.raises(RuntimeError, match="Pipeline still active"):
        environment.assert_idle(work)


def test_idle_window_can_switch():
    environment.assert_idle({"spark_apps": [], "airflow_runs": []})


@pytest.mark.parametrize("state", ["paused", "restarting"])
def test_uncertain_writer_state_is_not_idle(monkeypatch, state):
    monkeypatch.setattr(environment, "state", lambda _: state)
    with pytest.raises(RuntimeError, match="cannot certify"):
        environment.active_work()


def test_docker_failure_cannot_be_treated_as_absent_service(monkeypatch):
    def fail(*args, **kwargs): raise subprocess.CalledProcessError(1, "docker")
    monkeypatch.setattr(environment.subprocess, "run", fail)
    with pytest.raises(subprocess.CalledProcessError):
        environment.state("tlcn_spark_master")


def test_bronze_inventory_requires_same_dates_sources_and_counts():
    manifest = {"snapshots": [{"source": "guland", "batch_id": "20261027", "rows": 1350}]}
    audit = {"batches": [{"branch": "snapshots", "source": "guland", "batch_id": "20261027", "row_count": 1350}]}
    assert bronze_matches_manifest(audit, manifest)
    audit["batches"][0]["batch_id"] = "20261020"
    assert not bronze_matches_manifest(audit, manifest)


def test_calendar_records_missing_batch_instead_of_filling_it(tmp_path):
    (tmp_path / "config").mkdir()
    known = {"known_gaps": [{"source": "guland", "batch_id": "20261020", "reason": "website error"}]}
    (tmp_path / "config/source_gaps.json").write_text(json.dumps(known))
    data = {"batches_by_source": {"guland": ["20261016", "20261023", "20261027"]},
            "latest_by_source": {"guland": "20261027"}, "latest_observed_batch": "20261027"}
    result = crawl_inventory(tmp_path, data)
    assert result["all_gaps_declared"]
    assert result["missing_scheduled_batches"] == [{"source": "guland", "batch_id": "20261020", "declared": True, "reason": "website error"}]
    data["batches_by_source"]["guland"].remove("20261023")
    assert not crawl_inventory(tmp_path, data)["all_gaps_declared"]
