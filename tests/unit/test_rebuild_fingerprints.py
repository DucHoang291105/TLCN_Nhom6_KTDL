import json
import pytest
from src.common import compare_rebuild_counts as comparison


def payload(version="sha256_multiset_v2", digest="a"*64):
    return {"fingerprint_version": version, "tables": {"silver.listing_history": {
        "rows": 2, "distinct_keys": 2, "content_hash": digest,
        "hash_method": "sha256_of_sorted_full_row_sha256_multiset_v2"}}}


def test_equal_fingerprints_pass(tmp_path, monkeypatch):
    monkeypatch.setattr(comparison, "PROJECT_ROOT", tmp_path)
    (tmp_path / "docs/validation").mkdir(parents=True)
    first, second = tmp_path / "a.json", tmp_path / "b.json"
    first.write_text(json.dumps(payload())); second.write_text(json.dumps(payload()))
    comparison.main(str(first), str(second))
    assert json.loads((tmp_path / "docs/validation/deterministic_rebuild.json").read_text())["status"] == "PASS"


@pytest.mark.parametrize("other", [payload(digest="b"*64), payload(version="legacy_truncated_sum")])
def test_different_content_or_hash_version_fails(tmp_path, monkeypatch, other):
    monkeypatch.setattr(comparison, "PROJECT_ROOT", tmp_path)
    (tmp_path / "docs/validation").mkdir(parents=True)
    first, second = tmp_path / "a.json", tmp_path / "b.json"
    first.write_text(json.dumps(payload())); second.write_text(json.dumps(other))
    with pytest.raises(SystemExit):
        comparison.main(str(first), str(second))


def test_two_legacy_hash_files_are_not_mislabeled_as_full_sha256(tmp_path, monkeypatch):
    monkeypatch.setattr(comparison, "PROJECT_ROOT", tmp_path)
    (tmp_path / "docs/validation").mkdir(parents=True)
    first, second = tmp_path / "a.json", tmp_path / "b.json"
    first.write_text(json.dumps(payload(version="legacy")))
    second.write_text(json.dumps(payload(version="legacy")))
    with pytest.raises(SystemExit):
        comparison.main(str(first), str(second))


@pytest.mark.parametrize("case", ["empty", "duplicate_grain", "schema_change", "missing_table"])
def test_incomplete_or_invalid_rebuilds_cannot_pass(tmp_path, monkeypatch, case):
    monkeypatch.setattr(comparison, "PROJECT_ROOT", tmp_path)
    (tmp_path / "docs/validation").mkdir(parents=True)
    left, right = payload(), payload()
    name = "silver.listing_history"
    if case == "empty":
        left["tables"] = right["tables"] = {}
    elif case == "duplicate_grain":
        left["tables"][name]["distinct_keys"] = right["tables"][name]["distinct_keys"] = 1
    elif case == "schema_change":
        left["tables"][name]["schema"] = [{"name": "price", "type": "double"}]
        right["tables"][name]["schema"] = [{"name": "price", "type": "string"}]
    else:
        right["tables"] = {}
    first, second = tmp_path / "a.json", tmp_path / "b.json"
    first.write_text(json.dumps(left)); second.write_text(json.dumps(right))
    with pytest.raises(SystemExit):
        comparison.main(str(first), str(second))
    assert json.loads((tmp_path / "docs/validation/deterministic_rebuild.json").read_text())["status"] == "FAIL"


def test_quarantine_keeps_repeated_rejected_raw_observations(tmp_path, monkeypatch):
    monkeypatch.setattr(comparison, "PROJECT_ROOT", tmp_path)
    left = payload()
    row = left["tables"].pop("silver.listing_history")
    row["distinct_keys"] = 1
    left["tables"]["silver.listing_dq_quarantine"] = row
    first, second = tmp_path / "a.json", tmp_path / "b.json"
    first.write_text(json.dumps(left)); second.write_text(json.dumps(left))
    comparison.main(str(first), str(second))
    result = json.loads((tmp_path / "docs/validation/deterministic_rebuild.json").read_text())
    assert result["status"] == "PASS"
    assert not result["tables"][0]["grain_unique"]
    assert not result["tables"][0]["grain_uniqueness_required"]
