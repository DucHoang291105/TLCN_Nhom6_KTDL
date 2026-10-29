import pytest
from src.silver.history import identifier, merge_sql, BASE_COLUMNS, HISTORY_COLUMNS, KEYS
from src.silver.history import cache_hit


@pytest.mark.parametrize("value", ["x", "lakehouse.silver.listing_history", "history_stage_123"])
def test_identifier(value):
    assert identifier(value).replace("`", "") == value


@pytest.mark.parametrize("value", ["a;DROP TABLE x", "a.b.c.d", "a-b", "", "a`b", "x y", "../x"])
def test_identifier_rejects_injection(value):
    with pytest.raises(ValueError):
        identifier(value)


def test_merge_is_null_safe_and_can_remove_obsolete_versions():
    sql = merge_sql("lakehouse.silver.listing_history", "stage", HISTORY_COLUMNS, KEYS)
    assert "t.`source_id` = s.`source_id` AND t.`version_number` = s.`version_number`" in sql
    assert "WHEN MATCHED AND s._delete THEN DELETE" in sql
    assert "t.`valid_to` <=> s.`valid_to`" in sql
    assert "WHEN NOT MATCHED AND NOT s._delete THEN INSERT" in sql
    update = sql.split("UPDATE SET ")[1].split("WHEN NOT MATCHED")[0]
    assert "t.`source_id` =" not in update
    assert "t.`version_number` =" not in update


def test_history_contract_includes_metadata_corrections_and_scd2():
    assert len(HISTORY_COLUMNS) == len(set(HISTORY_COLUMNS))
    assert {"bronze_ingested_at", "scraped_at", "snapshot_date", "batch_id", "record_hash"} <= set(BASE_COLUMNS)
    assert HISTORY_COLUMNS[-4:] == ["version_number", "valid_from", "valid_to", "is_current_version"]


@pytest.mark.parametrize("changed", ["input_metadata", "history_metadata", "state_snapshot", "state_schema", "logic", "timezone"])
def test_noop_cache_invalidates_any_changed_guard(changed):
    expected = {k: "same" for k in ("input_metadata", "history_metadata", "state_snapshot", "state_schema", "logic", "timezone")}
    counts = {"observation_rows": 3, "input_source_ids": 2, "history_rows": 2}
    cached = {"guard": dict(expected), "result": dict(counts)}
    assert cache_hit(cached, expected, counts) == counts
    cached["guard"][changed] = "different"
    assert cache_hit(cached, expected, counts) is None


@pytest.mark.parametrize("bad", [None, -1, True, "3", 999])
def test_noop_cache_requires_real_iceberg_metadata_counts(bad):
    counts = {"observation_rows": 3, "input_source_ids": 2, "history_rows": 2}
    cached = {"guard": {"input": "same"}, "result": dict(counts, observation_rows=bad)}
    assert cache_hit(cached, {"input": "same"}, counts) is None


@pytest.mark.parametrize("cached", [None, [], "bad", {"guard": {}, "result": None}, {"guard": {}, "result": []}])
def test_malformed_cache_falls_back_to_validation(cached):
    assert cache_hit(cached, {}, {"observation_rows": 1, "input_source_ids": 1, "history_rows": 1}) is None


@pytest.mark.parametrize("summary, expected", [
    ({"total-records": "154343", "total-delete-files": "0"}, 154343),
    ({"total-records": "0", "total-delete-files": "0"}, 0),
    ({"total-records": "10", "total-delete-files": "0", "total-position-deletes": "1"}, None),
    ({"total-records": "10", "total-delete-files": "0", "total-equality-deletes": "1"}, None),
    ({"total-records": "10", "total-delete-files": "1"}, None),
    ({"total-records": "-1", "total-delete-files": "0"}, None),
    ({"total-records": "broken", "total-delete-files": "0"}, None),
    ({"total-records": "10", "total-delete-files": "broken"}, None),
    ({}, None),
    ({"total-records": "10"}, None),
])
def test_metadata_count_only_uses_safe_physical_record_counts(summary, expected):
    from types import SimpleNamespace
    from src.silver.history import metadata_count
    table = SimpleNamespace(currentSnapshot=lambda: SimpleNamespace(summary=lambda: summary))
    assert metadata_count(table) == expected


def test_empty_iceberg_table_has_zero_rows():
    from types import SimpleNamespace
    from src.silver.history import metadata_count
    assert metadata_count(SimpleNamespace(currentSnapshot=lambda: None)) == 0
