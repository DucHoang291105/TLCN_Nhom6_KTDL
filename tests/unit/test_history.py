import pytest
from src.silver.history import identifier, merge_sql, BASE_COLUMNS, HISTORY_COLUMNS, KEYS


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
