"""Pure-Python tests for the repost leakage check (no Spark)."""

from src.quality import repost_leakage_check as leak

TOKENS = ["nha", "mat", "tien", "quan", "binh", "thanh", "5", "tang", "so", "hong"]


def make_row(**overrides):
    row = {
        "source_id": "a1", "source_key": 1, "split": "train", "location_key": 10, "property_category_key": 1,
        "area": 60.0, "price": 6_000_000_000.0, "tokens": TOKENS,
        "price_per_m2": 100_000_000.0, "predicted_price_per_m2": 100_000_000.0,
    }
    row.update(overrides)
    return row


def default_rule():
    return (0.02, 0.03, 0.35)


def test_same_source_repost_across_splits_is_flagged():
    rows = [make_row(source_id="a1", split="train"), make_row(source_id="a2", split="test")]
    assert leak.find_leaks(rows, *default_rule()) == {"a2": True}


def test_other_source_near_duplicate_is_flagged_but_not_as_same_source():
    rows = [make_row(source_id="a1", split="train", source_key=1), make_row(source_id="b1", split="test", source_key=2)]
    assert leak.find_leaks(rows, *default_rule()) == {"b1": False}


def test_different_location_or_category_or_price_is_not_flagged():
    train = make_row(source_id="a1", split="train")
    for change in ({"location_key": 11}, {"property_category_key": 2}, {"price": 7_000_000_000.0}, {"area": 80.0}, {"tokens": ["khac", "han"]}):
        assert leak.find_leaks([train, make_row(source_id="a2", split="test", **change)], *default_rule()) == {}


def test_two_listings_in_the_same_split_are_not_a_leak():
    rows = [make_row(source_id="a1", split="test"), make_row(source_id="a2", split="test"), make_row(source_id="a3", split="train", location_key=99)]
    assert leak.find_leaks(rows, *default_rule()) == {}


def test_area_just_inside_tolerance_is_found_across_a_bin_edge():
    rows = [make_row(source_id="a1", split="train", area=100.0), make_row(source_id="a2", split="test", area=98.1)]
    assert leak.find_leaks(rows, *default_rule()) == {"a2": True}
    rows[1]["area"] = 97.0
    assert leak.find_leaks(rows, *default_rule()) == {}


def test_unusable_rows_are_ignored():
    rows = [make_row(source_id="a1", split="train", tokens=[]), make_row(source_id="a2", split="test"),
            make_row(source_id="a3", split="train", location_key=-1), make_row(source_id="a4", split="test", location_key=-1)]
    assert leak.find_leaks(rows, *default_rule()) == {}


def test_error_summary_share_within_band_ignores_invalid_pairs():
    summary = leak.error_summary([(100.0, 100.0), (130.0, 100.0), (100.0, 0.0), (None, 90.0)])
    assert summary["n"] == 2
    assert summary["share_within_25pct"] == 0.5


def test_leakage_effect_compares_all_clean_and_leaked_rows():
    test_rows = [
        make_row(source_id="t1", price_per_m2=100.0, predicted_price_per_m2=100.0),
        make_row(source_id="t2", price_per_m2=100.0, predicted_price_per_m2=100.0),
        make_row(source_id="t3", price_per_m2=200.0, predicted_price_per_m2=100.0),
        make_row(source_id="t4", price_per_m2=150.0, predicted_price_per_m2=100.0),
    ]
    result = leak.leakage_effect(test_rows, {"t1": True, "t2": False})
    assert result["test_listings"] == 4
    assert result["with_near_duplicate_in_train"] == 2
    assert result["same_source_near_duplicate"] == 1
    assert result["share_with_near_duplicate_in_train"] == 0.5
    assert result["without_near_duplicates"]["n"] == 2
    assert result["only_near_duplicates"]["median_ape"] == 0.0
    assert result["all_test"]["share_within_25pct"] == 0.5
    assert result["without_near_duplicates"]["share_within_25pct"] == 0.0
    assert result["change_when_removed"]["share_within_25pct"] == -0.5


def test_leakage_effect_with_no_leaks_changes_nothing():
    rows = [make_row(source_id="t1", price_per_m2=120.0, predicted_price_per_m2=100.0)]
    result = leak.leakage_effect(rows, {})
    assert result["with_near_duplicate_in_train"] == 0
    assert all(value == 0 for value in result["change_when_removed"].values())
