"""Pure-Python tests for the price-model selection script (no Spark)."""

import math
import random

from src.gold.gold_common import FLAG_COLUMNS
from src.gold.price_model_rules import MIN_EXTRA_TERM_TRAIN_ROWS, build_vocabulary, distance_band
from src.quality import price_model_selection as selection


def make_row(**overrides):
    row = {
        "source_id": "s1", "property_category_key": 1, "split": "train", "model_location_term": "d:10",
        "location_level": "DISTRICT", "location_key": 10, "area_band_key": 3, "room_band_key": 2,
        "price_per_m2": 50_000_000.0, "baseline_price_per_m2": 48_000_000.0, "distance_to_center_km": None, "ward": None,
        **{column: False for column in FLAG_COLUMNS},
    }
    row.update(overrides)
    return row


def test_distance_band_edges_and_unknown():
    assert distance_band(None) == "na"
    assert distance_band(2.99) == "0"
    assert distance_band(3.0) == "1"
    assert distance_band(7.0) == "2"
    assert distance_band(15.0) == "3"


def test_base_terms_have_no_extra_terms():
    assert selection.row_terms(make_row(), ()) == ["location=d:10", "area_band=3", "room_band=2"]


def test_extra_terms_only_when_selected_and_available():
    row = make_row(distance_to_center_km=5.0, ward="Phuong 1")
    terms = selection.row_terms(row, selection.ALL_TERMS)
    assert "dist_band=1" in terms and "loc_size=d:10|M" in terms and "ward=d:10|Phuong 1" in terms
    assert not any(term.startswith(("dist_band", "loc_size", "ward")) for term in selection.row_terms(row, ()))


def test_ward_is_scoped_to_district_and_pooled_gets_no_interaction():
    left = selection.row_terms(make_row(model_location_term="d:10", ward="Phuong 1"), ("ward",))
    right = selection.row_terms(make_row(model_location_term="d:11", ward="Phuong 1"), ("ward",))
    assert {t for t in left if t.startswith("ward")}.isdisjoint(t for t in right if t.startswith("ward"))
    pooled = selection.row_terms(make_row(model_location_term="p:Dong Nai", ward="X"), selection.ALL_TERMS)
    assert not any(t.startswith(("loc_size", "ward")) for t in pooled)


def test_vocabulary_drops_rare_extra_terms_but_keeps_flags():
    counts = {"location=d:1": 50, "location=d:2": 10, "area_band=3": 60, "room_band=2": 60, "flag=title_has_legal": 1,
              "loc_size=d:1|M": MIN_EXTRA_TERM_TRAIN_ROWS - 1, "ward=d:1|A": MIN_EXTRA_TERM_TRAIN_ROWS}
    vocabulary = build_vocabulary(counts)
    assert "flag=title_has_legal" in vocabulary.terms and "ward=d:1|A" in vocabulary.terms
    assert "loc_size=d:1|M" not in vocabulary.terms


def synthetic(n, seed):
    rng = random.Random(seed)
    rows = []
    for index in range(n):
        district = rng.choice(["d:1", "d:2", "d:3"])
        distance = rng.choice([None, 1.0, 5.0, 10.0, 20.0])
        legal = rng.random() < 0.3
        log_price = 17.0 + {"d:1": 0.4, "d:2": 0.0, "d:3": -0.3}[district] + (0.2 if legal else 0.0) \
            - (0.3 if distance == 20.0 else 0.0) + rng.gauss(0, 0.05)
        rows.append(make_row(
            source_id=f"r{seed}-{index}", model_location_term=district, location_key=int(district[2:]),
            area_band_key=rng.choice([2, 3, 4, 5]), price_per_m2=math.exp(log_price), baseline_price_per_m2=math.exp(17.0),
            distance_to_center_km=distance, title_has_legal=legal, ward=rng.choice(["A", "B"]),
        ))
    return rows


def test_term_comparison_recovers_signal_and_distance_helps():
    train, test = synthetic(900, 1), synthetic(300, 2)
    results = selection.term_comparison(train, test)
    assert set(results) == set(selection.TERM_GROUPS)
    base, with_distance = results["base_terms"]["overall"]["model"], results["distance"]["overall"]["model"]
    assert base["n"] == 300 == with_distance["n"] and base["r2_log"] > 0.5
    assert with_distance["mae"] < base["mae"]
    assert results["base_terms"]["overall"]["model_beats_baseline_mae"] is True


def test_folds_are_stable_and_cover_all_folds():
    assert selection.fold_of("abc") == selection.fold_of("abc")
    assert {selection.fold_of(f"id{i}") for i in range(200)} == set(range(selection.FOLDS))


def test_baseline_tables_use_peer_groups_with_fallback():
    rows = [make_row(source_id=f"a{i}", price_per_m2=100.0 + i) for i in range(6)]
    tables = selection.baseline_tables(rows)
    assert selection.baseline_price(tables, make_row()) == 102.5
    unseen = make_row(location_key=99, area_band_key=1, room_band_key=1)
    assert selection.baseline_price(tables, unseen) == 102.5
    assert selection.baseline_price(tables, make_row(location_key=-1)) == 102.5


def test_choose_configuration_keeps_plain_model_unless_clearly_beaten():
    rows = [make_row(source_id=f"r{i}", price_per_m2=100.0) for i in range(50)]
    perfect_model = [(row, 100.0, 300.0) for row in rows]
    choice = selection.choose_configuration({selection.RIDGE_LAMBDA: perfect_model})
    assert choice["chosen"]["weight"] == 1.0 and not choice["chosen"]["pooled_to_baseline"]
    perfect_baseline = [(row, 300.0, 100.0) for row in rows]
    choice = selection.choose_configuration({selection.RIDGE_LAMBDA: perfect_baseline})
    assert choice["chosen"]["weight"] == 0.0
    assert choice["chosen"]["oof_mae"] < choice["plain_model"]["oof_mae"]


def test_paired_bootstrap_detects_real_and_null_differences():
    rng = random.Random(3)
    better = selection.paired_bootstrap([5 + rng.gauss(0, 1) for _ in range(300)], resamples=300)
    assert better["significant_at_5pct"] and better["mean_diff"] > 4 and better["ci95"][0] > 0
    null = selection.paired_bootstrap([rng.gauss(0, 5) for _ in range(300)], resamples=300)
    assert not null["significant_at_5pct"]
    assert selection.paired_bootstrap([]) == {"n": 0}


def test_abs_error_differences_sign():
    assert selection.abs_error_differences([100.0], [150.0], [110.0]) == [40.0]


def test_prediction_agreement_detects_rows_that_differ_even_with_equal_totals():
    same = selection.prediction_agreement([(100.0, 100.0), (200.0, 200.5)])
    assert same["n"] == 2 and same["share_within_tolerance"] == 1.0

    swapped = selection.prediction_agreement([(100.0, 200.0), (200.0, 100.0), (50.0, 50.0)])
    assert swapped["share_within_tolerance"] < 0.5 and swapped["max_abs_log_difference"] > 0.6
    assert selection.prediction_agreement([(None, 100.0)])["n"] == 0


def test_error_ratio_summary_reports_quantiles_and_share_within_band():
    pairs = [(100.0, 100.0), (120.0, 100.0), (80.0, 100.0), (150.0, 100.0), (50.0, 100.0), (10.0, None), (0.0, 100.0)]
    result = selection.error_ratio_summary(pairs)

    assert result["n"] == 5
    assert result["actual_over_predicted_quantiles"]["0.5"] == 1.0
    assert result["actual_over_predicted_quantiles"]["0.1"] == 0.62
    assert result["actual_over_predicted_quantiles"]["0.9"] == 1.38
    assert result["share_within"] == {"25pct": 0.6, "50pct": 1.0}


def test_error_ratio_summary_handles_no_usable_rows():
    assert selection.error_ratio_summary([]) == {"n": 0}
    assert selection.error_ratio_summary([(100.0, None)]) == {"n": 0}
