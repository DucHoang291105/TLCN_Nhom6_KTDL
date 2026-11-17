"""Pure-Python tests for the B2 price-model experiment (no Spark)."""

import math
import random

from src.gold.gold_common import FLAG_COLUMNS
from src.gold.price_model_rules import MIN_EXTRA_TERM_TRAIN_ROWS, build_vocabulary, distance_band
from src.quality import price_model_experiment as experiment


def make_row(**overrides):
    row = {
        "source_id": "s1", "property_category_key": 1, "split": "train", "model_location_term": "d:10",
        "location_level": "DISTRICT", "area_band_key": 3, "room_band_key": 2, "price_per_m2": 50_000_000.0,
        "baseline_price_per_m2": 48_000_000.0, "distance_to_center_km": None, "ward": None,
        **{column: False for column in FLAG_COLUMNS},
    }
    row.update(overrides)
    return row


def test_distance_band_edges_and_unknown():
    assert distance_band(None) == "na"
    assert distance_band(0.0) == "0"
    assert distance_band(2.99) == "0"
    assert distance_band(3.0) == "1"
    assert distance_band(7.0) == "2"
    assert distance_band(15.0) == "3"


def test_v1_terms_have_no_extra_terms():
    terms = experiment.row_terms(make_row(), ())
    assert terms == ["location=d:10", "area_band=3", "room_band=2"]


def test_extra_terms_only_when_selected_and_available():
    row = make_row(distance_to_center_km=5.0, ward="Phuong 1")
    terms = experiment.row_terms(row, ("dist", "loc_size", "ward"))
    assert "dist_band=1" in terms
    assert "loc_size=d:10|M" in terms
    assert "ward=d:10|Phuong 1" in terms
    assert not any(term.startswith(("dist_band", "loc_size", "ward")) for term in experiment.row_terms(row, ()))


def test_ward_key_is_scoped_to_the_district():
    left = experiment.row_terms(make_row(model_location_term="d:10", ward="Phuong 1"), ("ward",))
    right = experiment.row_terms(make_row(model_location_term="d:11", ward="Phuong 1"), ("ward",))
    assert set(left) & {t for t in right if t.startswith("ward")} == set()


def test_province_level_gets_no_ward_and_pooled_gets_no_interaction():
    prov = experiment.row_terms(make_row(model_location_term="prov:7", ward="X"), ("loc_size", "ward"))
    assert "loc_size=prov:7|M" in prov and not any(t.startswith("ward") for t in prov)
    pooled = experiment.row_terms(make_row(model_location_term="p:Dong Nai", ward="X"), ("loc_size", "ward"))
    assert not any(t.startswith(("loc_size", "ward")) for t in pooled)


def test_vocabulary_drops_rare_extra_terms_but_keeps_flags():
    counts = {
        "location=d:1": 50, "location=d:2": 10, "area_band=3": 60, "room_band=2": 60,
        "flag=title_has_legal": 1, "loc_size=d:1|M": MIN_EXTRA_TERM_TRAIN_ROWS - 1,
        "ward=d:1|A": MIN_EXTRA_TERM_TRAIN_ROWS,
    }
    vocabulary = build_vocabulary(counts)
    assert "flag=title_has_legal" in vocabulary.terms
    assert "ward=d:1|A" in vocabulary.terms
    assert "loc_size=d:1|M" not in vocabulary.terms
    assert "location=d:1" in vocabulary.references


def synthetic(n, seed):
    rng = random.Random(seed)
    rows = []
    for index in range(n):
        district = rng.choice(["d:1", "d:2", "d:3"])
        area_band = rng.choice([2, 3, 4, 5])
        distance = rng.choice([None, 1.0, 5.0, 10.0, 20.0])
        legal = rng.random() < 0.3
        log_price = 17.0 + {"d:1": 0.4, "d:2": 0.0, "d:3": -0.3}[district] + (0.2 if legal else 0.0) \
            - (0.3 if distance == 20.0 else 0.0) + rng.gauss(0, 0.05)
        rows.append(make_row(
            source_id=f"r{seed}-{index}", model_location_term=district, area_band_key=area_band,
            price_per_m2=math.exp(log_price), baseline_price_per_m2=math.exp(17.0),
            distance_to_center_km=distance, title_has_legal=legal, ward=rng.choice(["A", "B"]),
        ))
    return rows


def test_fit_and_evaluate_recovers_signal_and_distance_helps():
    train, test = synthetic(900, 1), synthetic(300, 2)
    results = experiment.run_variants(train, test)
    assert set(results) == set(experiment.VARIANTS)
    v1 = results["v1_refit"]["overall"]["model"]
    with_distance = results["dist"]["overall"]["model"]
    assert v1["n"] == 300 == with_distance["n"]
    assert v1["r2_log"] > 0.5
    assert with_distance["mae"] < v1["mae"]  # the planted distance effect is found
    assert results["v1_refit"]["overall"]["model_beats_baseline_mae"] is True
    assert set(results["all"]["by_category"]) == set(experiment.CATEGORY_LABELS.values())
