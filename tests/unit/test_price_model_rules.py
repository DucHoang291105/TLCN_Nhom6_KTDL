import math
import random
from collections import defaultdict

from src.gold.price_model_rules import (
    INTERCEPT, MIN_LEVEL_TRAIN_ROWS, OTHER_LOCATION, build_vocabulary, cholesky, cholesky_solve,
    design_indices, fit_ridge, inverse_diagonal, listing_terms, location_level, normal_equation_entries,
    predict_log, premium_pct, regression_metrics,
)


def close(a, b, tol=1e-6):
    return math.isclose(a, b, rel_tol=tol, abs_tol=tol)


def test_location_level_prefers_district_then_province_then_other():
    assert location_level(MIN_LEVEL_TRAIN_ROWS, 0, 12, "Hà Nội") == "d:12"
    assert location_level(MIN_LEVEL_TRAIN_ROWS - 1, MIN_LEVEL_TRAIN_ROWS, 12, "Hà Nội") == "p:Hà Nội"
    assert location_level(3, 4, 12, "Hà Nội") == OTHER_LOCATION
    assert location_level(3, 50, 12, None) == OTHER_LOCATION


def test_listing_terms_include_unknown_rooms_and_only_true_flags():
    terms = listing_terms("d:7", 3, -1, {"title_has_legal": True, "title_has_elevator": False})
    assert terms == ["location=d:7", "area_band=3", "room_band=-1", "flag=title_has_legal"]


def test_vocabulary_drops_most_frequent_level_per_factor():
    counts = {
        "location=d:1": 50, "location=d:2": 30, "area_band=2": 10, "area_band=3": 70,
        "room_band=-1": 40, "room_band=2": 40, "flag=title_has_legal": 5, "flag=title_has_elevator": 0,
    }
    vocab = build_vocabulary(counts)
    assert vocab.terms[0] == INTERCEPT
    # Ties are broken by name, so the reference is deterministic.
    assert set(vocab.references) == {"location=d:1", "area_band=3", "room_band=-1"}
    assert "location=d:1" not in vocab.terms and "location=d:2" in vocab.terms
    assert "flag=title_has_legal" in vocab.terms
    assert "flag=title_has_elevator" not in vocab.terms


def test_design_indices_reference_and_unseen_levels():
    vocab = build_vocabulary({"location=d:1": 9, "location=d:2": 5, "area_band=3": 9, "room_band=2": 9, "flag=title_has_legal": 3})
    columns, unseen = design_indices(["location=d:1", "area_band=3", "room_band=2"], vocab)
    assert columns == [0] and unseen == 0  # every level is a reference
    columns, unseen = design_indices(["location=d:2", "area_band=3", "room_band=9", "flag=title_has_legal"], vocab)
    assert unseen == 1  # room band 9 never seen in training
    assert columns == sorted({0, vocab.index["location=d:2"], vocab.index["flag=title_has_legal"]})


def test_normal_equation_entries_upper_triangle_and_xty():
    entries = dict(normal_equation_entries([0, 2, 5], 1.5))
    assert entries[(0, 0)] == 1.0 and entries[(0, 5)] == 1.0 and entries[(5, 5)] == 1.0
    assert (5, 0) not in entries
    assert entries[(2, -1)] == 1.5


def test_cholesky_solves_small_system():
    matrix = [[4.0, 2.0, 0.6], [2.0, 5.0, 1.0], [0.6, 1.0, 3.0]]
    lower = cholesky(matrix)
    solution = cholesky_solve(lower, [1.0, 2.0, 3.0])
    for row, expected in zip(matrix, [1.0, 2.0, 3.0]):
        assert close(sum(a * x for a, x in zip(row, solution)), expected)


def test_cholesky_rejects_singular_matrix():
    try:
        cholesky([[1.0, 1.0], [1.0, 1.0]])
    except ValueError:
        return
    raise AssertionError("expected ValueError")


def _fit(rows, ridge):
    counts = defaultdict(int)
    for terms, _ in rows:
        for term in terms:
            counts[term] += 1
    vocab = build_vocabulary(counts)
    entries = defaultdict(float)
    for terms, y in rows:
        for key, value in normal_equation_entries(design_indices(terms, vocab)[0], y):
            entries[key] += value
    beta, lower = fit_ridge(entries, vocab.size, ridge)
    return vocab, beta, lower


def test_fit_recovers_known_coefficients_without_noise():
    truth = {INTERCEPT: 17.0, "location=d:2": 0.4, "area_band=4": -0.1, "flag=title_has_frontage": 0.25}
    rng = random.Random(7)
    rows = []
    for _ in range(400):
        terms = listing_terms(rng.choice(["d:1", "d:2"]), rng.choice([3, 4]), 2,
                              {"title_has_frontage": rng.random() < 0.3})
        y = truth[INTERCEPT] + sum(truth.get(term, 0.0) for term in terms)
        rows.append((terms, y))
    vocab, beta, _ = _fit(rows, ridge=0.0)
    for term, value in truth.items():
        assert close(beta[vocab.index[term]], value, 1e-9), term


def test_ridge_shrinks_terms_but_not_intercept():
    rows = [(listing_terms("d:1", 3, 2, {}), 10.0)] * 30 + [(listing_terms("d:2", 3, 2, {}), 12.0)] * 3
    vocab, beta_ols, _ = _fit(rows, ridge=0.0)
    _, beta_ridge, _ = _fit(rows, ridge=5.0)
    term = vocab.index["location=d:2"]
    assert close(beta_ols[term], 2.0)
    assert 0 < beta_ridge[term] < beta_ols[term]
    # The intercept is not penalised, so it stays near the level of y, not near 0.
    assert 9.9 < beta_ridge[0] < 10.5


def test_inverse_diagonal_matches_explicit_inverse():
    matrix = [[4.0, 1.0], [1.0, 3.0]]
    lower = cholesky(matrix)
    determinant = 4.0 * 3.0 - 1.0
    diagonal = inverse_diagonal(lower, [0, 1])
    assert close(diagonal[0], 3.0 / determinant) and close(diagonal[1], 4.0 / determinant)


def test_predict_and_premium():
    assert close(predict_log([0, 2], [17.0, 9.0, 0.1]), 17.1)
    assert close(premium_pct(math.log(1.2)), 0.2)
    assert premium_pct(0.0) == 0.0


def test_regression_metrics():
    metrics = regression_metrics([(100.0, 110.0), (200.0, 180.0), (50.0, 50.0), (0.0, 10.0)])
    assert metrics["n"] == 3  # the non-positive actual is ignored
    assert close(metrics["mae"], 10.0)
    assert close(metrics["mape"], (0.1 + 0.1 + 0.0) / 3)
    assert close(metrics["median_ape"], 0.1)
    assert metrics["r2_log"] < 1.0
    perfect = regression_metrics([(100.0, 100.0), (300.0, 300.0)])
    assert perfect["r2_log"] == 1.0 and perfect["mae"] == 0.0


def test_regression_metrics_empty():
    assert regression_metrics([])["n"] == 0


def test_job_row_terms_and_labels():
    from src.gold.build_price_model import level_label, row_terms
    from src.gold.gold_common import FLAG_COLUMNS

    row = {"district_train_rows": 3, "pooled_train_rows": 40, "location_key": 9, "province_name": "Đà Nẵng",
           "area_band_key": 2, "room_band_key": -1, **{column: column == "title_has_frontage" for column in FLAG_COLUMNS}}
    assert row_terms(row) == ["location=p:Đà Nẵng", "area_band=2", "room_band=-1", "flag=title_has_frontage"]
    bands = {"area": {2: "30 - 50 m²"}, "room": {-1: "Không rõ"}}
    assert level_label("location=d:9", {9: "Quận Hải Châu"}, bands) == ("Quận Hải Châu", 9)
    assert level_label("area_band=2", {}, bands) == ("30 - 50 m²", None)
    assert level_label("room_band=-1", {}, bands) == ("Không rõ", None)
    assert level_label("flag=title_has_frontage", {}, bands)[0] == "Mặt tiền được nêu"
    assert level_label("intercept", {}, bands)[1] is None
