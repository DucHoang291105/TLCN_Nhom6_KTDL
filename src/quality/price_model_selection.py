"""BQ2 – how the hedonic price model was built and checked against its baseline.

One script, three steps, all on the published ``fact_listing_price_model`` rows
(same deterministic train/test split and same baseline throughout; the test
split is never changed). Nothing is written to Gold; the result is
``docs/validation/price_model_selection.json``.

1. ``term_comparison``: the model with the base terms (district, area band, room
   band, 5 title flags) against the same model plus each extra term group
   (distance to the province centre, district x size group, ward) and all of
   them. The term groups were fixed in advance; this step only measures them.
2. ``blend_selection``: the final price mixes the model and the peer-group
   baseline in log space,

       prediction = exp(w * log(model) + (1 - w) * log(baseline)).

   Per category, w (0..1), the ridge penalty and whether pooled-province /
   "other" locations use the baseline alone are chosen by 5-fold
   cross-validation on the TRAIN split (out-of-fold MAE; the plain model is kept
   unless a choice beats it by 0.1%). The test split is used once, afterwards.
3. ``significance``: paired bootstrap of the per-listing absolute-error
   difference between the baseline and the chosen blend on the test split. If
   the 95% interval contains 0, the blend is not distinguishable from the
   baseline for that category and must be reported as equal, not better.

Self-check: refitting the extended model with the default ridge must reproduce
the pure-model predictions stored in Gold.

Run: .\\scripts\\run_spark.ps1 src\\quality\\price_model_selection.py
"""

from __future__ import annotations

import math
import random
import sys
import zlib
from collections import Counter, defaultdict
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from statistics import median as statistics_median
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.gold.gold_common import FLAG_COLUMNS, write_summary
from src.gold.gold_rules import MIN_PEER_GROUP_SIZE, UNKNOWN_KEY
from src.gold.price_model_rules import (
    DISTANCE_EDGES_KM, MIN_EXTRA_TERM_TRAIN_ROWS, RIDGE_LAMBDA, BlendRule, blend_prediction, build_vocabulary,
    design_indices, fit_ridge, listing_terms, normal_equation_entries, predict_log, regression_metrics,
)

CATEGORY_LABELS = {1: "nha_pho", 2: "can_ho", 3: "biet_thu", 4: "dat"}
TERM_GROUPS = {
    "base_terms": (),
    "distance": ("dist",),
    "district_size": ("loc_size",),
    "ward": ("ward",),
    "all_terms": ("dist", "loc_size", "ward"),
}
ALL_TERMS = TERM_GROUPS["all_terms"]
FOLDS = 5
RIDGE_GRID = (1.0, 5.0, 20.0)
WEIGHT_GRID = tuple(round(step / 10, 1) for step in range(10, -1, -1))  # 1.0 ... 0.0 (ties prefer the model)
MIN_GAIN = 1e-3  # a non-default choice must beat the plain model by 0.1% out-of-fold MAE
BASELINE_LEVELS = (
    ("LOC_CAT_AREA_ROOM", ("location_key", "area_band_key", "room_band_key")),
    ("LOC_CAT_AREA", ("location_key", "area_band_key")),
    ("LOC_CAT", ("location_key",)),
    ("CAT", ()),
)
BOOTSTRAP_SEED = 20261117
RESAMPLES = 2000
OVERALL_RESAMPLES = 500
MIN_AGREEMENT_SHARE = 0.999  # refit vs published prediction within 1% for at least this share of test listings


# ---- model terms and fitting -------------------------------------------------

def model_price_column(table: Any) -> str:
    """Column with the pure model prediction: ``model_price_per_m2`` once Gold
    publishes a blend, else ``predicted_price_per_m2`` (then it is the pure model)."""

    return "model_price_per_m2" if "model_price_per_m2" in table.columns else "predicted_price_per_m2"


def row_terms(row: dict[str, Any], groups: tuple[str, ...]) -> list[str]:
    """Model terms of one listing: the base terms plus the selected extra groups."""

    full = listing_terms(
        row["model_location_term"], row["area_band_key"], row["room_band_key"],
        {column: bool(row[column]) for column in FLAG_COLUMNS},
        distance_km=row.get("distance_to_center_km"), ward=row.get("ward"), extras=True,
    )
    prefix = {"dist": "dist_band=", "loc_size": "loc_size=", "ward": "ward="}
    extras = tuple(prefix.values())
    wanted = tuple(prefix[group] for group in groups)
    return [term for term in full if not term.startswith(extras)] + [term for term in full if term.startswith(wanted)]


def fit_terms(train: list[dict[str, Any]], groups: tuple[str, ...], ridge: float = RIDGE_LAMBDA) -> dict[int, Any]:
    """Fit one model per category; returns {category: (vocabulary, beta)}."""

    by_category: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in train:
        by_category[row["property_category_key"]].append(row)
    models = {}
    for category, rows in sorted(by_category.items()):
        term_lists = [row_terms(row, groups) for row in rows]
        counts: Counter[str] = Counter(term for terms in term_lists for term in terms)
        vocabulary = build_vocabulary(dict(counts))
        entries: dict[tuple[int, int], float] = defaultdict(float)
        for terms, row in zip(term_lists, rows):
            columns, _ = design_indices(terms, vocabulary)
            for key, value in normal_equation_entries(columns, math.log(row["price_per_m2"])):
                entries[key] += value
        beta, _ = fit_ridge(entries, vocabulary.size, ridge)
        models[category] = (vocabulary, beta)
    return models


def predict_price(models: dict[int, Any], row: dict[str, Any], groups: tuple[str, ...]) -> float | None:
    model = models.get(row["property_category_key"])
    if model is None:
        return None
    vocabulary, beta = model
    columns, _ = design_indices(row_terms(row, groups), vocabulary)
    return math.exp(predict_log(columns, beta))


# ---- step 1: term comparison -------------------------------------------------

def compare_against_baseline(test: list[dict[str, Any]], predictions: list[float | None]) -> dict[str, Any]:
    """Model vs baseline metrics overall, per category and per location level."""

    def metrics(selected: list[tuple[dict[str, Any], float | None]]) -> dict[str, Any]:
        model = regression_metrics((row["price_per_m2"], pred) for row, pred in selected if pred)
        baseline = regression_metrics((row["price_per_m2"], row["baseline_price_per_m2"]) for row, pred in selected if pred)
        return {"model": model, "baseline": baseline,
                "model_beats_baseline_mae": model["mae"] is not None and baseline["mae"] is not None and model["mae"] < baseline["mae"]}

    pairs = list(zip(test, predictions))
    return {
        "overall": metrics(pairs),
        "by_category": {label: metrics([(row, pred) for row, pred in pairs if row["property_category_key"] == key])
                        for key, label in CATEGORY_LABELS.items()},
        "by_location_level": {level: metrics([(row, pred) for row, pred in pairs if row["location_level"] == level])
                              for level in ("DISTRICT", "PROVINCE")},
    }


def error_ratio_summary(
    pairs: Iterable[tuple[float, float | None]],
    quantiles: tuple[float, ...] = (0.1, 0.25, 0.5, 0.75, 0.9),
    bands: tuple[float, ...] = (0.25, 0.5),
) -> dict[str, Any]:
    """Empirical spread of actual / predicted price per m² on the test split.

    A point prediction is only useful with its error: the 10th and 90th
    quantiles of ``actual / predicted`` give a range that held for 80% of the
    test listings, and ``share_within`` is the share whose actual price was
    within ±band of the prediction. These are empirical, in-sample-to-test
    figures, not a guarantee for new listings.
    """

    ratios = sorted(actual / predicted for actual, predicted in pairs if predicted and predicted > 0 and actual and actual > 0)
    n = len(ratios)
    if not n:
        return {"n": 0}

    def quantile(p: float) -> float:
        position = (n - 1) * p
        low = int(position)
        high = min(low + 1, n - 1)
        return ratios[low] + (ratios[high] - ratios[low]) * (position - low)

    return {
        "n": n,
        "actual_over_predicted_quantiles": {str(p): round(quantile(p), 4) for p in quantiles},
        "share_within": {f"{round(b * 100)}pct": round(sum(abs(r - 1) <= b for r in ratios) / n, 4) for b in bands},
    }


def term_comparison(train: list[dict[str, Any]], test: list[dict[str, Any]]) -> dict[str, Any]:
    results = {}
    for name, groups in TERM_GROUPS.items():
        models = fit_terms(train, groups)
        predictions = [predict_price(models, row, groups) for row in test]
        results[name] = {
            "term_groups": list(groups),
            "model_terms": {CATEGORY_LABELS.get(c, str(c)): vocabulary.size for c, (vocabulary, _) in models.items()},
            **compare_against_baseline(test, predictions),
        }
    return results


# ---- step 2: blend selection by cross-validation on train ---------------------

def prediction_agreement(pairs: Iterable[tuple[float | None, float]], tolerance: float = 0.01) -> dict[str, Any]:
    """How closely the refit predictions match the published ones, listing by listing.

    ``pairs`` holds ``(refit prediction, published prediction)``. Matching R² alone
    does not show that every listing gets the same prediction.
    """

    differences = [abs(math.log(refit) - math.log(published)) for refit, published in pairs if refit and published]
    within = sum(1 for value in differences if value <= math.log1p(tolerance))
    return {
        "n": len(differences),
        "tolerance_relative": tolerance,
        "share_within_tolerance": round(within / len(differences), 6) if differences else None,
        "max_abs_log_difference": round(max(differences), 6) if differences else None,
        "median_abs_log_difference": round(sorted(differences)[len(differences) // 2], 8) if differences else None,
    }


def fold_of(source_id: str) -> int:
    return zlib.crc32(source_id.encode("utf-8")) % FOLDS


def baseline_tables(train: list[dict[str, Any]]) -> list[dict[tuple, float]]:
    """Peer-group medians per level (groups < MIN_PEER_GROUP_SIZE dropped; the category level is always kept)."""

    tables = []
    for level, keys in BASELINE_LEVELS:
        groups: dict[tuple, list[float]] = {}
        for row in train:
            key = tuple(row[name] for name in keys)
            if all(value != UNKNOWN_KEY for value in key):
                groups.setdefault(key, []).append(row["price_per_m2"])
        minimum = 1 if level == "CAT" else MIN_PEER_GROUP_SIZE
        tables.append({key: statistics_median(values) for key, values in groups.items() if len(values) >= minimum})
    return tables


def baseline_price(tables: list[dict[tuple, float]], row: dict[str, Any]) -> float | None:
    for (_, keys), table in zip(BASELINE_LEVELS, tables):
        key = tuple(row[name] for name in keys)
        if all(value != UNKNOWN_KEY for value in key) and key in table:
            return table[key]
    return None


def final_price(row: dict[str, Any], model: float, baseline: float, weight: float, pooled_to_baseline: bool) -> float:
    return blend_prediction(model, baseline, BlendRule(weight, pooled_to_baseline), row["model_location_term"])


def mean_abs_error(rows: list[dict[str, Any]], predictions: list[float]) -> float:
    return sum(abs(row["price_per_m2"] - pred) for row, pred in zip(rows, predictions)) / len(rows)


def out_of_fold(train: list[dict[str, Any]], ridge: float) -> list[tuple[dict[str, Any], float, float]]:
    """(row, model, baseline) for every train row, each predicted from the other folds."""

    result = []
    for fold in range(FOLDS):
        fit = [row for row in train if fold_of(row["source_id"]) != fold]
        held = [row for row in train if fold_of(row["source_id"]) == fold]
        models = fit_terms(fit, ALL_TERMS, ridge)
        tables = baseline_tables(fit)
        for row in held:
            model, baseline = predict_price(models, row, ALL_TERMS), baseline_price(tables, row)
            if model and baseline:
                result.append((row, model, baseline))
    return result


def choose_configuration(oof_by_ridge: dict[float, list[tuple[dict[str, Any], float, float]]]) -> dict[str, Any]:
    """Lowest out-of-fold MAE; the plain model wins unless beaten by MIN_GAIN."""

    scores: list[dict[str, Any]] = []
    for ridge, triples in oof_by_ridge.items():
        rows = [row for row, _, _ in triples]
        for pooled in (False, True):
            for weight in WEIGHT_GRID:
                predictions = [final_price(row, model, baseline, weight, pooled) for row, model, baseline in triples]
                scores.append({"ridge": ridge, "weight": weight, "pooled_to_baseline": pooled,
                               "oof_mae": round(mean_abs_error(rows, predictions), 2)})
    plain = next(s for s in scores if s["ridge"] == RIDGE_LAMBDA and s["weight"] == 1.0 and not s["pooled_to_baseline"])
    best = min(scores, key=lambda s: s["oof_mae"])
    chosen = best if best["oof_mae"] < plain["oof_mae"] * (1 - MIN_GAIN) else plain
    baseline_only = next(s["oof_mae"] for s in scores if s["ridge"] == RIDGE_LAMBDA and s["weight"] == 0.0 and not s["pooled_to_baseline"])
    return {"chosen": chosen, "plain_model": plain, "baseline_only_oof_mae": baseline_only, "n_candidates": len(scores)}


# ---- step 3: significance -----------------------------------------------------

def abs_error_differences(actuals: list[float], baselines: list[float], blends: list[float]) -> list[float]:
    """|actual - baseline| - |actual - blend|; positive when the blend is closer."""

    return [abs(a - b) - abs(a - c) for a, b, c in zip(actuals, baselines, blends)]


def paired_bootstrap(diffs: list[float], resamples: int = RESAMPLES, seed: int = BOOTSTRAP_SEED) -> dict[str, Any]:
    n = len(diffs)
    if n == 0:
        return {"n": 0}
    rng = random.Random(seed)
    means = sorted(sum(rng.choices(diffs, k=n)) / n for _ in range(resamples))
    low, high = means[int(0.025 * resamples)], means[int(0.975 * resamples) - 1]
    return {
        "n": n,
        "mean_diff": round(sum(diffs) / n, 2),
        "ci95": [round(low, 2), round(high, 2)],
        "share_resamples_blend_better": round(sum(1 for m in means if m > 0) / resamples, 4),
        "share_listings_blend_closer": round(sum(1 for d in diffs if d > 0) / n, 4),
        "significant_at_5pct": low > 0 or high < 0,
    }


# ---- job -----------------------------------------------------------------------

def main() -> None:
    from pyspark.sql import functions as F

    from src.common.spark_session import build_spark_session, ensure_gold_namespace, ensure_silver_namespace

    spark = build_spark_session("GoldPriceModelSelection")
    gold, silver = ensure_gold_namespace(spark), ensure_silver_namespace(spark)
    started_at = datetime.now(timezone.utc)
    table = spark.table(f"{gold}.fact_listing_price_model")
    published = table.filter("NOT is_outlier_trimmed").select(
        "source_id", "property_category_key", "split", "location_key", "model_location_term", "location_level", "area_band_key",
        "room_band_key", "price_per_m2", "baseline_price_per_m2", F.col(model_price_column(table)).alias("gold_model_price_per_m2"))
    listings = spark.table(f"{gold}.fact_listing").select("source_id", "distance_to_center_km", *FLAG_COLUMNS)
    wards = spark.table(f"{silver}.listing_location").select("source_id", "ward")
    rows = [row.asDict() for row in published.join(listings, "source_id").join(wards, "source_id", "left").collect()]
    train = [row for row in rows if row["split"] == "train"]
    test = [row for row in rows if row["split"] == "test"]

    comparison = term_comparison(train, test)
    gold_model = regression_metrics((row["price_per_m2"], row["gold_model_price_per_m2"]) for row in test)
    refit = comparison["all_terms"]["overall"]["model"]
    difference = abs((refit["r2_log"] or 0) - (gold_model["r2_log"] or 0))
    refit_models = fit_terms(train, ALL_TERMS)
    agreement = prediction_agreement(
        (predict_price(refit_models, row, ALL_TERMS), row["gold_model_price_per_m2"]) for row in test
    )
    reproduces = bool(
        refit["n"] == gold_model["n"] and difference < 1e-3
        and agreement["n"] == gold_model["n"] and (agreement["share_within_tolerance"] or 0) >= MIN_AGREEMENT_SHARE
    )

    by_category: dict[str, Any] = {}
    pairs: dict[str, list] = {"model_only": [], "blend": [], "baseline": []}
    overall_diffs: list[float] = []
    for key, label in CATEGORY_LABELS.items():
        cat_train = [r for r in train if r["property_category_key"] == key]
        cat_test = [r for r in test if r["property_category_key"] == key]
        selection = choose_configuration({ridge: out_of_fold(cat_train, ridge) for ridge in RIDGE_GRID})
        chosen = selection["chosen"]
        models = fit_terms(cat_train, ALL_TERMS, chosen["ridge"])
        plain_models = models if chosen["ridge"] == RIDGE_LAMBDA else fit_terms(cat_train, ALL_TERMS, RIDGE_LAMBDA)
        actuals, baselines, blends, plain = [], [], [], []
        for row in cat_test:
            model, plain_model = predict_price(models, row, ALL_TERMS), predict_price(plain_models, row, ALL_TERMS)
            if not model or not plain_model:
                continue
            actuals.append(row["price_per_m2"])
            baselines.append(row["baseline_price_per_m2"])
            blends.append(final_price(row, model, row["baseline_price_per_m2"], chosen["weight"], chosen["pooled_to_baseline"]))
            plain.append(plain_model)
        metrics = {
            "model_only": regression_metrics(zip(actuals, plain)),
            "blend": regression_metrics(zip(actuals, blends)),
            "baseline": regression_metrics(zip(actuals, baselines)),
        }
        metrics["blend_beats_baseline_mae"] = metrics["blend"]["mae"] < metrics["baseline"]["mae"]
        metrics["blend_beats_model_only_mae"] = metrics["blend"]["mae"] < metrics["model_only"]["mae"]
        diffs = abs_error_differences(actuals, baselines, blends)
        overall_diffs += diffs
        for name, values in (("model_only", plain), ("blend", blends), ("baseline", baselines)):
            pairs[name] += list(zip(actuals, values))
        by_category[label] = {"train_rows": len(cat_train), "test_rows": len(actuals), "selection": selection,
                              "test": metrics, "significance": paired_bootstrap(diffs),
                              "error_ratio": error_ratio_summary(zip(actuals, blends))}

    overall = {name: regression_metrics(values) for name, values in pairs.items()}
    overall["blend_beats_baseline_mae"] = overall["blend"]["mae"] < overall["baseline"]["mae"]
    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "split": "published train/test split of fact_listing_price_model, rows outside the train trim range removed",
        "settings": {
            "ridge_lambda": RIDGE_LAMBDA, "min_extra_term_train_rows": MIN_EXTRA_TERM_TRAIN_ROWS,
            "distance_band_edges_km": list(DISTANCE_EDGES_KM), "folds": FOLDS, "ridge_grid": list(RIDGE_GRID),
            "weight_grid": list(WEIGHT_GRID), "min_gain_to_leave_plain_model": MIN_GAIN,
            "bootstrap_resamples": RESAMPLES, "bootstrap_seed": BOOTSTRAP_SEED,
        },
        "coverage": {
            "train_rows": len(train), "test_rows": len(test),
            "distance_known_share": round(sum(row["distance_to_center_km"] is not None for row in rows) / len(rows), 4),
            "ward_known_share": round(sum(bool(row["ward"]) for row in rows) / len(rows), 4),
        },
        "term_comparison": comparison,
        "blend_selection": {
            "method": "log-space blend of the model and the peer-group baseline; configuration chosen by "
                      f"{FOLDS}-fold cross-validation on train only; one evaluation on test",
            "overall_test": {**overall, "significance": paired_bootstrap(overall_diffs, resamples=OVERALL_RESAMPLES),
                             "error_ratio": error_ratio_summary(pairs["blend"])},
            "by_category": by_category,
        },
        "gold_model_reproduced": reproduces,
        "r2_log_difference_vs_gold_model": round(difference, 6),
        "prediction_agreement_vs_gold_model": agreement,
        "limitations": [
            "Cross-validation folds reuse the location terms and trim range fitted on the whole train split; "
            "only the baseline is rebuilt inside each fold. The outer test rows are never used for either.",
        ],
        "status": "PASS" if reproduces else "FAIL",
    }
    write_summary("price_model_selection.json", summary)
    spark.stop()


if __name__ == "__main__":
    main()
