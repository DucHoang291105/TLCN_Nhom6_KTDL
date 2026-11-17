"""B2 – can the BQ2 hedonic model beat its baseline with richer terms?

Compares ``hedonic_ols_v1`` (as published in Gold) with candidate variants on
the SAME train/test split and the SAME trimmed rows as v1; the test set is never
changed. Nothing is written to Gold: the result is
``docs/validation/price_model_experiment.json``. A variant is promoted to Gold
only if the numbers justify it.

Variants (all keep v1's location, area band, room band and 5 title flags):

- ``v1_refit``      v1 terms only. Must reproduce the published v1 test metrics
                    (self-check of this script, not a candidate).
- ``dist``          + distance-to-centre band (unknown is its own level).
- ``loc_size``      + district x size-group interaction (small <50 m2,
                    medium 50-120 m2, large >=120 m2), only for levels with
                    enough training rows.
- ``ward``          + ward within district (a proxy for project/building,
                    which is not observed), only for wards with enough rows.
- ``all``           dist + loc_size + ward.

Fitting is the same ridge OLS as v1 (``price_model_rules``), done on the driver
from the collected rows (about a hundred thousand), so no Spark job is needed
for the solve.

Run: .\\scripts\\run_spark.ps1 src\\quality\\price_model_experiment.py
"""

from __future__ import annotations

import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.gold.gold_common import FLAG_COLUMNS, write_summary
from src.gold.price_model_rules import (
    DISTANCE_EDGES_KM, MIN_EXTRA_TERM_TRAIN_ROWS, RIDGE_LAMBDA, build_vocabulary, design_indices, fit_ridge,
    listing_terms, normal_equation_entries, predict_log, regression_metrics,
)

CATEGORY_LABELS = {1: "nha_pho", 2: "can_ho", 3: "biet_thu", 4: "dat"}
VARIANTS = {
    "v1_refit": (),
    "dist": ("dist",),
    "loc_size": ("loc_size",),
    "ward": ("ward",),
    "all": ("dist", "loc_size", "ward"),
}


def row_terms(row: dict[str, Any], features: tuple[str, ...]) -> list[str]:
    """Model terms of one listing for a variant (the same terms the Gold job uses)."""

    full = listing_terms(
        row["model_location_term"], row["area_band_key"], row["room_band_key"],
        {column: bool(row[column]) for column in FLAG_COLUMNS},
        distance_km=row.get("distance_to_center_km"), ward=row.get("ward"), extras=True,
    )
    wanted = {"dist": "dist_band", "loc_size": "loc_size", "ward": "ward"}
    prefixes = tuple(f"{wanted[feature]}=" for feature in features)
    base = [term for term in full if not term.startswith(("dist_band=", "loc_size=", "ward="))]
    return base + [term for term in full if term.startswith(prefixes)]


def fit_variant(train: list[dict[str, Any]], features: tuple[str, ...]) -> dict[str, Any]:
    """Fit one variant per category; returns {category: (vocabulary, beta)}."""

    by_category: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in train:
        by_category[row["property_category_key"]].append(row)
    models = {}
    for category, rows in sorted(by_category.items()):
        term_lists = [row_terms(row, features) for row in rows]
        counts: Counter[str] = Counter(term for terms in term_lists for term in terms)
        vocabulary = build_vocabulary(dict(counts))
        entries: dict[tuple[int, int], float] = defaultdict(float)
        for terms, row in zip(term_lists, rows):
            columns, _ = design_indices(terms, vocabulary)
            for key, value in normal_equation_entries(columns, math.log(row["price_per_m2"])):
                entries[key] += value
        beta, _ = fit_ridge(entries, vocabulary.size, RIDGE_LAMBDA)
        models[category] = (vocabulary, beta)
    return models


def predict_price(models: dict[str, Any], row: dict[str, Any], features: tuple[str, ...]) -> float | None:
    model = models.get(row["property_category_key"])
    if model is None:
        return None
    vocabulary, beta = model
    columns, _ = design_indices(row_terms(row, features), vocabulary)
    return math.exp(predict_log(columns, beta))


def evaluate(test: list[dict[str, Any]], predictions: list[float | None]) -> dict[str, Any]:
    """Model vs baseline metrics overall, per category and per location level."""

    def metrics(selected: list[tuple[dict[str, Any], float | None]]) -> dict[str, Any]:
        model = regression_metrics((row["price_per_m2"], pred) for row, pred in selected if pred)
        baseline = regression_metrics((row["price_per_m2"], row["baseline_price_per_m2"]) for row, pred in selected if pred)
        return {
            "model": model, "baseline": baseline,
            "model_beats_baseline_mae": model["mae"] is not None and baseline["mae"] is not None and model["mae"] < baseline["mae"],
        }

    pairs = list(zip(test, predictions))
    return {
        "overall": metrics(pairs),
        "by_category": {
            label: metrics([(row, pred) for row, pred in pairs if row["property_category_key"] == key])
            for key, label in CATEGORY_LABELS.items()
        },
        "by_location_level": {
            level: metrics([(row, pred) for row, pred in pairs if row["location_level"] == level])
            for level in ("DISTRICT", "PROVINCE")
        },
    }


def run_variants(train: list[dict[str, Any]], test: list[dict[str, Any]]) -> dict[str, Any]:
    results = {}
    for name, features in VARIANTS.items():
        models = fit_variant(train, features)
        predictions = [predict_price(models, row, features) for row in test]
        results[name] = {
            "features": list(features),
            "model_terms": {CATEGORY_LABELS.get(c, str(c)): vocabulary.size for c, (vocabulary, _) in models.items()},
            **evaluate(test, predictions),
        }
    return results


def main() -> None:
    from pyspark.sql import functions as F

    from src.common.spark_session import build_spark_session, ensure_gold_namespace, ensure_silver_namespace

    spark = build_spark_session("GoldPriceModelExperiment")
    gold, silver = ensure_gold_namespace(spark), ensure_silver_namespace(spark)
    started_at = datetime.now(timezone.utc)

    published = spark.table(f"{gold}.fact_listing_price_model").select(
        "source_id", "property_category_key", "split", "is_outlier_trimmed", "model_location_term", "location_level",
        "area_band_key", "room_band_key", "price_per_m2", "baseline_price_per_m2",
        F.col("predicted_price_per_m2").alias("v1_published_price_per_m2"),
    )
    listings = spark.table(f"{gold}.fact_listing").select("source_id", "distance_to_center_km", *FLAG_COLUMNS)
    wards = spark.table(f"{silver}.listing_location").select("source_id", "ward")
    frame = published.join(listings, "source_id").join(wards, "source_id", "left").filter("NOT is_outlier_trimmed")
    rows = [row.asDict() for row in frame.collect()]
    train = [row for row in rows if row["split"] == "train"]
    test = [row for row in rows if row["split"] == "test"]

    results = run_variants(train, test)

    # Self-check: refitting the variant that is published in Gold (v1 -> no extra
    # terms, v2 -> all of them) must give the published test numbers.
    published_version = spark.table(f"{gold}.fact_listing_price_model").select("model_version").first()[0]
    checked_variant = "v1_refit" if published_version == "hedonic_ols_v1" else "all"
    published_v1 = evaluate(test, [row["v1_published_price_per_m2"] for row in test])["overall"]["model"]
    refit_v1 = results[checked_variant]["overall"]["model"]
    difference = abs((refit_v1["r2_log"] or 0) - (published_v1["r2_log"] or 0))
    reproduces = refit_v1["n"] == published_v1["n"] and difference < 1e-3

    coverage = {
        "test_rows": len(test),
        "train_rows": len(train),
        "distance_known_share": round(sum(row["distance_to_center_km"] is not None for row in rows) / len(rows), 4),
        "ward_known_share": round(sum(bool(row["ward"]) for row in rows) / len(rows), 4),
    }
    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "split": "same as hedonic_ols_v1 (published split, trimmed rows removed)",
        "ridge_lambda": RIDGE_LAMBDA,
        "min_extra_term_train_rows": MIN_EXTRA_TERM_TRAIN_ROWS,
        "distance_band_edges_km": list(DISTANCE_EDGES_KM),
        "coverage": coverage,
        "published_model_version": published_version,
        "published_variant_checked": checked_variant,
        "published_overall_test": published_v1,
        "variants": results,
        "reproduces_published_model": reproduces,
        "r2_log_difference_vs_published": round(difference, 6),
        "status": "PASS" if reproduces else "FAIL",
    }
    write_summary("price_model_experiment.json", summary)
    spark.stop()


if __name__ == "__main__":
    main()
