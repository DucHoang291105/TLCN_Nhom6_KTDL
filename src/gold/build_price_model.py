"""BQ2 – hedonic price model: what explains a listing's price per m².

One OLS model per property category on ``log(price_per_m2)`` with district,
area band, room band and the five title flags (rules and solver in
``price_model_rules.py``). Fitted on a deterministic 80% train split and
compared on the 20% test split with a baseline that predicts the median
price/m² of the listing's peer group (same three levels as BQ2, computed on
train rows only, then the category median).

``fact_listing_price_model``: one row per listing in scope with its split,
prediction, residual and baseline. A large positive ``residual_pct`` means the
asking price/m² is above what district, size, rooms and title flags predict;
it is the input for the BQ2 backtest.

``agg_price_model_coefficient``: one row per model term, reference levels
included with coefficient 0. ``premium_pct`` = exp(coefficient) - 1.

Scope: representative sale listings in a known district, in categories
nha_pho, can_ho, biet_thu, dat, with price/m² > 0 and a known area band.
Limits: asking prices, not transactions; flags record what titles mention;
coefficients are associations, not causal effects; project, floor, alley width
and legal status beyond the title are not observed.
"""

from __future__ import annotations

import math
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from operator import add
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.gold.gold_common import FLAG_COLUMNS, representatives, write_summary
from src.gold.gold_rules import MIN_PEER_GROUP_SIZE, MODEL_CATEGORY_LABELS, UNKNOWN_KEY, UNKNOWN_LABEL, load_bands
from src.gold.price_model_rules import (
    INTERCEPT, MIN_LEVEL_TRAIN_ROWS, MODEL_CATEGORY_KEYS, MODEL_VERSION, OTHER_LOCATION, RIDGE_LAMBDA,
    SPLIT_SEED, TRAIN_PERCENT, TRIM_QUANTILES, build_vocabulary, design_indices, fit_ridge, inverse_diagonal,
    listing_terms, location_level, normal_equation_entries, predict_log, premium_pct, regression_metrics,
    term_factor, term_level,
)

CATEGORIES = {key: (code, label) for code, (key, label) in MODEL_CATEGORY_LABELS.items()}
FLAG_LABELS = {
    "title_has_legal": "Pháp lý (sổ) được nêu",
    "title_has_furnished": "Nội thất được nêu",
    "title_has_frontage": "Mặt tiền được nêu",
    "title_has_elevator": "Thang máy được nêu",
    "title_has_car_access": "Ô tô tiếp cận được nêu",
}
BASELINE_LEVELS = (
    ("LOC_CAT_AREA_ROOM", ["property_category_key", "location_key", "area_band_key", "room_band_key"]),
    ("LOC_CAT_AREA", ["property_category_key", "location_key", "area_band_key"]),
    ("LOC_CAT", ["property_category_key", "location_key"]),
    ("CAT", ["property_category_key"]),
)
TERM_COLUMNS = ["property_category_key", "location_key", "province_name", "area_band_key", "room_band_key",
                "district_train_rows", "pooled_train_rows", *FLAG_COLUMNS]


def split_expression():
    """train/test by a seeded hash of source_id: stable across runs and machines."""

    from pyspark.sql import functions as F

    bucket = F.pmod(F.xxhash64(F.lit(SPLIT_SEED), F.col("source_id")), F.lit(100))
    return F.when(bucket < TRAIN_PERCENT, F.lit("train")).otherwise(F.lit("test"))


def row_terms(row: Any) -> list[str]:
    location = location_level(row["district_train_rows"], row["pooled_train_rows"], row["location_key"], row["province_name"])
    flags = {column: bool(row[column]) for column in FLAG_COLUMNS}
    return listing_terms(location, row["area_band_key"], row["room_band_key"], flags)


def level_label(term: str, districts: dict[int, str], band_labels: dict[str, dict[int, str]]) -> tuple[str, int | None]:
    factor, level = term_factor(term), term_level(term)
    if term == INTERCEPT:
        return "Hệ số chặn (mức tham chiếu)", None
    if factor == "location":
        if level.startswith("d:"):
            key = int(level[2:])
            return districts.get(key, level), key
        if level.startswith("p:"):
            return f"Quận/huyện ít tin – {level[2:]}", None
        return "Khu vực ít tin khác (gộp)", None
    if factor in ("area_band", "room_band"):
        dimension = "area" if factor == "area_band" else "room"
        return band_labels[dimension].get(int(level), level), None
    return FLAG_LABELS.get(level, level), None


def main() -> None:
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.storagelevel import StorageLevel

    from src.common.spark_session import build_spark_session, ensure_gold_namespace, write_iceberg_table

    spark = build_spark_session("GoldPriceModel")
    gold = ensure_gold_namespace(spark)
    started_at = datetime.now(timezone.utc)

    # ---- Scope -------------------------------------------------------------
    locations = spark.table(f"{gold}.dim_location").select("location_key", "province_name", "district_name", "location_level")
    reps = representatives(spark, gold).join(locations, "location_key", "left")
    scope = {"representative_listings": reps.count()}
    in_category = reps.filter(F.col("property_category_key").isin(*MODEL_CATEGORY_KEYS))
    in_district = in_category.filter(F.col("location_level") == "DISTRICT")
    priced = in_district.filter((F.col("price_per_m2") > 0) & (F.col("area_band_key") != UNKNOWN_KEY))
    n_category, n_district, n_priced = in_category.count(), in_district.count(), priced.count()
    scope["excluded_category_not_modelled"] = scope["representative_listings"] - n_category
    scope["excluded_location_not_district"] = n_category - n_district
    scope["excluded_no_price_per_m2_or_area_band"] = n_district - n_priced
    scope["listings_in_scope"] = n_priced

    base = (
        priced.select("source_id", "property_category_key", "location_key", "province_name", "area_band_key",
                      "room_band_key", "price_per_m2", *FLAG_COLUMNS)
        .withColumn("split", split_expression())
        .withColumn("log_price_per_m2", F.log("price_per_m2"))
    )
    low, high = TRIM_QUANTILES
    bounds = base.filter("split = 'train'").groupBy("property_category_key").agg(
        F.expr(f"percentile(log_price_per_m2, array({low}, {high}))").alias("_bounds")
    )
    base = base.join(bounds, "property_category_key", "left").withColumn(
        "is_outlier_trimmed",
        F.coalesce((F.col("log_price_per_m2") < F.col("_bounds")[0]) | (F.col("log_price_per_m2") > F.col("_bounds")[1]), F.lit(False)),
    ).drop("_bounds")
    fit_filter = (F.col("split") == "train") & ~F.col("is_outlier_trimmed")

    # Location level: own district, pooled province, or "other" (train counts only).
    district_rows = base.filter(fit_filter).groupBy("property_category_key", "location_key").agg(F.count("*").alias("district_train_rows"))
    base = base.join(district_rows, ["property_category_key", "location_key"], "left").fillna(0, subset=["district_train_rows"])
    pooled_rows = (
        base.filter(fit_filter & (F.col("district_train_rows") < MIN_LEVEL_TRAIN_ROWS))
        .groupBy("property_category_key", "province_name").agg(F.count("*").alias("pooled_train_rows"))
    )
    base = base.join(pooled_rows, ["property_category_key", "province_name"], "left").fillna(0, subset=["pooled_train_rows"])
    base = base.persist(StorageLevel.MEMORY_AND_DISK)
    fit = base.filter(fit_filter)

    # ---- Fit: vocabulary, normal equations, Cholesky -----------------------
    fit_rdd = fit.select(*TERM_COLUMNS, "log_price_per_m2").rdd
    term_counts: dict[int, dict[str, int]] = defaultdict(dict)
    for (category, term), count in fit_rdd.flatMap(
        lambda row: [((row["property_category_key"], term), 1) for term in row_terms(row)]
    ).reduceByKey(add).collect():
        term_counts[category][term] = count
    vocabularies = {category: build_vocabulary(counts) for category, counts in sorted(term_counts.items())}
    vocab_broadcast = spark.sparkContext.broadcast(vocabularies)

    def row_entries(row: Any) -> list[tuple[tuple[int, int, int], float]]:
        category = row["property_category_key"]
        columns, _ = design_indices(row_terms(row), vocab_broadcast.value[category])
        return [((category, i, j), value) for (i, j), value in normal_equation_entries(columns, row["log_price_per_m2"])]

    entries: dict[int, dict[tuple[int, int], float]] = defaultdict(dict)
    for (category, i, j), value in fit_rdd.flatMap(row_entries).reduceByKey(add).collect():
        entries[category][(i, j)] = value
    models = {category: fit_ridge(entries[category], vocab.size) for category, vocab in vocabularies.items()}
    beta_broadcast = spark.sparkContext.broadcast({category: beta for category, (beta, _) in models.items()})

    # ---- Predict every listing in scope ------------------------------------
    def predict(row: Any) -> tuple[str, str, float, int]:
        category = row["property_category_key"]
        terms = row_terms(row)
        columns, unseen = design_indices(terms, vocab_broadcast.value[category])
        return row["source_id"], term_level(terms[0]), float(predict_log(columns, beta_broadcast.value[category])), int(unseen)

    prediction_schema = T.StructType([
        T.StructField("source_id", T.StringType(), False),
        T.StructField("model_location_term", T.StringType(), False),
        T.StructField("predicted_log", T.DoubleType(), False),
        T.StructField("n_unseen_terms", T.IntegerType(), False),
    ])
    modelled = base.filter(F.col("property_category_key").isin(*vocabularies.keys()))
    predictions = spark.createDataFrame(modelled.select("source_id", *TERM_COLUMNS).rdd.map(predict), prediction_schema)

    # ---- Baseline: peer-group median from train rows only -----------------
    assessed = modelled.join(predictions, "source_id")
    for index, (level, keys) in enumerate(BASELINE_LEVELS):
        source = fit
        for key in keys:
            source = source.filter(F.col(key) != UNKNOWN_KEY)
        groups = source.groupBy(*keys).agg(F.count("*").alias("_n"), F.expr("percentile(price_per_m2, 0.5)").alias(f"_median_{index}"))
        if level != "CAT":
            groups = groups.filter(F.col("_n") >= MIN_PEER_GROUP_SIZE)
        assessed = assessed.join(groups.drop("_n"), keys, "left")
    medians = [F.col(f"_median_{index}") for index in range(len(BASELINE_LEVELS))]
    baseline_level = F.coalesce(*[F.when(median.isNotNull(), F.lit(level)) for median, (level, _) in zip(medians, BASELINE_LEVELS)])

    predicted = F.exp(F.col("predicted_log"))
    fact = assessed.select(
        "source_id", "property_category_key", "location_key", "area_band_key", "room_band_key",
        "split", "is_outlier_trimmed", "model_location_term", "price_per_m2",
        F.round(predicted, 2).alias("predicted_price_per_m2"),
        F.round(F.col("log_price_per_m2") - F.col("predicted_log"), 6).alias("residual_log"),
        F.round(F.col("price_per_m2") / predicted - 1, 6).alias("residual_pct"),
        F.round(F.coalesce(*medians), 2).alias("baseline_price_per_m2"),
        baseline_level.alias("baseline_level"),
        "n_unseen_terms",
        F.lit(MODEL_VERSION).alias("model_version"),
    )
    write_iceberg_table(fact, f"{gold}.fact_listing_price_model")
    written = spark.table(f"{gold}.fact_listing_price_model")

    # ---- Evaluation (computed from the published table) --------------------
    rows = written.select(
        "property_category_key", "split", "is_outlier_trimmed", "price_per_m2", "predicted_price_per_m2", "baseline_price_per_m2"
    ).rdd.map(tuple).collect()
    clean = [row for row in rows if not row[2]]

    def metrics(selected: list[tuple], column: int) -> dict[str, Any]:
        return regression_metrics((row[3], row[column]) for row in selected)

    districts = {row["location_key"]: row["district_name"] for row in locations.filter("location_level = 'DISTRICT'").collect()}
    band_labels = {dimension: {band.band_key: band.band_label for band in bands} for dimension, bands in load_bands().items()}
    for labels in band_labels.values():
        labels[UNKNOWN_KEY] = UNKNOWN_LABEL

    coefficient_rows = []
    by_category: dict[str, Any] = {}
    for category, vocab in vocabularies.items():
        code, label = CATEGORIES[category]
        beta, lower = models[category]
        fit_rows = [row for row in clean if row[0] == category and row[1] == "train"]
        test_rows = [row for row in clean if row[0] == category and row[1] == "test"]
        sse = sum((math.log(row[3]) - math.log(row[4])) ** 2 for row in fit_rows)
        dof = max(len(fit_rows) - vocab.size, 1)
        sigma2 = sse / dof
        inference_columns = [index for index, term in enumerate(vocab.terms) if term == INTERCEPT or term_factor(term) == "flag"]
        variances = inverse_diagonal(lower, inference_columns)
        train_rows = dict(vocab.train_rows)

        flags: dict[str, Any] = {}
        terms = [(term, index) for index, term in enumerate(vocab.terms)] + [(term, None) for term in vocab.references]
        for term, index in terms:
            coefficient = beta[index] if index is not None else 0.0
            std_error = math.sqrt(sigma2 * variances[index]) if index in variances else None
            # A % premium is meaningful for 0/1 terms only; the intercept is the
            # reference log price, so its premium and interval stay empty.
            is_intercept = term == INTERCEPT
            premium = None if is_intercept else premium_pct(coefficient)
            low_pct = premium_pct(coefficient - 1.96 * std_error) if std_error is not None and not is_intercept else None
            high_pct = premium_pct(coefficient + 1.96 * std_error) if std_error is not None and not is_intercept else None
            text, location_key = level_label(term, districts, band_labels)
            coefficient_rows.append((
                category, term_factor(term) if term != INTERCEPT else INTERCEPT, term_level(term), text, location_key,
                index is None, int(train_rows.get(term, len(fit_rows) if term == INTERCEPT else 0)),
                round(coefficient, 6), round(premium, 6) if premium is not None else None,
                round(std_error, 6) if std_error is not None else None,
                round(low_pct, 6) if low_pct is not None else None,
                round(high_pct, 6) if high_pct is not None else None,
                MODEL_VERSION,
            ))
            if term_factor(term) == "flag" and index is not None:
                flags[term_level(term)] = {
                    "train_rows_with_flag": train_rows.get(term, 0),
                    "coefficient": round(coefficient, 6),
                    "std_error": round(std_error, 6),
                    "premium_pct": round(premium_pct(coefficient), 4),
                    "ci95_premium_pct": [round(low_pct, 4), round(high_pct, 4)],
                    "significant_at_5pct": abs(coefficient) > 1.96 * std_error,
                }
        model_test, baseline_test = metrics(test_rows, 4), metrics(test_rows, 5)
        by_category[code] = {
            "category_label": label,
            "fit_rows": len(fit_rows),
            "test_rows": len(test_rows),
            "model_terms": vocab.size,
            "reference_levels": list(vocab.references),
            "train_r2_log": metrics(fit_rows, 4)["r2_log"],
            "test_model": model_test,
            "test_baseline": baseline_test,
            "model_beats_baseline_mae": model_test["mae"] is not None and baseline_test["mae"] is not None and model_test["mae"] < baseline_test["mae"],
            "flags": flags,
        }

    coefficient_schema = T.StructType([
        T.StructField("property_category_key", T.IntegerType(), False),
        T.StructField("factor", T.StringType(), False),
        T.StructField("level", T.StringType(), False),
        T.StructField("level_label", T.StringType(), True),
        T.StructField("location_key", T.IntegerType(), True),
        T.StructField("is_reference", T.BooleanType(), False),
        T.StructField("train_rows", T.LongType(), False),
        T.StructField("coefficient", T.DoubleType(), False),
        T.StructField("premium_pct", T.DoubleType(), True),
        T.StructField("std_error", T.DoubleType(), True),
        T.StructField("ci95_low_pct", T.DoubleType(), True),
        T.StructField("ci95_high_pct", T.DoubleType(), True),
        T.StructField("model_version", T.StringType(), False),
    ])
    write_iceberg_table(spark.createDataFrame(coefficient_rows, coefficient_schema), f"{gold}.agg_price_model_coefficient")

    all_test = [row for row in clean if row[1] == "test"]
    overall_model, overall_baseline = metrics(all_test, 4), metrics(all_test, 5)
    split_counts = Counter((row[1], bool(row[2])) for row in rows)
    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "output_tables": ["lakehouse.gold.fact_listing_price_model", "lakehouse.gold.agg_price_model_coefficient"],
        "model_version": MODEL_VERSION,
        "method": "OLS on log(price_per_m2) per property category, ridge on non-intercept terms, normal equations + Cholesky",
        "regressors": ["district (pooled if rare)", "area band", "room band (unknown = own level)", *FLAG_COLUMNS],
        "settings": {
            "split": f"pmod(xxhash64({SPLIT_SEED}, source_id), 100) < {TRAIN_PERCENT} -> train",
            "split_seed": SPLIT_SEED,
            "train_percent": TRAIN_PERCENT,
            "min_level_train_rows": MIN_LEVEL_TRAIN_ROWS,
            "ridge_lambda": RIDGE_LAMBDA,
            "trim_quantiles_log_price_per_m2": list(TRIM_QUANTILES),
            "baseline": "median price/m2 of train peer group: LOC_CAT_AREA_ROOM -> LOC_CAT_AREA -> LOC_CAT (>= "
                        f"{MIN_PEER_GROUP_SIZE} listings) -> category median",
        },
        "scope": scope,
        "rows": {
            "train_fit": split_counts[("train", False)],
            "train_trimmed": split_counts[("train", True)],
            "test_evaluated": split_counts[("test", False)],
            "test_outside_trim_range": split_counts[("test", True)],
            "rows_with_unseen_level": written.filter("n_unseen_terms > 0").count(),
            "location_terms_pooled": written.filter(
                F.col("model_location_term").startswith("p:") | (F.col("model_location_term") == OTHER_LOCATION)
            ).count(),
        },
        "test_baseline_level_counts": {
            str(row["baseline_level"]): int(row["count"])
            for row in written.filter("split = 'test' AND NOT is_outlier_trimmed").groupBy("baseline_level").count().collect()
        },
        "overall_test": {
            "model": overall_model,
            "baseline": overall_baseline,
            "model_beats_baseline_mae": overall_model["mae"] is not None and overall_baseline["mae"] is not None and overall_model["mae"] < overall_baseline["mae"],
        },
        "by_category": by_category,
        "interpretation": [
            "premium_pct = exp(coefficient) - 1: price/m2 difference vs. the reference, other terms held fixed.",
            "Flags only mean the title mentions a feature; FALSE means not mentioned, not absent.",
            "Coefficients are associations in asking prices, not causal effects or transaction prices.",
            "Test metrics exclude test rows outside the train trim range (reported separately).",
        ],
        "status": "PASS",
    }
    write_summary("price_model_summary.json", summary)
    base.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
