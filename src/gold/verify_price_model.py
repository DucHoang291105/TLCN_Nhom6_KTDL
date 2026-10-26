"""Independent checks of the BQ2 hedonic price model tables (called by verify_gold)."""

import json
from pathlib import Path


def verify_price_model(spark, gold):
    from pyspark.sql import functions as F

    from src.gold.build_price_model import split_expression
    from src.gold.gold_rules import UNKNOWN_KEY
    from src.gold.price_model_rules import INTERCEPT, MODEL_CATEGORY_KEYS, MODEL_VERSION

    model = spark.table(f"{gold}.fact_listing_price_model")
    coefficients = spark.table(f"{gold}.agg_price_model_coefficient")
    locations = spark.table(f"{gold}.dim_location").select("location_key", "location_level")
    reps = spark.table(f"{gold}.fact_listing").filter("is_dup_representative")
    checks, metrics = {}, {}

    # Scope recomputed from fact_listing: same filters as build_price_model.
    expected = (
        reps.join(locations, "location_key", "left")
        .filter(F.col("property_category_key").isin(*MODEL_CATEGORY_KEYS) & (F.col("location_level") == "DISTRICT")
                & (F.col("price_per_m2") > 0) & (F.col("area_band_key") != UNKNOWN_KEY))
        .select("source_id")
    )
    actual = model.select("source_id")
    checks["price_model_covers_scope"] = actual.exceptAll(expected).count() == 0 and expected.exceptAll(actual).count() == 0
    rows = model.count()
    metrics["price_model_rows"] = rows

    checks["price_model_values_valid"] = model.filter(
        "predicted_price_per_m2 IS NULL OR predicted_price_per_m2 <= 0 OR baseline_price_per_m2 IS NULL "
        "OR baseline_price_per_m2 <= 0 OR residual_log IS NULL OR split NOT IN ('train', 'test') "
        f"OR model_version <> '{MODEL_VERSION}'"
    ).count() == 0
    checks["price_model_residual_consistent"] = model.filter(
        F.abs(F.col("residual_log") - (F.log("price_per_m2") - F.log("predicted_price_per_m2"))) > 1e-4
    ).count() == 0
    checks["price_model_split_recomputed"] = model.filter(F.col("split") != split_expression()).count() == 0
    test_share = model.filter("split = 'test'").count() / rows if rows else 0.0
    metrics["price_model_test_share"] = round(test_share, 6)
    checks["price_model_test_share_near_20pct"] = 0.17 <= test_share <= 0.23

    categories = [row[0] for row in model.select("property_category_key").distinct().collect()]
    per_category = coefficients.groupBy("property_category_key").agg(
        F.sum(F.when(F.col("factor") == INTERCEPT, 1).otherwise(0)).alias("intercepts"),
        F.sum(F.col("is_reference").cast("int")).alias("references"),
        F.sum(F.when(F.col("is_reference") & (F.col("coefficient") != 0), 1).otherwise(0)).alias("bad_references"),
    ).collect()
    checks["price_model_coefficients_cover_categories"] = sorted(r["property_category_key"] for r in per_category) == sorted(categories)
    checks["price_model_one_intercept_per_category"] = all(r["intercepts"] == 1 for r in per_category)
    checks["price_model_references_are_zero"] = all(r["bad_references"] == 0 and 1 <= r["references"] <= 3 for r in per_category)
    # Both values are rounded to 6 decimals, so compare with a relative tolerance.
    # The intercept has no % premium (it is the reference log price itself).
    terms = coefficients.filter(F.col("factor") != INTERCEPT)
    checks["price_model_premium_consistent"] = (
        terms.filter(F.col("premium_pct").isNull()
                     | (F.abs(F.col("premium_pct") - (F.exp("coefficient") - 1)) > 1e-5 * F.greatest(F.lit(1.0), F.exp("coefficient")))).count() == 0
        and coefficients.filter((F.col("factor") == INTERCEPT) & F.col("premium_pct").isNotNull()).count() == 0
    )
    checks["price_model_flag_errors_present"] = coefficients.filter(
        "factor = 'flag' AND NOT is_reference AND (std_error IS NULL OR std_error <= 0)"
    ).count() == 0

    summary_path = Path(__file__).resolve().parents[2] / "docs/validation/price_model_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.is_file() else {}
    checks["price_model_summary_matches_table"] = (
        summary.get("status") == "PASS"
        and summary.get("scope", {}).get("listings_in_scope") == rows
        and summary.get("model_version") == MODEL_VERSION
    )
    return checks, metrics
