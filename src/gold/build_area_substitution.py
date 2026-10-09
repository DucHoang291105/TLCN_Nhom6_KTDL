"""BQ3 – cheaper districts offering comparable property in the same province.

Built from ``agg_peer_group_benchmark`` at level LOC_CAT_AREA (published groups
only, so both sides have >= MIN_PEER_GROUP_SIZE listings). A pair is kept when
the alternative district has a lower median price per m² and a title-flag
profile at least MIN_SUBSTITUTION_SIMILARITY similar (same formula as
``gold_rules.feature_similarity``).
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.gold.gold_common import write_summary
from src.gold.gold_rules import FEATURE_FLAGS, MIN_PEER_GROUP_SIZE, MIN_SUBSTITUTION_SIMILARITY


def main() -> None:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from src.common.spark_session import build_spark_session, ensure_gold_namespace, write_iceberg_table

    spark = build_spark_session("GoldAreaSubstitution")
    gold = ensure_gold_namespace(spark)
    started_at = datetime.now(timezone.utc)

    districts = spark.table(f"{gold}.dim_location").filter("location_level = 'DISTRICT'").select("location_key", "province_name")
    groups = (
        spark.table(f"{gold}.agg_peer_group_benchmark")
        .filter("benchmark_level = 'LOC_CAT_AREA'")
        .join(districts, "location_key")
    )
    side_columns = ["location_key", "n_listings", "median_price_per_m2", "median_price", "median_distance_km", *[f"share_{f}" for f in FEATURE_FLAGS]]
    origin = groups.select("province_name", "property_category_key", "area_band_key", *[F.col(c).alias(f"o_{c}") for c in side_columns])
    alternative = groups.select("province_name", "property_category_key", "area_band_key", *[F.col(c).alias(f"a_{c}") for c in side_columns])

    similarity = F.lit(1.0) - sum(F.abs(F.col(f"o_share_{f}") - F.col(f"a_share_{f}")) for f in FEATURE_FLAGS) / F.lit(len(FEATURE_FLAGS))
    pairs = (
        origin.join(alternative, ["province_name", "property_category_key", "area_band_key"])
        .filter(F.col("o_location_key") != F.col("a_location_key"))
        .filter(F.col("a_median_price_per_m2") < F.col("o_median_price_per_m2"))
        .withColumn("feature_similarity", F.round(similarity, 6))
        .filter(F.col("feature_similarity") >= MIN_SUBSTITUTION_SIMILARITY)
    )
    rank = Window.partitionBy("origin_location_key", "property_category_key", "area_band_key").orderBy(
        F.desc("price_gap_pct"), F.col("alternative_location_key")
    )
    substitution = (
        pairs.select(
            F.col("o_location_key").alias("origin_location_key"),
            F.col("a_location_key").alias("alternative_location_key"),
            "property_category_key", "area_band_key",
            F.col("o_n_listings").alias("origin_n_listings"),
            F.col("a_n_listings").alias("alternative_n_listings"),
            F.col("o_median_price_per_m2").alias("origin_median_price_per_m2"),
            F.col("a_median_price_per_m2").alias("alternative_median_price_per_m2"),
            F.round(1 - F.col("a_median_price_per_m2") / F.col("o_median_price_per_m2"), 6).alias("price_gap_pct"),
            (F.col("o_median_price") - F.col("a_median_price")).alias("typical_budget_saving"),
            F.round(F.col("a_median_distance_km") - F.col("o_median_distance_km"), 3).alias("distance_diff_km"),
            "feature_similarity",
        )
        .withColumn("substitution_rank", F.row_number().over(rank))
    )
    write_iceberg_table(substitution, f"{gold}.agg_area_substitution")
    written = spark.table(f"{gold}.agg_area_substitution")

    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "min_peer_group_size": MIN_PEER_GROUP_SIZE,
        "min_feature_similarity": MIN_SUBSTITUTION_SIMILARITY,
        "district_groups": groups.count(),
        "substitution_pairs": written.count(),
        "origins_with_alternative": written.select("origin_location_key", "property_category_key", "area_band_key").distinct().count(),
        "median_price_gap_pct": written.agg(F.expr("percentile(price_gap_pct, 0.5)")).first()[0],
        "status": "PASS",
    }
    write_summary("gold_area_substitution_summary.json", summary)
    spark.stop()


if __name__ == "__main__":
    main()
