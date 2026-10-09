"""BQ2 – price benchmark against comparable listings.

``agg_peer_group_benchmark``: published peer groups (>= MIN_PEER_GROUP_SIZE
representative listings) at three levels, most specific first:
LOC_CAT_AREA_ROOM -> LOC_CAT_AREA -> LOC_CAT. A NULL band key in a coarser
level means "all bands".

``fact_listing_price_assessment``: one row per representative listing with
the most specific published peer group, its price ratio and P25–P75 position.

Scope: a descriptive benchmark. ``p25_p75`` only means "inside the peer
group's middle half", not a fair price; peers can still differ in project,
quality or legal status. ``feature_count_vs_peer`` counts title flags against
the group's expectation; it does not measure how much of a price gap the
features explain (that needs a hedonic model, not built in this phase).
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.gold.gold_common import FLAG_COLUMNS, quartiles, representatives, share, write_summary
from src.gold.gold_rules import BENCHMARK_LEVELS, MIN_PEER_GROUP_SIZE, UNKNOWN_KEY

LEVEL_KEYS = {
    "LOC_CAT_AREA_ROOM": ["location_key", "property_category_key", "area_band_key", "room_band_key"],
    "LOC_CAT_AREA": ["location_key", "property_category_key", "area_band_key"],
    "LOC_CAT": ["location_key", "property_category_key"],
}
ALL_KEYS = LEVEL_KEYS["LOC_CAT_AREA_ROOM"]


def main() -> None:
    from functools import reduce

    from pyspark.sql import functions as F

    from src.common.spark_session import build_spark_session, ensure_gold_namespace, write_iceberg_table

    spark = build_spark_session("GoldPriceBenchmark")
    gold = ensure_gold_namespace(spark)
    started_at = datetime.now(timezone.utc)

    listings = representatives(spark, gold)
    eligible = listings.filter(
        (F.col("location_key") != UNKNOWN_KEY) & (F.col("property_category_key") != UNKNOWN_KEY)
        & (F.col("price_per_m2") > 0)
    )

    groups = []
    for level in BENCHMARK_LEVELS:
        keys = LEVEL_KEYS[level]
        scope = eligible
        for key in keys:
            scope = scope.filter(F.col(key) != UNKNOWN_KEY)
        stats = scope.groupBy(*keys).agg(
            F.count("*").alias("n_listings"),
            quartiles("price_per_m2").alias("_q"),
            F.expr("percentile(price, 0.5)").alias("median_price"),
            F.expr("percentile(area, 0.5)").alias("median_area"),
            F.expr("percentile(distance_to_center_km, 0.5)").alias("median_distance_km"),
            *[share(c).alias(c.replace("title_has_", "share_")) for c in FLAG_COLUMNS],
        ).filter(F.col("n_listings") >= MIN_PEER_GROUP_SIZE)
        for key in ALL_KEYS:
            if key not in keys:
                stats = stats.withColumn(key, F.lit(None).cast("int"))
        groups.append(stats.withColumn("benchmark_level", F.lit(level)))

    benchmark = reduce(lambda a, b: a.unionByName(b), groups).select(
        F.concat_ws("|", "benchmark_level", *[F.coalesce(F.col(k).cast("string"), F.lit("*")) for k in ALL_KEYS]).alias("peer_group_id"),
        "benchmark_level", *ALL_KEYS, "n_listings",
        F.col("_q")[0].alias("p25_price_per_m2"),
        F.col("_q")[1].alias("median_price_per_m2"),
        F.col("_q")[2].alias("p75_price_per_m2"),
        "median_price", "median_area", "median_distance_km",
        *[c.replace("title_has_", "share_") for c in FLAG_COLUMNS],
    )
    write_iceberg_table(benchmark, f"{gold}.agg_peer_group_benchmark")
    benchmark = spark.table(f"{gold}.agg_peer_group_benchmark")

    # Most specific published peer group for every representative listing.
    peer_columns = ["peer_group_id", "benchmark_level", "n_listings", "p25_price_per_m2", "median_price_per_m2", "p75_price_per_m2",
                    *[c.replace("title_has_", "share_") for c in FLAG_COLUMNS]]
    assessed = listings
    for index, level in enumerate(BENCHMARK_LEVELS):
        keys = LEVEL_KEYS[level]
        level_groups = benchmark.filter(F.col("benchmark_level") == level).select(
            *keys, *[F.col(c).alias(f"_{index}_{c}") for c in peer_columns]
        )
        assessed = assessed.join(level_groups, keys, "left")
    for column in peer_columns:
        assessed = assessed.withColumn(column, F.coalesce(*[F.col(f"_{i}_{column}") for i in range(len(BENCHMARK_LEVELS))]))
    # Coalesce: NULL price_per_m2 (negotiable price) must not fall through to "p25_p75".
    has_peer = F.col("peer_group_id").isNotNull() & F.coalesce(F.col("price_per_m2") > 0, F.lit(False))

    flag_count = sum(F.col(c).cast("int") for c in FLAG_COLUMNS)
    peer_flag_expectation = sum(F.col(c.replace("title_has_", "share_")) for c in FLAG_COLUMNS)
    position = (
        F.when(~has_peer, "khong_du_du_lieu")
        .when(F.col("price_per_m2") < F.col("p25_price_per_m2"), "duoi_p25")
        .when(F.col("price_per_m2") > F.col("p75_price_per_m2"), "tren_p75")
        .otherwise("p25_p75")
    )
    assessment = assessed.select(
        "source_id", "location_key", "property_category_key", "area_band_key", "room_band_key",
        "price", "area", "price_per_m2",
        F.when(has_peer, F.col("peer_group_id")).alias("peer_group_id"),
        F.when(has_peer, F.col("benchmark_level")).alias("benchmark_level"),
        F.when(has_peer, F.col("n_listings")).alias("peer_n"),
        F.when(has_peer, F.col("p25_price_per_m2")).alias("peer_p25_price_per_m2"),
        F.when(has_peer, F.col("median_price_per_m2")).alias("peer_median_price_per_m2"),
        F.when(has_peer, F.col("p75_price_per_m2")).alias("peer_p75_price_per_m2"),
        F.when(has_peer, F.round(F.col("price_per_m2") / F.col("median_price_per_m2"), 6)).alias("price_ratio"),
        position.alias("price_position"),
        flag_count.alias("feature_count"),
        F.when(has_peer, F.round(flag_count - peer_flag_expectation, 6)).alias("feature_count_vs_peer"),
        (position.eqNullSafe("duoi_p25") & ~F.col("legal_known")).alias("is_low_price_missing_legal"),
    )
    write_iceberg_table(assessment, f"{gold}.fact_listing_price_assessment")
    written = spark.table(f"{gold}.fact_listing_price_assessment")

    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "min_peer_group_size": MIN_PEER_GROUP_SIZE,
        "representative_listings": listings.count(),
        "benchmark_groups_by_level": {str(r["benchmark_level"]): int(r["count"]) for r in benchmark.groupBy("benchmark_level").count().collect()},
        "assessment_rows": written.count(),
        "assessment_by_level": {str(r["benchmark_level"]): int(r["count"]) for r in written.groupBy("benchmark_level").count().collect()},
        "price_position_counts": {str(r["price_position"]): int(r["count"]) for r in written.groupBy("price_position").count().collect()},
        "low_price_missing_legal": written.filter("is_low_price_missing_legal").count(),
        "status": "PASS",
    }
    write_summary("gold_price_benchmark_summary.json", summary)
    spark.stop()


if __name__ == "__main__":
    main()
