from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.gold.build_price_benchmark import ALL_KEYS, LEVEL_KEYS
from src.gold.gold_common import quartiles, representatives, write_summary
from src.gold.gold_rules import BENCHMARK_LEVELS, MIN_PEER_GROUP_SIZE, UNKNOWN_KEY

THRESHOLDS = (MIN_PEER_GROUP_SIZE, 10)


def main() -> None:
    from pyspark.sql import functions as F

    from src.common.spark_session import build_spark_session, ensure_gold_namespace

    spark = build_spark_session("GoldBenchmarkSensitivity")
    gold = ensure_gold_namespace(spark)
    started_at = datetime.now(timezone.utc)

    listings = representatives(spark, gold)
    total = listings.count()
    eligible = listings.filter(
        (F.col("location_key") != UNKNOWN_KEY) & (F.col("property_category_key") != UNKNOWN_KEY)
        & (F.col("price_per_m2") > 0)
    )


    stats_by_level = {}
    for level in BENCHMARK_LEVELS:
        keys = LEVEL_KEYS[level]
        scope = eligible
        for key in keys:
            scope = scope.filter(F.col(key) != UNKNOWN_KEY)
        stats_by_level[level] = scope.groupBy(*keys).agg(
            F.count("*").alias("n_listings"), quartiles("price_per_m2").alias("_q")
        ).cache()

    def assess(min_size: int):
        peer_columns = ["peer_n", "p25", "p75"]
        assessed = listings.select("source_id", "price_per_m2", *ALL_KEYS)
        for index, level in enumerate(BENCHMARK_LEVELS):
            keys = LEVEL_KEYS[level]
            level_groups = stats_by_level[level].filter(F.col("n_listings") >= min_size).select(
                *keys,
                F.col("n_listings").alias(f"_{index}_peer_n"),
                F.col("_q")[0].alias(f"_{index}_p25"),
                F.col("_q")[2].alias(f"_{index}_p75"),
                F.lit(level).alias(f"_{index}_level"),
            )
            assessed = assessed.join(level_groups, keys, "left")
        count = len(BENCHMARK_LEVELS)
        for column in [*peer_columns, "level"]:
            assessed = assessed.withColumn(column, F.coalesce(*[F.col(f"_{i}_{column}") for i in range(count)]))
        has_peer = F.col("level").isNotNull() & F.coalesce(F.col("price_per_m2") > 0, F.lit(False))
        pos_expr = (
            F.when(~has_peer, "khong_du_du_lieu")
            .when(F.col("price_per_m2") < F.col("p25"), "duoi_p25")
            .when(F.col("price_per_m2") > F.col("p75"), "tren_p75")
            .otherwise("p25_p75")
        )
        return assessed.select(
            "source_id",
            F.when(has_peer, F.col("level")).alias("level"),
            F.when(has_peer, F.col("peer_n")).alias("peer_n"),
            pos_expr.alias("pos"),
        ).cache()

    results = {min_size: assess(min_size) for min_size in THRESHOLDS}

    def counts(frame, column):
        return {str(r[column]): int(r["count"]) for r in frame.groupBy(column).count().collect()}

    per_threshold = {}
    for min_size, frame in results.items():
        no_data = frame.filter("pos = 'khong_du_du_lieu'").count()
        peer_sizes = frame.filter("peer_n IS NOT NULL")
        per_threshold[str(min_size)] = {
            "groups_by_level": {
                level: stats_by_level[level].filter(F.col("n_listings") >= min_size).count() for level in BENCHMARK_LEVELS
            },
            "listings_by_level": counts(frame, "level"),
            "price_position_counts": counts(frame, "pos"),
            "no_data_listings": no_data,
            "no_data_share": round(no_data / total, 6),
            "median_peer_n": peer_sizes.approxQuantile("peer_n", [0.5], 0.0)[0] if peer_sizes.count() else None,
        }

    base, strict = results[THRESHOLDS[0]], results[THRESHOLDS[1]]
    joined = base.alias("a").join(strict.alias("b"), "source_id")
    both_with_peer = joined.filter("a.pos <> 'khong_du_du_lieu' AND b.pos <> 'khong_du_du_lieu'")
    comparison = {
        "listings": total,
        "position_changed": joined.filter("a.pos <> b.pos").count(),
        "peer_level_changed": joined.filter("NOT (a.level <=> b.level)").count(),
        "lost_peer_group": joined.filter("a.pos <> 'khong_du_du_lieu' AND b.pos = 'khong_du_du_lieu'").count(),
        "position_changed_among_listings_with_peer_in_both": both_with_peer.filter("a.pos <> b.pos").count(),
        "listings_with_peer_in_both": both_with_peer.count(),
    }


    gold_assessment = spark.table(f"{gold}.fact_listing_price_assessment").select(
        "source_id", F.col("price_position").alias("g_position"), F.col("benchmark_level").alias("g_level")
    )
    check = base.join(gold_assessment, "source_id", "full_outer")
    mismatches = check.filter("NOT (pos <=> g_position) OR NOT (level <=> g_level)").count()

    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "thresholds": list(THRESHOLDS),
        "per_threshold": per_threshold,
        "comparison_5_vs_10": comparison,
        "reproduces_gold_assessment_at_published_threshold": mismatches == 0,
        "gold_assessment_mismatches": mismatches,
        "status": "PASS" if mismatches == 0 else "FAIL",
    }
    write_summary("benchmark_sensitivity.json", summary)
    spark.stop()


if __name__ == "__main__":
    main()
