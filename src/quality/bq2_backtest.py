"""BQ2 backtest: does being priced above peers when first seen relate to what
happens to the listing afterwards?

Population: representative sale listings from the crawl sources (Batdongsan,
Guland, Nhadatvui) observed in at least two batches, so a later price change
could have been seen.

Exposure is measured on the **first observed price** (version 1 of
``listing_history``), not the current price: the current BQ2 position is
computed after any price cut, which would move a listing that dropped its
price towards the cheap side and bias the comparison.

- Position at first sight against the listing's BQ2 peer group P25/P75.
- Hedonic residual at first sight: log(first price/m²) - log(model prediction).

Outcomes: at least one observed price drop (``fact_listing_price_change``),
days between first and last observation, and absence from the source's two
latest batches. Tests: two-proportion z-test, Cochran–Mantel–Haenszel by
source, Mann–Whitney U (``backtest_stats.py``).

Limits: a few weeks of twice-weekly crawls; listed_days is an observation
window, not time to sale; absence is not a sale; peer groups use current
prices. Results are associations and preliminary.

Run: .\\scripts\\run_spark.ps1 src\\quality\\bq2_backtest.py
"""

from __future__ import annotations

import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.gold.build_listing_lifecycle import CRAWL_SOURCES
from src.gold.gold_common import write_summary
from src.quality.backtest_stats import (
    bucket, cmh_test, mann_whitney_from_counts, median_from_counts, quantile_edges, two_proportion_test,
)

ALPHA = 0.05
RESIDUAL_GROUPS = 5
POSITIONS = ("duoi_p25", "p25_p75", "tren_p75", "khong_du_du_lieu")

# Columns of the collected rows.
SOURCE, INITIAL_POSITION, CURRENT_POSITION, HAS_DROP, LISTED_DAYS, DISAPPEARED, RESIDUAL, SPLIT = range(8)


def describe(rows: list[tuple]) -> dict[str, Any]:
    n = len(rows)
    drops = sum(1 for row in rows if row[HAS_DROP])
    judged = [row for row in rows if row[DISAPPEARED] is not None]
    return {
        "listings": n,
        "with_price_drop": drops,
        "price_drop_rate": round(drops / n, 6) if n else None,
        "median_listed_days": median_from_counts(Counter(row[LISTED_DAYS] for row in rows)),
        "disappeared_rate": round(sum(1 for row in judged if row[DISAPPEARED]) / len(judged), 6) if judged else None,
    }


def compare(group_a: list[tuple], group_b: list[tuple]) -> dict[str, Any]:
    """Group A vs group B on the three outcomes, with the source-stratified test."""

    drop_a, drop_b = sum(1 for r in group_a if r[HAS_DROP]), sum(1 for r in group_b if r[HAS_DROP])
    strata = []
    for source in CRAWL_SOURCES:
        a = [r for r in group_a if r[SOURCE] == source]
        b = [r for r in group_b if r[SOURCE] == source]
        a_drop, b_drop = sum(1 for r in a if r[HAS_DROP]), sum(1 for r in b if r[HAS_DROP])
        strata.append((a_drop, len(a) - a_drop, b_drop, len(b) - b_drop))
    judged_a = [r for r in group_a if r[DISAPPEARED] is not None]
    judged_b = [r for r in group_b if r[DISAPPEARED] is not None]
    drop_test = two_proportion_test(drop_a, len(group_a), drop_b, len(group_b))
    stratified = cmh_test(strata)
    days_test = mann_whitney_from_counts(Counter(r[LISTED_DAYS] for r in group_a), Counter(r[LISTED_DAYS] for r in group_b))
    return {
        "price_drop": drop_test,
        "price_drop_by_source_cmh": stratified,
        "listed_days_mann_whitney": days_test,
        "disappeared": two_proportion_test(
            sum(1 for r in judged_a if r[DISAPPEARED]), len(judged_a),
            sum(1 for r in judged_b if r[DISAPPEARED]), len(judged_b),
        ),
        "price_drop_higher_and_significant": bool(
            drop_test["p_value"] is not None and drop_test["difference"] > 0 and drop_test["p_value"] < ALPHA
            and stratified["p_value"] is not None and stratified["p_value"] < ALPHA
        ),
    }


def residual_analysis(rows: list[tuple]) -> dict[str, Any]:
    scored = [row for row in rows if row[RESIDUAL] is not None]
    if len(scored) < RESIDUAL_GROUPS * 10:
        return {"listings": len(scored), "note": "too few listings with a model residual"}
    edges = quantile_edges([row[RESIDUAL] for row in scored], RESIDUAL_GROUPS)
    groups: dict[int, list[tuple]] = defaultdict(list)
    for row in scored:
        groups[bucket(row[RESIDUAL], edges)].append(row)
    top = groups[RESIDUAL_GROUPS]
    rest = [row for index, members in groups.items() if index != RESIDUAL_GROUPS for row in members]
    return {
        "listings": len(scored),
        "residual_quintile_edges_log": [round(edge, 6) for edge in edges],
        "by_quintile": {f"Q{index}": describe(groups[index]) for index in range(1, RESIDUAL_GROUPS + 1)},
        "top_quintile_vs_rest": compare(top, rest),
    }


def main() -> None:
    from pyspark.sql import functions as F

    from src.common.spark_session import build_spark_session, ensure_gold_namespace, ensure_silver_namespace

    spark = build_spark_session("BQ2Backtest")
    gold = ensure_gold_namespace(spark)
    silver = ensure_silver_namespace(spark)
    started_at = datetime.now(timezone.utc)

    lifecycle = spark.table(f"{gold}.fact_listing_lifecycle").filter(
        F.col("source").isin(*CRAWL_SOURCES) & (F.col("observation_count") >= 2)
    ).select("source_id", "source", "listed_days", "is_disappeared", "first_seen_date", "last_seen_date")
    listings = spark.table(f"{gold}.fact_listing").filter("is_dup_representative").select("source_id", "area")
    first_price = spark.table(f"{silver}.listing_history").groupBy("source_id").agg(
        F.expr("min_by(price, version_number)").alias("initial_price")
    )
    assessment = spark.table(f"{gold}.fact_listing_price_assessment").select(
        "source_id", "peer_p25_price_per_m2", "peer_p75_price_per_m2", F.col("price_position").alias("current_position")
    )
    model = spark.table(f"{gold}.fact_listing_price_model").select("source_id", "predicted_price_per_m2", "split")
    changes = spark.table(f"{gold}.fact_listing_price_change").groupBy("source_id").agg(
        F.max((F.col("direction") == "giam").cast("int")).alias("_drop")
    )

    base = (
        lifecycle.join(listings, "source_id").join(first_price, "source_id", "left")
        .join(assessment, "source_id", "left").join(model, "source_id", "left").join(changes, "source_id", "left")
    )
    initial_ppm2 = F.when((F.col("area") > 0) & (F.col("initial_price") > 0), F.col("initial_price") / F.col("area"))
    base = base.withColumn("initial_price_per_m2", initial_ppm2)
    initial_position = (
        F.when(F.col("initial_price_per_m2").isNull() | F.col("peer_p25_price_per_m2").isNull(), "khong_du_du_lieu")
        .when(F.col("initial_price_per_m2") < F.col("peer_p25_price_per_m2"), "duoi_p25")
        .when(F.col("initial_price_per_m2") > F.col("peer_p75_price_per_m2"), "tren_p75")
        .otherwise("p25_p75")
    )
    residual = F.when(
        (F.col("predicted_price_per_m2") > 0) & (F.col("initial_price_per_m2") > 0),
        F.log("initial_price_per_m2") - F.log("predicted_price_per_m2"),
    )
    rows = base.select(
        "source", initial_position.alias("initial_position"), "current_position",
        F.coalesce(F.col("_drop") == 1, F.lit(False)).alias("has_drop"),
        "listed_days", "is_disappeared", residual.alias("residual"), "split",
    ).rdd.map(tuple).collect()
    window = base.agg(F.min("first_seen_date").alias("first"), F.max("last_seen_date").alias("last")).first()

    by_initial = {position: [row for row in rows if row[INITIAL_POSITION] == position] for position in POSITIONS}
    by_current = {position: [row for row in rows if row[CURRENT_POSITION] == position] for position in POSITIONS}
    expensive = by_initial["tren_p75"]
    not_expensive = by_initial["duoi_p25"] + by_initial["p25_p75"]
    position_test = compare(expensive, not_expensive)
    residual_all = residual_analysis(rows)
    residual_test_split = residual_analysis([row for row in rows if row[SPLIT] == "test"])

    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "preliminary": True,
        "observation_window": {"first_seen": str(window["first"]), "last_seen": str(window["last"])},
        "population": {
            "definition": "representative sale listings from crawl sources observed in >= 2 batches",
            "listings": len(rows),
            "by_source": dict(Counter(row[SOURCE] for row in rows)),
            "with_initial_position": len(rows) - len(by_initial["khong_du_du_lieu"]),
            "with_model_residual": sum(1 for row in rows if row[RESIDUAL] is not None),
        },
        "hypotheses": {
            "H1": "Listings above the peer-group P75 when first seen cut their price more often than the others.",
            "H2": "Listings priced above the hedonic model when first seen (top residual quintile) cut their price more often.",
            "H3": "Both groups stay observable for longer (listed_days).",
            "significance_level": ALPHA,
        },
        "by_initial_position": {position: describe(members) for position, members in by_initial.items()},
        "H1_tren_p75_vs_rest": position_test,
        "H2_residual_all_listings": residual_all,
        "H2_residual_test_split_only": residual_test_split,
        "reference_current_position_biased": {
            "note": "Position on the current price (after any cut). Shown only to illustrate the bias; not used for conclusions.",
            "by_current_position": {position: describe(members) for position, members in by_current.items()},
        },
        "conclusion": {
            "H1_supported": position_test["price_drop_higher_and_significant"],
            "H2_supported": bool(residual_all.get("top_quintile_vs_rest", {}).get("price_drop_higher_and_significant")),
            "H2_supported_on_test_split": bool(residual_test_split.get("top_quintile_vs_rest", {}).get("price_drop_higher_and_significant")),
        },
        "limitations": [
            "Short window of twice-weekly crawls: few listings can show a price change.",
            "listed_days is the span between first and last observation, not time to sale.",
            "Absence from the two latest batches is not a sale.",
            "Peer-group P25/P75 are computed on current prices.",
            "Associations only; listings were not randomly assigned to price positions.",
        ],
        "status": "PASS",
    }
    write_summary("bq2_backtest.json", summary)
    spark.stop()


if __name__ == "__main__":
    main()
