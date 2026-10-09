"""BQ1 – what a budget buys, and which listings are not dominated.

``agg_budget_tradeoff``: grain (price_band, location, model_category) over
representative listings with a known price band.

``fact_budget_pareto``: one row per representative listing with known price
and area; ``is_pareto_efficient`` is TRUE when no other listing in the same
(price_band, location, model_category) is cheaper-or-equal and at least as
good on area, rooms and every title flag, and strictly better on one.
"""

from __future__ import annotations

import sys
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.gold.gold_common import FLAG_COLUMNS, representatives, share, write_summary
from src.gold.gold_rules import UNKNOWN_KEY, pareto_efficient_ids

GROUP_KEYS = ["price_band_key", "location_key", "property_category_key"]


def pareto_group(entry: tuple[tuple[int, int, int], Iterable[Any]]) -> list[tuple[Any, ...]]:
    key, rows = entry
    items = [
        {
            "source_id": row["source_id"],
            "price": row["price"],
            "area": row["area"],
            "rooms": row["rooms"] or 0,
            "flags": tuple(int(bool(row[c])) for c in FLAG_COLUMNS),
        }
        for row in rows
    ]
    efficient = pareto_efficient_ids(items)
    return [(item["source_id"], *key, item["source_id"] in efficient, len(items), len(efficient)) for item in items]


def main() -> None:
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    from src.common.spark_session import build_spark_session, ensure_gold_namespace, write_iceberg_table

    spark = build_spark_session("GoldBudgetTradeoff")
    gold = ensure_gold_namespace(spark)
    started_at = datetime.now(timezone.utc)

    listings = representatives(spark, gold).filter(F.col("price_band_key") != UNKNOWN_KEY)
    tradeoff = listings.groupBy(*GROUP_KEYS).agg(
        F.count("*").alias("n_listings"),
        F.expr("percentile(price, 0.5)").alias("median_price"),
        F.expr("percentile(area, 0.5)").alias("median_area"),
        F.expr("percentile(rooms, 0.5)").alias("median_rooms"),
        F.expr("percentile(price_per_m2, 0.5)").alias("median_price_per_m2"),
        F.expr("percentile(distance_to_center_km, 0.5)").alias("median_distance_km"),
        F.round(F.avg(F.col("rooms_known").cast("double")), 6).alias("share_rooms_known"),
        *[share(c).alias(c.replace("title_has_", "share_")) for c in FLAG_COLUMNS],
    )
    write_iceberg_table(tradeoff, f"{gold}.agg_budget_tradeoff")

    candidates = listings.filter((F.col("price") > 0) & (F.col("area") > 0)).select(
        "source_id", *GROUP_KEYS, "price", "area", "rooms", *FLAG_COLUMNS
    )
    schema = T.StructType([
        T.StructField("source_id", T.StringType(), False),
        *[T.StructField(k, T.IntegerType(), False) for k in GROUP_KEYS],
        T.StructField("is_pareto_efficient", T.BooleanType(), False),
        T.StructField("group_size", T.IntegerType(), False),
        T.StructField("frontier_size", T.IntegerType(), False),
    ])
    pareto = spark.createDataFrame(
        candidates.rdd.map(lambda r: (tuple(r[k] for k in GROUP_KEYS), r)).groupByKey().flatMap(pareto_group),
        schema,
    )
    write_iceberg_table(pareto, f"{gold}.fact_budget_pareto")
    written = spark.table(f"{gold}.fact_budget_pareto")

    groups = spark.table(f"{gold}.agg_budget_tradeoff")
    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "tradeoff_groups": groups.count(),
        "tradeoff_listings": int(groups.agg(F.sum("n_listings")).first()[0] or 0),
        "pareto_rows": written.count(),
        "pareto_efficient_rows": written.filter("is_pareto_efficient").count(),
        "groups_with_ge_5_listings": groups.filter("n_listings >= 5").count(),
        "status": "PASS",
    }
    write_summary("gold_budget_tradeoff_summary.json", summary)
    spark.stop()


if __name__ == "__main__":
    main()
