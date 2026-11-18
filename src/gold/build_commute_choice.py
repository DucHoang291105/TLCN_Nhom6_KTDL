"""BQ3 by place of work or study – cheaper districts with an equivalent commute.

The province-centre distance used by ``agg_area_substitution`` says how remote
a district is, not how far it is from where the buyer goes every day: Quận 7
and Gò Vấp are both ~7 km from the centre but ~14 km apart. This job places
every district at the median coordinate of its representative listings and
measures the straight-line distance to a list of destinations
(``config/commute_destinations.csv`` plus every province centre).

Outputs:

- ``agg_commute_district``: destination × district × property category × area
  band, with the commute distance, the estimated cost at a common target area
  (median of the districts' median areas in that comparison), and
  ``is_commute_efficient`` – no other district is both no farther and no more
  expensive.
- ``agg_commute_substitution``: for a buyer heading to a destination who looks
  at an origin district, the cheaper districts whose commute is at most
  COMMUTE_TOLERANCE_KM longer. Cost, similarity and group-size rules are the
  ones of ``agg_area_substitution`` (same target area = origin median area).

The computation runs on the driver: a few thousand groups.
"""

from __future__ import annotations

import statistics
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.gold.gold_common import representatives, write_summary
from src.gold.gold_rules import (
    COMMUTE_TOLERANCE_KM, FEATURE_FLAGS, MAX_FLAG_SHARE_GAP, MIN_CENTROID_LISTINGS, MIN_PEER_GROUP_SIZE,
    MIN_SUBSTITUTION_SIMILARITY, commute_allowed, commute_efficient_keys, cost_at_target_area, feature_similarity,
    load_destinations, max_flag_share_gap,
)
from src.silver.build_location import haversine_km


def main() -> None:
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    from src.common.spark_session import build_spark_session, ensure_gold_namespace, ensure_silver_namespace, write_iceberg_table

    spark = build_spark_session("GoldCommuteChoice")
    silver = ensure_silver_namespace(spark)
    gold = ensure_gold_namespace(spark)
    started_at = datetime.now(timezone.utc)

    districts = spark.table(f"{gold}.dim_location").filter("location_level = 'DISTRICT'").select("location_key", "province_name", "district_name")
    coords = spark.table(f"{silver}.listing_location").filter("lat IS NOT NULL AND lon IS NOT NULL").select("source_id", "lat", "lon")
    centroids = {
        r["location_key"]: r for r in (
            representatives(spark, gold).select("source_id", "location_key").join(coords, "source_id").join(districts, "location_key")
            .groupBy("location_key")
            .agg(F.count("*").alias("n"), F.expr("percentile(lat, 0.5)").alias("lat"), F.expr("percentile(lon, 0.5)").alias("lon"))
            .filter(F.col("n") >= MIN_CENTROID_LISTINGS)
            .collect()
        )
    }
    share_columns = [f"share_{f}" for f in FEATURE_FLAGS]
    groups = [
        r.asDict() for r in spark.table(f"{gold}.agg_peer_group_benchmark")
        .filter("benchmark_level = 'LOC_CAT_AREA'")
        .join(districts, "location_key")
        .select("location_key", "province_name", "property_category_key", "area_band_key", "n_listings",
                "median_price_per_m2", "median_area", *share_columns)
        .collect()
        if r["location_key"] in centroids
    ]
    destinations = load_destinations()

    district_rows, substitution_rows = [], []
    for dest in destinations:
        by_segment: dict[tuple[int, int], list[dict]] = {}
        for g in groups:
            if g["province_name"] == dest["province_name"]:
                c = centroids[g["location_key"]]
                km = round(haversine_km(c["lat"], c["lon"], dest["lat"], dest["lon"]), 3)
                by_segment.setdefault((g["property_category_key"], g["area_band_key"]), []).append({**g, "commute_km": km, "centroid": c})
        for (category, band), members in by_segment.items():
            target = float(statistics.median(m["median_area"] for m in members))
            for m in members:
                m["cost"] = cost_at_target_area(m["median_price_per_m2"], target)
            efficient = commute_efficient_keys([{"key": m["location_key"], "commute_km": m["commute_km"], "cost": m["cost"]} for m in members])
            for rank, m in enumerate(sorted(members, key=lambda x: (x["commute_km"], x["location_key"])), start=1):
                district_rows.append((
                    dest["destination_id"], dest["destination_name"], dest["destination_kind"], dest["province_name"],
                    dest["lat"], dest["lon"],
                    m["location_key"], category, band, m["n_listings"], m["centroid"]["n"],
                    float(m["centroid"]["lat"]), float(m["centroid"]["lon"]), m["commute_km"], rank,
                    float(m["median_price_per_m2"]), float(m["median_area"]), target, float(m["cost"]), m["location_key"] in efficient,
                ))
            shares = {m["location_key"]: {f: m[f"share_{f}"] for f in FEATURE_FLAGS} for m in members}
            for o in members:
                origin_target = float(o["median_area"])
                origin_cost = cost_at_target_area(o["median_price_per_m2"], origin_target)
                options = []
                for a in members:
                    if a is o or not commute_allowed(o["commute_km"], a["commute_km"]):
                        continue
                    alt_cost = cost_at_target_area(a["median_price_per_m2"], origin_target)
                    similarity = feature_similarity(shares[o["location_key"]], shares[a["location_key"]])
                    gap = max_flag_share_gap(shares[o["location_key"]], shares[a["location_key"]])
                    if alt_cost < origin_cost and similarity >= MIN_SUBSTITUTION_SIMILARITY and gap <= MAX_FLAG_SHARE_GAP:
                        options.append((a, alt_cost, similarity, gap))
                options.sort(key=lambda x: (-(1 - x[0]["median_price_per_m2"] / o["median_price_per_m2"]), x[0]["location_key"]))
                for rank, (a, alt_cost, similarity, gap) in enumerate(options, start=1):
                    substitution_rows.append((
                        dest["destination_id"], o["location_key"], a["location_key"], category, band,
                        o["n_listings"], a["n_listings"], o["commute_km"], a["commute_km"], round(a["commute_km"] - o["commute_km"], 3),
                        float(o["median_price_per_m2"]), float(a["median_price_per_m2"]),
                        round(1 - a["median_price_per_m2"] / o["median_price_per_m2"], 6),
                        origin_target, float(origin_cost), float(alt_cost), float(origin_cost - alt_cost), similarity, gap, rank,
                    ))

    district_schema = T.StructType([
        T.StructField("destination_id", T.StringType(), False), T.StructField("destination_name", T.StringType(), False),
        T.StructField("destination_kind", T.StringType(), False), T.StructField("province_name", T.StringType(), False),
        T.StructField("destination_lat", T.DoubleType(), False), T.StructField("destination_lon", T.DoubleType(), False),
        T.StructField("location_key", T.IntegerType(), False), T.StructField("property_category_key", T.IntegerType(), False),
        T.StructField("area_band_key", T.IntegerType(), False), T.StructField("n_listings", T.LongType(), False),
        T.StructField("centroid_n_listings", T.LongType(), False), T.StructField("centroid_lat", T.DoubleType(), False),
        T.StructField("centroid_lon", T.DoubleType(), False), T.StructField("commute_km", T.DoubleType(), False),
        T.StructField("commute_rank", T.IntegerType(), False), T.StructField("median_price_per_m2", T.DoubleType(), False),
        T.StructField("median_area", T.DoubleType(), False), T.StructField("target_area", T.DoubleType(), False),
        T.StructField("estimated_cost_at_target_area", T.DoubleType(), False), T.StructField("is_commute_efficient", T.BooleanType(), False),
    ])
    substitution_schema = T.StructType([
        T.StructField("destination_id", T.StringType(), False), T.StructField("origin_location_key", T.IntegerType(), False),
        T.StructField("alternative_location_key", T.IntegerType(), False), T.StructField("property_category_key", T.IntegerType(), False),
        T.StructField("area_band_key", T.IntegerType(), False), T.StructField("origin_n_listings", T.LongType(), False),
        T.StructField("alternative_n_listings", T.LongType(), False), T.StructField("origin_commute_km", T.DoubleType(), False),
        T.StructField("alternative_commute_km", T.DoubleType(), False), T.StructField("commute_diff_km", T.DoubleType(), False),
        T.StructField("origin_median_price_per_m2", T.DoubleType(), False), T.StructField("alternative_median_price_per_m2", T.DoubleType(), False),
        T.StructField("price_gap_pct", T.DoubleType(), False), T.StructField("target_area", T.DoubleType(), False),
        T.StructField("estimated_cost_origin_at_target_area", T.DoubleType(), False),
        T.StructField("estimated_cost_alternative_at_target_area", T.DoubleType(), False),
        T.StructField("estimated_saving_at_target_area", T.DoubleType(), False),
        T.StructField("feature_similarity", T.DoubleType(), False), T.StructField("max_flag_share_gap", T.DoubleType(), False),
        T.StructField("substitution_rank", T.IntegerType(), False),
    ])
    write_iceberg_table(spark.createDataFrame(district_rows, district_schema), f"{gold}.agg_commute_district")
    write_iceberg_table(spark.createDataFrame(substitution_rows, substitution_schema), f"{gold}.agg_commute_substitution")
    written = spark.table(f"{gold}.agg_commute_substitution")

    named = [d for d in destinations if not d["destination_id"].startswith("center_")]
    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "distance": "straight-line (haversine) km from the district centroid to the destination",
        "min_centroid_listings": MIN_CENTROID_LISTINGS,
        "commute_tolerance_km": COMMUTE_TOLERANCE_KM,
        "min_peer_group_size": MIN_PEER_GROUP_SIZE,
        "min_feature_similarity": MIN_SUBSTITUTION_SIMILARITY,
        "max_flag_share_gap": MAX_FLAG_SHARE_GAP,
        "destinations": len(destinations),
        "named_destinations": len(named),
        "districts_with_centroid": len(centroids),
        "district_groups": len(groups),
        "district_rows": len(district_rows),
        "efficient_district_rows": sum(1 for r in district_rows if r[-1]),
        "substitution_pairs": len(substitution_rows),
        "origins_with_alternative": written.select("destination_id", "origin_location_key", "property_category_key", "area_band_key").distinct().count(),
        "median_price_gap_pct": written.agg(F.expr("percentile(price_gap_pct, 0.5)")).first()[0],
        "status": "PASS",
    }
    write_summary("gold_commute_choice_summary.json", summary)
    spark.stop()


if __name__ == "__main__":
    main()
