"""Export the Gold examples used by the progress report to JSON.

Runs on Spark (Iceberg access) and writes ``docs/report/data/report_data.json``;
``build_report.py`` then renders the Word report on the host without Spark.
Examples are selected by deterministic rules (largest group, fixed ordering),
so re-running on the same data yields the same report.
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

OUTPUT = PROJECT_ROOT / "docs/report/data/report_data.json"
FOCUS_PROVINCE = "Hồ Chí Minh"


def rows(frame, limit: int | None = None) -> list[dict]:
    frame = frame.limit(limit) if limit else frame
    return [row.asDict(recursive=True) for row in frame.collect()]


def to_json(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(type(value))


def main() -> None:
    from pyspark.sql import functions as F

    from src.common.spark_session import build_spark_session, ensure_gold_namespace, ensure_silver_namespace

    spark = build_spark_session("ExportReportData")
    gold = ensure_gold_namespace(spark)
    silver = ensure_silver_namespace(spark)
    t = lambda name: spark.table(f"{gold}.{name}")  # noqa: E731

    location = t("dim_location").select("location_key", "province_name", "district_name")
    category = t("dim_property_category").select("property_category_key", "model_category", "category_label")
    price_band = t("dim_price_band").select(F.col("band_key").alias("price_band_key"), F.col("band_label").alias("price_band"))
    area_band = t("dim_area_band").select(F.col("band_key").alias("area_band_key"), F.col("band_label").alias("area_band"))
    room_band = t("dim_room_band").select(F.col("band_key").alias("room_band_key"), F.col("band_label").alias("room_band"))
    source = t("dim_source").select("source_key", "source")
    fact = t("fact_listing")
    reps = fact.filter("is_dup_representative")
    data: dict = {"generated_at": datetime.now(timezone.utc).isoformat(), "focus_province": FOCUS_PROVINCE}

    # Silver feature: category x flag rates (sale + rent, all current listings)
    feature = spark.table(f"{silver}.listing_feature")
    data["feature_by_category"] = rows(
        feature.groupBy("model_category").agg(
            F.count("*").alias("n"),
            *[F.round(F.avg(F.col(f"title_has_{f}").cast("double")), 4).alias(f) for f in ("legal", "furnished", "frontage", "elevator", "car_access")],
        ).orderBy(F.desc("n"))
    )
    data["feature_by_source"] = rows(
        feature.groupBy("source").agg(
            F.count("*").alias("n"), F.round(F.avg("feature_completeness_score"), 4).alias("avg_completeness"),
            F.round(F.avg(F.col("rooms_known").cast("double")), 4).alias("rooms_known"),
            F.round(F.avg(F.col("legal_known").cast("double")), 4).alias("legal_known"),
        ).orderBy(F.desc("n"))
    )
    data["feature_examples"] = rows(
        spark.table(f"{silver}.silver_listings_current_27").select("source_id", "title")
        .join(feature, "source_id")
        .filter("title_has_legal AND title_has_car_access AND title_has_elevator")
        .select("source", "title", "model_category").orderBy("source_id"), 5
    )

    # Gold foundation
    data["fact_by_source"] = rows(
        fact.join(source, "source_key").groupBy("source").agg(
            F.count("*").alias("fact_rows"), F.sum(F.col("is_cross_source_dup_suspect").cast("int")).alias("dup_suspect"),
        ).orderBy(F.desc("fact_rows"))
    )
    data["fact_by_category_price_band"] = rows(
        reps.join(category, "property_category_key").join(price_band, "price_band_key")
        .groupBy("category_label", "price_band_key", "price_band").count().orderBy("category_label", "price_band_key")
    )

    # 3f market overview: top provinces
    overview = t("agg_market_overview").join(category, "property_category_key")
    top_provinces = [r["province_name"] for r in overview.groupBy("province_name").agg(F.sum("n_listings").alias("n")).orderBy(F.desc("n")).limit(5).collect()]
    data["market_overview"] = rows(
        overview.filter(F.col("province_name").isin(top_provinces) & F.col("model_category").isin("nha_pho", "can_ho", "dat"))
        .select("province_name", "category_label", "n_listings", "median_price", "median_price_per_m2", "p25_price_per_m2", "p75_price_per_m2", "median_area")
        .orderBy("province_name", "category_label")
    )
    data["top_provinces"] = top_provinces

    # 3a / BQ2: position distribution and one concrete peer group
    assess = t("fact_listing_price_assessment").join(category, "property_category_key")
    data["position_by_category"] = rows(assess.groupBy("category_label", "price_position").count().orderBy("category_label", "price_position"))
    data["position_feature_gap"] = rows(
        assess.filter("price_position <> 'khong_du_du_lieu'").groupBy("price_position").agg(
            F.count("*").alias("n"), F.round(F.avg("feature_count"), 3).alias("avg_feature_count"),
            F.round(F.avg("feature_count_vs_peer"), 3).alias("avg_feature_count_vs_peer"),
            F.round(F.avg(F.col("is_low_price_missing_legal").cast("double")), 4).alias("share_low_missing_legal"),
        ).orderBy("price_position")
    )
    peer = (
        t("agg_peer_group_benchmark").filter("benchmark_level = 'LOC_CAT_AREA_ROOM'")
        .join(location, "location_key").join(category, "property_category_key").join(area_band, "area_band_key").join(room_band, "room_band_key")
        .filter((F.col("province_name") == FOCUS_PROVINCE) & (F.col("model_category") == "can_ho") & F.col("district_name").isNotNull())
        .orderBy(F.desc("n_listings"), "peer_group_id").limit(1)
    )
    peer_group = rows(peer)[0]
    data["bq2_peer_group"] = peer_group
    titles = spark.table(f"{silver}.silver_listings_current_27").select("source_id", "title")
    group_listings = assess.filter(F.col("peer_group_id") == peer_group["peer_group_id"]).join(titles, "source_id")
    data["bq2_examples"] = rows(
        group_listings.filter("price_position = 'thap'").orderBy("price_ratio", "source_id").limit(3)
        .unionByName(group_listings.filter("price_position = 'hop_ly'").orderBy(F.abs(F.col("price_ratio") - 1), "source_id").limit(2))
        .unionByName(group_listings.filter("price_position = 'cao'").orderBy(F.desc("price_ratio"), "source_id").limit(3))
        .select("source_id", "title", "price", "area", "price_per_m2", "price_ratio", "price_position", "feature_count", "feature_count_vs_peer")
    )

    # 3b / BQ1: what 3-5 billion buys in the focus province, plus a Pareto example
    tradeoff = t("agg_budget_tradeoff").join(location, "location_key").join(category, "property_category_key").join(price_band, "price_band_key")
    budget = tradeoff.filter((F.col("province_name") == FOCUS_PROVINCE) & (F.col("price_band_key") == 4) & F.col("district_name").isNotNull() & (F.col("n_listings") >= 20))
    data["bq1_budget_label"] = "3 - 5 tỷ"
    for code in ("nha_pho", "can_ho"):
        data[f"bq1_tradeoff_{code}"] = rows(
            budget.filter(F.col("model_category") == code).select(
                "district_name", "n_listings", "median_area", "median_rooms", "median_price_per_m2", "median_distance_km",
                "share_legal", "share_frontage", "share_car_access", "share_elevator",
            ).orderBy(F.desc("median_area"), "district_name")
        )
    pareto = t("fact_budget_pareto").join(location, "location_key").join(category, "property_category_key")
    data["bq1_pareto_overall"] = rows(
        pareto.join(price_band, "price_band_key").groupBy("price_band_key", "price_band").agg(
            F.count("*").alias("n"), F.sum(F.col("is_pareto_efficient").cast("int")).alias("efficient"),
        ).orderBy("price_band_key")
    )
    pareto_group = rows(
        pareto.filter((F.col("province_name") == FOCUS_PROVINCE) & (F.col("price_band_key") == 4) & (F.col("model_category") == "nha_pho") & F.col("district_name").isNotNull())
        .groupBy("location_key", "district_name").agg(F.first("group_size").alias("group_size"), F.first("frontier_size").alias("frontier_size"))
        .filter("group_size >= 30").orderBy(F.desc("group_size"), "district_name"), 1
    )[0]
    data["bq1_pareto_group"] = pareto_group
    frontier = (
        pareto.filter((F.col("location_key") == pareto_group["location_key"]) & (F.col("price_band_key") == 4) & (F.col("model_category") == "nha_pho"))
        .join(fact.select("source_id", "price", "area", "rooms", "title_has_legal", "title_has_car_access", "title_has_frontage", "title_has_elevator", "title_has_furnished"), "source_id")
        .join(titles, "source_id")
    )
    data["bq1_pareto_points"] = rows(frontier.select("source_id", "price", "area", "rooms", "is_pareto_efficient"))
    data["bq1_pareto_examples"] = rows(
        frontier.filter("is_pareto_efficient").orderBy("price", "source_id")
        .select("title", "price", "area", "rooms", "title_has_legal", "title_has_car_access", "title_has_frontage", "title_has_elevator"), 6
    )

    # 3c / BQ3: alternatives for the origin with most candidate districts
    origin_loc = location.select(F.col("location_key").alias("origin_location_key"), F.col("district_name").alias("origin_district"), F.col("province_name"))
    alt_loc = location.select(F.col("location_key").alias("alternative_location_key"), F.col("district_name").alias("alternative_district"))
    substitution = t("agg_area_substitution").join(origin_loc, "origin_location_key").join(alt_loc, "alternative_location_key").join(category, "property_category_key").join(area_band, "area_band_key")
    focus = substitution.filter((F.col("province_name") == FOCUS_PROVINCE) & (F.col("model_category") == "nha_pho"))
    # Origin = the most-listed district (where buyers look most) with >= 5 alternatives.
    origin = rows(
        focus.filter(F.col("origin_district").isNotNull()).groupBy("origin_district", "area_band", "area_band_key")
        .agg(F.count("*").alias("count"), F.first("origin_n_listings").alias("origin_n_listings"))
        .filter("count >= 5").orderBy(F.desc("origin_n_listings"), "origin_district", "area_band_key"), 1
    )[0]
    data["bq3_origin"] = origin
    data["bq3_examples"] = rows(
        focus.filter((F.col("origin_district") == origin["origin_district"]) & (F.col("area_band_key") == origin["area_band_key"]))
        .select("alternative_district", "origin_median_price_per_m2", "alternative_median_price_per_m2", "price_gap_pct",
                "typical_budget_saving", "distance_diff_km", "feature_similarity", "origin_n_listings", "alternative_n_listings", "substitution_rank")
        .orderBy("substitution_rank"), 8
    )
    data["bq3_by_category"] = rows(substitution.groupBy("category_label").agg(
        F.count("*").alias("pairs"), F.round(F.expr("percentile(price_gap_pct, 0.5)"), 4).alias("median_gap"),
        F.round(F.expr("percentile(distance_diff_km, 0.5)"), 2).alias("median_distance_diff"),
    ).orderBy(F.desc("pairs")))

    # 3d DQ KPI and 3e repricing
    data["dq_kpi"] = rows(t("agg_dq_kpi").orderBy(F.desc("bronze_rows")))
    data["price_change_by_category"] = rows(
        t("fact_listing_price_change").join(category, "property_category_key").groupBy("category_label", "direction").agg(
            F.count("*").alias("events"), F.round(F.expr("percentile(price_change_pct, 0.5)"), 4).alias("median_pct"),
        ).orderBy("category_label", "direction")
    )

    # Schemas of every table added in this phase (Appendix C)
    new_tables = [f"{silver}.listing_feature"] + [f"{gold}.{name}" for name in (
        "dim_source", "dim_date", "dim_location", "dim_property_category", "dim_price_band", "dim_dq_status",
        "fact_listing", "agg_peer_group_benchmark", "fact_listing_price_assessment", "agg_budget_tradeoff",
        "fact_budget_pareto", "agg_area_substitution", "agg_dq_kpi", "fact_listing_price_change", "agg_market_overview",
    )]
    data["schemas"] = {
        name.split(".", 1)[1]: [[field.name, field.dataType.simpleString()] for field in spark.table(name).schema]
        for name in new_tables
    }

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=to_json) + "\n", encoding="utf-8")
    print(f"Wrote {OUTPUT}")
    spark.stop()


if __name__ == "__main__":
    main()
