"""Build ``lakehouse.gold.fact_listing``: one row per current sale listing.

Filters: ``is_rent = FALSE`` (rent and unknown intent are excluded) and
``dq_status <> 'REJECT'``. Every FK resolves to a dimension row; unmapped
values point to ``-1``. Cross-source duplicates are flagged, never deleted:
aggregates must use ``is_dup_representative = TRUE``.
"""

from __future__ import annotations

import json
import math
import sys
from collections import Counter, defaultdict
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.gold.gold_rules import (
    DUP_AREA_TOLERANCE, DUP_MIN_TITLE_JACCARD, DUP_PRICE_TOLERANCE, UNKNOWN_KEY,
    band_key, cluster_pairs, load_bands, pick_representative, title_tokens,
)

MAX_DUP_PAIRS = 500_000
FLAG_COLUMNS = [
    "title_has_legal", "title_has_furnished", "title_has_frontage",
    "title_has_elevator", "title_has_car_access",
]
KNOWN_COLUMNS = ["price_known", "area_known", "rooms_known", "location_known", "legal_known"]
FK_COLUMNS = [
    "source_key", "date_key", "posted_date_key", "location_key", "property_category_key",
    "price_band_key", "area_band_key", "unit_price_band_key", "room_band_key", "dq_status_key",
]
FACT_COLUMNS = [
    "source_id", *FK_COLUMNS,
    "price", "area", "price_per_m2", "rooms", "distance_to_center_km",
    *FLAG_COLUMNS, *KNOWN_COLUMNS, "feature_completeness_score",
    "dup_group_id", "is_cross_source_dup_suspect", "is_dup_representative",
    "title", "ad_url", "scraped_at",
]
BANDS = load_bands()


def assign_bands(rows: Iterator[Any]) -> Iterator[tuple[Any, ...]]:
    for row in rows:
        yield (
            row["source_id"],
            band_key(row["price"], BANDS["price"]),
            band_key(row["area"], BANDS["area"]),
            band_key(row["price_per_m2"], BANDS["unit_price"]),
            band_key(row["rooms"], BANDS["room"]),
            title_tokens(row["title"]),
        )


def main() -> None:
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.storagelevel import StorageLevel

    from src.common.spark_session import build_spark_session, ensure_gold_namespace, ensure_silver_namespace, write_iceberg_table

    spark = build_spark_session("GoldFactListing")
    silver = ensure_silver_namespace(spark)
    gold = ensure_gold_namespace(spark)
    started_at = datetime.now(timezone.utc)

    current = spark.table(f"{silver}.silver_listings_current_27").select(
        "source_id", "source", "title", "price", "area", "rooms", "price_per_m2",
        "is_rent", "scraped_at", "posted_at", "ad_url",
    )
    feature = spark.table(f"{silver}.listing_feature").select(
        "source_id", "dq_status", "model_category", *FLAG_COLUMNS, *KNOWN_COLUMNS, "feature_completeness_score",
    )
    location = spark.table(f"{silver}.listing_location").select(
        "source_id", "province_name_model", "district_name_model", "distance_to_center_km",
    )
    base = current.join(feature, "source_id").join(location, "source_id", "left").persist(StorageLevel.MEMORY_AND_DISK)

    filter_counts = {
        "current_rows": base.count(),
        "excluded_is_rent_true": base.filter(F.col("is_rent") == True).count(),  # noqa: E712
        "excluded_is_rent_null": base.filter(F.col("is_rent").isNull()).count(),
        "excluded_dq_reject": base.filter((F.col("is_rent") == False) & (F.col("dq_status") == "REJECT")).count(),  # noqa: E712
    }
    sale = base.filter((F.col("is_rent") == False) & (F.col("dq_status") != "REJECT"))  # noqa: E712
    filter_counts["fact_rows_expected"] = sale.count()

    band_schema = T.StructType([
        T.StructField("source_id", T.StringType(), False),
        T.StructField("price_band_key", T.IntegerType(), False),
        T.StructField("area_band_key", T.IntegerType(), False),
        T.StructField("unit_price_band_key", T.IntegerType(), False),
        T.StructField("room_band_key", T.IntegerType(), False),
        T.StructField("title_tokens", T.ArrayType(T.StringType(), False), False),
    ])
    bands = spark.createDataFrame(
        sale.select("source_id", "price", "area", "price_per_m2", "rooms", "title").rdd.mapPartitions(assign_bands),
        band_schema,
    )

    dim_source = spark.table(f"{gold}.dim_source").select("source", "source_key")
    dim_category = spark.table(f"{gold}.dim_property_category").select("model_category", "property_category_key")
    dim_dq = spark.table(f"{gold}.dim_dq_status").select("dq_status", "dq_status_key")
    dim_location = spark.table(f"{gold}.dim_location").filter(F.col("location_key") != UNKNOWN_KEY)
    dim_date = spark.table(f"{gold}.dim_date").select("date_key")

    def date_key(column: str):
        return F.date_format(F.to_date(column), "yyyyMMdd").cast("int")

    fact = (
        sale.join(bands, "source_id")
        .join(F.broadcast(dim_source), "source", "left")
        .join(F.broadcast(dim_category), "model_category", "left")
        .join(F.broadcast(dim_dq), "dq_status", "left")
        .join(
            F.broadcast(dim_location.select("location_key", "province_name", "district_name")),
            (F.col("province_name_model") == F.col("province_name"))
            & F.col("district_name_model").eqNullSafe(F.col("district_name")),
            "left",
        )
        .withColumn("date_key", date_key("scraped_at"))
        .withColumn("posted_date_key", date_key("posted_at"))
    )
    for column in ("date_key", "posted_date_key"):
        known_dates = F.broadcast(dim_date.withColumnRenamed("date_key", f"_{column}"))
        fact = fact.join(known_dates, F.col(column) == F.col(f"_{column}"), "left").withColumn(
            column, F.coalesce(F.col(f"_{column}"), F.lit(UNKNOWN_KEY))
        ).drop(f"_{column}")
    for column in ("source_key", "property_category_key", "dq_status_key", "location_key"):
        fact = fact.withColumn(column, F.coalesce(F.col(column), F.lit(UNKNOWN_KEY)).cast("int"))
    enriched = fact.persist(StorageLevel.MEMORY_AND_DISK)

    # Cross-source duplicate candidates, blocked by location, category and a
    # log-area bin of width DUP_AREA_TOLERANCE (neighbour bins included).
    log_step = math.log1p(DUP_AREA_TOLERANCE)
    candidates = enriched.filter(
        (F.col("location_key") != UNKNOWN_KEY) & (F.col("property_category_key") != UNKNOWN_KEY)
        & (F.col("price") > 0) & (F.col("area") > 0) & (F.size("title_tokens") > 0)
    ).select(
        "source_id", "source", "location_key", "property_category_key", "price", "area", "title_tokens",
        F.floor(F.log(F.col("area")) / F.lit(log_step)).cast("long").alias("area_bin"),
    )
    left = candidates.select([F.col(c).alias(f"l_{c}") for c in candidates.columns])
    right = candidates.withColumn("area_bin", F.explode(F.array(F.col("area_bin") - 1, F.col("area_bin"), F.col("area_bin") + 1)))
    right = right.select([F.col(c).alias(f"r_{c}") for c in right.columns])
    jaccard = F.size(F.array_intersect("l_title_tokens", "r_title_tokens")) / F.size(F.array_union("l_title_tokens", "r_title_tokens"))
    pairs = (
        left.join(
            right,
            (F.col("l_location_key") == F.col("r_location_key"))
            & (F.col("l_property_category_key") == F.col("r_property_category_key"))
            & (F.col("l_area_bin") == F.col("r_area_bin"))
            & (F.col("l_source_id") < F.col("r_source_id"))
            & (F.col("l_source") != F.col("r_source")),
        )
        .filter(F.abs(F.col("l_area") - F.col("r_area")) <= F.lit(DUP_AREA_TOLERANCE) * F.greatest("l_area", "r_area"))
        .filter(F.abs(F.col("l_price") - F.col("r_price")) <= F.lit(DUP_PRICE_TOLERANCE) * F.greatest("l_price", "r_price"))
        .filter(jaccard >= F.lit(DUP_MIN_TITLE_JACCARD))
        .select("l_source_id", "r_source_id", "l_source", "r_source")
        .distinct()
    )
    pair_count = pairs.count()
    if pair_count > MAX_DUP_PAIRS:
        raise RuntimeError(f"Too many duplicate pairs ({pair_count:,}); tighten the blocking rule")
    pair_rows = pairs.collect()
    pair_by_sources = Counter(" | ".join(sorted((r["l_source"], r["r_source"]))) for r in pair_rows)
    groups = cluster_pairs((r["l_source_id"], r["r_source_id"]) for r in pair_rows)

    dup_schema = T.StructType([
        T.StructField("source_id", T.StringType(), False),
        T.StructField("dup_group_id", T.StringType(), False),
        T.StructField("is_dup_representative", T.BooleanType(), False),
    ])
    dup_rows: list[tuple[str, str, bool]] = []
    if groups:
        members_by_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
        member_ids = spark.createDataFrame([(member,) for member in groups], "source_id STRING")
        for row in enriched.join(member_ids, "source_id").select("source_id", "feature_completeness_score", "scraped_at").collect():
            members_by_group[groups[row["source_id"]]].append(row.asDict())
        for group_id, members in members_by_group.items():
            representative = pick_representative(members)
            dup_rows.extend((m["source_id"], group_id, m["source_id"] == representative) for m in members)
    duplicates = spark.createDataFrame(dup_rows, dup_schema)

    fact = (
        enriched.join(duplicates, "source_id", "left")
        .withColumn("is_cross_source_dup_suspect", F.col("dup_group_id").isNotNull())
        .withColumn("is_dup_representative", F.coalesce(F.col("is_dup_representative"), F.lit(True)))
        .select(*FACT_COLUMNS)
    )
    output_table = f"{gold}.fact_listing"
    write_iceberg_table(fact, output_table)
    written = spark.table(output_table).persist(StorageLevel.MEMORY_AND_DISK)
    output_rows = written.count()
    if output_rows != filter_counts["fact_rows_expected"]:
        raise RuntimeError(f"fact_listing rows {output_rows:,} != expected {filter_counts['fact_rows_expected']:,}")

    unknown_fk_counts = written.agg(*[
        F.sum(F.when(F.col(c) == UNKNOWN_KEY, 1).otherwise(0)).alias(c) for c in FK_COLUMNS
    ]).first().asDict()
    suspect_by_source = {
        str(r["source"]): int(r["count"])
        for r in written.filter("is_cross_source_dup_suspect").join(
            spark.table(f"{gold}.dim_source"), "source_key"
        ).groupBy("source").count().collect()
    }
    suspect_rows = written.filter("is_cross_source_dup_suspect").count()
    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "output": output_table,
        "filter_counts": filter_counts,
        "output_rows": output_rows,
        "unknown_fk_counts": {k: int(v or 0) for k, v in unknown_fk_counts.items()},
        "dedup_rule": {
            "area_tolerance": DUP_AREA_TOLERANCE,
            "price_tolerance": DUP_PRICE_TOLERANCE,
            "min_title_jaccard": DUP_MIN_TITLE_JACCARD,
            "blocking": "same location_key + property_category_key, adjacent log-area bins, different source",
        },
        "dedup": {
            "candidate_pairs": pair_count,
            "pairs_by_source_pair": dict(pair_by_sources.most_common()),
            "dup_groups": len(set(groups.values())),
            "suspect_rows": suspect_rows,
            "suspect_rate": round(suspect_rows / output_rows, 6) if output_rows else 0.0,
            "suspect_rows_by_source": suspect_by_source,
            "representative_rows": written.filter("is_dup_representative").count(),
        },
        "status": "PASS",
    }
    for output_dir in (PROJECT_ROOT / "outputs/validation", PROJECT_ROOT / "docs/validation"):
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "gold_fact_listing_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    written.unpersist()
    enriched.unpersist()
    base.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
