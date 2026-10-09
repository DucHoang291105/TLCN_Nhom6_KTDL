"""Build ``lakehouse.gold.fact_listing``: one row per current sale listing.

Filters: ``is_rent = FALSE`` (rent and unknown intent are excluded) and
``dq_status <> 'REJECT'``. Every FK resolves to a dimension row; unmapped
values point to ``-1``. Cross-source duplicates are *suspected* by a
heuristic and flagged, never deleted: aggregates use
``is_dup_representative = TRUE`` (one representative per resolved group).
This does not guarantee one row per real property.

Location: a listing whose text province contradicts its coordinates (LQ05)
gets ``location_key = -1`` and no distance, so it is not counted as a
confirmed location in any aggregate.
"""

from __future__ import annotations

import csv
import hashlib
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
    DUP_AREA_TOLERANCE, DUP_MIN_TITLE_JACCARD, DUP_PRICE_TOLERANCE, DUP_SENSITIVITY, UNKNOWN_KEY,
    band_key, cluster_pairs, load_bands, mutual_best_pairs, pick_representative, resolve_dup_groups,
    title_tokens,
)

MAX_DUP_PAIRS = 500_000
DUP_SAMPLE_SIZE = 40
LOOSE_AREA, LOOSE_PRICE, LOOSE_JACCARD = DUP_SENSITIVITY["loose"]
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
    "location_dq_status", "is_location_conflict", "is_province_inferred_from_coordinates",
    *FLAG_COLUMNS, *KNOWN_COLUMNS, "feature_completeness_score",
    "dup_group_id", "dup_group_status", "is_cross_source_dup_suspect", "is_dup_representative",
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
        "location_dq_status",
        F.array_contains("location_dq_reasons", "LQ05_PROVINCE_COORDINATE_CONFLICT").alias("is_location_conflict"),
        F.array_contains("location_dq_reasons", "LQ04_PROVINCE_INFERRED_BY_NEAREST_CENTER").alias("is_province_inferred_from_coordinates"),
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
    # A contradicted province is not a confirmed location.
    fact = fact.withColumn(
        "location_key", F.when(F.coalesce(F.col("is_location_conflict"), F.lit(False)), F.lit(None)).otherwise(F.col("location_key"))
    )
    for column in ("is_location_conflict", "is_province_inferred_from_coordinates"):
        fact = fact.withColumn(column, F.coalesce(F.col(column), F.lit(False)))
    for column in ("source_key", "property_category_key", "dq_status_key", "location_key"):
        fact = fact.withColumn(column, F.coalesce(F.col(column), F.lit(UNKNOWN_KEY)).cast("int"))
    enriched = fact.persist(StorageLevel.MEMORY_AND_DISK)

    # Cross-source duplicate candidates under the *loosest* sensitivity
    # setting, blocked by location, category and a log-area bin (neighbour
    # bins included). Each setting is then applied in Python.
    log_step = math.log1p(LOOSE_AREA)
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
    area_gap = F.abs(F.col("l_area") - F.col("r_area")) / F.greatest("l_area", "r_area")
    price_gap = F.abs(F.col("l_price") - F.col("r_price")) / F.greatest("l_price", "r_price")
    pairs = (
        left.join(
            right,
            (F.col("l_location_key") == F.col("r_location_key"))
            & (F.col("l_property_category_key") == F.col("r_property_category_key"))
            & (F.col("l_area_bin") == F.col("r_area_bin"))
            & (F.col("l_source_id") < F.col("r_source_id"))
            & (F.col("l_source") != F.col("r_source")),
        )
        .select(
            F.col("l_source_id").alias("left"), F.col("r_source_id").alias("right"),
            F.col("l_source").alias("left_source"), F.col("r_source").alias("right_source"),
            F.round(jaccard, 6).alias("jaccard"), F.round(price_gap, 6).alias("price_gap"), F.round(area_gap, 6).alias("area_gap"),
        )
        .filter((F.col("area_gap") <= LOOSE_AREA) & (F.col("price_gap") <= LOOSE_PRICE) & (F.col("jaccard") >= LOOSE_JACCARD))
        .distinct()
    )
    pair_count_loose = pairs.count()
    if pair_count_loose > MAX_DUP_PAIRS:
        raise RuntimeError(f"Too many duplicate pairs ({pair_count_loose:,}); tighten the blocking rule")
    loose_pairs = [row.asDict() for row in pairs.collect()]
    source_of = {p["left"]: p["left_source"] for p in loose_pairs} | {p["right"]: p["right_source"] for p in loose_pairs}

    def run_setting(area_tol: float, price_tol: float, min_jaccard: float) -> tuple[dict[str, str], dict[str, str], dict[str, Any]]:
        selected = [p for p in loose_pairs if p["area_gap"] <= area_tol and p["price_gap"] <= price_tol and p["jaccard"] >= min_jaccard]
        accepted = mutual_best_pairs(selected)
        groups = cluster_pairs(accepted)
        status = resolve_dup_groups(groups, source_of)
        sizes = Counter(groups.values())
        stats = {
            "candidate_pairs": len(selected),
            "mutual_best_pairs": len(accepted),
            "dup_groups": len(status),
            "ambiguous_groups": sum(1 for v in status.values() if v == "AMBIGUOUS"),
            "suspect_rows": len(groups),
            "rows_removed_from_aggregates": sum(sizes[g] - 1 for g, v in status.items() if v == "RESOLVED"),
            "max_group_size": max(sizes.values(), default=0),
        }
        return groups, status, stats

    sensitivity = {}
    for name, (area_tol, price_tol, min_jaccard) in DUP_SENSITIVITY.items():
        groups_s, status_s, stats = run_setting(area_tol, price_tol, min_jaccard)
        sensitivity[name] = stats
        if name == "default":
            groups, group_status = groups_s, status_s
    default_pairs = [p for p in loose_pairs if p["area_gap"] <= DUP_AREA_TOLERANCE and p["price_gap"] <= DUP_PRICE_TOLERANCE and p["jaccard"] >= DUP_MIN_TITLE_JACCARD]
    accepted_default = set(mutual_best_pairs(default_pairs))
    pair_by_sources = Counter(
        " | ".join(sorted((p["left_source"], p["right_source"]))) for p in default_pairs if (p["left"], p["right"]) in accepted_default
    )

    dup_schema = T.StructType([
        T.StructField("source_id", T.StringType(), False),
        T.StructField("dup_group_id", T.StringType(), False),
        T.StructField("dup_group_status", T.StringType(), False),
        T.StructField("is_dup_representative", T.BooleanType(), False),
    ])
    dup_rows: list[tuple[str, str, str, bool]] = []
    if groups:
        members_by_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
        member_ids = spark.createDataFrame([(member,) for member in groups], "source_id STRING")
        for row in enriched.join(member_ids, "source_id").select("source_id", "feature_completeness_score", "scraped_at").collect():
            members_by_group[groups[row["source_id"]]].append(row.asDict())
        for group_id, members in members_by_group.items():
            status = group_status[group_id]
            # Ambiguous groups are not collapsed: every member stays counted.
            representative = pick_representative(members) if status == "RESOLVED" else None
            dup_rows.extend(
                (m["source_id"], group_id, status, representative is None or m["source_id"] == representative)
                for m in members
            )
    duplicates = spark.createDataFrame(dup_rows, dup_schema)

    # Deterministic sample of accepted pairs for manual review.
    sample_keys = sorted(accepted_default, key=lambda pair: hashlib.sha256("|".join(pair).encode()).hexdigest())[:DUP_SAMPLE_SIZE]
    if sample_keys:
        sample_ids = spark.createDataFrame([(i,) for pair in sample_keys for i in pair], "source_id STRING").distinct()
        info = {
            r["source_id"]: r.asDict()
            for r in enriched.join(sample_ids, "source_id").select("source_id", "source", "title", "price", "area", "ad_url").collect()
        }
        metrics = {(p["left"], p["right"]): p for p in default_pairs}
        sample_path = PROJECT_ROOT / "docs/validation/dedup_pair_sample.csv"
        sample_path.parent.mkdir(parents=True, exist_ok=True)
        with sample_path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["left_source_id", "right_source_id", "group_status", "jaccard", "price_gap", "area_gap",
                             "left_title", "right_title", "left_price", "right_price", "left_area", "right_area", "manual_verdict"])
            for left_id, right_id in sample_keys:
                a, b, m = info[left_id], info[right_id], metrics[(left_id, right_id)]
                writer.writerow([left_id, right_id, group_status[groups[left_id]], m["jaccard"], m["price_gap"], m["area_gap"],
                                 a["title"], b["title"], a["price"], b["price"], a["area"], b["area"], ""])

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
    # How much the dedup heuristic moves a headline aggregate.
    categories = spark.table(f"{gold}.dim_property_category").select("property_category_key", "model_category")
    median_effect = {
        str(r["model_category"]): {"all_rows": r["all_rows"], "representatives": r["reps"]}
        for r in written.join(categories, "property_category_key").groupBy("model_category").agg(
            F.expr("percentile(price_per_m2, 0.5)").alias("all_rows"),
            F.expr("percentile(CASE WHEN is_dup_representative THEN price_per_m2 END, 0.5)").alias("reps"),
        ).collect()
    }
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
            "pair_selection": "mutual best match per source pair (Jaccard, then price gap, then area gap)",
            "ambiguous_groups": "groups with two listings of one source are kept, not collapsed",
        },
        "location": {
            "conflict_rows_set_to_unknown_location": written.filter("is_location_conflict").count(),
            "province_inferred_from_coordinates_rows": written.filter("is_province_inferred_from_coordinates").count(),
        },
        "dedup_sensitivity": sensitivity,
        "median_price_per_m2_by_category": median_effect,
        "dedup": {
            "candidate_pairs": sensitivity["default"]["candidate_pairs"],
            "mutual_best_pairs": sensitivity["default"]["mutual_best_pairs"],
            "ambiguous_groups": sensitivity["default"]["ambiguous_groups"],
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
