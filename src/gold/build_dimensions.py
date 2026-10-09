"""Build the 9 Gold dimensions from Silver and versioned configuration.

Every dimension has an INT surrogate key and exactly one ``-1`` "Không rõ" row
so fact foreign keys are never NULL. Keys are assigned deterministically from
sorted natural keys, so rebuilding the same input yields the same keys.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.bronze.audit_bronze_state import read_sources_config
from src.gold.gold_rules import (
    DQ_STATUS_KEYS, MODEL_CATEGORY_LABELS, UNKNOWN_KEY, UNKNOWN_LABEL, load_bands,
)

BAND_TABLES = {
    "price": "dim_price_band",
    "area": "dim_area_band",
    "unit_price": "dim_unit_price_band",
    "room": "dim_room_band",
}


def nullable(schema):
    from pyspark.sql import types as T

    return T.StructType([T.StructField(field.name, field.dataType, True) for field in schema])


def write_table(frame, table: str) -> int:
    from src.common.spark_session import write_iceberg_table

    write_iceberg_table(frame, table)
    return frame.sparkSession.table(table).count()


def main() -> None:
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    from src.common.spark_session import build_spark_session, ensure_gold_namespace, ensure_silver_namespace

    spark = build_spark_session("GoldDimensions")
    silver = ensure_silver_namespace(spark)
    gold = ensure_gold_namespace(spark)
    started_at = datetime.now(timezone.utc)
    counts: dict[str, int] = {}

    # dim_source
    sources = read_sources_config(PROJECT_ROOT / "config/sources.yaml")
    source_rows = [(UNKNOWN_KEY, UNKNOWN_LABEL, None)] + [
        (key, source, "crawl" if branch == "snapshots" else "historical")
        for key, (branch, source) in enumerate(
            sorted((branch, source) for branch, names in sources.items() for source in names), start=1
        )
    ]
    counts["dim_source"] = write_table(
        spark.createDataFrame(source_rows, "source_key INT, source STRING, source_type STRING"),
        f"{gold}.dim_source",
    )

    # dim_property_category
    category_rows = [(UNKNOWN_KEY, "khong_ro", UNKNOWN_LABEL)] + [
        (key, code, label) for code, (key, label) in MODEL_CATEGORY_LABELS.items()
    ]
    counts["dim_property_category"] = write_table(
        spark.createDataFrame(category_rows, "property_category_key INT, model_category STRING, category_label STRING"),
        f"{gold}.dim_property_category",
    )

    # dim_dq_status (REJECT never reaches Gold)
    dq_rows = [(UNKNOWN_KEY, UNKNOWN_LABEL)] + [(key, status) for status, key in DQ_STATUS_KEYS.items()]
    counts["dim_dq_status"] = write_table(
        spark.createDataFrame(dq_rows, "dq_status_key INT, dq_status STRING"), f"{gold}.dim_dq_status"
    )

    # Band dimensions from config/gold_bands.csv
    band_schema = T.StructType([
        T.StructField("band_key", T.IntegerType(), False),
        T.StructField("band_code", T.StringType(), False),
        T.StructField("band_label", T.StringType(), False),
        T.StructField("lower_bound", T.DoubleType(), True),
        T.StructField("upper_bound", T.DoubleType(), True),
    ])
    for dimension, bands in load_bands().items():
        rows = [(UNKNOWN_KEY, "unknown", UNKNOWN_LABEL, None, None)] + [
            (band.band_key, band.band_code, band.band_label, band.lower, band.upper) for band in bands
        ]
        table = BAND_TABLES[dimension]
        counts[table] = write_table(spark.createDataFrame(rows, band_schema), f"{gold}.{table}")

    # dim_location: grain province x district from listing_location
    location = spark.table(f"{silver}.listing_location").filter(F.col("province_name_model").isNotNull())
    centers = location.groupBy("province_name_model").agg(
        F.first("center_name", ignorenulls=True).alias("center_name"),
        F.first("center_lat", ignorenulls=True).alias("center_lat"),
        F.first("center_lon", ignorenulls=True).alias("center_lon"),
    )
    places = location.select("province_name_model", "district_name_model").distinct().join(centers, "province_name_model")
    order = Window.orderBy(F.col("province_name_model"), F.col("district_name_model").asc_nulls_first())
    places = places.select(
        F.row_number().over(order).cast("int").alias("location_key"),
        F.col("province_name_model").alias("province_name"),
        F.col("district_name_model").alias("district_name"),
        F.when(F.col("district_name_model").isNull(), "PROVINCE").otherwise("DISTRICT").alias("location_level"),
        "center_name", "center_lat", "center_lon",
    )
    unknown_location = spark.createDataFrame(
        [(UNKNOWN_KEY, UNKNOWN_LABEL, None, "UNKNOWN", None, None, None)], nullable(places.schema)
    )
    counts["dim_location"] = write_table(unknown_location.unionByName(places), f"{gold}.dim_location")

    # dim_date: every day between the earliest and latest posted/observed date
    current = spark.table(f"{silver}.silver_listings_current_27")
    bounds = current.select(
        F.least(F.min(F.to_date("scraped_at")), F.min(F.to_date("posted_at"))).alias("start"),
        F.greatest(F.max(F.to_date("scraped_at")), F.max(F.to_date("posted_at"))).alias("end"),
    )
    days = bounds.select(F.explode(F.sequence("start", "end")).alias("full_date"))
    dates = days.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.dayofmonth("full_date").alias("day"),
        F.weekofyear("full_date").alias("week_of_year"),
        F.date_format("full_date", "E").alias("day_name"),
    )
    unknown_date = spark.createDataFrame([(UNKNOWN_KEY, None, None, None, None, None, None, UNKNOWN_LABEL)], nullable(dates.schema))
    counts["dim_date"] = write_table(unknown_date.unionByName(dates), f"{gold}.dim_date")

    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "namespace": gold,
        "table_counts": counts,
        "status": "PASS",
    }
    for output_dir in (PROJECT_ROOT / "outputs/validation", PROJECT_ROOT / "docs/validation"):
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "gold_dimensions_summary.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
