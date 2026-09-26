import os
import re
import csv
import json
import argparse
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# ARGUMENTS
# ============================================================

parser = argparse.ArgumentParser(
    description=(
        "Compare two real-estate crawl snapshots "
        "stored in Bronze."
    )
)

parser.add_argument(
    "--old-batch",
    required=True,
    help="Batch cu, dinh dang YYYYMMDD",
)

parser.add_argument(
    "--new-batch",
    required=True,
    help="Batch moi, dinh dang YYYYMMDD",
)

args = parser.parse_args()

OLD_BATCH = args.old_batch
NEW_BATCH = args.new_batch


def validate_batch_id(value: str) -> None:
    if not re.fullmatch(r"\d{8}", value):
        raise ValueError(
            f"Batch '{value}' khong dung YYYYMMDD"
        )


validate_batch_id(OLD_BATCH)
validate_batch_id(NEW_BATCH)

if OLD_BATCH == NEW_BATCH:
    raise ValueError(
        "OLD_BATCH va NEW_BATCH khong duoc giong nhau"
    )


# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = "/opt/project"

BRONZE_ROOT = (
    "s3a://lakehouse-bronze/"
    "real_estate/snapshots"
)


SOURCES = {
    "batdongsan": {
        "id_candidates": [
            "listing_id",
        ],

        "page_candidates": [
            "_source_page",
            "page_number_attr",
        ],

        "title_candidates": [
            "title",
        ],

        "price_candidates": [
            "price_text",
            "price",
        ],

        "area_candidates": [
            "area_text",
            "area",
        ],

        "rooms_candidates": [
            "bedrooms_text",
            "bedrooms_aria",
            "rooms",
        ],

        "location_candidates": [
            "location_text",
        ],

        "category_candidates": [
            "category_id",
            "product_type",
        ],

        "lat_candidates": [
            "lat",
        ],

        "lon_candidates": [
            "lon",
            "lng",
        ],
    },

    "guland": {
        "id_candidates": [
            "listing_id",
            "source_id",
        ],

        "page_candidates": [
            "_source_page",
            "source_page",
        ],

        "title_candidates": [
            "title",
        ],

        "price_candidates": [
            "price_text",
            "price",
        ],

        "area_candidates": [
            "area_text",
            "area",
        ],

        "rooms_candidates": [
            "rooms",
            "bedrooms",
            "bedrooms_text",
        ],

        "location_candidates": [
            "location_rows",
            "location_text",
            "address",
        ],

        "category_candidates": [
            "category_name",
            "category_id",
        ],

        "lat_candidates": [
            "lat",
        ],

        "lon_candidates": [
            "lng",
            "lon",
        ],
    },

    "nhadatvui": {
        "id_candidates": [
            "listing_id",
            "_id",
        ],

        "page_candidates": [
            "_source_page",
        ],

        "title_candidates": [
            "title",
        ],

        "price_candidates": [
            "price",
            "price_text",
        ],

        "area_candidates": [
            "area",
            "area_text",
        ],

        "rooms_candidates": [
            "rooms",
        ],

        "location_candidates": [
            "address",
            "ward_name",
            "province_name",
        ],

        "category_candidates": [
            "property_subtype",
            "product_name",
            "product_slug",
        ],

        "lat_candidates": [
            "lat",
        ],

        "lon_candidates": [
            "lng",
            "lon",
        ],
    },
}


# ============================================================
# OUTPUT
# ============================================================

OUTPUT_DIR = Path(
    f"{PROJECT_ROOT}/outputs/validation/"
    "snapshot_comparison/"
    f"{OLD_BATCH}_vs_{NEW_BATCH}"
)

OUTPUT_DIR.mkdir(
    parents=True,
    exist_ok=True,
)

SUMMARY_CSV = (
    OUTPUT_DIR
    / "snapshot_comparison_summary.csv"
)

SUMMARY_JSON = (
    OUTPUT_DIR
    / "snapshot_comparison_summary.json"
)


# ============================================================
# MINIO
# ============================================================

MINIO_USER = os.getenv(
    "MINIO_ROOT_USER"
)

MINIO_PASSWORD = os.getenv(
    "MINIO_ROOT_PASSWORD"
)

if not MINIO_USER or not MINIO_PASSWORD:
    raise RuntimeError(
        "Khong tim thay MINIO_ROOT_USER "
        "hoac MINIO_ROOT_PASSWORD"
    )


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName(
        f"CompareSnapshots_"
        f"{OLD_BATCH}_vs_{NEW_BATCH}"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel(
    "WARN"
)


# ============================================================
# S3A / MINIO
# ============================================================

hadoop_conf = (
    spark.sparkContext
    ._jsc
    .hadoopConfiguration()
)

hadoop_conf.set(
    "fs.s3a.endpoint",
    "http://minio:9000"
)

hadoop_conf.set(
    "fs.s3a.access.key",
    MINIO_USER
)

hadoop_conf.set(
    "fs.s3a.secret.key",
    MINIO_PASSWORD
)

hadoop_conf.set(
    "fs.s3a.path.style.access",
    "true"
)

hadoop_conf.set(
    "fs.s3a.connection.ssl.enabled",
    "false"
)

hadoop_conf.set(
    "fs.s3a.impl",
    "org.apache.hadoop.fs.s3a.S3AFileSystem"
)

hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a."
    "SimpleAWSCredentialsProvider"
)


# ============================================================
# HELPERS
# ============================================================

def first_existing_column(
    df,
    candidates,
):
    for name in candidates:
        if name in df.columns:
            return name

    return None


def normalized_column(
    df,
    candidates,
):
    """
    Tim cot dau tien ton tai va normalize text.

    Neu khong co cot nao thi tra ve <missing>.
    """

    column_name = first_existing_column(
        df,
        candidates,
    )

    if column_name is None:
        return F.lit("<missing>")

    return (
        F.lower(
            F.trim(
                F.regexp_replace(
                    F.coalesce(
                        F.col(column_name)
                        .cast("string"),
                        F.lit("<null>"),
                    ),
                    r"\s+",
                    " ",
                )
            )
        )
    )


def pct(
    numerator,
    denominator,
):
    if denominator == 0:
        return 0.0

    return round(
        numerator / denominator * 100,
        2,
    )


def prepare_dataframe(
    df,
    config,
):
    """
    Tao cac cot canonical dung chung cho 3 source.
    """

    id_col = first_existing_column(
        df,
        config["id_candidates"],
    )

    if id_col is None:
        raise RuntimeError(
            "Khong tim thay listing ID. "
            f"Columns={df.columns}"
        )

    page_col = first_existing_column(
        df,
        config["page_candidates"],
    )

    prepared = (
        df
        .withColumn(
            "_listing_id",
            F.trim(
                F.col(id_col)
                .cast("string")
            ),
        )
        .withColumn(
            "_cmp_title",
            normalized_column(
                df,
                config["title_candidates"],
            ),
        )
        .withColumn(
            "_cmp_price",
            normalized_column(
                df,
                config["price_candidates"],
            ),
        )
        .withColumn(
            "_cmp_area",
            normalized_column(
                df,
                config["area_candidates"],
            ),
        )
        .withColumn(
            "_cmp_rooms",
            normalized_column(
                df,
                config["rooms_candidates"],
            ),
        )
        .withColumn(
            "_cmp_location",
            normalized_column(
                df,
                config["location_candidates"],
            ),
        )
        .withColumn(
            "_cmp_category",
            normalized_column(
                df,
                config["category_candidates"],
            ),
        )
        .withColumn(
            "_cmp_lat",
            normalized_column(
                df,
                config["lat_candidates"],
            ),
        )
        .withColumn(
            "_cmp_lon",
            normalized_column(
                df,
                config["lon_candidates"],
            ),
        )
    )

    prepared = prepared.filter(
        F.col("_listing_id").isNotNull()
        & (
            F.col("_listing_id") != ""
        )
    )

    # Hash chi tu business fields.
    # Khong dua scraped_at, batch_id, page vao hash.
    prepared = prepared.withColumn(
        "_record_hash",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("_cmp_title"),
                F.col("_cmp_price"),
                F.col("_cmp_area"),
                F.col("_cmp_rooms"),
                F.col("_cmp_location"),
                F.col("_cmp_category"),
                F.col("_cmp_lat"),
                F.col("_cmp_lon"),
            ),
            256,
        ),
    )

    return (
        prepared,
        page_col,
    )


def deduplicate_batch(
    df,
    page_col,
):
    """
    Mot listing_id bi lap trong cung batch
    chi giu mot observation.

    Uu tien page nho hon.
    Neu co scraped_at thi lay record moi hon
    trong cung page.
    """

    ordering = []

    if page_col is not None:
        ordering.append(
            F.col(page_col)
            .cast("int")
            .asc_nulls_last()
        )

    if "_scraped_at" in df.columns:
        ordering.append(
            F.col("_scraped_at")
            .desc_nulls_last()
        )

    if not ordering:
        ordering = [
            F.lit(1)
        ]

    window = (
        Window
        .partitionBy("_listing_id")
        .orderBy(*ordering)
    )

    return (
        df
        .withColumn(
            "_rn",
            F.row_number().over(window),
        )
        .filter(
            F.col("_rn") == 1
        )
        .drop("_rn")
    )


def get_max_page(
    df,
    page_col,
):
    if page_col is None:
        return None

    row = (
        df
        .select(
            F.max(
                F.col(page_col)
                .cast("int")
            ).alias("max_page")
        )
        .first()
    )

    return row["max_page"]


# ============================================================
# START
# ============================================================

print("=" * 82)
print("SNAPSHOT BATCH COMPARISON")
print(
    f"{OLD_BATCH}  VS  {NEW_BATCH}"
)
print("=" * 82)

results = []


# ============================================================
# EACH SOURCE
# ============================================================

for source_name, config in SOURCES.items():

    print()
    print("=" * 82)
    print(
        f"SOURCE: {source_name}"
    )
    print("=" * 82)

    old_path = (
        f"{BRONZE_ROOT}/"
        f"{source_name}/"
        f"batch_id={OLD_BATCH}"
    )

    new_path = (
        f"{BRONZE_ROOT}/"
        f"{source_name}/"
        f"batch_id={NEW_BATCH}"
    )

    print(
        f"OLD: {old_path}"
    )

    print(
        f"NEW: {new_path}"
    )

    old_raw = (
        spark.read
        .parquet(old_path)
    )

    new_raw = (
        spark.read
        .parquet(new_path)
    )

    raw_rows_old = old_raw.count()
    raw_rows_new = new_raw.count()

    # --------------------------------------------------------
    # PREPARE
    # --------------------------------------------------------

    (
        old_df,
        old_page_col,
    ) = prepare_dataframe(
        old_raw,
        config,
    )

    (
        new_df,
        new_page_col,
    ) = prepare_dataframe(
        new_raw,
        config,
    )

    valid_rows_old = old_df.count()
    valid_rows_new = new_df.count()

    old_max_page = get_max_page(
        old_df,
        old_page_col,
    )

    new_max_page = get_max_page(
        new_df,
        new_page_col,
    )

    compare_max_page = None

    if (
        old_max_page is not None
        and new_max_page is not None
    ):
        compare_max_page = min(
            old_max_page,
            new_max_page,
        )

    # --------------------------------------------------------
    # UNIQUE BEFORE DEDUP
    # --------------------------------------------------------

    unique_old = (
        old_df
        .select("_listing_id")
        .distinct()
        .count()
    )

    unique_new = (
        new_df
        .select("_listing_id")
        .distinct()
        .count()
    )

    duplicates_old = (
        valid_rows_old
        - unique_old
    )

    duplicates_new = (
        valid_rows_new
        - unique_new
    )

    # --------------------------------------------------------
    # DEDUP WITHIN EACH BATCH
    # --------------------------------------------------------

    old_clean = deduplicate_batch(
        old_df,
        old_page_col,
    )

    new_clean = deduplicate_batch(
        new_df,
        new_page_col,
    )

    old_ids = (
        old_clean
        .select("_listing_id")
    )

    new_ids = (
        new_clean
        .select("_listing_id")
    )

    # --------------------------------------------------------
    # FULL-BATCH ID COMPARISON
    #
    # Khong gioi han cung page range.
    # Vi vi tri page cua listing co the thay doi.
    # --------------------------------------------------------

    common_ids_df = (
        old_ids
        .join(
            new_ids,
            on="_listing_id",
            how="inner",
        )
    )

    common_ids = (
        common_ids_df.count()
    )

    # Listing quan sat o batch moi
    # ma KHONG ton tai o bat ky page nao
    # trong batch cu.
    new_observed_df = (
        new_ids
        .join(
            old_ids,
            on="_listing_id",
            how="left_anti",
        )
    )

    new_observed_ids = (
        new_observed_df.count()
    )

    # Co trong batch cu nhung khong thay
    # trong batch moi.
    #
    # KHONG duoc hieu la SOLD.
    not_observed_df = (
        old_ids
        .join(
            new_ids,
            on="_listing_id",
            how="left_anti",
        )
    )

    not_observed_again = (
        not_observed_df.count()
    )

    # --------------------------------------------------------
    # COMPARE CONTENT OF COMMON IDs
    # --------------------------------------------------------

    old_compare = (
        old_clean
        .select(
            "_listing_id",

            F.col("_record_hash")
            .alias("old_hash"),

            F.col("_cmp_price")
            .alias("old_price"),

            F.col("_cmp_area")
            .alias("old_area"),

            F.col("_cmp_rooms")
            .alias("old_rooms"),

            F.col("_cmp_title")
            .alias("old_title"),

            F.col("_cmp_location")
            .alias("old_location"),

            F.col("_cmp_category")
            .alias("old_category"),
        )
    )

    new_compare = (
        new_clean
        .select(
            "_listing_id",

            F.col("_record_hash")
            .alias("new_hash"),

            F.col("_cmp_price")
            .alias("new_price"),

            F.col("_cmp_area")
            .alias("new_area"),

            F.col("_cmp_rooms")
            .alias("new_rooms"),

            F.col("_cmp_title")
            .alias("new_title"),

            F.col("_cmp_location")
            .alias("new_location"),

            F.col("_cmp_category")
            .alias("new_category"),
        )
    )

    common = (
        old_compare
        .join(
            new_compare,
            on="_listing_id",
            how="inner",
        )
    )

    unchanged_df = (
        common
        .filter(
            F.col("old_hash")
            == F.col("new_hash")
        )
    )

    changed_df = (
        common
        .filter(
            F.col("old_hash")
            != F.col("new_hash")
        )
    )

    unchanged_ids = (
        unchanged_df.count()
    )

    changed_ids = (
        changed_df.count()
    )

    # --------------------------------------------------------
    # FIELD CHANGES
    # --------------------------------------------------------

    price_changed = (
        common
        .filter(
            ~F.col(
                "old_price"
            ).eqNullSafe(
                F.col("new_price")
            )
        )
        .count()
    )

    area_changed = (
        common
        .filter(
            ~F.col(
                "old_area"
            ).eqNullSafe(
                F.col("new_area")
            )
        )
        .count()
    )

    rooms_changed = (
        common
        .filter(
            ~F.col(
                "old_rooms"
            ).eqNullSafe(
                F.col("new_rooms")
            )
        )
        .count()
    )

    title_changed = (
        common
        .filter(
            ~F.col(
                "old_title"
            ).eqNullSafe(
                F.col("new_title")
            )
        )
        .count()
    )

    location_changed = (
        common
        .filter(
            ~F.col(
                "old_location"
            ).eqNullSafe(
                F.col("new_location")
            )
        )
        .count()
    )

    category_changed = (
        common
        .filter(
            ~F.col(
                "old_category"
            ).eqNullSafe(
                F.col("new_category")
            )
        )
        .count()
    )

    # --------------------------------------------------------
    # SAME-PAGE-SCOPE STATISTICS
    #
    # Chi de tham khao do on dinh cua top N pages.
    # KHONG dung de ket luan tin moi.
    # --------------------------------------------------------

    scope_unique_old = None
    scope_unique_new = None
    scope_common_ids = None
    scope_overlap_pct = None

    if (
        compare_max_page is not None
        and old_page_col is not None
        and new_page_col is not None
    ):

        old_scope_ids = (
            old_clean
            .filter(
                F.col(old_page_col)
                .cast("int")
                <= compare_max_page
            )
            .select("_listing_id")
            .distinct()
        )

        new_scope_ids = (
            new_clean
            .filter(
                F.col(new_page_col)
                .cast("int")
                <= compare_max_page
            )
            .select("_listing_id")
            .distinct()
        )

        scope_unique_old = (
            old_scope_ids.count()
        )

        scope_unique_new = (
            new_scope_ids.count()
        )

        scope_common_ids = (
            old_scope_ids
            .join(
                new_scope_ids,
                on="_listing_id",
                how="inner",
            )
            .count()
        )

        scope_overlap_pct = pct(
            scope_common_ids,
            scope_unique_new,
        )

    # --------------------------------------------------------
    # SAVE CHANGED SAMPLE
    # --------------------------------------------------------

    changed_sample_path = (
        OUTPUT_DIR
        / (
            f"{source_name}_"
            "changed_sample.csv"
        )
    )

    changed_sample = (
        changed_df
        .select(
            F.col("_listing_id")
            .alias("listing_id"),

            "old_price",
            "new_price",

            "old_area",
            "new_area",

            "old_rooms",
            "new_rooms",

            "old_title",
            "new_title",

            "old_location",
            "new_location",
        )
        .limit(100)
        .collect()
    )

    with open(
        changed_sample_path,
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as f:

        writer = csv.writer(f)

        writer.writerow(
            [
                "listing_id",
                "old_price",
                "new_price",
                "old_area",
                "new_area",
                "old_rooms",
                "new_rooms",
                "old_title",
                "new_title",
                "old_location",
                "new_location",
            ]
        )

        for row in changed_sample:
            writer.writerow(
                list(row)
            )

    # --------------------------------------------------------
    # SAVE NEW IDs SAMPLE
    # --------------------------------------------------------

    new_sample_path = (
        OUTPUT_DIR
        / (
            f"{source_name}_"
            "new_observed_sample.csv"
        )
    )

    new_sample = (
        new_observed_df
        .limit(100)
        .collect()
    )

    with open(
        new_sample_path,
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as f:

        writer = csv.writer(f)

        writer.writerow(
            ["listing_id"]
        )

        for row in new_sample:
            writer.writerow(
                [row["_listing_id"]]
            )

    # --------------------------------------------------------
    # RESULT
    # --------------------------------------------------------

    result = {
        "source":
            source_name,

        "old_batch":
            OLD_BATCH,

        "new_batch":
            NEW_BATCH,

        "raw_rows_old":
            raw_rows_old,

        "raw_rows_new":
            raw_rows_new,

        "valid_rows_old":
            valid_rows_old,

        "valid_rows_new":
            valid_rows_new,

        "unique_old":
            unique_old,

        "unique_new":
            unique_new,

        "duplicates_old":
            duplicates_old,

        "duplicates_new":
            duplicates_new,

        "common_ids":
            common_ids,

        "new_observed_ids":
            new_observed_ids,

        "not_observed_again":
            not_observed_again,

        "unchanged_ids":
            unchanged_ids,

        "changed_ids":
            changed_ids,

        "price_changed":
            price_changed,

        "area_changed":
            area_changed,

        "rooms_changed":
            rooms_changed,

        "title_changed":
            title_changed,

        "location_changed":
            location_changed,

        "category_changed":
            category_changed,

        "old_overlap_pct":
            pct(
                common_ids,
                unique_old,
            ),

        "new_overlap_pct":
            pct(
                common_ids,
                unique_new,
            ),

        "new_observed_pct":
            pct(
                new_observed_ids,
                unique_new,
            ),

        "changed_pct_of_common":
            pct(
                changed_ids,
                common_ids,
            ),

        "old_max_page":
            old_max_page,

        "new_max_page":
            new_max_page,

        "compare_max_page":
            compare_max_page,

        "scope_unique_old":
            scope_unique_old,

        "scope_unique_new":
            scope_unique_new,

        "scope_common_ids":
            scope_common_ids,

        "scope_overlap_pct":
            scope_overlap_pct,
    }

    results.append(
        result
    )

    # --------------------------------------------------------
    # PRINT RESULT
    # --------------------------------------------------------

    print()
    print("-" * 82)
    print(
        f"RESULT - {source_name}"
    )
    print("-" * 82)

    for key, value in result.items():
        print(
            f"{key:26}: {value}"
        )


# ============================================================
# SAVE SUMMARY CSV
# ============================================================

if results:

    fieldnames = list(
        results[0].keys()
    )

    with open(
        SUMMARY_CSV,
        "w",
        encoding="utf-8-sig",
        newline="",
    ) as f:

        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
        )

        writer.writeheader()
        writer.writerows(results)


# ============================================================
# SAVE SUMMARY JSON
# ============================================================

json_output = {
    "old_batch":
        OLD_BATCH,

    "new_batch":
        NEW_BATCH,

    "generated_at":
        datetime.now(
            timezone.utc
        ).isoformat(),

    "notes": {
        "new_observed_ids": (
            "Co trong batch moi nhung khong "
            "quan sat thay o bat ky page nao "
            "trong batch cu."
        ),

        "not_observed_again": (
            "Co trong batch cu nhung khong "
            "quan sat thay trong batch moi. "
            "KHONG dong nghia da ban."
        ),

        "scope_metrics": (
            "Chi dung de danh gia overlap "
            "trong cung pham vi page. "
            "Khong dung de xac dinh tin moi."
        ),
    },

    "results":
        results,
}

with open(
    SUMMARY_JSON,
    "w",
    encoding="utf-8",
) as f:

    json.dump(
        json_output,
        f,
        ensure_ascii=False,
        indent=2,
    )


# ============================================================
# FINAL
# ============================================================

print()
print("=" * 82)
print("SNAPSHOT COMPARISON SUCCESS")
print("=" * 82)

print(
    f"OLD BATCH: {OLD_BATCH}"
)

print(
    f"NEW BATCH: {NEW_BATCH}"
)

print(
    f"OUTPUT   : {OUTPUT_DIR}"
)

print(
    f"CSV      : {SUMMARY_CSV}"
)

print(
    f"JSON     : {SUMMARY_JSON}"
)

print("=" * 82)

spark.stop()