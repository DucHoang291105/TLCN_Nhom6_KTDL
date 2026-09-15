from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(r"D:\Code\TLCN_BDS_Lakehouse")

DATA_DIR = PROJECT_ROOT / "data" / "incoming" / "historical"
OUTPUT_DIR = PROJECT_ROOT / "outputs" / "profiling"
DOCS_DIR = PROJECT_ROOT / "docs" / "data"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DOCS_DIR.mkdir(parents=True, exist_ok=True)

KEY_COLUMNS = [
    "source",
    "source_id",
    "ad_id",
    "title",
    "price",
    "price_str",
    "area",
    "rooms",
    "address",
    "ward",
    "district_id",
    "district_name",
    "category_id",
    "category_name",
    "lat",
    "lon",
    "ad_url",
    "posted_at",
    "scraped_at",
    "price_m",
    "price_per_m2",
    "has_coord",
    "is_rent",
]

DATE_COLUMNS = [
    "posted_at",
    "scraped_at",
    "published_at",
    "snapshot_at",
]


def read_csv_safe(path):
    encodings = [
        "utf-8-sig",
        "utf-8",
        "cp1258",
        "latin1",
    ]

    for encoding in encodings:
        try:
            return pd.read_csv(
                path,
                encoding=encoding,
                low_memory=False
            ), encoding
        except UnicodeDecodeError:
            continue

    raise RuntimeError(f"Không đọc được file: {path}")


inventory = []
column_profiles = []
schemas = {}

csv_files = sorted(DATA_DIR.rglob("*.csv"))

print("=" * 80)
print("DATA SOURCE PROFILING")
print("=" * 80)
print(f"Tìm thấy {len(csv_files)} file CSV\n")

for file_path in csv_files:

    source = file_path.parent.name

    print("-" * 80)
    print(f"SOURCE : {source}")
    print(f"FILE   : {file_path.name}")

    df, encoding = read_csv_safe(file_path)

    rows = len(df)
    cols = len(df.columns)

    schemas[source] = list(df.columns)

    print(f"ROWS   : {rows:,}")
    print(f"COLS   : {cols}")
    print(f"ENCODING: {encoding}")

    print("\nCOLUMNS:")
    for col in df.columns:
        print(f"  - {col}")

    duplicate_rows = int(df.duplicated().sum())

    source_id_duplicate = None

    if "source_id" in df.columns:
        source_id_duplicate = int(
            df["source_id"]
            .dropna()
            .duplicated()
            .sum()
        )

    date_min = None
    date_max = None
    date_column_used = None

    for date_col in DATE_COLUMNS:

        if date_col in df.columns:

            parsed = pd.to_datetime(
                df[date_col],
                errors="coerce"
            )

            if parsed.notna().any():

                date_column_used = date_col
                date_min = parsed.min()
                date_max = parsed.max()

                break

    print(f"\nDUPLICATE ROWS: {duplicate_rows:,}")

    if source_id_duplicate is not None:
        print(
            f"DUPLICATE source_id: "
            f"{source_id_duplicate:,}"
        )

    if date_column_used:
        print(
            f"DATE RANGE ({date_column_used}): "
            f"{date_min} -> {date_max}"
        )

    print("\nKEY COLUMNS:")

    for col in KEY_COLUMNS:

        status = "YES" if col in df.columns else "NO"

        print(
            f"  {col:<20} : {status}"
        )

    inventory.append({
        "source": source,
        "file": file_path.name,
        "rows": rows,
        "columns": cols,
        "encoding": encoding,
        "duplicate_rows": duplicate_rows,
        "duplicate_source_id": source_id_duplicate,
        "date_column": date_column_used,
        "date_min": date_min,
        "date_max": date_max,
    })

    for col in df.columns:

        null_count = int(df[col].isna().sum())

        null_pct = (
            null_count / rows * 100
            if rows > 0
            else 0
        )

        unique_count = int(
            df[col]
            .nunique(dropna=True)
        )

        column_profiles.append({
            "source": source,
            "column": col,
            "dtype": str(df[col].dtype),
            "rows": rows,
            "null_count": null_count,
            "null_pct": round(null_pct, 2),
            "unique_count": unique_count,
        })

    print()


# ==================================================
# Schema comparison
# ==================================================

all_columns = sorted(
    set(
        col
        for schema in schemas.values()
        for col in schema
    )
)

schema_rows = []

for column in all_columns:

    row = {
        "column": column
    }

    for source in schemas:

        row[source] = (
            "YES"
            if column in schemas[source]
            else "NO"
        )

    schema_rows.append(row)

schema_df = pd.DataFrame(schema_rows)


# ==================================================
# Export CSV
# ==================================================

inventory_df = pd.DataFrame(inventory)

inventory_csv = (
    OUTPUT_DIR /
    "source_inventory.csv"
)

columns_csv = (
    OUTPUT_DIR /
    "column_profile.csv"
)

schema_csv = (
    OUTPUT_DIR /
    "schema_comparison.csv"
)

inventory_df.to_csv(
    inventory_csv,
    index=False,
    encoding="utf-8-sig"
)

pd.DataFrame(
    column_profiles
).to_csv(
    columns_csv,
    index=False,
    encoding="utf-8-sig"
)

schema_df.to_csv(
    schema_csv,
    index=False,
    encoding="utf-8-sig"
)


# ==================================================
# Create Markdown inventory
# ==================================================

md_path = (
    DOCS_DIR /
    "data_source_inventory.md"
)

with open(
    md_path,
    "w",
    encoding="utf-8"
) as f:

    f.write(
        "# Data Source Inventory\n\n"
    )

    f.write(
        "## Tổng quan nguồn dữ liệu\n\n"
    )

    f.write(
        "| Source | File | Rows | Columns | "
        "Duplicate rows | Date range |\n"
    )

    f.write(
        "|---|---|---:|---:|---:|---|\n"
    )

    for item in inventory:

        date_range = ""

        if item["date_min"] is not None:

            date_range = (
                f"{item['date_min']} "
                f"→ {item['date_max']}"
            )

        f.write(
            f"| {item['source']} "
            f"| {item['file']} "
            f"| {item['rows']:,} "
            f"| {item['columns']} "
            f"| {item['duplicate_rows']:,} "
            f"| {date_range} |\n"
        )

    f.write(
        "\n## Ghi chú\n\n"
    )

    f.write(
        "- Đây là dữ liệu historical đầu vào "
        "cho Data Lakehouse.\n"
    )

    f.write(
        "- Kết quả profiling chi tiết nằm tại "
        "`outputs/profiling/`.\n"
    )

print("=" * 80)
print("HOÀN THÀNH")
print("=" * 80)

print(
    f"Inventory : {inventory_csv}"
)

print(
    f"Columns   : {columns_csv}"
)

print(
    f"Schema    : {schema_csv}"
)

print(
    f"Markdown  : {md_path}"
)