"""Draw the manual-labelling sample used to measure Silver feature accuracy.

Stratified by source (SAMPLE_PER_SOURCE current sale listings each), ordered
by a salted SHA-256 of ``source_id`` so the same Silver data always yields the
same sample. Writes two files:

- ``docs/validation/labeling/labeling_sheet.csv``: what the labeller sees
  (title, price, area, address, URL) plus empty label columns. System
  predictions are deliberately not shown, so they cannot bias the labeller.
- ``docs/validation/labeling/labeling_predictions.csv``: the system's
  model_category and title flags for the same rows, joined only at evaluation
  (``src/quality/evaluate_labels.py``).

Run: .\\scripts\\run_spark.ps1 "src\\quality\\build_labeling_sample.py"
"""

from __future__ import annotations

import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

OUT_DIR = PROJECT_ROOT / "docs/validation/labeling"
SAMPLE_PER_SOURCE = 22
SALT = "labeling-v1"
FLAGS = ("legal", "furnished", "frontage", "elevator", "car_access")
SHEET_COLUMNS = [
    "sample_id", "source", "title", "price", "area", "rooms", "address", "url",
    "true_category", *[f"true_{flag}" for flag in FLAGS], "labeler", "labeled_at", "note",
]
PREDICTION_COLUMNS = [
    "sample_id", "source_id", "source", "category_name", "category_evidence", "model_category",
    "model_category_method", "category_title_conflict", *[f"title_has_{flag}" for flag in FLAGS],
]


def main() -> None:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    from src.common.spark_session import build_spark_session, ensure_silver_namespace

    spark = build_spark_session("BuildLabelingSample")
    silver = ensure_silver_namespace(spark)
    current = spark.table(f"{silver}.silver_listings_current_27").select(
        "source_id", "title", "price", "area", "rooms", "address", "ad_url", "source_url", "is_rent",
    )
    feature = spark.table(f"{silver}.listing_feature").select(
        "source_id", "source", "category_name", "category_evidence", "model_category", "model_category_method",
        "category_title_conflict", *[f"title_has_{flag}" for flag in FLAGS],
    )
    order = Window.partitionBy("source").orderBy(F.sha2(F.concat(F.lit(SALT), F.col("source_id")), 256))
    sample = (
        current.filter("is_rent = false AND title IS NOT NULL").join(feature, "source_id")
        .withColumn("_rank", F.row_number().over(order))
        .filter(F.col("_rank") <= SAMPLE_PER_SOURCE)
        .orderBy("source", "_rank")
        .collect()
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    sheet_path, prediction_path = OUT_DIR / "labeling_sheet.csv", OUT_DIR / "labeling_predictions.csv"
    if sheet_path.exists() and any(row.get("true_category") for row in csv.DictReader(sheet_path.open(encoding="utf-8-sig"))):
        raise SystemExit(f"{sheet_path} already has labels; refusing to overwrite them")
    with sheet_path.open("w", encoding="utf-8-sig", newline="") as sheet, prediction_path.open("w", encoding="utf-8-sig", newline="") as pred:
        sheet_writer, pred_writer = csv.DictWriter(sheet, SHEET_COLUMNS), csv.DictWriter(pred, PREDICTION_COLUMNS)
        sheet_writer.writeheader()
        pred_writer.writeheader()
        for index, row in enumerate(sample, start=1):
            sample_id = f"L{index:03d}"
            sheet_writer.writerow({
                "sample_id": sample_id, "source": row["source"], "title": row["title"],
                "price": row["price"], "area": row["area"], "rooms": row["rooms"], "address": row["address"],
                "url": row["ad_url"] or row["source_url"],
            })
            pred_writer.writerow({"sample_id": sample_id, **{c: row[c] for c in PREDICTION_COLUMNS if c != "sample_id"}})
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sample_size": len(sample),
        "per_source": SAMPLE_PER_SOURCE,
        "salt": SALT,
        "population": "silver_listings_current_27 sale listings (is_rent = false) with a title",
    }
    (OUT_DIR / "sample_manifest.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
