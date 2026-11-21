"""BQ2 – does a reposted listing leak from the train split into the test split?

The price model is fitted on a train split and judged on a test split, both
chosen by a hash of ``source_id``. Duplicates are only removed *between*
sources, so the same property reposted inside one source (a new ``source_id``)
can land in train while its repost lands in test. The model would then be
scored on a listing it has effectively seen, and the test error would look
better than it is.

This check reuses the published duplicate rule (same location, same category,
area and price within a tolerance, similar title tokens) but, unlike the rule
in ``fact_listing``, applies it between a TEST listing and a TRAIN listing
whatever their source. It then recomputes the test error with the test
listings that have a near-duplicate in train removed. If the error barely
moves, repost leakage does not explain the result; if it rises clearly, the
original figures are optimistic by that much.

Nothing is written to Gold. The result is
``docs/validation/repost_leakage_check.json``. The check is run under the
three duplicate sensitivity settings (strict, default, loose).

Run: .\\scripts\\run_spark.ps1 src\\quality\\repost_leakage_check.py
"""

from __future__ import annotations

import math
import sys
from collections import defaultdict
from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.gold.gold_common import write_summary
from src.gold.gold_rules import DUP_SENSITIVITY, UNKNOWN_KEY, jaccard, title_tokens, within
from src.gold.price_model_rules import regression_metrics

WITHIN_BAND = 0.25


def is_near_duplicate(left: Mapping[str, Any], right: Mapping[str, Any], area_tol: float, price_tol: float, min_jaccard: float) -> bool:
    """The published duplicate rule without the "different source" condition."""

    for key in ("location_key", "property_category_key"):
        if left[key] == UNKNOWN_KEY or left[key] != right[key]:
            return False
    if not (left["area"] and right["area"] and left["price"] and right["price"]):
        return False
    return (
        within(left["area"], right["area"], area_tol)
        and within(left["price"], right["price"], price_tol)
        and jaccard(left["tokens"], right["tokens"]) >= min_jaccard
    )


def usable(row: Mapping[str, Any]) -> bool:
    return (
        row["location_key"] not in (None, UNKNOWN_KEY) and row["property_category_key"] not in (None, UNKNOWN_KEY)
        and bool(row["area"]) and row["area"] > 0 and bool(row["price"]) and row["price"] > 0 and bool(row["tokens"])
    )


def find_leaks(rows: Iterable[Mapping[str, Any]], area_tol: float, price_tol: float, min_jaccard: float) -> dict[str, bool]:
    """Test listings with a near-duplicate in train.

    Returns ``{test source_id: True if at least one such train listing comes from the
    same source}``. Candidates are blocked by location, category and a log-area bin
    (neighbouring bins included), so the area tolerance is never missed at a bin edge.
    """

    step = math.log1p(area_tol)
    index: dict[tuple[Any, Any, int], list[Mapping[str, Any]]] = defaultdict(list)
    test: list[Mapping[str, Any]] = []
    for row in rows:
        if not usable(row):
            continue
        if row["split"] == "train":
            index[(row["location_key"], row["property_category_key"], math.floor(math.log(row["area"]) / step))].append(row)
        elif row["split"] == "test":
            test.append(row)
    leaks: dict[str, bool] = {}
    for row in test:
        bucket = math.floor(math.log(row["area"]) / step)
        for neighbour in (bucket - 1, bucket, bucket + 1):
            for other in index.get((row["location_key"], row["property_category_key"], neighbour), ()):
                if is_near_duplicate(row, other, area_tol, price_tol, min_jaccard):
                    leaks[row["source_id"]] = leaks.get(row["source_id"], False) or row["source_key"] == other["source_key"]
    return leaks


def error_summary(pairs: list[tuple[float, float]]) -> dict[str, Any]:
    """Standard error metrics plus the share of listings within 25% of the prediction."""

    valid = [(a, p) for a, p in pairs if a and p and a > 0 and p > 0]
    out = regression_metrics(valid)
    out["share_within_25pct"] = round(sum(1 for a, p in valid if abs(a / p - 1) <= WITHIN_BAND) / len(valid), 4) if valid else None
    return out


def leakage_effect(test_rows: list[Mapping[str, Any]], leaks: Mapping[str, bool]) -> dict[str, Any]:
    """Test error on all rows, on rows without a near-duplicate in train, and on the leaked rows."""

    def pairs(selected: Iterable[Mapping[str, Any]]) -> list[tuple[float, float]]:
        return [(row["price_per_m2"], row["predicted_price_per_m2"]) for row in selected]

    clean = [row for row in test_rows if row["source_id"] not in leaks]
    leaked = [row for row in test_rows if row["source_id"] in leaks]
    all_metrics, clean_metrics = error_summary(pairs(test_rows)), error_summary(pairs(clean))
    change = {
        key: round(clean_metrics[key] - all_metrics[key], 6)
        for key in ("median_ape", "mape", "share_within_25pct")
        if clean_metrics.get(key) is not None and all_metrics.get(key) is not None
    }
    return {
        "test_listings": len(test_rows),
        "with_near_duplicate_in_train": len(leaked),
        "share_with_near_duplicate_in_train": round(len(leaked) / len(test_rows), 6) if test_rows else None,
        "same_source_near_duplicate": sum(1 for row in leaked if leaks[row["source_id"]]),
        "all_test": all_metrics,
        "without_near_duplicates": clean_metrics,
        "only_near_duplicates": error_summary(pairs(leaked)),
        "change_when_removed": change,
    }


def main() -> None:
    from pyspark.sql import functions as F

    from src.common.spark_session import build_spark_session, ensure_gold_namespace

    spark = build_spark_session("GoldRepostLeakageCheck")
    gold = ensure_gold_namespace(spark)
    started_at = datetime.now(timezone.utc)
    model = spark.table(f"{gold}.fact_listing_price_model").filter("NOT is_outlier_trimmed").select(
        "source_id", "split", "location_key", "property_category_key", "price_per_m2", "predicted_price_per_m2")
    listings = spark.table(f"{gold}.fact_listing").select("source_id", "source_key", "area", "price", "title")
    rows = []
    for record in model.join(listings, "source_id").collect():
        row = record.asDict()
        row["tokens"] = title_tokens(row.pop("title"))
        rows.append(row)
    test_rows = [row for row in rows if row["split"] == "test"]

    settings = {}
    for name, (area_tol, price_tol, min_jaccard) in DUP_SENSITIVITY.items():
        leaks = find_leaks(rows, area_tol, price_tol, min_jaccard)
        settings[name] = {
            "area_tolerance": area_tol, "price_tolerance": price_tol, "min_title_jaccard": min_jaccard,
            **leakage_effect(test_rows, leaks),
        }
    summary = {
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "scope": "published train/test split of fact_listing_price_model, rows outside the train trim range removed",
        "train_listings": sum(1 for row in rows if row["split"] == "train"),
        "test_listings": len(test_rows),
        "settings": settings,
        "limitations": [
            "Near-duplicates are found with the same title-token rule used for cross-source duplicates; reposts with a changed price, area or title are not found.",
            "The check measures how much the test error changes when listings with a near-duplicate in train are removed. It does not retrain the model without them.",
        ],
        "status": "PASS",
    }
    write_summary("repost_leakage_check.json", summary)
    spark.stop()


if __name__ == "__main__":
    main()
