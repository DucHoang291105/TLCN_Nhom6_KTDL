"""Small helpers shared by the Gold Business Question jobs."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FLAG_COLUMNS = [
    "title_has_legal", "title_has_furnished", "title_has_frontage",
    "title_has_elevator", "title_has_car_access",
]


def write_summary(file_name: str, summary: dict[str, Any]) -> None:
    for output_dir in (PROJECT_ROOT / "outputs/validation", PROJECT_ROOT / "docs/validation"):
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / file_name).write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def representatives(spark, gold: str):
    """Sale listings counted once across sources (cross-source duplicates removed)."""

    return spark.table(f"{gold}.fact_listing").filter("is_dup_representative")


def quartiles(column: str):
    """Exact P25/P50/P75 (deterministic, unlike approximate percentiles)."""

    from pyspark.sql import functions as F

    return F.expr(f"percentile({column}, array(0.25, 0.5, 0.75))")


def share(column: str):
    from pyspark.sql import functions as F

    return F.round(F.avg(F.col(column).cast("double")), 6)
