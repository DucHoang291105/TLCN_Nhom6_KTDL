"""Build or incrementally reconcile SCD2 listing history in Iceberg."""
from __future__ import annotations
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from src.common.spark_session import build_spark_session, ensure_silver_namespace
from src.silver.history import sync_history


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("incremental", "rebuild"), default="incremental")
    args = parser.parse_args()
    spark = build_spark_session("BuildListingHistory")
    try:
        ns = ensure_silver_namespace(spark)
        observations = spark.table(f"{ns}.listing_observation")
        summary = sync_history(spark, observations, f"{ns}.listing_history",
                               f"{ns}.listing_history_state", args.mode)
        summary.update(generated_at=datetime.now(timezone.utc).isoformat(),
                       input_table=f"{ns}.listing_observation", observation_rows=observations.count())
        for folder in ("outputs/validation", "docs/validation"):
            out = PROJECT_ROOT / folder / "silver_history_summary.json"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
