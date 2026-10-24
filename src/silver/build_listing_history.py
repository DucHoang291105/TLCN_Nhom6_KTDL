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
from src.silver.history import sync_history_table
from src.common.atomic_io import write_json_atomic


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("incremental", "rebuild"), default="incremental")
    parser.add_argument("--force-verify", action="store_true", help="Bypass metadata no-op cache and revalidate full input")
    args = parser.parse_args()
    spark = build_spark_session("BuildListingHistory")
    try:
        ns = ensure_silver_namespace(spark)
        summary = sync_history_table(spark, f"{ns}.listing_observation", f"{ns}.listing_history",
                                    f"{ns}.listing_history_state", args.mode, args.force_verify)
        summary.update(generated_at=datetime.now(timezone.utc).isoformat(),
                       input_table=f"{ns}.listing_observation")
        for folder in ("outputs/validation", "docs/validation"):
            out = PROJECT_ROOT / folder / "silver_history_summary.json"
            write_json_atomic(out, summary)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
