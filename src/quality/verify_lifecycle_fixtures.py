"""Spark integration cases: disappearance, reappearance, NULL price gaps, per-source calendars."""
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main():
    from src.common.spark_session import build_spark_session
    from src.gold.build_listing_lifecycle import build_lifecycle_frame
    from src.gold.gold_common import write_summary
    spark = build_spark_session("LifecycleFixtureTests")
    rows = []
    def add(source, sid, day, price):
        rows.append((source, sid, date(2026, 10, day), datetime(2026, 10, day), f"202610{day:02d}", str(day), price))
    for day, price in [(2, 100.0), (6, 90.0), (13, 90.0), (16, 80.0)]:
        add("batdongsan", "active", day, price)
    add("batdongsan", "missing_one", 13, 100.0)
    add("batdongsan", "missing_two", 6, 100.0)
    add("batdongsan", "returned", 2, 100.0)
    add("batdongsan", "returned", 16, 100.0)
    for day, price in [(2, 100.0), (6, None), (13, 120.0), (16, 0.0)]:
        add("batdongsan", "null_price_gap", day, price)
    add("guland", "single_batch", 2, 100.0)
    add("nhadatvui", "different_calendar", 6, 100.0)
    add("mogi", "historical", 2, 100.0)
    obs = spark.createDataFrame(rows, "source string, source_id string, snapshot_date date, scraped_at timestamp, batch_id string, record_hash string, price double")
    # Nhadatvui latest batch has zero accepted rows: must still count as missed.
    calendar_rows = [("batdongsan", f"202610{d:02d}") for d in (2, 6, 13, 16)] + [("guland", "20261002"), ("nhadatvui", "20261002"), ("nhadatvui", "20261006"), ("nhadatvui", "20261009")]
    calendar = spark.createDataFrame(calendar_rows, "source string, batch_id string")
    sources = spark.createDataFrame([(s, i) for i, s in enumerate(("batdongsan", "guland", "nhadatvui", "mogi"))], "source string, source_key int")
    result = {r["source_id"]: r.asDict() for r in build_lifecycle_frame(obs, calendar, sources).collect()}
    checks = {
        "one_row_per_listing": len(result) == 8,
        "first_last_and_span": result["active"]["listed_days"] == 14 and result["active"]["observation_count"] == 4,
        "price_changes": result["active"]["price_change_count"] == 2,
        "latest_observed": result["active"]["lifecycle_status"] == "OBSERVED_LATEST",
        "one_missing_not_disappeared": result["missing_one"]["is_disappeared"] is False and result["missing_one"]["missed_latest_batch_count"] == 1,
        "two_missing_disappeared": result["missing_two"]["is_disappeared"] is True,
        "reappearance_resets_absence": result["returned"]["is_disappeared"] is False,
        "null_or_zero_price_not_bridged": result["null_price_gap"]["price_change_count"] == 0,
        "one_batch_insufficient": result["single_batch"]["is_disappeared"] is None and result["single_batch"]["lifecycle_status"] == "INSUFFICIENT_BATCHES",
        "source_specific_calendar": result["different_calendar"]["latest_source_batch_id"] == "20261009" and result["different_calendar"]["is_disappeared"] is False,
        "batch_without_accepted_rows_counted": result["different_calendar"]["missed_latest_batch_count"] == 1,
        "historical_not_applicable": result["historical"]["is_disappeared"] is None and result["historical"]["lifecycle_status"] == "NOT_APPLICABLE",
    }
    write_summary("lifecycle_fixture_tests.json", {"checks": checks, "status": "PASS" if all(checks.values()) else "FAIL"})
    spark.stop()
    if not all(checks.values()):
        raise RuntimeError(f"Lifecycle fixture failures: {[k for k,v in checks.items() if not v]}")


if __name__ == "__main__":
    main()
