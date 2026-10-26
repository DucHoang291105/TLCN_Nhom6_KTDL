"""Read-only A+C integration checks; run after the Spark pipeline and provisioning."""
import copy
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from src.common.atomic_io import write_json_atomic

ROOT = Path(__file__).resolve().parents[2]


def read(name):
    return json.loads((ROOT / "docs/validation" / (name + ".json")).read_text(encoding="utf-8"))


def main():
    from src.dashboard.provision_superset import Superset, DASHBOARDS, trino_rows
    run, silver, gold = read("integration_airflow_run"), read("silver_final_verification"), read("gold_verification")
    dashboard = read("superset_dashboard_check")
    checks = {
        "airflow_24_tasks": run["state"] == "success" and len(run["tasks"]) == 24 and all(t["state"] == "success" for t in run["tasks"]),
        "silver_30": silver["status"] == "PASS" and len(silver["checks"]) == 30 and all(silver["checks"].values()),
        "gold_107": gold["status"] == "PASS" and len(gold["checks"]) == 107 and all(gold["checks"].values()),
        "dashboard_29_charts": dashboard["status"] == "PASS" and len(dashboard["chart_checks"]) == 29 and all(c["match"] for c in dashboard["chart_checks"]),
        "dashboards_checked_after_dag": datetime.fromisoformat(dashboard["generated_at"]) >= datetime.fromisoformat(run["end_date"].replace("Z", "+00:00")),
    }
    counts = {}
    for layer, tables in {"silver": silver["table_counts"], "gold": gold["table_counts"]}.items():
        for table, expected in tables.items():
            actual = int(trino_rows(f"SELECT COUNT(*) FROM iceberg.{layer}.{table}")[0][0])
            counts[f"{layer}.{table}"] = {"expected": expected, "actual": actual}
    checks["all_30_trino_counts_match_spark"] = len(counts) == 30 and all(c["expected"] == c["actual"] for c in counts.values())
    api = Superset()
    dashboards = []
    for spec in DASHBOARDS:
        matches = api.find("dashboard", "slug", spec["slug"])
        checks[spec["slug"]] = len(matches) == 1
        dashboards.append({"slug": spec["slug"], "matches": len(matches)})
    listing_chart = next(c for c in dashboard["chart_checks"] if c["chart"] == "Tin rao bán đang phân tích")
    detail = api.call("GET", f"chart/{listing_chart['chart_id']}")["result"]
    context = json.loads(detail["query_context"])
    filtered = []
    for province in ("Hồ Chí Minh", "Hà Nội"):
        query = copy.deepcopy(context)
        query["force"] = True
        query["queries"][0]["filters"] = [{"col": "province_name", "op": "==", "val": province}]
        result = api.call("POST", "chart/data", json=query)["result"][0]
        actual = result["data"][0]["Tin"] if result.get("data") else None
        expected = trino_rows("SELECT SUM(n_listings) FROM iceberg.gold.agg_market_overview WHERE province_name='" + province + "'")[0][0]
        filtered.append({"province": province, "expected": expected, "actual": actual,
                         "match": result.get("status") == "success" and actual == expected})
    checks["interactive_province_filters_match_gold"] = all(case["match"] for case in filtered)
    guard_code = ("import docker; from src.orchestration.pipeline import require_catalog_write_window; "
                  "c=docker.from_env();\ntry:\n require_catalog_write_window(c)\nexcept RuntimeError:\n print('BLOCKED_AS_EXPECTED')\nfinally:\n c.close()")
    guard = subprocess.check_output(["docker", "exec", "tlcn_airflow", "python", "-c", guard_code], text=True)
    checks["spark_writer_blocked_while_trino_active"] = "BLOCKED_AS_EXPECTED" in guard
    labels = read("label_accuracy")
    summary = {"generated_at": datetime.now(timezone.utc).isoformat(), "run_id": run["run_id"],
               "checks": checks, "table_counts": counts, "dashboards": dashboards, "filter_checks": filtered,
               "manual_labeling": {k: labels[k] for k in ("sample_rows", "labelled_rows", "category_accuracy", "status")},
               "operation_mode": "Sequential Spark write window then Trino/Superset query window. Concurrent reading/writing on SQLite is not certified.",
               "status": "PASS" if all(checks.values()) else "FAIL"}
    write_json_atomic(ROOT / "docs/validation/pipeline_dashboard_integration.json", summary)
    print(json.dumps({"checks": checks, "status": summary["status"]}, indent=2))
    if summary["status"] != "PASS":
        raise SystemExit("Integration checks failed")


if __name__ == "__main__":
    main()
