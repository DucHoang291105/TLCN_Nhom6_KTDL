"""Build figures and a Vietnamese handoff from verified JSON, not fake UI screenshots."""
from __future__ import annotations
import argparse
import hashlib
import json
import subprocess
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from xml.etree import ElementTree
from src.report import build_week1_evidence as figures

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "docs/evidence/iceberg_20261023"


def read(name):
    return json.loads((ROOT / f"docs/validation/{name}.json").read_text(encoding="utf-8"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ci-run-id", required=True)
    args = parser.parse_args()
    history, demo, silver, gold, manifest = (read(n) for n in (
        "history_incremental_verification", "iceberg_features", "silver_final_verification", "gold_verification", "snapshot_manifest"))
    for payload in (history, demo, silver, gold, manifest):
        if payload["status"] != "PASS":
            raise RuntimeError("Evidence requires all verification payloads PASS")
    ci = json.loads(subprocess.check_output(["gh", "run", "view", args.ci_run_id, "--json",
                    "conclusion,jobs,headSha,url,workflowName,createdAt,updatedAt"], cwd=ROOT, encoding="utf-8"))
    if ci["conclusion"] != "success" or not all(j["conclusion"] == "success" for j in ci["jobs"]):
        raise RuntimeError("GitHub Actions run has not succeeded")
    current_ci = ROOT / "docs/validation/github_actions_tests.json"
    current_ci.write_text(json.dumps(ci, indent=2) + "\n", encoding="utf-8")
    tests = ElementTree.parse(ROOT / "docs/validation/pytest.xml").getroot().find("testsuite")
    if tests is None or int(tests.get("failures", 0)) or int(tests.get("errors", 0)) or int(tests.get("skipped", 0)):
        raise RuntimeError("Unit test evidence is not fully PASS")
    checksum_checks = []
    for entry in manifest["snapshots"]:
        path = ROOT / f"data/incoming/snapshots/{entry['source']}/{entry['batch_id']}/{entry['source']}_raw.csv"
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1024*1024), b""):
                digest.update(block)
        checksum_checks.append(digest.hexdigest() == entry["sha256"])
    if not all(checksum_checks):
        raise RuntimeError("Raw snapshot checksum changed")
    checksum = {"generated_at": datetime.now(timezone.utc).isoformat(), "files_compared": len(checksum_checks),
                "raw_csv_unchanged": True, "status": "PASS"}
    (ROOT / "docs/validation/snapshot_incremental_reuse.json").write_text(json.dumps(checksum, indent=2) + "\n", encoding="utf-8")
    airflow = read("airflow_incremental_task_states")
    if airflow["state"] != "success" or not airflow["tasks"] or any(t["state"] != "success" for t in airflow["tasks"]):
        raise RuntimeError("Airflow evidence requires an actual fully successful run")
    OUT.mkdir(parents=True, exist_ok=True)
    figures.OUT = OUT
    merged, rebuilt, comparison = history["incremental"], history["full_rebuild"], history["comparison"]
    figures.table_figure("Iceberg SCD2 - MERGE vs full rebuild", ["Check / metric", "MERGE", "Full rebuild"], [
        ["Operation", merged["operation"], rebuilt["operation"]],
        ["Input listing IDs", f"{merged['input_source_ids']:,}", f"{rebuilt['input_source_ids']:,}"],
        ["Recomputed listing IDs", f"{merged['affected_source_ids']:,}", f"{rebuilt['affected_source_ids']:,}"],
        ["Output history rows", f"{comparison['merge']['rows']:,}", f"{comparison['rebuild']['rows']:,}"],
        ["Different rows (EXCEPT ALL)", str(comparison['merge_only_rows']), str(comparison['rebuild_only_rows'])],
        ["Wall time (seconds)", str(merged['elapsed_seconds']), str(rebuilt['elapsed_seconds'])],
        ["Full content SHA256 equal", str(comparison['equal']), str(comparison['equal'])],
        ["Retry creates no snapshot", str(history['checks']['retry_no_write']), "not applicable"]],
        "01_merge_vs_rebuild.png", "Real Docker/Spark run; same cached input. Timing is one local measurement, not a performance SLA.")
    figures.table_figure("Iceberg integration checks", ["Executed check", "Result"],
        [[name, "PASS" if value else "FAIL"] for name,value in demo["checks"].items()] +
        [[name, value["status"]] for name,value in history["fixture_checks"].items()], "02_iceberg_checks.png")
    figures.table_figure("Pipeline verification | input through 20261023", ["Output / test", "Result"], [
        ["Raw CSV snapshots", str(len(manifest['snapshots']))],
        ["Silver current", f"{silver['table_counts']['silver_listings_current_27']:,}"],
        ["Silver history", f"{silver['table_counts']['listing_history']:,}"],
        ["Silver checks", f"{sum(silver['checks'].values())}/{len(silver['checks'])} PASS"],
        ["Gold checks", f"{sum(gold['checks'].values())}/{len(gold['checks'])} PASS"],
        ["Unit tests", f"{tests.get('tests')} PASS"],
        ["GitHub Actions", ", ".join(j['name'] + ': ' + j['conclusion'] for j in ci['jobs'])],
        ["Airflow tasks", str(dict(Counter(t['state'] for t in airflow['tasks'])))],
        ["Unchanged raw CSV checksums", str(len(checksum_checks))]], "03_pipeline_validation.png")
    figures.table_figure("Airflow | " + airflow["run_id"], ["Task", "State", "Duration"],
        [[t["task_id"],t["state"],f"{t['duration']:.1f}s" if t.get("duration") is not None else "-"] for t in airflow["tasks"]],
        "04_airflow_tasks.png", "Rendered actual Airflow API export, not a browser screenshot. History mode: incremental.")
    report = f"""# Kết quả hoàn thiện pipeline và Iceberg

Sinh từ JSON kiểm chứng ngày {datetime.now(timezone.utc).isoformat()}. Không nhập tay số liệu.

## Đã thực hiện

- Tiếp nhận {len(manifest['snapshots'])} CSV snapshot, gồm dữ liệu mới 20/10 và 23/10.
- Chuẩn hóa tên CSV Nhadatvui theo hợp đồng đầu vào; không sửa nội dung.
- Bronze tái sử dụng batch hoàn tất và preflight kiểm tra checksum bất biến.
- History SCD2 dùng MERGE theo `(source_id, version_number)`, partition `snapshot_date`.
- Có bảng state/checkpoint; retry, dữ liệu đến muộn, sửa/xóa quan sát đều có kiểm thử thực chạy.
- Thực thi demo time travel và schema evolution trên bảng sandbox, không đổi schema production.
- Tích hợp chế độ incremental/rebuild vào Airflow và PowerShell; thêm CI pytest Python 3.10/3.12.

## Kết quả thu được

| Chỉ tiêu | Kết quả |
|---|---:|
| Snapshot CSV | {len(manifest['snapshots'])} |
| Dòng snapshot Bronze | {manifest['snapshot_rows']:,} |
| Silver observation | {silver['table_counts']['listing_observation']:,} |
| Silver current | {silver['table_counts']['silver_listings_current_27']:,} |
| Silver history | {silver['table_counts']['listing_history']:,} |
| Gold fact listing | {gold['table_counts']['fact_listing']:,} |
| Silver checks | {sum(silver['checks'].values())}/{len(silver['checks'])} PASS |
| Gold checks | {sum(gold['checks'].values())}/{len(gold['checks'])} PASS |
| Unit tests | {tests.get('tests')} PASS |
| SCD2 fixture | {history['fixture_check_count']} PASS |
| Iceberg feature demo | {len(demo['checks'])} PASS |
| Airflow task | {len(airflow['tasks'])}/{len(airflow['tasks'])} SUCCESS |

MERGE tính lại {merged['affected_source_ids']:,}/{merged['input_source_ids']:,} listing ID;
đầu ra {comparison['merge']['rows']:,} dòng, cùng SHA256 với full rebuild:
`{comparison['merge']['sha256_sorted_row_hashes']}`.
EXCEPT ALL hai chiều: {comparison['merge_only_rows']}/{comparison['rebuild_only_rows']} dòng khác.
Thời gian thực đo: MERGE {merged['elapsed_seconds']} giây, rebuild {rebuilt['elapsed_seconds']} giây.
Đây là một lần đo local, không khẳng định MERGE luôn nhanh hơn rebuild.

CI: [{ci['workflowName']}]({ci['url']}), commit `{ci['headSha']}`.
Airflow run: `{airflow['run_id']}`, chế độ history `{airflow['history_mode']}`.
{len(checksum_checks)} checksum CSV giữ nguyên sau xử lý.

## Lưu ý khi báo cáo

- Guland không có batch 20/10; không giả tạo hoặc lấy batch 23/10 đổi ngày thành 20/10.
- Chỉ history incremental; Silver Core và Gold vẫn rebuild. Không làm thay phần mô hình BQ/dashboard.
- `valid_to=NULL` không chứng minh tin vẫn đang đăng; vắng hai batch không đồng nghĩa đã bán.
- Các PNG trong thư mục này là hình tổng hợp từ JSON/API thật, **không phải ảnh chụp UI**.
- [Hướng dẫn vận hành](../../operations/iceberg_history.md), [SQL demo](../../demo/iceberg_features.sql).
"""
    (OUT / "README.md").write_text(report, encoding="utf-8")
    (OUT / "figure_sources.json").write_text(json.dumps({"generated_at": datetime.now(timezone.utc).isoformat(),
        "kind": "rendered verification figures, not UI screenshots", "airflow_run_id": airflow["run_id"], "ci_run_url": ci["url"],
        "sources": ["history_incremental_verification.json", "iceberg_features.json", "silver_final_verification.json", "gold_verification.json",
                    "snapshot_manifest.json", "pytest.xml", "github_actions_tests.json", "airflow_incremental_task_states.json"]}, indent=2) + "\n", encoding="utf-8")
    print(f"Evidence written: {OUT}")


if __name__ == "__main__":
    main()
