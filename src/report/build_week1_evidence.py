"""Render report figures from real verification JSON; these are not UI screenshots."""
import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "docs/evidence/week1_20261016"


def read(name):
    return json.loads((ROOT / f"docs/validation/{name}.json").read_text(encoding="utf-8"))


def table_figure(title, columns, rows, filename, subtitle=""):
    fig, ax = plt.subplots(figsize=(13, max(4, len(rows) * .34 + 2)))
    fig.patch.set_facecolor("#f7f9fc")
    ax.axis("off")
    ax.set_title(title, loc="left", fontsize=17, weight="bold", pad=30)
    table = ax.table(cellText=rows, colLabels=columns, loc="center", cellLoc="left", colLoc="left")
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.6)
    for (row, col), cell in table.get_celld().items():
        cell.set_edgecolor("#dfe6ee")
        if row == 0:
            cell.set_facecolor("#17375e")
            cell.set_text_props(color="white", weight="bold")
        else:
            cell.set_facecolor("#ffffff" if row % 2 else "#eaf0f7")
    fig.text(.02, .025, subtitle or "Source: docs/validation JSON. Rendered evidence; not a browser screenshot.", fontsize=9, color="#526477")
    fig.tight_layout(rect=(0, .06, 1, 1))
    fig.savefig(OUT / filename, dpi=170, facecolor=fig.get_facecolor())
    plt.close(fig)


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    bronze, silver, gold, lifecycle = (read(n) for n in ("bronze_state_audit", "silver_final_verification", "gold_verification", "gold_listing_lifecycle_summary"))
    manifest = read("snapshot_manifest")
    rows = [["Snapshot CSV files", str(len(manifest["snapshots"])), manifest["status"]],
        ["Snapshot Bronze rows", f"{bronze['branch_totals']['snapshots']:,}", bronze["status"]],
        ["All Bronze rows", f"{bronze['global_total']:,}", bronze["status"]],
        ["Silver observation", f"{silver['table_counts']['listing_observation']:,}", silver["status"]],
        ["Silver current", f"{silver['table_counts']['silver_listings_current_27']:,}", silver["status"]],
        ["Silver history", f"{silver['table_counts']['listing_history']:,}", silver["status"]],
        ["Gold lifecycle", f"{lifecycle['rows']:,}", lifecycle["status"]],
        ["Silver verification", f"{sum(silver['checks'].values())}/{len(silver['checks'])}", silver["status"]],
        ["Gold verification", f"{sum(gold['checks'].values())}/{len(gold['checks'])}", gold["status"]]]
    table_figure("Week 1 - Pipeline verification | batch 20261016", ["Output / check", "Value", "Status"], rows, "01_pipeline_verification.png")
    rows = [[r["source"], r["lifecycle_status"], f"{r['count']:,}"] for r in lifecycle["status_counts"]]
    table_figure("Observed listing lifecycle by source", ["Source", "Lifecycle status", "Listings"], rows, "02_listing_lifecycle.png",
        "NOT_OBSERVED_2_BATCHES = absent from latest two crawls, NOT sold. Historical sources: NOT_APPLICABLE.")
    run = read("airflow_task_states")
    if run["state"] != "success" or len(run["tasks"]) != 23 or any(t["state"] != "success" for t in run["tasks"]):
        raise RuntimeError("Report requires an actual successful 23-task Airflow run")
    rows = [[t["task_id"], t["state"], f"{t['duration']:.1f}s" if t.get("duration") is not None else "-"] for t in run["tasks"]]
    table_figure(f"Airflow run: {run['state'].upper()} | {run['run_id']}", ["Task", "State", "Duration"], rows, "03_airflow_task_states.png",
        "Exported from live Airflow REST API. Schedule: Tue/Fri 22:00 Asia/Ho_Chi_Minh. This is a rendered API export, not a UI screenshot.")
    failure = read("airflow_failure_gate")
    rows = [["Expected negative test", "Missing batch 20990101; no crawl or data write"],
        ["Run state", failure["state"]],
        ["Failed tasks", str(sum(t["state"] == "failed" for t in failure["tasks"]))],
        ["Blocked downstream tasks", str(sum(t["state"] == "upstream_failed" for t in failure["tasks"]))],
        ["Failure-gate verification", failure["verification_status"]]]
    table_figure("Airflow failure propagation - deliberate negative test", ["Check", "Result"], rows, "04_airflow_failure_gate.png",
        "Source: airflow_failure_gate.json, exported from the actual Airflow run. Expected failure; downstream tasks never ran.")
    (OUT / "figure_sources.json").write_text(json.dumps({"generated_at": datetime.now(timezone.utc).isoformat(),
        "kind": "rendered verification figures, not UI screenshots", "run_id": run["run_id"],
        "sources": ["bronze_state_audit.json", "silver_final_verification.json", "gold_verification.json", "gold_listing_lifecycle_summary.json", "airflow_task_states.json", "airflow_failure_gate.json"]}, indent=2) + "\n")
    from xml.etree import ElementTree
    tests = ElementTree.parse(ROOT / "docs/validation/pytest.xml").getroot().find("testsuite")
    fixture = read("lifecycle_fixture_tests")
    repricing = read("gold_repricing_summary")
    if int(tests.attrib.get("failures", 0)) or int(tests.attrib.get("errors", 0)):
        raise RuntimeError("Unit test report is not PASS")
    run_minutes = round((datetime.fromisoformat(run["end_date"].replace("Z", "+00:00")) - datetime.fromisoformat(run["start_date"].replace("Z", "+00:00"))).total_seconds() / 60, 2)
    status_rows = "\n".join(f"| {r['source']} | {r['lifecycle_status']} | {r['count']:,} |" for r in lifecycle["status_counts"])
    unchanged = None
    original_path = ROOT / "outputs/week1_snapshot_manifest_before_run.json"
    if original_path.exists():
        original = json.loads(original_path.read_text(encoding="utf-8"))
        key = lambda x: {(r['source'], r['batch_id']): r['sha256'] for r in x['snapshots']}
        current = {}
        for entry in manifest["snapshots"]:
            source, batch = entry["source"], entry["batch_id"]
            checksum = hashlib.sha256()
            with (ROOT / f"data/incoming/snapshots/{source}/{batch}/{source}_raw.csv").open("rb") as handle:
                for block in iter(lambda: handle.read(1024 * 1024), b""):
                    checksum.update(block)
            current[(source, batch)] = checksum.hexdigest()
        unchanged = key(original) == key(manifest) == current
        (ROOT / "docs/validation/snapshot_reuse_verification.json").write_text(json.dumps({
            "files_compared": len(manifest["snapshots"]), "raw_csv_unchanged": unchanged,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "method": "Rehash all raw CSV after successful Airflow execution; compare with original and preflight checksums", "status": "PASS" if unchanged else "FAIL"}, indent=2) + "\n")
        if not unchanged:
            raise RuntimeError("Raw snapshot files changed during execution")
    report = f"""# Báo cáo pipeline và vận hành — tuần 16–22/10/2026

Phạm vi: phần A trong giai đoạn hoàn thiện; dữ liệu tới batch **20261016**. Số liệu được xuất từ JSON kiểm chứng và Airflow API, không nhập tay.

## Đã thực hiện

- Bổ sung và đưa lên Git LFS đủ 8 batch cho mỗi nguồn Batdongsan/Guland/Nhadatvui: 24 CSV.
- Nạp snapshot vào Bronze, kiểm tra manifest/checksum và audit đủ 9 nguồn.
- Rebuild Silver Core, History, Location, Feature; verify Silver.
- Rebuild Gold và xây `lakehouse.gold.fact_listing_lifecycle`, một dòng/source_id.
- Bổ sung kiểm định lifecycle độc lập, đối chiếu observation và repricing, fixture Spark và unit tests.
- Hoàn thiện Dockerfile Airflow, cấu hình bền qua restart, DAG 23 task, lịch 22:00 thứ 3/thứ 6 giờ Việt Nam.
- Dùng lại CSV sẵn có; chỉ tự crawl Guland/Nhadatvui khi thiếu batch đúng ngày hiện tại. Batdongsan được sensor tiếp nhận từ Chrome extension.
- Thử lỗi có chủ đích ở bước chuẩn bị dữ liệu: task đầu FAIL, toàn bộ 22 task sau bị chặn.

## Kết quả thu được

| Đầu ra | Kết quả |
|---|---:|
| CSV snapshot | {len(manifest['snapshots'])} |
| Dòng Bronze snapshot | {bronze['branch_totals']['snapshots']:,} |
| Tổng Bronze, gồm historical | {bronze['global_total']:,} |
| Silver observation | {silver['table_counts']['listing_observation']:,} |
| Silver current | {silver['table_counts']['silver_listings_current_27']:,} |
| Silver history | {silver['table_counts']['listing_history']:,} |
| Gold fact_listing (tin bán) | {gold['table_counts']['fact_listing']:,} |
| Gold fact_listing_lifecycle | {lifecycle['rows']:,} |
| Sự kiện đổi giá của tin bán | {repricing['price_change_events']:,} |
| Silver verification | {sum(silver['checks'].values())}/{len(silver['checks'])} PASS |
| Gold verification | {sum(gold['checks'].values())}/{len(gold['checks'])} PASS |
| Unit tests | {tests.attrib['tests']} PASS |
| Fixture Spark lifecycle | {sum(fixture['checks'].values())}/{len(fixture['checks'])} PASS |
| Airflow run | {run['run_id']} |
| Airflow tasks | {len(run['tasks'])}/{len(run['tasks'])} SUCCESS |
| Thời gian chạy DAG thực tế | {run_minutes} phút |
| Raw CSV giữ nguyên checksum | {unchanged} |

## Vòng đời theo nguồn

| Nguồn | Trạng thái | Số listing |
|---|---|---:|
{status_rows}

`listed_days` là khoảng thời gian từ lần quan sát đầu tới lần quan sát cuối. `NOT_OBSERVED_2_BATCHES` chỉ là không thấy trong hai batch cuối của chính nguồn đó, **không phải đã bán**. Các nguồn historical có `is_disappeared=NULL` và `NOT_APPLICABLE`.

## Minh chứng và cách chạy

- [Hướng dẫn vận hành](../operations/week1_pipeline.md).
- [Hình xuất từ kết quả thật](../evidence/week1_20261016/): 4 PNG và nguồn JSON. Đây là hình dựng từ kết quả, không phải ảnh chụp trình duyệt.
- [Airflow run](../validation/airflow_task_states.json), [failure gate](../validation/airflow_failure_gate.json).
- [Lifecycle summary](../validation/gold_listing_lifecycle_summary.json), [Gold verification](../validation/gold_verification.json).
- [Snapshot manifest](../validation/snapshot_manifest.json), [kiểm tra dùng lại dữ liệu](../validation/snapshot_reuse_verification.json).

```powershell
docker compose --profile orchestration up -d --build
.\\scripts\\run_airflow_pipeline.ps1 -BatchId 20261016
```

Airflow UI: http://localhost:8082. Quy trình tái chạy, lịch batch và file cần xuất từ extension được ghi trong hướng dẫn vận hành.

## Giới hạn và bước tiếp theo

- Chưa có ảnh chụp trực tiếp giao diện Airflow trong phiên này vì công cụ trình duyệt chưa kết nối; thư mục minh chứng có checklist vị trí cần chụp. Đã có hình xuất từ API và kết quả kiểm chứng thật.
- Nhánh Batdongsan cần thao tác export CSV của người vận hành. Pipeline tự động từ lúc file đã được đặt vào thư mục batch.
- Không suy luận đã bán từ việc vắng trong phạm vi crawl; cần giữ phạm vi crawl ổn định giữa các batch.
- Pipeline tuần này rebuild đầy đủ. Incremental MERGE INTO, time travel/schema evolution và CI thuộc tuần sau.
- Kết quả deterministic 26 bảng ngày 09/10 trong repo là bằng chứng của mốc cũ; chưa dùng để khẳng định deterministic cho bộ dữ liệu mới và bảng lifecycle.
"""
    (ROOT / "docs/report/Tien_do_A_tuan1.md").write_text(report, encoding="utf-8")
    print(OUT)


if __name__ == "__main__":
    main()
