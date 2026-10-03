"""Audit every configured and discovered Bronze source/batch on MinIO."""
from __future__ import annotations
import csv, json, sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path: sys.path.insert(0, str(PROJECT_ROOT))
from src.common.spark_session import build_spark_session

BRONZE_ROOT = "s3a://lakehouse-bronze/real_estate"
BRANCHES = ("historical", "snapshots")

def read_sources_config(path: Path) -> dict[str, list[str]]:
    result = {branch: [] for branch in BRANCHES}; current = None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if stripped.endswith(":") and stripped[:-1] in result: current = stripped[:-1]
        elif current and stripped.startswith("- "): result[current].append(stripped[2:].strip())
    return result

def list_names(spark, uri: str) -> list[str]:
    jvm = spark.sparkContext._jvm; conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri); fs = path.getFileSystem(conf)
    if not fs.exists(path): return []
    return sorted(s.getPath().getName() for s in fs.listStatus(path) if s.isDirectory())

def main() -> None:
    spark = build_spark_session("BronzeStateAudit")
    expected = read_sources_config(PROJECT_ROOT / "config/sources.yaml"); rows = []; branch_totals = {}
    for branch in BRANCHES:
        branch_uri = f"{BRONZE_ROOT}/{branch}"; actual = list_names(spark, branch_uri); configured = expected[branch]; branch_total = 0
        for source in sorted(set(configured) | set(actual)):
            state = "EXPECTED_AND_FOUND" if source in configured and source in actual else ("EXPECTED_BUT_MISSING" if source in configured else "FOUND_BUT_NOT_CONFIGURED")
            batches = list_names(spark, f"{branch_uri}/{source}") if source in actual else []
            batches = [name for name in batches if name.startswith("batch_id=")]
            if not batches:
                rows.append({"branch":branch,"source":source,"batch_id":None,"source_state":state,"status":"MISSING" if source not in actual else "NO_BATCH","row_count":0,"column_count":0,"columns":[],"error":None}); continue
            for batch_dir in batches:
                batch_id = batch_dir.split("=",1)[1]; path = f"{branch_uri}/{source}/{batch_dir}"
                try:
                    frame = spark.read.parquet(path); count = frame.count(); branch_total += count
                    rows.append({"branch":branch,"source":source,"batch_id":batch_id,"source_state":state,"status":"READABLE","row_count":count,"column_count":len(frame.columns),"columns":frame.columns,"error":None})
                except Exception as exc:
                    rows.append({"branch":branch,"source":source,"batch_id":batch_id,"source_state":state,"status":"UNREADABLE","row_count":0,"column_count":0,"columns":[],"error":str(exc)[:1000]})
        branch_totals[branch] = branch_total
    source_totals = {}
    for row in rows:
        key=f"{row['branch']}/{row['source']}"; source_totals[key]=source_totals.get(key,0)+row["row_count"]
    status = "PASS" if all(r["status"]=="READABLE" and r["source_state"]=="EXPECTED_AND_FOUND" for r in rows) else "WARN"
    summary={"generated_at":datetime.now(timezone.utc).isoformat(),"bronze_root":BRONZE_ROOT,"expected_sources":expected,"branch_totals":branch_totals,"source_totals":source_totals,"global_total":sum(branch_totals.values()),"batches":rows,"status":status,"count_notes":{"87660":"Reported selected snapshot set; audit lists every stored batch.","138824":"Previous ingestion state; compare source/batch composition with this audit."}}
    for output_dir in (PROJECT_ROOT/"outputs/validation",PROJECT_ROOT/"docs/validation"):
        output_dir.mkdir(parents=True,exist_ok=True); (output_dir/"bronze_state_audit.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
        with (output_dir/"bronze_state_audit.csv").open("w",encoding="utf-8-sig",newline="") as handle:
            fields=["branch","source","batch_id","source_state","status","row_count","column_count","columns","error"]; writer=csv.DictWriter(handle,fieldnames=fields); writer.writeheader()
            for row in rows: flat=dict(row); flat["columns"]="|".join(row["columns"]); writer.writerow(flat)
    print(json.dumps(summary,ensure_ascii=False,indent=2)); spark.stop()

if __name__ == "__main__": main()
