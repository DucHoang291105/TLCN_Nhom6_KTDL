"""Verify final Silver Iceberg tables and emit machine-readable evidence."""
from __future__ import annotations
import json, sys
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import Window
from pyspark.sql import functions as F

PROJECT_ROOT=Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path: sys.path.insert(0,str(PROJECT_ROOT))
from src.common.spark_session import build_spark_session,ensure_silver_namespace
from src.common.utils import CANONICAL_COLUMNS

EXPECTED={"batdongsan","guland","nhadatvui","chotot","mogi","alonhadat","luachonnhadat","muaban","homedy"}

def main() -> None:
    spark=build_spark_session("VerifySilver"); ns=ensure_silver_namespace(spark); failures=[]
    names=["listing_observation","listing_dq_quarantine","crawl_current_27","historical_current_27","silver_listings_current_27","listing_history","listing_location"]
    frames={name:spark.table(f"{ns}.{name}") for name in names}; counts={name:df.count() for name,df in frames.items()}
    current=frames["silver_listings_current_27"]; observation=frames["listing_observation"]; history=frames["listing_history"]; location=frames["listing_location"]
    source_counts={str(r["source"]):int(r["count"]) for r in current.groupBy("source").count().collect()}
    if set(source_counts)!=EXPECTED: failures.append(f"source set mismatch: {sorted(source_counts)}")
    if any(v<=0 for v in source_counts.values()): failures.append("one or more sources have zero rows")
    if current.columns!=CANONICAL_COLUMNS: failures.append("final current is not exact canonical 27-column order")
    null_ids=current.filter(F.col("source_id").isNull()).count(); duplicate_ids=counts["silver_listings_current_27"]-current.select("source_id").distinct().count()
    bad_prefix=current.filter(F.col("source_id")!=F.concat_ws("_",F.col("source"),F.col("ad_id"))).count()
    observation_duplicates=(observation.groupBy("source","ad_id","batch_id").count().filter("count>1").count())
    location_duplicates=location.groupBy("source_id").count().filter("count>1").count(); location_orphans=location.join(current.select("source_id"),"source_id","left_anti").count()
    hist_window=Window.partitionBy("source_id").orderBy("version_number")
    history_duplicate_hashes=history.withColumn("prev",F.lag("record_hash").over(hist_window)).filter(F.col("prev")==F.col("record_hash")).count()
    history_bad_sequence=(history.groupBy("source_id").agg(F.min("version_number").alias("min"),F.max("version_number").alias("max"),F.count("*").alias("n")).filter("min<>1 OR max<>n").count())
    checks={"source_set_9":set(source_counts)==EXPECTED,"schema_exact_27":current.columns==CANONICAL_COLUMNS,"source_id_not_null":null_ids==0,"source_id_unique":duplicate_ids==0,"source_id_convention":bad_prefix==0,"observation_grain_unique":observation_duplicates==0,"current_union_count":counts["silver_listings_current_27"]==counts["crawl_current_27"]+counts["historical_current_27"],"location_coverage":counts["listing_location"]==counts["silver_listings_current_27"],"location_unique":location_duplicates==0,"location_fk":location_orphans==0,"history_no_consecutive_duplicate":history_duplicate_hashes==0,"history_version_sequence":history_bad_sequence==0}
    failures.extend(name for name,passed in checks.items() if not passed)
    summary={"generated_at":datetime.now(timezone.utc).isoformat(),"catalog_namespace":ns,"table_counts":counts,"source_counts":source_counts,"checks":checks,"metrics":{"null_source_ids":null_ids,"duplicate_source_ids":duplicate_ids,"bad_source_id_prefix":bad_prefix,"observation_duplicate_keys":observation_duplicates,"location_duplicate_ids":location_duplicates,"location_orphans":location_orphans,"history_consecutive_duplicate_hashes":history_duplicate_hashes,"history_bad_version_sequences":history_bad_sequence},"status":"PASS" if not failures else "FAIL","failures":failures}
    for out in (PROJECT_ROOT/"outputs/validation",PROJECT_ROOT/"docs/validation"):
        out.mkdir(parents=True,exist_ok=True); (out/"silver_final_verification.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(summary,ensure_ascii=False,indent=2)); spark.stop()
    if failures: raise RuntimeError("Silver verification failed: "+", ".join(failures))

if __name__=="__main__": main()
