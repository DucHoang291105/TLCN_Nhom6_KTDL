"""Build an Iceberg listing history containing only business-state changes."""
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

def main() -> None:
    spark=build_spark_session("BuildListingHistory"); namespace=ensure_silver_namespace(spark)
    observation_table=f"{namespace}.listing_observation"; history_table=f"{namespace}.listing_history"
    observations=spark.table(observation_table)
    observed=F.coalesce(F.col("snapshot_date").cast("timestamp"),F.col("scraped_at"),F.col("bronze_ingested_at"))
    order=Window.partitionBy("source_id").orderBy(observed,F.col("scraped_at").asc_nulls_last(),F.col("batch_id"),F.col("record_hash"))
    changed=observations.withColumn("_previous_hash",F.lag("record_hash").over(order)).filter(F.col("_previous_hash").isNull()|(F.col("_previous_hash")!=F.col("record_hash")))
    versions=Window.partitionBy("source_id").orderBy(observed,F.col("scraped_at").asc_nulls_last(),F.col("batch_id"),F.col("record_hash"))
    history=changed.withColumn("version_number",F.row_number().over(versions)).withColumn("valid_from",observed)
    validity=Window.partitionBy("source_id").orderBy("version_number")
    history=history.withColumn("valid_to",F.lead("valid_from").over(validity)).withColumn("is_current_version",F.col("valid_to").isNull()).select(*(CANONICAL_COLUMNS+["batch_id","snapshot_date","bronze_ingested_at","record_hash","dq_status","dq_reasons","completeness_score","version_number","valid_from","valid_to","is_current_version"]))
    history.writeTo(history_table).using("iceberg").tableProperty("format-version","2").createOrReplace()
    verified=spark.table(history_table); rows=verified.count(); distinct_ids=verified.select("source_id").distinct().count()
    duplicate_consecutive=verified.withColumn("prev",F.lag("record_hash").over(Window.partitionBy("source_id").orderBy("version_number"))).filter(F.col("prev")==F.col("record_hash")).count()
    if duplicate_consecutive: raise RuntimeError(f"History has {duplicate_consecutive} consecutive duplicate hash rows")
    summary={"generated_at":datetime.now(timezone.utc).isoformat(),"input_table":observation_table,"output_table":history_table,"observation_rows":observations.count(),"history_rows":rows,"source_id_count":distinct_ids,"changed_versions":rows-distinct_ids,"consecutive_duplicate_hashes":duplicate_consecutive,"status":"PASS"}
    for out in (PROJECT_ROOT/"outputs/validation",PROJECT_ROOT/"docs/validation"):
        out.mkdir(parents=True,exist_ok=True); (out/"silver_history_summary.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(summary,ensure_ascii=False,indent=2)); spark.stop()

if __name__=="__main__": main()
