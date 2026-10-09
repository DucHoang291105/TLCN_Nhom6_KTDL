"""Verify final Silver Iceberg tables and emit machine-readable evidence."""
from __future__ import annotations
import json, sys
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import Window
from pyspark.sql import functions as F

PROJECT_ROOT=Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path: sys.path.insert(0,str(PROJECT_ROOT))
from src.common.spark_session import build_spark_session,ensure_silver_namespace,table_location
from src.common.utils import CANONICAL_COLUMNS

EXPECTED={"batdongsan","guland","nhadatvui","chotot","mogi","alonhadat","luachonnhadat","muaban","homedy"}

def main() -> None:
    spark=build_spark_session("VerifySilver"); ns=ensure_silver_namespace(spark); failures=[]
    names=["listing_observation","listing_dq_quarantine","crawl_current_27","historical_current_27","silver_listings_current_27","listing_history","listing_location","listing_feature"]
    frames={name:spark.table(f"{ns}.{name}") for name in names}; counts={name:df.count() for name,df in frames.items()}
    current=frames["silver_listings_current_27"]; observation=frames["listing_observation"]; history=frames["listing_history"]; location=frames["listing_location"]; feature=frames["listing_feature"]
    source_counts={str(r["source"]):int(r["count"]) for r in current.groupBy("source").count().collect()}
    if set(source_counts)!=EXPECTED: failures.append(f"source set mismatch: {sorted(source_counts)}")
    if any(v<=0 for v in source_counts.values()): failures.append("one or more sources have zero rows")
    if current.columns!=CANONICAL_COLUMNS: failures.append("final current is not exact canonical 27-column order")
    null_ids=current.filter(F.col("source_id").isNull()).count(); duplicate_ids=counts["silver_listings_current_27"]-current.select("source_id").distinct().count()
    bad_prefix=current.filter(F.col("source_id")!=F.concat_ws("_",F.col("source"),F.col("ad_id"))).count()
    observation_duplicates=(observation.groupBy("source","ad_id","batch_id").count().filter("count>1").count())
    location_duplicates=location.groupBy("source_id").count().filter("count>1").count(); location_orphans=location.join(current.select("source_id"),"source_id","left_anti").count()
    feature_duplicates=feature.groupBy("source_id").count().filter("count>1").count(); feature_null_ids=feature.filter(F.col("source_id").isNull()).count(); feature_orphans=feature.join(current.select("source_id"),"source_id","left_anti").count()
    current_ids=current.select("source_id")
    # Equal counts do not prove equal key sets: compare both directions.
    feature_missing=current_ids.join(feature.select("source_id"),"source_id","left_anti").count()
    location_missing=current_ids.join(location.select("source_id"),"source_id","left_anti").count()
    evidence_values={"SOURCE_STRUCTURED","SOURCE_LABEL","ENDPOINT_CONTEXT","NONE"}
    bad_evidence=observation.filter(F.col("category_evidence").isNull()|~F.col("category_evidence").isin(*evidence_values)).count()
    # Evidence NONE must mean no category, and a category must have evidence.
    evidence_mismatch=observation.filter((F.col("category_evidence")=="NONE")!=F.col("category_name").isNull()).count()
    feature_bad_scores=feature.filter(F.col("feature_completeness_score").isNull()|(F.col("feature_completeness_score")<0)|(F.col("feature_completeness_score")>1)).count()
    flag_columns=["title_has_legal","title_has_furnished","title_has_frontage","title_has_elevator","title_has_car_access"]
    flag_rates={c:round(float(v),6) for c,v in feature.agg(*[F.avg(F.col(c).cast("double")).alias(c) for c in flag_columns]).first().asDict().items()}
    mapped_category_rate=round(feature.filter(F.col("model_category")!="khong_ro").count()/max(counts["listing_feature"],1),6)
    warnings=[f"{c} TRUE rate {v:.2%} outside (0%, 80%]" for c,v in flag_rates.items() if not 0<v<=0.8]
    if mapped_category_rate<0.98: warnings.append(f"model_category mapped rate {mapped_category_rate:.2%} < 98%")
    hist_window=Window.partitionBy("source_id").orderBy("version_number")
    history_duplicate_hashes=history.withColumn("prev",F.lag("record_hash").over(hist_window)).filter(F.col("prev")==F.col("record_hash")).count()
    history_bad_sequence=(history.groupBy("source_id").agg(F.min("version_number").alias("min"),F.max("version_number").alias("max"),F.count("*").alias("n")).filter("min<>1 OR max<>n").count())
    checks={"source_set_9":set(source_counts)==EXPECTED,"schema_exact_27":current.columns==CANONICAL_COLUMNS,"source_id_not_null":null_ids==0,"source_id_unique":duplicate_ids==0,"source_id_convention":bad_prefix==0,"observation_grain_unique":observation_duplicates==0,"current_union_count":counts["silver_listings_current_27"]==counts["crawl_current_27"]+counts["historical_current_27"],"location_coverage":counts["listing_location"]==counts["silver_listings_current_27"],"location_unique":location_duplicates==0,"location_fk":location_orphans==0,"history_no_consecutive_duplicate":history_duplicate_hashes==0,"history_version_sequence":history_bad_sequence==0,"feature_coverage":counts["listing_feature"]==counts["silver_listings_current_27"],"feature_source_id_not_null":feature_null_ids==0,"feature_unique":feature_duplicates==0,"feature_fk":feature_orphans==0,"feature_score_range":feature_bad_scores==0,"feature_source_id_set_equal":feature_missing==0 and feature_orphans==0,"location_source_id_set_equal":location_missing==0 and location_orphans==0,"observation_category_evidence_valid":bad_evidence==0,"observation_category_evidence_consistent":evidence_mismatch==0}
    locations={name:spark.sql(f"DESCRIBE TABLE EXTENDED {ns}.{name}").filter("col_name='Location'").first()["data_type"].rstrip("/") for name in names}
    checks["tables_in_silver_bucket"]=all(loc==table_location(f"{ns}.{name}") for name,loc in locations.items())
    failures.extend(name for name,passed in checks.items() if not passed)
    summary={"generated_at":datetime.now(timezone.utc).isoformat(),"catalog_namespace":ns,"table_counts":counts,"source_counts":source_counts,"checks":checks,"metrics":{"null_source_ids":null_ids,"duplicate_source_ids":duplicate_ids,"bad_source_id_prefix":bad_prefix,"observation_duplicate_keys":observation_duplicates,"location_duplicate_ids":location_duplicates,"location_orphans":location_orphans,"history_consecutive_duplicate_hashes":history_duplicate_hashes,"history_bad_version_sequences":history_bad_sequence,"feature_null_ids":feature_null_ids,"feature_duplicate_ids":feature_duplicates,"feature_orphans":feature_orphans,"feature_bad_scores":feature_bad_scores,"feature_missing_ids":feature_missing,"location_missing_ids":location_missing,"observation_bad_category_evidence":bad_evidence,"observation_category_evidence_mismatch":evidence_mismatch,"model_category_mapped_rate":mapped_category_rate,"title_flag_true_rates":flag_rates},"warnings":warnings,"table_locations":locations,"status":"PASS" if not failures else "FAIL","failures":failures}
    for out in (PROJECT_ROOT/"outputs/validation",PROJECT_ROOT/"docs/validation"):
        out.mkdir(parents=True,exist_ok=True); (out/"silver_final_verification.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(summary,ensure_ascii=False,indent=2)); spark.stop()
    if failures: raise RuntimeError("Silver verification failed: "+", ".join(failures))

if __name__=="__main__": main()
