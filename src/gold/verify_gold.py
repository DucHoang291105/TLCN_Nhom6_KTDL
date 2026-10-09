"""Verify Gold Iceberg tables directly and emit machine-readable evidence."""
from __future__ import annotations
import json, sys
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import functions as F

PROJECT_ROOT=Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path: sys.path.insert(0,str(PROJECT_ROOT))
from src.common.spark_session import build_spark_session,ensure_gold_namespace,ensure_silver_namespace,table_location
from src.gold.gold_rules import MIN_PEER_GROUP_SIZE,MIN_SUBSTITUTION_SIMILARITY,UNKNOWN_KEY,load_bands

DIMENSIONS={"dim_source":"source_key","dim_date":"date_key","dim_location":"location_key","dim_property_category":"property_category_key","dim_price_band":"band_key","dim_area_band":"band_key","dim_unit_price_band":"band_key","dim_room_band":"band_key","dim_dq_status":"dq_status_key"}
BAND_TABLES={"price":"dim_price_band","area":"dim_area_band","unit_price":"dim_unit_price_band","room":"dim_room_band"}
BQ_GRAINS={"agg_peer_group_benchmark":["peer_group_id"],"fact_listing_price_assessment":["source_id"],"agg_budget_tradeoff":["price_band_key","location_key","property_category_key"],"fact_budget_pareto":["source_id"],"agg_area_substitution":["origin_location_key","alternative_location_key","property_category_key","area_band_key"],"agg_dq_kpi":["source_key"],"fact_listing_price_change":["source_id","version_number"],"agg_market_overview":["province_name","property_category_key"]}
BQ_FKS={"location_key":"dim_location","origin_location_key":"dim_location","alternative_location_key":"dim_location","property_category_key":"dim_property_category","area_band_key":"dim_area_band","room_band_key":"dim_room_band","price_band_key":"dim_price_band","source_key":"dim_source"}
FACT_FKS={"source_key":"dim_source","date_key":"dim_date","posted_date_key":"dim_date","location_key":"dim_location","property_category_key":"dim_property_category","price_band_key":"dim_price_band","area_band_key":"dim_area_band","unit_price_band_key":"dim_unit_price_band","room_band_key":"dim_room_band","dq_status_key":"dim_dq_status"}

def main() -> None:
    spark=build_spark_session("VerifyGold"); silver=ensure_silver_namespace(spark); gold=ensure_gold_namespace(spark); checks={}; metrics={}
    dims={name:spark.table(f"{gold}.{name}") for name in DIMENSIONS}; fact=spark.table(f"{gold}.fact_listing")
    counts={name:df.count() for name,df in dims.items()}; counts["fact_listing"]=fact.count()
    for name,key in DIMENSIONS.items():
        df=dims[name]; unknown=df.filter(F.col(key)==UNKNOWN_KEY).count(); duplicate=counts[name]-df.select(key).distinct().count(); null_keys=df.filter(F.col(key).isNull()).count()
        checks[f"{name}_one_unknown_row"]=unknown==1; checks[f"{name}_key_unique"]=duplicate==0 and null_keys==0
    for dimension,table in BAND_TABLES.items():
        rows=sorted((r["lower_bound"],r["upper_bound"]) for r in dims[table].filter(F.col("band_key")!=UNKNOWN_KEY).collect())
        contiguous=bool(rows) and all(a[1] is not None and a[1]==b[0] for a,b in zip(rows,rows[1:])) and rows[-1][1] is None
        checks[f"{table}_contiguous"]=contiguous and len(rows)==len(load_bands()[dimension])
    checks["fact_grain_unique"]=counts["fact_listing"]==fact.select("source_id").distinct().count()
    for column,dim in FACT_FKS.items():
        key=DIMENSIONS[dim]; nulls=fact.filter(F.col(column).isNull()).count()
        orphans=fact.join(dims[dim].select(F.col(key).alias(column)),column,"left_anti").count()
        metrics[f"{column}_unknown"]=fact.filter(F.col(column)==UNKNOWN_KEY).count(); metrics[f"{column}_orphans"]=orphans
        checks[f"fact_fk_{column}"]=nulls==0 and orphans==0
    current=spark.table(f"{silver}.silver_listings_current_27"); feature=spark.table(f"{silver}.listing_feature")
    expected=current.select("source_id","is_rent").join(feature.select("source_id","dq_status"),"source_id").filter((F.col("is_rent")==False)&(F.col("dq_status")!="REJECT")).count()  # noqa: E712
    metrics["fact_expected_rows"]=expected; checks["fact_row_count_matches_silver"]=counts["fact_listing"]==expected
    groups=fact.filter("is_cross_source_dup_suspect").groupBy("dup_group_id").agg(F.sum(F.col("is_dup_representative").cast("int")).alias("reps"),F.count("*").alias("n"))
    checks["dup_one_representative_per_group"]=groups.filter("reps<>1").count()==0
    checks["dup_groups_have_two_members"]=groups.filter("n<2").count()==0
    checks["non_suspect_is_representative"]=fact.filter("NOT is_cross_source_dup_suspect AND NOT is_dup_representative").count()==0
    metrics["dup_suspect_rows"]=fact.filter("is_cross_source_dup_suspect").count(); metrics["representative_rows"]=fact.filter("is_dup_representative").count()
    # Step 3: Business Question tables
    bq={name:spark.table(f"{gold}.{name}") for name in BQ_GRAINS}
    for name,df in bq.items():
        counts[name]=df.count(); checks[f"{name}_grain_unique"]=counts[name]==df.select(*BQ_GRAINS[name]).distinct().count()
        orphans=0
        for column,dim in BQ_FKS.items():
            if column in df.columns:
                orphans+=df.filter(F.col(column).isNotNull()).join(dims[dim].select(F.col(DIMENSIONS[dim]).alias(column)),column,"left_anti").count()
        checks[f"{name}_fk_valid"]=orphans==0
    reps=fact.filter("is_dup_representative"); metrics["representative_rows"]=reps.count()
    peer=bq["agg_peer_group_benchmark"]; assess=bq["fact_listing_price_assessment"]
    checks["benchmark_min_group_size"]=peer.filter(F.col("n_listings")<MIN_PEER_GROUP_SIZE).count()==0
    recomputed=reps.filter((F.col("location_key")!=UNKNOWN_KEY)&(F.col("property_category_key")!=UNKNOWN_KEY)&(F.col("price_per_m2")>0)).groupBy("location_key","property_category_key").count()
    published=peer.filter("benchmark_level='LOC_CAT'").select("location_key","property_category_key",F.col("n_listings").alias("count"))
    checks["benchmark_counts_match_fact"]=published.exceptAll(recomputed).count()==0
    checks["assessment_covers_representatives"]=counts["fact_listing_price_assessment"]==metrics["representative_rows"]
    checks["price_ratio_positive"]=assess.filter(F.col("price_ratio").isNotNull()&(F.col("price_ratio")<=0)).count()==0
    checks["assessment_position_consistent"]=assess.filter((F.col("price_position")=="khong_du_du_lieu")!=F.col("peer_group_id").isNull()).count()==0
    priced=reps.filter(F.col("price_band_key")!=UNKNOWN_KEY)
    checks["budget_tradeoff_sum_matches"]=bq["agg_budget_tradeoff"].agg(F.sum("n_listings")).first()[0]==priced.count()
    pareto=bq["fact_budget_pareto"]
    checks["pareto_covers_priced_listings"]=counts["fact_budget_pareto"]==priced.filter((F.col("price")>0)&(F.col("area")>0)).count()
    checks["pareto_every_group_has_frontier"]=pareto.groupBy("price_band_key","location_key","property_category_key").agg(F.max(F.col("is_pareto_efficient").cast("int")).alias("m")).filter("m<>1").count()==0
    sub=bq["agg_area_substitution"]; districts=dims["dim_location"].select("location_key","province_name")
    checks["substitution_cheaper_and_similar"]=sub.filter((F.col("alternative_median_price_per_m2")>=F.col("origin_median_price_per_m2"))|(F.col("feature_similarity")<MIN_SUBSTITUTION_SIMILARITY)).count()==0
    same_province=sub.join(districts.withColumnRenamed("location_key","origin_location_key").withColumnRenamed("province_name","p1"),"origin_location_key").join(districts.withColumnRenamed("location_key","alternative_location_key").withColumnRenamed("province_name","p2"),"alternative_location_key")
    checks["substitution_same_province"]=same_province.filter("p1<>p2").count()==0
    kpi=bq["agg_dq_kpi"].agg(*[F.sum(c).alias(c) for c in ("bronze_rows","reject_rows","observation_rows","current_rows","fact_rows","representative_rows")]).first()
    quarantine=spark.table(f"{silver}.listing_dq_quarantine").count(); observation=spark.table(f"{silver}.listing_observation").count()
    checks["dq_kpi_matches_silver_and_gold"]=(kpi["reject_rows"]==quarantine and kpi["observation_rows"]==observation and kpi["current_rows"]==current.count() and kpi["fact_rows"]==counts["fact_listing"] and kpi["representative_rows"]==metrics["representative_rows"] and kpi["bronze_rows"]-kpi["reject_rows"]>=kpi["observation_rows"])
    checks["price_change_nonzero"]=bq["fact_listing_price_change"].filter((F.col("price_change")==0)|(F.col("previous_price")<=0)).count()==0
    checks["market_overview_sum_matches"]=bq["agg_market_overview"].agg(F.sum("n_listings")).first()[0]==metrics["representative_rows"]
    locations={name:spark.sql(f"DESCRIBE TABLE EXTENDED {gold}.{name}").filter("col_name='Location'").first()["data_type"].rstrip("/") for name in [*DIMENSIONS,"fact_listing",*BQ_GRAINS]}
    checks["tables_in_gold_bucket"]=all(loc==table_location(f"{gold}.{name}") for name,loc in locations.items())
    failures=[name for name,passed in checks.items() if not passed]
    summary={"generated_at":datetime.now(timezone.utc).isoformat(),"namespace":gold,"table_counts":counts,"checks":checks,"metrics":metrics,"table_locations":locations,"status":"PASS" if not failures else "FAIL","failures":failures}
    for out in (PROJECT_ROOT/"outputs/validation",PROJECT_ROOT/"docs/validation"):
        out.mkdir(parents=True,exist_ok=True); (out/"gold_verification.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(summary,ensure_ascii=False,indent=2)); spark.stop()
    if failures: raise RuntimeError("Gold verification failed: "+", ".join(failures))

if __name__=="__main__": main()
