"""Deterministic Bronze -> Iceberg Silver Data Foundation for all 9 sources."""
from __future__ import annotations
import json, os, sys
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from pyspark.sql import DataFrame, Row, Window
from pyspark.sql import functions as F, types as T
from pyspark.storagelevel import StorageLevel

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path: sys.path.insert(0, str(PROJECT_ROOT))
from src.common.spark_session import build_spark_session, ensure_silver_namespace, write_iceberg_table
from src.common.utils import CANONICAL_COLUMNS, RECORD_HASH_FIELDS
from src.silver.build_listing_core import OBSERVATION_METADATA_COLUMNS, build_batdongsan_observation, build_guland_observation, build_nhadatvui_observation

SNAPSHOT_ROOT = "s3a://lakehouse-bronze/real_estate/snapshots"
HISTORICAL_ROOT = "s3a://lakehouse-bronze/real_estate/historical"
CRAWL_SOURCES = ("batdongsan", "guland", "nhadatvui")
HISTORICAL_SOURCES = ("chotot", "mogi", "alonhadat", "luachonnhadat", "muaban", "homedy")
SOURCE_BUILDERS = {"batdongsan":build_batdongsan_observation,"guland":build_guland_observation,"nhadatvui":build_nhadatvui_observation}

OBSERVATION_SCHEMA = T.StructType([
    T.StructField("source",T.StringType(),True),T.StructField("source_group",T.StringType(),True),T.StructField("source_id",T.StringType(),True),T.StructField("ad_id",T.StringType(),True),T.StructField("title",T.StringType(),True),
    T.StructField("price",T.DoubleType(),True),T.StructField("price_str",T.StringType(),True),T.StructField("area",T.DoubleType(),True),T.StructField("rooms",T.IntegerType(),True),T.StructField("address",T.StringType(),True),
    T.StructField("ward",T.StringType(),True),T.StructField("district_id",T.StringType(),True),T.StructField("district_name",T.StringType(),True),T.StructField("category_id",T.StringType(),True),T.StructField("category_name",T.StringType(),True),
    T.StructField("lat",T.DoubleType(),True),T.StructField("lon",T.DoubleType(),True),T.StructField("image",T.StringType(),True),T.StructField("ad_url",T.StringType(),True),T.StructField("source_url",T.StringType(),True),
    T.StructField("posted_at",T.TimestampType(),True),T.StructField("scraped_at",T.TimestampType(),True),T.StructField("page_fetched",T.IntegerType(),True),T.StructField("price_m",T.DoubleType(),True),T.StructField("price_per_m2",T.DoubleType(),True),
    T.StructField("has_coord",T.BooleanType(),True),T.StructField("is_rent",T.BooleanType(),True),T.StructField("batch_id",T.StringType(),True),T.StructField("snapshot_date",T.DateType(),True),T.StructField("bronze_ingested_at",T.TimestampType(),True),
    T.StructField("bronze_source_file",T.StringType(),True),T.StructField("bronze_path",T.StringType(),True),T.StructField("record_hash",T.StringType(),True),T.StructField("dq_status",T.StringType(),True),T.StructField("dq_reasons",T.ArrayType(T.StringType()),True),T.StructField("completeness_score",T.DoubleType(),True),
    T.StructField("category_evidence",T.StringType(),True),
])

def transform_partition(rows: Iterator[Row], builder: Callable[..., dict[str,Any]]) -> Iterator[tuple[Any,...]]:
    columns=CANONICAL_COLUMNS+OBSERVATION_METADATA_COLUMNS
    for row in rows:
        raw=row.asDict(recursive=True); bronze_path=raw.pop("_bronze_path",None); batch_id=str(raw.get("_batch_id") or "")
        observation=builder(raw,batch_id=batch_id,bronze_path=bronze_path)
        yield tuple(observation.get(column) for column in columns)

def read_crawl_source(spark, source: str) -> tuple[DataFrame,int]:
    path=f"{SNAPSHOT_ROOT}/{source}/batch_id=*"
    bronze=spark.read.option("basePath",f"{SNAPSHOT_ROOT}/{source}").parquet(path).withColumn("_bronze_path",F.input_file_name())
    count=bronze.count(); builder=SOURCE_BUILDERS[source]
    frame=spark.createDataFrame(bronze.rdd.mapPartitions(lambda rows: transform_partition(rows,builder)),OBSERVATION_SCHEMA)
    return frame,count

def _bool(column):
    text=F.lower(F.trim(column.cast("string")))
    return F.when(text.isin("true","1","yes"),F.lit(True)).when(text.isin("false","0","no"),F.lit(False)).otherwise(F.lit(None).cast("boolean"))

def _timestamp(column):
    text=F.trim(column.cast("string"))
    parsed=(F.when(text.rlike(r"^[0-9]{13}$"),F.to_timestamp(F.from_unixtime(text.cast("double")/1000.0)))
            .when(text.rlike(r"^[0-9]{10}$"),F.to_timestamp(F.from_unixtime(text.cast("double"))))
            .otherwise(F.to_timestamp(text)))
    return F.when((F.year(parsed)>=2000)&(F.year(parsed)<=2100),parsed).otherwise(F.lit(None).cast("timestamp"))

def read_historical_source(spark, source: str) -> tuple[DataFrame,int]:
    path=f"{HISTORICAL_ROOT}/{source}/batch_id=*"
    raw=spark.read.option("basePath",f"{HISTORICAL_ROOT}/{source}").parquet(path).withColumn("_bronze_path",F.input_file_name())
    count=raw.count()
    string_cols={"source","source_group","source_id","ad_id","title","price_str","address","ward","district_id","district_name","category_id","category_name","image","ad_url","source_url"}
    double_cols={"price","area","lat","lon","price_m","price_per_m2"}; int_cols={"rooms","page_fetched"}; timestamp_cols={"posted_at","scraped_at"}
    expressions=[]
    for name in CANONICAL_COLUMNS:
        col=F.col(name) if name in raw.columns else F.lit(None)
        if name in string_cols: col=F.trim(col.cast("string"))
        elif name in double_cols: col=col.cast("double")
        elif name in int_cols: col=col.cast("int")
        elif name in timestamp_cols: col=_timestamp(col)
        elif name in {"has_coord","is_rent"}: col=_bool(col)
        expressions.append(col.alias(name))
    base=raw.select(*expressions,F.col("_batch_id").cast("string").alias("batch_id"),F.col("_ingested_at").cast("timestamp").alias("bronze_ingested_at"),F.col("_source_file").cast("string").alias("bronze_source_file"),F.col("_bronze_path").alias("bronze_path"))
    base=base.withColumn("source",F.lower(F.coalesce(F.col("source"),F.lit(source)))).withColumn("ad_id",F.trim(F.col("ad_id")))
    expected_id=F.concat_ws("_",F.col("source"),F.col("ad_id"))
    base=base.withColumn("source_id",F.when(F.col("ad_id").isNotNull(),expected_id).otherwise(F.lit(None).cast("string")))
    base=base.withColumn("snapshot_date",F.to_date(F.col("scraped_at")))
    base=base.withColumn("record_hash",F.sha2(F.to_json(F.struct(*[F.col(c) for c in RECORD_HASH_FIELDS])),256))
    checks=[(F.col("source").isNull()|F.col("ad_id").isNull()|F.col("title").isNull(),"DQ01"),(F.col("price").isNull(),"DQ04"),(F.col("area").isNull()|(F.col("area")<=0),"DQ08")]
    reasons=F.array(*[F.when(condition,F.lit(code)) for condition,code in checks])
    base=base.withColumn("dq_reasons",F.filter(reasons,lambda x:x.isNotNull()))
    base=base.withColumn("dq_status",F.when(F.array_contains(F.col("dq_reasons"),"DQ01"),"REJECT").when(F.size("dq_reasons")>0,"WARN").otherwise("PASS"))
    score_fields=["title","price","area","rooms","address","ward","district_name","category_name","lat","lon","image","ad_url","posted_at"]
    score=sum(F.when(F.col(c).isNotNull(),F.lit(1.0)).otherwise(F.lit(0.0)) for c in score_fields)/F.lit(float(len(score_fields)))
    # Historical exports carry the source's own category label.
    base=base.withColumn("category_evidence",F.when(F.col("category_name").isNotNull(),F.lit("SOURCE_LABEL")).otherwise(F.lit("NONE")))
    return base.withColumn("completeness_score",F.round(score,6)).select(*(CANONICAL_COLUMNS+OBSERVATION_METADATA_COLUMNS)),count

def current_from(observations: DataFrame) -> DataFrame:
    window=Window.partitionBy("source_id").orderBy(F.col("snapshot_date").desc_nulls_last(),F.col("scraped_at").desc_nulls_last(),F.col("completeness_score").desc_nulls_last(),F.col("record_hash").asc_nulls_last())
    return observations.withColumn("_rank",F.row_number().over(window)).filter("_rank=1").select(*CANONICAL_COLUMNS)

def write_table(frame: DataFrame, table: str) -> None:
    write_iceberg_table(frame,table)

def counts_by(frame: DataFrame, column: str) -> dict[str,int]:
    return {str(r[column]):int(r["count"]) for r in frame.groupBy(column).count().collect()}

def main() -> None:
    spark=build_spark_session("BuildSilverDataFoundation"); namespace=ensure_silver_namespace(spark); started=datetime.now(timezone.utc)
    frames=[]; source_input={}
    for source in CRAWL_SOURCES:
        frame,count=read_crawl_source(spark,source); frames.append(frame); source_input[source]=count
    for source in HISTORICAL_SOURCES:
        frame,count=read_historical_source(spark,source); frames.append(frame); source_input[source]=count
    normalized=frames[0]
    for frame in frames[1:]: normalized=normalized.unionByName(frame)
    normalized=normalized.persist(StorageLevel.MEMORY_AND_DISK)
    quarantine=normalized.filter("dq_status='REJECT'").persist(StorageLevel.MEMORY_AND_DISK)
    accepted=normalized.filter("dq_status<>'REJECT'")
    dedup=Window.partitionBy("source","ad_id","batch_id").orderBy(F.col("completeness_score").desc_nulls_last(),F.col("scraped_at").desc_nulls_last(),F.col("page_fetched").asc_nulls_last(),F.col("record_hash").asc_nulls_last())
    observations=accepted.withColumn("_rank",F.row_number().over(dedup)).filter("_rank=1").drop("_rank").persist(StorageLevel.MEMORY_AND_DISK)
    crawl_obs=observations.filter(F.col("source").isin(*CRAWL_SOURCES)); historical_obs=observations.filter(F.col("source").isin(*HISTORICAL_SOURCES))
    crawl_current=current_from(crawl_obs).persist(StorageLevel.MEMORY_AND_DISK); historical_current=current_from(historical_obs).persist(StorageLevel.MEMORY_AND_DISK)
    final_current=crawl_current.unionByName(historical_current).persist(StorageLevel.MEMORY_AND_DISK)
    tables={"listing_observation":observations,"listing_dq_quarantine":quarantine,"crawl_current_27":crawl_current,"historical_current_27":historical_current,"silver_listings_current_27":final_current}
    for name,frame in tables.items(): write_table(frame,f"{namespace}.{name}")
    read_counts={name:spark.table(f"{namespace}.{name}").count() for name in tables}
    for name,frame in tables.items():
        if read_counts[name]!=frame.count(): raise RuntimeError(f"Iceberg read-back mismatch: {name}")
    if spark.table(f"{namespace}.silver_listings_current_27").columns!=CANONICAL_COLUMNS: raise RuntimeError("Final current schema/order mismatch")
    normalized_rows=normalized.count(); quarantine_rows=quarantine.count(); accepted_rows=accepted.count(); observation_rows=observations.count()
    summary={"started_at":started.isoformat(),"finished_at":datetime.now(timezone.utc).isoformat(),"catalog_namespace":namespace,"source_input_rows":source_input,"normalized_rows":normalized_rows,"accepted_before_dedup":accepted_rows,"quarantine_rows":quarantine_rows,"duplicates_dropped":accepted_rows-observation_rows,"observation_rows":observation_rows,"crawl_current_rows":crawl_current.count(),"historical_current_rows":historical_current.count(),"final_current_rows":final_current.count(),"final_source_counts":counts_by(final_current,"source"),"dq_status_counts":counts_by(observations.unionByName(quarantine),"dq_status"),"iceberg_readback_counts":read_counts,"status":"PASS"}
    for out in (PROJECT_ROOT/"outputs/validation",PROJECT_ROOT/"docs/validation"):
        out.mkdir(parents=True,exist_ok=True); (out/"silver_foundation_summary.json").write_text(json.dumps(summary,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps(summary,ensure_ascii=False,indent=2)); spark.stop()

if __name__=="__main__": main()
