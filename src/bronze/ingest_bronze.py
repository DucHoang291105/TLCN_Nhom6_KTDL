"""Ingest or verify the immutable historical batch; never overwrite it."""
from __future__ import annotations
import json

from src.bronze.historical_contract import BATCH_ID, SOURCES
from src.bronze.historical_lineage import ROOT, MANIFEST, audit_historical, inspect_input, raw_csv, validate_pinned
from src.common.atomic_io import write_json_atomic


def main():
    from pyspark.sql import functions as F
    from src.common.spark_session import build_spark_session
    spark = build_spark_session('BronzeHistoricalIngestion')
    try:
        target = ROOT / MANIFEST
        pinned = json.loads(target.read_text(encoding='utf-8')) if target.exists() else {}
        for source, spec in SOURCES.items():
            path = ROOT / 'data/incoming/historical' / source / spec['file']
            metadata = inspect_input(path, source)
            validate_pinned(source, metadata, pinned)
            output = f's3a://lakehouse-bronze/real_estate/historical/{source}/batch_id={BATCH_ID}'
            jpath = spark._jvm.org.apache.hadoop.fs.Path(output)
            fs = jpath.getFileSystem(spark._jsc.hadoopConfiguration())
            if fs.exists(jpath):
                print(f'REUSE immutable {source}/{BATCH_ID}; full content verification follows')
                continue
            frame = (raw_csv(spark, path).withColumn('_source_name', F.lit(source))
                     .withColumn('_source_file', F.lit(spec['file'])).withColumn('_batch_id', F.lit(BATCH_ID))
                     .withColumn('_ingestion_mode', F.lit('historical_backfill'))
                     .withColumn('_ingested_at', F.current_timestamp())
                     .withColumn('_source_sha256', F.lit(metadata['sha256']))
                     .withColumn('_source_size_bytes', F.lit(metadata['size_bytes'])))
            frame.write.mode('errorifexists').parquet(output)
        result = audit_historical(spark)
        write_json_atomic(ROOT / 'outputs/validation/bronze_ingestion_summary.json', result)
        print(json.dumps({'status': result['status'], 'total_rows': result['total_rows']}))
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
