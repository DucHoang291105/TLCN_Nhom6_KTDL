-- Executed Spark SQL, isolated sandbox; use a new table name to rerun.

CREATE TABLE lakehouse.smoke_test.features_20261024_100129 (id BIGINT, price BIGINT, snapshot_date DATE) USING iceberg PARTITIONED BY (snapshot_date) LOCATION 's3://lakehouse-silver/_smoke_test/features_20261024_100129' TBLPROPERTIES ('format-version'='2');

INSERT INTO lakehouse.smoke_test.features_20261024_100129 VALUES (1, 100, DATE '2026-10-16'), (2, 200, DATE '2026-10-20');

MERGE INTO lakehouse.smoke_test.features_20261024_100129 t USING (SELECT CAST(1 AS BIGINT) id, CAST(120 AS BIGINT) price, DATE '2026-10-16' snapshot_date UNION ALL SELECT CAST(3 AS BIGINT), CAST(300 AS BIGINT), DATE '2026-10-23') s ON t.id=s.id WHEN MATCHED THEN UPDATE SET price=s.price WHEN NOT MATCHED THEN INSERT *;

SELECT * FROM lakehouse.smoke_test.features_20261024_100129 ORDER BY id;

SELECT * FROM lakehouse.smoke_test.features_20261024_100129 VERSION AS OF 6946067691564840274 ORDER BY id;

SELECT file_path FROM lakehouse.smoke_test.features_20261024_100129.files;

ALTER TABLE lakehouse.smoke_test.features_20261024_100129 ADD COLUMNS (quality_note STRING);

SELECT file_path FROM lakehouse.smoke_test.features_20261024_100129.files;

SELECT id, quality_note FROM lakehouse.smoke_test.features_20261024_100129 ORDER BY id;

ALTER TABLE lakehouse.smoke_test.features_20261024_100129 RENAME COLUMN quality_note TO review_note;

SELECT snapshot_id, operation, summary FROM lakehouse.smoke_test.features_20261024_100129.snapshots ORDER BY committed_at;

SELECT partition, record_count FROM lakehouse.smoke_test.features_20261024_100129.partitions ORDER BY partition;

DESCRIBE TABLE lakehouse.smoke_test.features_20261024_100129;
