-- Trino catalog = iceberg (docker/trino/catalog/iceberg.properties).
-- Spark catalog = lakehouse. Both point to the same Iceberg REST catalog.
SELECT source, lifecycle_status, count(*) AS listings
FROM iceberg.gold.fact_listing_lifecycle
GROUP BY source, lifecycle_status
ORDER BY source, lifecycle_status;

SELECT source_id, source, first_seen_date, last_seen_date,
       observation_count, listed_days, price_change_count,
       latest_source_batch_id, previous_source_batch_id,
       is_disappeared, lifecycle_status
FROM iceberg.gold.fact_listing_lifecycle
WHERE source = 'batdongsan' AND price_change_count > 0
ORDER BY price_change_count DESC, source_id
LIMIT 20;

-- These are NOT confirmed sales. All historical sources must have a NULL flag.
SELECT source, count(*) AS absent_from_latest_two_batches
FROM iceberg.gold.fact_listing_lifecycle
WHERE is_disappeared = true
GROUP BY source;
