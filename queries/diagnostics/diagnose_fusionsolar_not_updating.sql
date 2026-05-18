-- ============================================
-- Diagnose why FusionSolar data is not updating
-- ============================================

-- 1. Check MAX timestamp in raw FusionSolar historical data
SELECT 
    '1. Raw FusionSolar Historical Data - MAX Timestamp' as check_point,
    MAX(collect_time) as max_collect_time,
    DATE(MAX(collect_time)) as max_date,
    COUNT(*) as total_rows,
    COUNT(DISTINCT dev_id) as device_count
FROM "MMSR"."raw"."fusionsolar_historical_data";

-- 2. Check MAX timestamp in staging FusionSolar perf_unpivoted
SELECT 
    '2. Staging FusionSolar Perf Unpivoted - MAX Timestamp' as check_point,
    MAX(timestamp) as max_timestamp,
    DATE(MAX(timestamp)) as max_date,
    COUNT(*) as total_rows,
    COUNT(DISTINCT dev_id) as device_count
FROM "MMSR"."staging"."stg_fusionsolar__perf_unpivoted";

-- 3. Compare: Is there new data in raw that's not in staging?
SELECT 
    '3. New Data Check' as check_point,
    raw_max.max_collect_time as raw_max_timestamp,
    staging_max.max_timestamp as staging_max_timestamp,
    CASE 
        WHEN raw_max.max_collect_time > staging_max.max_timestamp THEN 
            'New data available in raw - should be picked up by incremental'
        WHEN raw_max.max_collect_time = staging_max.max_timestamp THEN 
            'No new data - raw and staging are in sync'
        ELSE 
            'Staging is ahead of raw (unusual)'
    END as status,
    raw_max.max_collect_time - staging_max.max_timestamp as time_difference
FROM (
    SELECT MAX(collect_time) as max_collect_time
    FROM "MMSR"."raw"."fusionsolar_historical_data"
) raw_max
CROSS JOIN (
    SELECT MAX(timestamp) as max_timestamp
    FROM "MMSR"."staging"."stg_fusionsolar__perf_unpivoted"
) staging_max;

-- 4. Check recent data in raw (last 7 days)
SELECT 
    '4. Raw Data - Last 7 Days' as check_point,
    DATE(collect_time) as date,
    COUNT(*) as row_count,
    COUNT(DISTINCT dev_id) as device_count,
    MIN(collect_time) as min_time,
    MAX(collect_time) as max_time
FROM "MMSR"."raw"."fusionsolar_historical_data"
WHERE collect_time >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(collect_time)
ORDER BY date DESC;

-- 5. Check recent data in staging (last 7 days)
SELECT 
    '5. Staging Data - Last 7 Days' as check_point,
    DATE(timestamp) as date,
    COUNT(*) as row_count,
    COUNT(DISTINCT dev_id) as device_count,
    MIN(timestamp) as min_time,
    MAX(timestamp) as max_time
FROM "MMSR"."staging"."stg_fusionsolar__perf_unpivoted"
WHERE timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(timestamp)
ORDER BY date DESC;

-- 6. Check if there are any rows in raw that should be picked up by incremental
-- (i.e., collect_time > MAX(timestamp) in staging)
SELECT 
    '6. Rows in Raw That Should Be Picked Up' as check_point,
    COUNT(*) as rows_to_pickup,
    MIN(collect_time) as min_collect_time,
    MAX(collect_time) as max_collect_time,
    COUNT(DISTINCT dev_id) as device_count
FROM "MMSR"."raw"."fusionsolar_historical_data" h
WHERE h.collect_time > (
    SELECT COALESCE(MAX(timestamp), '1900-01-01'::timestamp)
    FROM "MMSR"."staging"."stg_fusionsolar__perf_unpivoted"
);

-- 7. Check iSolarCloud for comparison (to see if it's updating)
SELECT 
    '7. iSolarCloud Comparison - Raw MAX' as check_point,
    MAX(timestamp) as max_timestamp,
    DATE(MAX(timestamp)) as max_date
FROM "MMSR"."raw"."isolarcloud_historical_data";

SELECT 
    '8. iSolarCloud Comparison - Staging MAX' as check_point,
    MAX(timestamp) as max_timestamp,
    DATE(MAX(timestamp)) as max_date
FROM "MMSR"."staging"."stg_isolarcloud__perf_unpivoted";

