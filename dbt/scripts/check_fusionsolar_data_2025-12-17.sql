-- Diagnostic script to check FusionSolar data flow for 2025-12-17
-- Run this to identify where FusionSolar data is missing

-- 1. Check staging layer
SELECT 
    'stg_fusionsolar__perf_unpivoted' as layer,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp,
    COUNT(DISTINCT dev_id) as device_count,
    COUNT(DISTINCT plant_name) as site_count
FROM staging.stg_fusionsolar__perf_unpivoted
WHERE DATE(timestamp) = '2025-12-17'
UNION ALL

-- 2. Check mart_meter_performance_5min
SELECT 
    'mart_meter_performance_5min' as layer,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp,
    COUNT(DISTINCT asset_id) as device_count,
    COUNT(DISTINCT site_name) as site_count
FROM mart.mart_meter_performance_5min
WHERE system = 'fusionsolar' 
    AND date_key = '2025-12-17'
UNION ALL

-- 3. Check mart_sensor_measurements_5min
SELECT 
    'mart_sensor_measurements_5min' as layer,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp,
    COUNT(DISTINCT asset_id) as device_count,
    COUNT(DISTINCT site_name) as site_count
FROM mart.mart_sensor_measurements_5min
WHERE system = 'fusionsolar' 
    AND date_key = '2025-12-17'
UNION ALL

-- 4. Check mart_inverter_performance_5min
SELECT 
    'mart_inverter_performance_5min' as layer,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp,
    COUNT(DISTINCT asset_id) as device_count,
    COUNT(DISTINCT site_name) as site_count
FROM mart.mart_inverter_performance_5min
WHERE system = 'fusionsolar' 
    AND date_key = '2025-12-17'
UNION ALL

-- 5. Check fact_inverter_calculations_5min
SELECT 
    'fact_inverter_calculations_5min' as layer,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp,
    COUNT(DISTINCT inverter_id) as device_count,
    COUNT(DISTINCT site_name) as site_count
FROM mart.fact_inverter_calculations_5min
WHERE system = 'fusionsolar' 
    AND date_key = '2025-12-17'
UNION ALL

-- 6. Check fact_site_calculations_5min
SELECT 
    'fact_site_calculations_5min' as layer,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp,
    NULL::bigint as device_count,
    COUNT(DISTINCT site_name) as site_count
FROM mart.fact_site_calculations_5min
WHERE system = 'fusionsolar' 
    AND date_key = '2025-12-17'
UNION ALL

-- 7. Check mart_sensor_daily
SELECT 
    'mart_sensor_daily' as layer,
    COUNT(*) as row_count,
    NULL::timestamp as min_timestamp,
    NULL::timestamp as max_timestamp,
    COUNT(DISTINCT asset_id) as device_count,
    COUNT(DISTINCT site_name) as site_count
FROM mart.mart_sensor_daily
WHERE system = 'fusionsolar' 
    AND date_key = '2025-12-17'
UNION ALL

-- 8. Check mart_site_performance_daily (FINAL)
SELECT 
    'mart_site_performance_daily' as layer,
    COUNT(*) as row_count,
    NULL::timestamp as min_timestamp,
    NULL::timestamp as max_timestamp,
    NULL::bigint as device_count,
    COUNT(DISTINCT site_id) as site_count
FROM mart.mart_site_performance_daily
WHERE system = 'fusionsolar' 
    AND date_key = '2025-12-17';

-- Additional: Check max date_key in mart_site_performance_daily for FusionSolar
SELECT 
    'Max date_key in mart_site_performance_daily (FusionSolar)' as info,
    MAX(date_key) as max_date_key,
    COUNT(*) as total_rows
FROM mart.mart_site_performance_daily
WHERE system = 'fusionsolar';

