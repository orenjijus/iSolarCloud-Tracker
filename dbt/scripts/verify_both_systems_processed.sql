-- Verification script to check if both iSolarCloud and FusionSolar data are processed
-- Run this after dbt run to verify data from both systems

-- 1. Check raw data availability
SELECT 
    '=== RAW DATA AVAILABILITY ===' as check_type,
    '' as system,
    '' as date_key,
    '' as row_count,
    '' as min_timestamp,
    '' as max_timestamp
UNION ALL
SELECT 
    'Raw iSolarCloud' as check_type,
    'iSolarCloud' as system,
    '2025-12-17' as date_key,
    COUNT(*)::text as row_count,
    MIN(timestamp)::text as min_timestamp,
    MAX(timestamp)::text as max_timestamp
FROM raw.isolarcloud_historical_data
WHERE DATE(timestamp) = '2025-12-17'
UNION ALL
SELECT 
    'Raw FusionSolar' as check_type,
    'FusionSolar' as system,
    '2025-12-17' as date_key,
    COUNT(*)::text as row_count,
    MIN(collect_time)::text as min_timestamp,
    MAX(collect_time)::text as max_timestamp
FROM raw.fusionsolar_historical_data
WHERE DATE(collect_time) = '2025-12-17'
UNION ALL

-- 2. Check staging data
SELECT 
    '=== STAGING DATA ===' as check_type,
    '' as system,
    '' as date_key,
    '' as row_count,
    '' as min_timestamp,
    '' as max_timestamp
UNION ALL
SELECT 
    'Staging iSolarCloud' as check_type,
    'iSolarCloud' as system,
    '2025-12-17' as date_key,
    COUNT(*)::text as row_count,
    MIN(timestamp)::text as min_timestamp,
    MAX(timestamp)::text as max_timestamp
FROM staging.stg_isolarcloud__perf_unpivoted
WHERE DATE(timestamp) = '2025-12-17'
UNION ALL
SELECT 
    'Staging FusionSolar' as check_type,
    'FusionSolar' as system,
    '2025-12-17' as date_key,
    COUNT(*)::text as row_count,
    MIN(timestamp)::text as min_timestamp,
    MAX(timestamp)::text as max_timestamp
FROM staging.stg_fusionsolar__perf_unpivoted
WHERE DATE(timestamp) = '2025-12-17'
UNION ALL

-- 3. Check MAX timestamp in staging (for incremental filter)
SELECT 
    '=== MAX TIMESTAMP IN STAGING (for incremental check) ===' as check_type,
    '' as system,
    '' as date_key,
    '' as row_count,
    '' as min_timestamp,
    MAX(timestamp)::text as max_timestamp
FROM staging.stg_isolarcloud__perf_unpivoted
UNION ALL
SELECT 
    'MAX iSolarCloud' as check_type,
    'iSolarCloud' as system,
    '' as date_key,
    '' as row_count,
    '' as min_timestamp,
    MAX(timestamp)::text as max_timestamp
FROM staging.stg_isolarcloud__perf_unpivoted
UNION ALL
SELECT 
    'MAX FusionSolar' as check_type,
    'FusionSolar' as system,
    '' as date_key,
    '' as row_count,
    '' as min_timestamp,
    MAX(timestamp)::text as max_timestamp
FROM staging.stg_fusionsolar__perf_unpivoted
UNION ALL

-- 4. Check mart_site_performance_daily (FINAL)
SELECT 
    '=== MART SITE PERFORMANCE DAILY (FINAL) ===' as check_type,
    '' as system,
    '' as date_key,
    '' as row_count,
    '' as min_timestamp,
    '' as max_timestamp
UNION ALL
SELECT 
    'Mart iSolarCloud' as check_type,
    system,
    date_key::text,
    COUNT(*)::text as row_count,
    '' as min_timestamp,
    '' as max_timestamp
FROM mart.mart_site_performance_daily
WHERE system = 'isolarcloud' 
    AND date_key = '2025-12-17'
GROUP BY system, date_key
UNION ALL
SELECT 
    'Mart FusionSolar' as check_type,
    system,
    date_key::text,
    COUNT(*)::text as row_count,
    '' as min_timestamp,
    '' as max_timestamp
FROM mart.mart_site_performance_daily
WHERE system = 'fusionsolar' 
    AND date_key = '2025-12-17'
GROUP BY system, date_key;

-- Summary: Check if both systems have data
SELECT 
    '=== SUMMARY ===' as summary,
    COUNT(DISTINCT CASE WHEN system = 'isolarcloud' THEN 1 END) as isolarcloud_sites,
    COUNT(DISTINCT CASE WHEN system = 'fusionsolar' THEN 1 END) as fusionsolar_sites,
    COUNT(*) as total_sites
FROM mart.mart_site_performance_daily
WHERE date_key = '2025-12-17';

