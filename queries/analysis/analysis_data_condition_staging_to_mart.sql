-- ============================================
-- ANALISIS KONDISI DATA: STAGING → MART
-- ============================================
-- Jalankan query ini untuk mendapatkan snapshot kondisi data saat ini.
-- Gunakan hasil untuk mengisi report di dbt/docs/DATA_CONDITION_REPORT.md
--
-- Catatan: Sesuaikan schema jika perlu (MMSR.staging, MMSR.mart, MMSR.dimensions)
-- ============================================

-- ============================================
-- 1. RAW LAYER (sumber dari harvesters)
-- ============================================
SELECT '1_RAW' as layer, 'FusionSolar historical' as object_name,
    COUNT(*)::bigint as total_rows,
    MIN(collect_time)::text as min_timestamp,
    MAX(collect_time)::text as max_timestamp,
    COUNT(DISTINCT dev_id) as unique_devices,
    NULL::bigint as unique_metrics
FROM raw.fusionsolar_historical_data
UNION ALL
SELECT '1_RAW', 'iSolarCloud historical',
    COUNT(*)::bigint,
    MIN(timestamp)::text,
    MAX(timestamp)::text,
    COUNT(DISTINCT device_ps_key),
    NULL
FROM raw.isolarcloud_historical_data
UNION ALL
SELECT '1_RAW', 'FusionSolar devices',
    COUNT(*)::bigint, NULL, NULL, NULL, NULL
FROM raw.fusionsolar_devices
UNION ALL
SELECT '1_RAW', 'iSolarCloud devices',
    COUNT(*)::bigint, NULL, NULL, NULL, NULL
FROM raw.isolarcloud_devices;


-- ============================================
-- 2. STAGING LAYER - Ringkasan per tabel
-- ============================================
SELECT '2_STAGING' as layer, 'stg_fusionsolar__perf_unpivoted' as object_name,
    COUNT(*)::bigint as total_rows,
    MIN(timestamp)::text as min_timestamp,
    MAX(timestamp)::text as max_timestamp,
    COUNT(DISTINCT dev_id) as unique_devices,
    COUNT(DISTINCT metric_id) as unique_metrics
FROM "MMSR"."staging"."stg_fusionsolar__perf_unpivoted"
UNION ALL
SELECT '2_STAGING', 'stg_isolarcloud__perf_unpivoted',
    COUNT(*)::bigint,
    MIN(timestamp)::text,
    MAX(timestamp)::text,
    COUNT(DISTINCT device_ps_key),
    COUNT(DISTINCT metric_id)
FROM "MMSR"."staging"."stg_isolarcloud__perf_unpivoted";


-- ============================================
-- 3. STAGING - Jumlah data per device type
-- ============================================
-- FusionSolar: inverter (1), sensor (10), meter (17)
WITH fs_by_type AS (
    SELECT 
        CASE d.dev_type_id
            WHEN 1 THEN 'Inverter'
            WHEN 10 THEN 'Sensor'
            WHEN 17 THEN 'Meter'
            ELSE 'Other'
        END as device_type,
        COUNT(*)::bigint as row_count,
        COUNT(DISTINCT p.dev_id) as device_count,
        COUNT(DISTINCT p.metric_id) as metric_count,
        MIN(p.timestamp)::text as min_ts,
        MAX(p.timestamp)::text as max_ts
    FROM "MMSR"."staging"."stg_fusionsolar__perf_unpivoted" p
    JOIN "MMSR"."staging"."stg_fusionsolar__devices" d ON p.dev_id = d.dev_id
    GROUP BY d.dev_type_id
),
iso_by_type AS (
    SELECT 
        CASE d.device_type
            WHEN 1 THEN 'Inverter'
            WHEN 5 THEN 'Sensor'
            WHEN 7 THEN 'Meter'
            ELSE 'Other'
        END as device_type,
        COUNT(*)::bigint as row_count,
        COUNT(DISTINCT p.device_ps_key) as device_count,
        COUNT(DISTINCT p.metric_id) as metric_count,
        MIN(p.timestamp)::text as min_ts,
        MAX(p.timestamp)::text as max_ts
    FROM "MMSR"."staging"."stg_isolarcloud__perf_unpivoted" p
    JOIN "MMSR"."staging"."stg_isolarcloud__devices" d ON p.device_ps_key = d.device_ps_key
    GROUP BY d.device_type
)
SELECT '2_STAGING_BY_TYPE' as layer, 'FusionSolar_' || device_type as object_name,
    row_count as total_rows, min_ts as min_timestamp, max_ts as max_timestamp,
    device_count as unique_devices, metric_count as unique_metrics
FROM fs_by_type
UNION ALL
SELECT '2_STAGING_BY_TYPE', 'iSolarCloud_' || device_type,
    row_count, min_ts, max_ts, device_count, metric_count
FROM iso_by_type
ORDER BY object_name;


-- ============================================
-- 4. DIMENSIONS
-- ============================================
SELECT '4_DIMENSIONS' as layer, 'dim_assets' as object_name,
    COUNT(*)::bigint as total_rows,
    NULL as min_timestamp, NULL as max_timestamp,
    COUNT(DISTINCT asset_id) as unique_assets,
    NULL::bigint as unique_metrics
FROM "MMSR"."dimensions"."dim_assets"
UNION ALL
SELECT '4_DIMENSIONS', 'dim_date_generated',
    COUNT(*)::bigint, NULL, NULL, NULL, NULL
FROM "MMSR"."dimensions"."dim_date_generated";


-- ============================================
-- 5. MART LAYER - Tabel 5 menit
-- ============================================
SELECT '5_MART_5MIN' as layer, 'mart_inverter_performance_5min' as object_name,
    COUNT(*)::bigint as total_rows,
    MIN(timestamp)::text as min_timestamp,
    MAX(timestamp)::text as max_timestamp,
    COUNT(DISTINCT asset_id) as unique_assets,
    COUNT(DISTINCT metric_id) as unique_metrics
FROM "MMSR"."mart"."mart_inverter_performance_5min"
UNION ALL
SELECT '5_MART_5MIN', 'mart_sensor_measurements_5min',
    COUNT(*)::bigint, MIN(timestamp)::text, MAX(timestamp)::text,
    COUNT(DISTINCT asset_id), COUNT(DISTINCT metric_id)
FROM "MMSR"."mart"."mart_sensor_measurements_5min"
UNION ALL
SELECT '5_MART_5MIN', 'mart_meter_performance_5min',
    COUNT(*)::bigint, MIN(timestamp)::text, MAX(timestamp)::text,
    COUNT(DISTINCT asset_id), COUNT(DISTINCT metric_id)
FROM "MMSR"."mart"."mart_meter_performance_5min";


-- ============================================
-- 6. MART 5MIN - Per system (FusionSolar vs iSolarCloud)
-- ============================================
SELECT 
    '5_MART_5MIN_BY_SYSTEM' as layer,
    'mart_inverter_performance_5min' as mart_name,
    system,
    COUNT(*)::bigint as total_rows,
    MIN(timestamp)::text as min_timestamp,
    MAX(timestamp)::text as max_timestamp,
    COUNT(DISTINCT asset_id) as unique_assets,
    COUNT(DISTINCT metric_id) as unique_metrics
FROM "MMSR"."mart"."mart_inverter_performance_5min"
GROUP BY system
UNION ALL
SELECT '5_MART_5MIN_BY_SYSTEM', 'mart_sensor_measurements_5min', system,
    COUNT(*)::bigint, MIN(timestamp)::text, MAX(timestamp)::text,
    COUNT(DISTINCT asset_id), COUNT(DISTINCT metric_id)
FROM "MMSR"."mart"."mart_sensor_measurements_5min"
GROUP BY system
UNION ALL
SELECT '5_MART_5MIN_BY_SYSTEM', 'mart_meter_performance_5min', system,
    COUNT(*)::bigint, MIN(timestamp)::text, MAX(timestamp)::text,
    COUNT(DISTINCT asset_id), COUNT(DISTINCT metric_id)
FROM "MMSR"."mart"."mart_meter_performance_5min"
GROUP BY system
ORDER BY mart_name, system;


-- ============================================
-- 7. MART LAYER - Tabel daily
-- ============================================
SELECT '7_MART_DAILY' as layer, 'mart_site_performance_daily' as object_name,
    COUNT(*)::bigint as total_rows,
    MIN(date_key)::text as min_timestamp,
    MAX(date_key)::text as max_timestamp,
    COUNT(DISTINCT site_id) as unique_sites,
    NULL::bigint as unique_metrics
FROM "MMSR"."mart"."mart_site_performance_daily"
UNION ALL
SELECT '7_MART_DAILY', 'mart_sensor_daily',
    COUNT(*)::bigint, MIN(date_key)::text, MAX(date_key)::text,
    COUNT(DISTINCT asset_id), NULL
FROM "MMSR"."mart"."mart_sensor_daily"
UNION ALL
SELECT '7_MART_DAILY', 'mart_inverter_yield_daily',
    COUNT(*)::bigint, MIN(date_key)::text, MAX(date_key)::text,
    COUNT(DISTINCT asset_id), NULL
FROM "MMSR"."mart"."mart_inverter_yield_daily"
UNION ALL
SELECT '7_MART_DAILY', 'mart_simulation_targets_daily',
    COUNT(*)::bigint, MIN(date_key)::text, MAX(date_key)::text,
    COUNT(DISTINCT site_code), NULL
FROM "MMSR"."mart"."mart_simulation_targets_daily";


-- ============================================
-- 8. MART - Site performance daily per site (ringkasan)
-- ============================================
SELECT 
    site_id,
    site_name,
    system,
    MIN(date_key) as first_date,
    MAX(date_key) as last_date,
    COUNT(DISTINCT date_key) as days_with_data,
    ROUND(SUM(daily_energy_mwh)::numeric, 2) as total_energy_mwh
FROM "MMSR"."mart"."mart_site_performance_daily"
GROUP BY site_id, site_name, system
ORDER BY system, site_name;


-- ============================================
-- 9. MART LAYER - Monthly & weekly
-- ============================================
SELECT '9_MART_OTHER' as layer, 'mart_site_performance_monthly' as object_name,
    COUNT(*)::bigint as total_rows,
    NULL as min_timestamp, NULL as max_timestamp,
    COUNT(DISTINCT site_id) as unique_sites,
    NULL::bigint as unique_metrics
FROM "MMSR"."mart"."mart_site_performance_monthly"
UNION ALL
SELECT '9_MART_OTHER', 'mart_site_kpi_monthly',
    COUNT(*)::bigint, NULL, NULL, NULL, NULL
FROM "MMSR"."mart"."mart_site_kpi_monthly"
UNION ALL
SELECT '9_MART_OTHER', 'mart_simulation_targets_monthly',
    COUNT(*)::bigint, NULL, NULL, NULL, NULL
FROM "MMSR"."mart"."mart_simulation_targets_monthly"
UNION ALL
SELECT '9_MART_OTHER', 'mart_weekly_log',
    COUNT(*)::bigint, NULL, NULL, NULL, NULL
FROM "MMSR"."mart"."mart_weekly_log";


-- ============================================
-- 10. DATA QUALITY - Cek null / invalid di mart 5min
-- ============================================
SELECT 
    '10_QUALITY' as check_type,
    'mart_inverter_5min' as table_name,
    COUNT(*) FILTER (WHERE metric_value IS NULL) as null_metric_count,
    COUNT(*) FILTER (WHERE metric_value < -1e6 OR metric_value > 1e9) as outlier_likely_count,
    COUNT(*) as total_rows
FROM "MMSR"."mart"."mart_inverter_performance_5min"
UNION ALL
SELECT '10_QUALITY', 'mart_sensor_5min',
    COUNT(*) FILTER (WHERE metric_value IS NULL),
    COUNT(*) FILTER (WHERE metric_value < -1e6 OR metric_value > 1e9),
    COUNT(*)
FROM "MMSR"."mart"."mart_sensor_measurements_5min"
UNION ALL
SELECT '10_QUALITY', 'mart_meter_5min',
    COUNT(*) FILTER (WHERE metric_value IS NULL),
    COUNT(*) FILTER (WHERE metric_value < -1e6 OR metric_value > 1e9),
    COUNT(*)
FROM "MMSR"."mart"."mart_meter_performance_5min";


-- ============================================
-- 11. COVERAGE - Tanggal terakhir data per mart 5min
-- ============================================
SELECT 
    'mart_inverter_performance_5min' as table_name,
    system,
    MAX(timestamp) as last_timestamp,
    MAX(date_key) as last_date_key
FROM "MMSR"."mart"."mart_inverter_performance_5min"
GROUP BY system
UNION ALL
SELECT 'mart_sensor_measurements_5min', system, MAX(timestamp), MAX(date_key)
FROM "MMSR"."mart"."mart_sensor_measurements_5min"
GROUP BY system
UNION ALL
SELECT 'mart_meter_performance_5min', system, MAX(timestamp), MAX(date_key)
FROM "MMSR"."mart"."mart_meter_performance_5min"
GROUP BY system
ORDER BY table_name, system;


-- ============================================
-- 12. FAKT TABLES (calculated metrics)
-- ============================================
SELECT '12_FACTS' as layer, 'fact_inverter_calculations_5min' as object_name,
    COUNT(*)::bigint as total_rows,
    MIN(timestamp)::text as min_timestamp,
    MAX(timestamp)::text as max_timestamp,
    COUNT(DISTINCT inverter_id) as unique_assets,
    NULL::bigint as unique_metrics
FROM "MMSR"."mart"."fact_inverter_calculations_5min"
UNION ALL
SELECT '12_FACTS', 'fact_sensor_calculations_5min',
    COUNT(*)::bigint, MIN(timestamp)::text, MAX(timestamp)::text,
    COUNT(DISTINCT sensor_id), NULL
FROM "MMSR"."mart"."fact_sensor_calculations_5min"
UNION ALL
SELECT '12_FACTS', 'fact_site_calculations_5min',
    COUNT(*)::bigint, MIN(timestamp)::text, MAX(timestamp)::text,
    COUNT(DISTINCT site_id), NULL
FROM "MMSR"."mart"."fact_site_calculations_5min";


-- ============================================
-- 13. DATA SIZE (storage per tabel)
-- ============================================
-- Total size = tabel + index (pg_total_relation_size)
-- Schema: sesuaikan jika pakai schema lain (raw, staging, mart, dimensions)
SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size_pretty,
    pg_total_relation_size(c.oid) AS total_size_bytes
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname IN ('raw', 'staging', 'mart', 'dimensions')
ORDER BY pg_total_relation_size(c.oid) DESC;


-- ============================================
-- 14. DATA SIZE - Ringkasan per schema
-- ============================================
SELECT
    n.nspname AS schema_name,
    pg_size_pretty(SUM(pg_total_relation_size(c.oid))) AS total_size_pretty,
    SUM(pg_total_relation_size(c.oid)) AS total_size_bytes,
    COUNT(*) AS table_count
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname IN ('raw', 'staging', 'mart', 'dimensions')
GROUP BY n.nspname
ORDER BY SUM(pg_total_relation_size(c.oid)) DESC;
