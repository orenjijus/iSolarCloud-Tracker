-- ============================================
-- Diagnose why data for 2025-12-15 is not appearing in mart_site_performance_daily
-- ============================================

-- 1. Check raw.isolarcloud_historical_data for 2025-12-15
SELECT 
    '1. Raw iSolarCloud Historical Data' as layer,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp,
    COUNT(DISTINCT device_ps_key) as device_count
FROM "MMSR"."raw"."isolarcloud_historical_data" h
WHERE DATE(h.timestamp) = '2025-12-15';

-- 2. Check staging.stg_isolarcloud__perf_unpivoted for 2025-12-15
SELECT 
    '2. Staging Perf Unpivoted' as layer,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp,
    COUNT(DISTINCT device_ps_key) as device_count,
    COUNT(DISTINCT metric_id) as metric_count
FROM "MMSR"."staging"."stg_isolarcloud__perf_unpivoted" p
WHERE DATE(p.timestamp) = '2025-12-15';

-- 3. Check MAX timestamp in staging to see if incremental would pick it up
SELECT 
    '3. Staging MAX Timestamp Check' as layer,
    MAX(timestamp) as max_timestamp,
    DATE(MAX(timestamp)) as max_date,
    CASE 
        WHEN MAX(timestamp) >= '2025-12-15 00:00:00'::timestamp THEN 'Data exists for 2025-12-15'
        ELSE 'No data for 2025-12-15'
    END as status
FROM "MMSR"."staging"."stg_isolarcloud__perf_unpivoted";

-- 4. Check mart_meter_performance_5min for 2025-12-15
SELECT 
    '4. Mart Meter Performance 5min' as layer,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp,
    COUNT(DISTINCT asset_id) as meter_count,
    COUNT(DISTINCT site_name) as site_count
FROM "MMSR"."mart"."mart_meter_performance_5min" m
WHERE DATE(m.timestamp) = '2025-12-15'
    AND m.system = 'isolarcloud';

-- 5. Check MAX timestamp in mart_meter_performance_5min for isolarcloud
SELECT 
    '5. Mart Meter MAX Timestamp (iSolarCloud)' as layer,
    MAX(timestamp) as max_timestamp,
    DATE(MAX(timestamp)) as max_date,
    CASE 
        WHEN MAX(timestamp) >= '2025-12-15 00:00:00'::timestamp THEN 'Data exists for 2025-12-15'
        ELSE 'No data for 2025-12-15'
    END as status
FROM "MMSR"."mart"."mart_meter_performance_5min"
WHERE system = 'isolarcloud';

-- 6. Check mart_sensor_measurements_5min for 2025-12-15
SELECT 
    '6. Mart Sensor Measurements 5min' as layer,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp,
    COUNT(DISTINCT asset_id) as sensor_count,
    COUNT(DISTINCT site_name) as site_count
FROM "MMSR"."mart"."mart_sensor_measurements_5min" s
WHERE DATE(s.timestamp) = '2025-12-15'
    AND s.system = 'isolarcloud';

-- 7. Check mart_sensor_daily for 2025-12-15
SELECT 
    '7. Mart Sensor Daily' as layer,
    COUNT(*) as row_count,
    MIN(date_key) as min_date,
    MAX(date_key) as max_date,
    COUNT(DISTINCT asset_id) as sensor_count,
    COUNT(DISTINCT site_name) as site_count
FROM "MMSR"."mart"."mart_sensor_daily" sd
WHERE sd.date_key = '2025-12-15';

-- 8. Check MAX date_key in mart_sensor_daily
SELECT 
    '8. Mart Sensor Daily MAX Date' as layer,
    MAX(date_key) as max_date_key,
    CASE 
        WHEN MAX(date_key) >= '2025-12-15'::date THEN 'Data exists for 2025-12-15'
        ELSE 'No data for 2025-12-15'
    END as status
FROM "MMSR"."mart"."mart_sensor_daily";

-- 9. Check fact_site_calculations_5min for 2025-12-15
SELECT 
    '9. Fact Site Calculations 5min' as layer,
    COUNT(*) as row_count,
    MIN(date_key) as min_date,
    MAX(date_key) as max_date,
    COUNT(DISTINCT site_name) as site_count
FROM "MMSR"."mart"."fact_site_calculations_5min" fsc
WHERE fsc.date_key = '2025-12-15';

-- 10. Check MAX date_key in fact_site_calculations_5min
SELECT 
    '10. Fact Site Calculations MAX Date' as layer,
    MAX(date_key) as max_date_key,
    CASE 
        WHEN MAX(date_key) >= '2025-12-15'::date THEN 'Data exists for 2025-12-15'
        ELSE 'No data for 2025-12-15'
    END as status
FROM "MMSR"."mart"."fact_site_calculations_5min";

-- 11. Check mart_site_performance_daily for 2025-12-15
SELECT 
    '11. Mart Site Performance Daily' as layer,
    COUNT(*) as row_count,
    MIN(date_key) as min_date,
    MAX(date_key) as max_date,
    COUNT(DISTINCT site_name) as site_count,
    SUM(CASE WHEN daily_energy_mwh IS NOT NULL THEN 1 ELSE 0 END) as sites_with_energy,
    SUM(CASE WHEN daily_ghi_kwh_m2 IS NOT NULL THEN 1 ELSE 0 END) as sites_with_ghi
FROM "MMSR"."mart"."mart_site_performance_daily" spd
WHERE spd.date_key = '2025-12-15';

-- 12. Check MAX date_key in mart_site_performance_daily (THIS IS THE KEY CHECK)
SELECT 
    '12. Mart Site Performance Daily MAX Date' as layer,
    MAX(date_key) as max_date_key,
    CASE 
        WHEN MAX(date_key) >= '2025-12-15'::date THEN 'Data exists for 2025-12-15 (but might be NULL)'
        WHEN MAX(date_key) = '2025-12-14'::date THEN 'MAX is 2025-12-14, incremental should process 2025-12-15'
        ELSE CONCAT('MAX is ', MAX(date_key)::text, ', incremental should process 2025-12-15')
    END as status
FROM "MMSR"."mart"."mart_site_performance_daily";

-- 13. Check if there are any rows for 2025-12-15 with NULL values (incremental might have created empty rows)
SELECT 
    '13. Mart Site Performance Daily - 2025-12-15 Rows (including NULLs)' as layer,
    date_key,
    site_name,
    daily_energy_mwh,
    daily_ghi_kwh_m2,
    daily_poa_weighted_kwh_m2,
    availability_percent
FROM "MMSR"."mart"."mart_site_performance_daily" spd
WHERE spd.date_key = '2025-12-15'
ORDER BY site_name;

-- 14. Check surrounding dates for context (14, 15, 16 Dec)
SELECT 
    '14. Mart Site Performance Daily - Context (14-16 Dec)' as layer,
    date_key,
    COUNT(*) as site_count,
    SUM(CASE WHEN daily_energy_mwh IS NOT NULL THEN 1 ELSE 0 END) as sites_with_energy,
    SUM(CASE WHEN daily_ghi_kwh_m2 IS NOT NULL THEN 1 ELSE 0 END) as sites_with_ghi
FROM "MMSR"."mart"."mart_site_performance_daily" spd
WHERE spd.date_key BETWEEN '2025-12-14' AND '2025-12-16'
GROUP BY date_key
ORDER BY date_key;

