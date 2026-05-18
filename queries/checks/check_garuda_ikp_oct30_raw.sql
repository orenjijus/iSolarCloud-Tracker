-- ============================================
-- Check Raw Data for Garuda Metalindo (IKP) on 2025-10-30
-- Site: ISO_SITE_1445767 (ps_id = 1445767)
-- ============================================

-- 1. Check raw.isolarcloud_historical_data
SELECT 
    'Raw iSolarCloud Historical Data' as source,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp
FROM "MMSR"."raw"."isolarcloud_historical_data" h
JOIN "MMSR"."raw"."isolarcloud_devices" d ON h.device_ps_key = d.device_ps_key
WHERE d.ps_id = 1445767
    AND DATE(h.timestamp) = '2025-10-30';

-- 2. Check staging.stg_isolarcloud__perf_unpivoted
SELECT 
    'Staging iSolarCloud Perf Unpivoted' as source,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp,
    COUNT(DISTINCT device_ps_key) as device_count,
    COUNT(DISTINCT metric_id) as metric_count
FROM "MMSR"."staging"."stg_isolarcloud__perf_unpivoted" p
JOIN "MMSR"."staging"."stg_isolarcloud__devices" d ON p.device_ps_key = d.device_ps_key
WHERE d.ps_id = 1445767
    AND DATE(p.timestamp) = '2025-10-30';

-- 3. Check mart_meter_performance_5min
SELECT 
    'Mart Meter Performance 5min' as source,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp,
    COUNT(DISTINCT asset_id) as meter_count,
    COUNT(DISTINCT metric_name) as metric_count
FROM "MMSR"."mart"."mart_meter_performance_5min" m
WHERE m.site_name = 'Garuda Metalindo (IKP)'
    AND DATE(m.timestamp) = '2025-10-30';

-- 4. Check mart_sensor_measurements_5min
SELECT 
    'Mart Sensor Measurements 5min' as source,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp,
    COUNT(DISTINCT asset_id) as sensor_count,
    COUNT(DISTINCT metric_id) as metric_count
FROM "MMSR"."mart"."mart_sensor_measurements_5min" s
WHERE s.site_name = 'Garuda Metalindo (IKP)'
    AND DATE(s.timestamp) = '2025-10-30';

-- 5. Check mart_inverter_performance_5min
SELECT 
    'Mart Inverter Performance 5min' as source,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp,
    COUNT(DISTINCT asset_id) as inverter_count,
    COUNT(DISTINCT metric_id) as metric_count
FROM "MMSR"."mart"."mart_inverter_performance_5min" i
WHERE i.site_name = 'Garuda Metalindo (IKP)'
    AND DATE(i.timestamp) = '2025-10-30';

-- 6. Check mart_site_performance_daily
SELECT 
    'Mart Site Performance Daily' as source,
    date_key,
    site_name,
    daily_energy_mwh,
    daily_ghi_kwh_m2,
    daily_poa_weighted_kwh_m2,
    availability_percent,
    power_available_hours,
    unavailability_hours
FROM "MMSR"."mart"."mart_site_performance_daily" d
WHERE d.site_name = 'Garuda Metalindo (IKP)'
    AND d.date_key = '2025-10-30';

-- 7. Check surrounding dates for context (29, 30, 31 Oct)
SELECT 
    'Mart Site Performance Daily - Context' as source,
    date_key,
    site_name,
    daily_energy_mwh,
    daily_ghi_kwh_m2,
    daily_poa_weighted_kwh_m2,
    availability_percent
FROM "MMSR"."mart"."mart_site_performance_daily" d
WHERE d.site_name = 'Garuda Metalindo (IKP)'
    AND d.date_key BETWEEN '2025-10-29' AND '2025-10-31'
ORDER BY date_key;

-- 8. Check raw data for surrounding dates
SELECT 
    'Raw iSolarCloud - Context (29-31 Oct)' as source,
    DATE(timestamp) as date_key,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp
FROM "MMSR"."raw"."isolarcloud_historical_data" h
JOIN "MMSR"."raw"."isolarcloud_devices" d ON h.device_ps_key = d.device_ps_key
WHERE d.ps_id = 1445767
    AND DATE(h.timestamp) BETWEEN '2025-10-29' AND '2025-10-31'
GROUP BY DATE(timestamp)
ORDER BY date_key;

