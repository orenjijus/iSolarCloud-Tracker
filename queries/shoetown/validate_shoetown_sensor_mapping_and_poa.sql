-- ============================================
-- Shoetown Sensor Mapping & POA Validation
-- ============================================
-- This query validates:
-- 1. Grid connection dates for all Shoetown sensors
-- 2. Sensor replacement timeline
-- 3. Daily POA per sensor vs weighted average
-- ============================================

-- ============================================
-- 1. Check Grid Connection Dates for All Shoetown Sensors
-- ============================================
WITH sensor_grid_dates AS (
    SELECT 
        d.device_ps_key,
        d.device_name,
        d.grid_connection_date,
        d.device_type,
        d.type_name,
        sc.device_id,
        sc.dev_name,
        sc.sensor_type,
        sc.sensor_capacity,
        CASE 
            WHEN sc.device_id LIKE '%1479456%' THEN 
                CONCAT('ISO_', sc.device_id)
            ELSE sc.device_id
        END as full_device_id
    FROM raw.isolarcloud_devices d
    LEFT JOIN staging.seed_sensor_config sc 
        ON CONCAT('ISO_', d.device_ps_key) = sc.device_id
    WHERE d.ps_id = '1479456'
        AND (
            d.device_name LIKE '%IRR%' 
            OR d.device_name LIKE '%PYR%'
            OR d.device_name LIKE '%Meteo%'
            OR sc.sensor_type IN ('POA', 'GHI')
        )
)
SELECT 
    '=== GRID CONNECTION DATES ===' as section,
    device_name,
    full_device_id as device_id,
    sensor_type,
    sensor_capacity,
    grid_connection_date,
    CASE 
        WHEN grid_connection_date IS NULL THEN 'NO_DATE'
        WHEN device_name LIKE '%old%' OR device_name LIKE '%Old%' THEN 'OLD_SENSOR'
        ELSE 'NEW_SENSOR'
    END as sensor_status
FROM sensor_grid_dates
ORDER BY 
    grid_connection_date NULLS LAST,
    device_name;

-- ============================================
-- 2. Daily POA Per Sensor (for validation)
-- ============================================
WITH daily_poa_per_sensor AS (
    SELECT 
        date_key,
        sensor_dev_name,
        device_id,
        sensor_capacity_kwp,
        daily_irradiance_kwh_m2,
        daily_irradiance_kwh_m2 * sensor_capacity_kwp as weighted_contribution
    FROM mart.mart_sensor_daily
    WHERE sensor_type = 'POA'
        AND site_name = 'Shoetown Ligung Indonesia'
        AND date_key >= '2025-10-01'::date
        AND daily_irradiance_kwh_m2 IS NOT NULL
        AND sensor_capacity_kwp IS NOT NULL
)
SELECT 
    '=== DAILY POA PER SENSOR ===' as section,
    date_key,
    sensor_dev_name,
    device_id,
    ROUND(sensor_capacity_kwp::numeric, 2) as capacity_kwp,
    ROUND(daily_irradiance_kwh_m2::numeric, 6) as poa_kwh_m2,
    ROUND(weighted_contribution::numeric, 2) as weighted_contribution
FROM daily_poa_per_sensor
ORDER BY date_key, sensor_dev_name;

-- ============================================
-- 3. Daily Weighted Average POA (for comparison with Excel)
-- ============================================
WITH daily_poa_summary AS (
    SELECT 
        date_key,
        SUM(daily_irradiance_kwh_m2 * sensor_capacity_kwp) as sum_poa_x_capacity,
        SUM(sensor_capacity_kwp) as sum_capacity,
        SUM(daily_irradiance_kwh_m2 * sensor_capacity_kwp) / NULLIF(SUM(sensor_capacity_kwp), 0) as weighted_avg_poa,
        COUNT(DISTINCT device_id) as sensor_count,
        STRING_AGG(DISTINCT sensor_dev_name, ', ' ORDER BY sensor_dev_name) as active_sensors
    FROM mart.mart_sensor_daily
    WHERE sensor_type = 'POA'
        AND site_name = 'Shoetown Ligung Indonesia'
        AND date_key >= '2025-10-01'::date
        AND daily_irradiance_kwh_m2 IS NOT NULL
        AND sensor_capacity_kwp IS NOT NULL
    GROUP BY date_key
)
SELECT 
    '=== DAILY WEIGHTED AVERAGE POA ===' as section,
    date_key,
    ROUND(sum_poa_x_capacity::numeric, 2) as sum_poa_x_capacity,
    ROUND(sum_capacity::numeric, 2) as sum_capacity,
    ROUND(weighted_avg_poa::numeric, 6) as weighted_avg_poa,
    sensor_count,
    active_sensors
FROM daily_poa_summary
ORDER BY date_key;

