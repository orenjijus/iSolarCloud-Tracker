-- Check mart_sensor_daily results
-- View POA sensors per site to debug individual device values

-- 1. Overview: POA sensors per site
SELECT 
    date_key,
    site_name,
    sensor_type,
    COUNT(DISTINCT asset_id) as sensor_count,
    COUNT(*) as total_records
FROM mart.mart_sensor_daily
WHERE sensor_type = 'POA'
GROUP BY date_key, site_name, sensor_type
ORDER BY date_key DESC, site_name
LIMIT 50;

-- 2. Detailed POA values per device (for debugging)
-- Replace 'GM 1' with your site name
SELECT 
    date_key,
    site_name,
    sensor_dev_name,
    asset_id,
    daily_irradiance_kwh_m2 as poa_kwh_m2,
    sensor_capacity_kwp,
    measurement_count,
    timestamp_count
FROM mart.mart_sensor_daily
WHERE sensor_type = 'POA'
    AND site_name = 'GM 1'  -- Change to your site
    AND date_key >= CURRENT_DATE - INTERVAL '7 days'  -- Last 7 days
ORDER BY date_key DESC, asset_id;

-- 3. Compare POA values across all sensors in a site
SELECT 
    date_key,
    site_name,
    sensor_dev_name,
    asset_id,
    daily_irradiance_kwh_m2,
    sensor_capacity_kwp,
    -- Calculate weighted contribution
    daily_irradiance_kwh_m2 * sensor_capacity_kwp as weighted_contribution
FROM mart.mart_sensor_daily
WHERE sensor_type = 'POA'
    AND site_name = 'GM 1'  -- Change to your site
    AND date_key = '2025-01-15'  -- Change to your date
ORDER BY daily_irradiance_kwh_m2 DESC;

-- 4. Check GHI sensors
SELECT 
    date_key,
    site_name,
    sensor_dev_name,
    asset_id,
    daily_irradiance_kwh_m2 as ghi_kwh_m2,
    measurement_count
FROM mart.mart_sensor_daily
WHERE sensor_type = 'GHI'
    AND date_key >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY date_key DESC, site_name, asset_id;

-- 5. Check Weather sensors
SELECT 
    date_key,
    site_name,
    sensor_dev_name,
    asset_id,
    avg_ambient_temperature_c,
    max_ambient_temperature_c,
    avg_wind_speed_m_s,
    avg_humidity_pct
FROM mart.mart_sensor_daily
WHERE sensor_type = 'Weather'
    AND date_key >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY date_key DESC, site_name, asset_id;

