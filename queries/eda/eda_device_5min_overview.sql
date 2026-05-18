-- EDA Device 5min: Overview (meter, sensor, inverter) — EXCLUDE Samator & Klinik
-- Purpose: Data coverage per device type for ML. Filter: site_name NOT ILIKE '%Samator%' AND NOT ILIKE '%Klinik%'.

-- 1. Meter 5min: total, date range, sites, assets
SELECT
    'meter' AS device_type,
    COUNT(*) AS total_rows,
    MIN(timestamp) AS min_ts,
    MAX(timestamp) AS max_ts,
    COUNT(DISTINCT site_name) AS num_sites,
    COUNT(DISTINCT asset_id) AS num_assets
FROM mart.mart_meter_performance_5min
WHERE site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%';

-- 2. Sensor 5min: total, date range, sites, assets
SELECT
    'sensor' AS device_type,
    COUNT(*) AS total_rows,
    MIN(timestamp) AS min_ts,
    MAX(timestamp) AS max_ts,
    COUNT(DISTINCT site_name) AS num_sites,
    COUNT(DISTINCT asset_id) AS num_assets
FROM mart.mart_sensor_measurements_5min
WHERE site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%';

-- 3. Inverter 5min: total, date range, sites, assets
SELECT
    'inverter' AS device_type,
    COUNT(*) AS total_rows,
    MIN(timestamp) AS min_ts,
    MAX(timestamp) AS max_ts,
    COUNT(DISTINCT site_name) AS num_sites,
    COUNT(DISTINCT asset_id) AS num_assets
FROM mart.mart_inverter_performance_5min
WHERE site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%';

-- 4. Meter: rows per site (excl Samator/Klinik)
SELECT
    site_name,
    system,
    COUNT(DISTINCT asset_id) AS num_assets,
    COUNT(*) AS total_rows,
    MIN(date_key) AS first_date,
    MAX(date_key) AS last_date
FROM mart.mart_meter_performance_5min
WHERE site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%'
GROUP BY site_name, system
ORDER BY total_rows DESC;

-- 5. Sensor: rows per site (excl Samator/Klinik)
SELECT
    site_name,
    system,
    COUNT(DISTINCT asset_id) AS num_assets,
    COUNT(*) AS total_rows,
    MIN(date_key) AS first_date,
    MAX(date_key) AS last_date
FROM mart.mart_sensor_measurements_5min
WHERE site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%'
GROUP BY site_name, system
ORDER BY total_rows DESC;

-- 6. Inverter: rows per site (excl Samator/Klinik)
SELECT
    site_name,
    system,
    COUNT(DISTINCT asset_id) AS num_assets,
    COUNT(*) AS total_rows,
    MIN(date_key) AS first_date,
    MAX(date_key) AS last_date
FROM mart.mart_inverter_performance_5min
WHERE site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%'
GROUP BY site_name, system
ORDER BY total_rows DESC;
