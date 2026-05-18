-- EDA Device 5min: Missing metric_value (meter, sensor, inverter) — EXCLUDE Samator & Klinik

-- Meter: null metric_value per metric_name
SELECT
    metric_name,
    COUNT(*) AS total,
    COUNT(metric_value) AS non_null,
    COUNT(*) - COUNT(metric_value) AS null_count
FROM mart.mart_meter_performance_5min
WHERE site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%'
GROUP BY metric_name
ORDER BY total DESC;

-- Sensor: null metric_value per metric_name
SELECT
    metric_name,
    COUNT(*) AS total,
    COUNT(metric_value) AS non_null,
    COUNT(*) - COUNT(metric_value) AS null_count
FROM mart.mart_sensor_measurements_5min
WHERE site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%'
GROUP BY metric_name
ORDER BY total DESC;

-- Inverter: null metric_value per metric_name
SELECT
    metric_name,
    COUNT(*) AS total,
    COUNT(metric_value) AS non_null,
    COUNT(*) - COUNT(metric_value) AS null_count
FROM mart.mart_inverter_performance_5min
WHERE site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%'
GROUP BY metric_name
ORDER BY total DESC;
