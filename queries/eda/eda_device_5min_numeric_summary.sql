-- EDA Device 5min: Numeric summary (avg/min/max) per metric — EXCLUDE Samator & Klinik

-- Meter: global stats per metric_name
SELECT
    metric_name,
    COUNT(*) AS n,
    ROUND(AVG(metric_value)::numeric, 4) AS avg_val,
    MIN(metric_value) AS min_val,
    MAX(metric_value) AS max_val,
    ROUND(STDDEV(metric_value)::numeric, 4) AS stddev_val
FROM mart.mart_meter_performance_5min
WHERE site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%'
  AND metric_value IS NOT NULL
GROUP BY metric_name
ORDER BY n DESC;

-- Sensor: global stats per metric_name
SELECT
    metric_name,
    COUNT(*) AS n,
    ROUND(AVG(metric_value)::numeric, 4) AS avg_val,
    MIN(metric_value) AS min_val,
    MAX(metric_value) AS max_val,
    ROUND(STDDEV(metric_value)::numeric, 4) AS stddev_val
FROM mart.mart_sensor_measurements_5min
WHERE site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%'
  AND metric_value IS NOT NULL
GROUP BY metric_name
ORDER BY n DESC;

-- Inverter: global stats per metric_name
SELECT
    metric_name,
    COUNT(*) AS n,
    ROUND(AVG(metric_value)::numeric, 4) AS avg_val,
    MIN(metric_value) AS min_val,
    MAX(metric_value) AS max_val,
    ROUND(STDDEV(metric_value)::numeric, 4) AS stddev_val
FROM mart.mart_inverter_performance_5min
WHERE site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%'
  AND metric_value IS NOT NULL
GROUP BY metric_name
ORDER BY n DESC;
