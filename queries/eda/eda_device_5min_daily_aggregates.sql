-- EDA Device 5min: Daily aggregates for notebook (load without full 5min dump)
-- EXCLUDE Samator & Klinik. Use in Python: pd.read_sql(..., engine) per query.

-- Meter: daily per (site_name, asset_id, metric_name) — count, avg, min, max
SELECT
    date_key,
    site_name,
    system,
    asset_id,
    metric_name,
    metric_unit,
    COUNT(*) AS n_points,
    ROUND(AVG(metric_value)::numeric, 6) AS avg_value,
    MIN(metric_value) AS min_value,
    MAX(metric_value) AS max_value
FROM mart.mart_meter_performance_5min
WHERE site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%'
  AND metric_value IS NOT NULL
GROUP BY date_key, site_name, system, asset_id, metric_name, metric_unit
ORDER BY date_key, site_name, asset_id, metric_name;

-- Sensor: daily per (site_name, asset_id, metric_name)
SELECT
    date_key,
    site_name,
    system,
    asset_id,
    metric_name,
    metric_unit,
    COUNT(*) AS n_points,
    ROUND(AVG(metric_value)::numeric, 6) AS avg_value,
    MIN(metric_value) AS min_value,
    MAX(metric_value) AS max_value
FROM mart.mart_sensor_measurements_5min
WHERE site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%'
  AND metric_value IS NOT NULL
GROUP BY date_key, site_name, system, asset_id, metric_name, metric_unit
ORDER BY date_key, site_name, asset_id, metric_name;

-- Inverter: daily per (site_name, asset_id, metric_name)
SELECT
    date_key,
    site_name,
    system,
    asset_id,
    metric_name,
    metric_unit,
    COUNT(*) AS n_points,
    ROUND(AVG(metric_value)::numeric, 6) AS avg_value,
    MIN(metric_value) AS min_value,
    MAX(metric_value) AS max_value
FROM mart.mart_inverter_performance_5min
WHERE site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%'
  AND metric_value IS NOT NULL
GROUP BY date_key, site_name, system, asset_id, metric_name, metric_unit
ORDER BY date_key, site_name, asset_id, metric_name;
