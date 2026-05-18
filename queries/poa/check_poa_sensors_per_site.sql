-- ============================================
-- Check POA Sensors per Site
-- ============================================
-- To understand POA calculation differences
-- ============================================

-- Check POA sensors for sites with low POA match
SELECT 
    sc.device_id,
    sc.sensor_type,
    sc.sensor_capacity,
    s.site_name,
    COUNT(DISTINCT s.date_key) as days_with_data,
    MIN(s.date_key) as first_date,
    MAX(s.date_key) as last_date
FROM "MMSR"."mart"."mart_sensor_measurements_5min" s
JOIN "MMSR"."staging"."seed_sensor_config" sc 
    ON s.asset_id = CONCAT(
        CASE 
            WHEN s.system = 'fusionsolar' THEN 'FS'
            WHEN s.system = 'isolarcloud' THEN 'ISO'
            ELSE UPPER(LEFT(s.system, 3))
        END, '_', sc.device_id
    )
WHERE sc.sensor_type = 'POA'
    AND s.metric_name = 'daily_irradiance'
    AND s.site_name IN (
        'Shoetown Ligung Indonesia',
        'PLTS Frina Lestari Nusantara',
        'PLTS Rooftop Sumatera Prima Fibreboard',
        'Charoen Pokphand Bandung',
        'Garuda Metalindo (IKP)'
    )
    AND s.date_key >= '2025-01-01'
GROUP BY sc.device_id, sc.sensor_type, sc.sensor_capacity, s.site_name
ORDER BY s.site_name, sc.device_id;

-- Check POA weighted calculation for one day (example)
WITH poa_per_sensor AS (
    SELECT 
        s.date_key,
        s.site_name,
        sc.device_id,
        CAST(REPLACE(sc.sensor_capacity::text, ',', '.') AS NUMERIC) as poa_capacity_kwp,
        MAX(CASE 
            WHEN s.system = 'fusionsolar' THEN s.metric_value / 3.6
            WHEN s.system = 'isolarcloud' AND s.metric_unit = 'Wh/㎡' THEN s.metric_value / 1000.0
            WHEN s.metric_unit = 'MJ/m²' THEN s.metric_value / 3.6
            WHEN s.metric_unit = 'W/m²' THEN s.metric_value / 1000.0
            ELSE s.metric_value
        END) as max_daily_poa_kwh_m2
    FROM "MMSR"."mart"."mart_sensor_measurements_5min" s
    JOIN "MMSR"."staging"."seed_sensor_config" sc 
        ON s.asset_id = CONCAT(
            CASE 
                WHEN s.system = 'fusionsolar' THEN 'FS'
                WHEN s.system = 'isolarcloud' THEN 'ISO'
                ELSE UPPER(LEFT(s.system, 3))
            END, '_', sc.device_id
        )
    WHERE sc.sensor_type = 'POA'
        AND s.metric_name = 'daily_irradiance'
        AND s.site_name = 'Shoetown Ligung Indonesia'
        AND s.date_key = '2025-10-01'
    GROUP BY s.date_key, s.site_name, sc.device_id, sc.sensor_capacity
)
SELECT 
    date_key,
    site_name,
    device_id,
    poa_capacity_kwp,
    max_daily_poa_kwh_m2,
    max_daily_poa_kwh_m2 * poa_capacity_kwp as weighted_value,
    SUM(max_daily_poa_kwh_m2 * poa_capacity_kwp) OVER (PARTITION BY date_key, site_name) as total_weighted,
    SUM(poa_capacity_kwp) OVER (PARTITION BY date_key, site_name) as total_capacity,
    SUM(max_daily_poa_kwh_m2 * poa_capacity_kwp) OVER (PARTITION BY date_key, site_name) / 
        NULLIF(SUM(poa_capacity_kwp) OVER (PARTITION BY date_key, site_name), 0) as calculated_weighted_avg
FROM poa_per_sensor
ORDER BY device_id;

