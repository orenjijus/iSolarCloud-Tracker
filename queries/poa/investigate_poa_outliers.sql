-- ============================================
-- Investigate POA Outliers - Non-Systematic Sites
-- ============================================
-- Focus on PLTS Rooftop Sumatera Prima and Shoetown
-- ============================================

-- ============================================
-- 1. PLTS Rooftop Sumatera Prima - Outlier Days
-- ============================================
WITH db_data AS (
    SELECT 
        d.date_key,
        d.daily_poa_weighted_kwh_m2
    FROM "MMSR"."mart"."mart_site_performance_daily" d
    WHERE d.site_name = 'PLTS Rooftop Sumatera Prima Fibreboard'
),
excel_data AS (
    SELECT 
        TO_DATE(date_key, 'MM/DD/YYYY') as date_key,
        poa_actual_kwh_m2::NUMERIC as daily_poa_weighted_kwh_m2
    FROM public.site_daily_performance_excel_except_mmki
    WHERE site_name = 'PLTS Rooftop Sumatera Prima Fibreboard'
)
SELECT 
    COALESCE(db.date_key, excel.date_key) as date_key,
    ROUND(db.daily_poa_weighted_kwh_m2::numeric, 4) as db_poa,
    ROUND(excel.daily_poa_weighted_kwh_m2::numeric, 4) as excel_poa,
    ROUND((db.daily_poa_weighted_kwh_m2 - excel.daily_poa_weighted_kwh_m2)::numeric, 4) as diff_absolute,
    ROUND(((db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)) - 1) * 100, 2) as diff_percentage,
    ROUND((db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0))::numeric, 4) as ratio,
    CASE 
        WHEN db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0) > 2 THEN 'DB_MUCH_HIGHER (>2x)'
        WHEN db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0) < 0.5 THEN 'EXCEL_MUCH_HIGHER (>2x)'
        WHEN db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0) > 1.5 THEN 'DB_HIGHER (1.5-2x)'
        WHEN db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0) < 0.67 THEN 'EXCEL_HIGHER (1.5-2x)'
        WHEN db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0) > 1.2 THEN 'DB_HIGHER (1.2-1.5x)'
        WHEN db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0) < 0.83 THEN 'EXCEL_HIGHER (1.2-1.5x)'
        ELSE 'CLOSE'
    END as outlier_type
FROM db_data db
INNER JOIN excel_data excel
    ON db.date_key = excel.date_key
WHERE ABS(db.daily_poa_weighted_kwh_m2 - excel.daily_poa_weighted_kwh_m2) > 0.01
ORDER BY 
    ABS((db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)) - 1) DESC,
    date_key DESC;

-- ============================================
-- 2. Shoetown - Outlier Days (Excluding January)
-- ============================================
WITH db_data AS (
    SELECT 
        d.date_key,
        d.daily_poa_weighted_kwh_m2
    FROM "MMSR"."mart"."mart_site_performance_daily" d
    WHERE d.site_name = 'Shoetown Ligung Indonesia'
        AND d.date_key >= '2025-02-01'
),
excel_data AS (
    SELECT 
        TO_DATE(date_key, 'MM/DD/YYYY') as date_key,
        poa_actual_kwh_m2::NUMERIC as daily_poa_weighted_kwh_m2
    FROM public.site_daily_performance_excel_except_mmki
    WHERE site_name = 'Shoetown Ligung Indonesia'
        AND TO_DATE(date_key, 'MM/DD/YYYY') >= '2025-02-01'
)
SELECT 
    COALESCE(db.date_key, excel.date_key) as date_key,
    ROUND(db.daily_poa_weighted_kwh_m2::numeric, 4) as db_poa,
    ROUND(excel.daily_poa_weighted_kwh_m2::numeric, 4) as excel_poa,
    ROUND((db.daily_poa_weighted_kwh_m2 - excel.daily_poa_weighted_kwh_m2)::numeric, 4) as diff_absolute,
    ROUND(((db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)) - 1) * 100, 2) as diff_percentage,
    ROUND((db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0))::numeric, 4) as ratio,
    CASE 
        WHEN db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0) > 2 THEN 'DB_MUCH_HIGHER (>2x)'
        WHEN db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0) < 0.5 THEN 'EXCEL_MUCH_HIGHER (>2x)'
        WHEN db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0) > 1.3 THEN 'DB_HIGHER (1.3-2x)'
        WHEN db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0) < 0.77 THEN 'EXCEL_HIGHER (1.3-2x)'
        ELSE 'CLOSE'
    END as outlier_type
FROM db_data db
INNER JOIN excel_data excel
    ON db.date_key = excel.date_key
WHERE ABS(db.daily_poa_weighted_kwh_m2 - excel.daily_poa_weighted_kwh_m2) > 0.01
ORDER BY 
    ABS((db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)) - 1) DESC,
    date_key DESC
LIMIT 50;

-- ============================================
-- 3. Check POA Sensor Availability by Date
-- ============================================
-- For PLTS Rooftop Sumatera Prima
SELECT 
    s.date_key,
    sc.device_id,
    CASE WHEN sc.sensor_capacity IS NULL THEN 'NULL' ELSE CAST(REPLACE(sc.sensor_capacity::text, ',', '.') AS NUMERIC)::text END as capacity,
    COUNT(*) as data_points,
    MAX(CASE 
        WHEN s.system = 'fusionsolar' THEN s.metric_value / 3.6
        WHEN s.system = 'isolarcloud' AND s.metric_unit = 'Wh/㎡' THEN s.metric_value / 1000.0
        WHEN s.metric_unit = 'MJ/m²' THEN s.metric_value / 3.6
        WHEN s.metric_unit = 'W/m²' THEN s.metric_value / 1000.0
        ELSE s.metric_value
    END) as max_poa_value
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
    AND s.site_name = 'PLTS Rooftop Sumatera Prima Fibreboard'
    AND s.date_key >= '2025-10-01'
GROUP BY s.date_key, sc.device_id, sc.sensor_capacity
ORDER BY s.date_key, sc.device_id;

