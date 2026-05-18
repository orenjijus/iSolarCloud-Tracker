-- Export POA Data for Excel Comparison - November 2025
-- This query outputs data in a format easy to copy-paste to Excel
-- 
-- Instructions:
-- 1. Change site_name to your target site
-- 2. Run the query
-- 3. Copy results to Excel
-- 4. Compare with your Excel calculations

-- ============================================
-- FORMAT 1: Daily Summary (One row per day)
-- Best for: Comparing daily weighted average POA with Excel
-- ============================================
SELECT 
    date_key as "Date",
    site_name as "Site Name",
    -- Weighted Average POA (main value to compare)
    ROUND(
        (SUM(daily_irradiance_kwh_m2 * sensor_capacity_kwp) / NULLIF(SUM(sensor_capacity_kwp), 0))::numeric, 
        6
    ) as "Weighted Avg POA (kWh/m²)",
    -- Supporting data
    ROUND(SUM(sensor_capacity_kwp)::numeric, 2) as "Total Capacity (kWp)",
    COUNT(DISTINCT asset_id) as "Sensor Count",
    ROUND(MIN(daily_irradiance_kwh_m2)::numeric, 6) as "Min POA (kWh/m²)",
    ROUND(MAX(daily_irradiance_kwh_m2)::numeric, 6) as "Max POA (kWh/m²)",
    ROUND(AVG(daily_irradiance_kwh_m2)::numeric, 6) as "Simple Avg POA (kWh/m²)",
    -- Calculation components for verification
    ROUND(SUM(daily_irradiance_kwh_m2 * sensor_capacity_kwp)::numeric, 2) as "Sum (POA × Capacity)",
    ROUND(SUM(sensor_capacity_kwp)::numeric, 2) as "Sum Capacity"
FROM mart.mart_sensor_daily
WHERE sensor_type = 'POA'
    AND site_name = 'Garuda Metalindo 1'  -- ⚠️ CHANGE THIS to your target site
    AND date_key >= '2025-11-01'::date
    AND date_key <= '2025-11-30'::date
    AND daily_irradiance_kwh_m2 IS NOT NULL
    AND sensor_capacity_kwp IS NOT NULL
GROUP BY date_key, site_name
ORDER BY date_key;

-- ============================================
-- FORMAT 2: Per Device Per Day (Pivot-friendly)
-- Best for: Creating pivot table in Excel to verify calculations
-- ============================================
/*
SELECT 
    date_key as "Date",
    site_name as "Site Name",
    sensor_dev_name as "Device Name",
    asset_id as "Asset ID",
    ROUND(daily_irradiance_kwh_m2::numeric, 6) as "POA (kWh/m²)",
    ROUND(sensor_capacity_kwp::numeric, 2) as "Capacity (kWp)",
    ROUND((daily_irradiance_kwh_m2 * sensor_capacity_kwp)::numeric, 2) as "Weighted Contribution",
    measurement_count as "Measurement Count"
FROM mart.mart_sensor_daily
WHERE sensor_type = 'POA'
    AND site_name = 'Garuda Metalindo 1'  -- ⚠️ CHANGE THIS to your target site
    AND date_key >= '2025-11-01'::date
    AND date_key <= '2025-11-30'::date
    AND daily_irradiance_kwh_m2 IS NOT NULL
    AND sensor_capacity_kwp IS NOT NULL
ORDER BY date_key, sensor_dev_name;
*/

-- ============================================
-- FORMAT 3: Cross-tab (Devices as columns)
-- Best for: Visual comparison of device values side-by-side
-- ============================================
/*
SELECT 
    date_key as "Date",
    site_name as "Site Name",
    MAX(CASE WHEN sensor_dev_name = 'IRR-NE-B' THEN ROUND(daily_irradiance_kwh_m2::numeric, 6) END) as "IRR-NE-B POA",
    MAX(CASE WHEN sensor_dev_name = 'IRR-NW-A' THEN ROUND(daily_irradiance_kwh_m2::numeric, 6) END) as "IRR-NW-A POA",
    MAX(CASE WHEN sensor_dev_name = 'IRR-SE-A' THEN ROUND(daily_irradiance_kwh_m2::numeric, 6) END) as "IRR-SE-A POA",
    MAX(CASE WHEN sensor_dev_name = 'IRR-SW-B' THEN ROUND(daily_irradiance_kwh_m2::numeric, 6) END) as "IRR-SW-B POA",
    -- Weighted average
    ROUND(
        (SUM(daily_irradiance_kwh_m2 * sensor_capacity_kwp) / NULLIF(SUM(sensor_capacity_kwp), 0))::numeric, 
        6
    ) as "Weighted Avg POA"
FROM mart.mart_sensor_daily
WHERE sensor_type = 'POA'
    AND site_name = 'Garuda Metalindo 1'  -- ⚠️ CHANGE THIS to your target site
    AND date_key >= '2025-11-01'::date
    AND date_key <= '2025-11-30'::date
    AND daily_irradiance_kwh_m2 IS NOT NULL
    AND sensor_capacity_kwp IS NOT NULL
GROUP BY date_key, site_name
ORDER BY date_key;
*/

