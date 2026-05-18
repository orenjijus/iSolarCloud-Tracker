-- Export POA Data in Excel-Friendly Format
-- Output format designed to be easily copied to Excel for comparison
-- 
-- Instructions:
-- 1. Change site_name to your target site
-- 2. Run the query
-- 3. Copy all results (Ctrl+A, Ctrl+C)
-- 4. Paste to Excel starting from cell A1
-- 5. Excel will auto-format columns correctly

-- ============================================
-- FORMAT A: Daily Summary (Recommended for Excel)
-- One row per day with weighted average and breakdown
-- ============================================
SELECT 
    date_key as "Date",
    site_name as "Site",
    -- Main value to compare
    ROUND(
        (SUM(daily_irradiance_kwh_m2 * sensor_capacity_kwp) / NULLIF(SUM(sensor_capacity_kwp), 0))::numeric, 
        6
    ) as "Weighted_Avg_POA",
    -- Calculation components (for verification)
    ROUND(SUM(daily_irradiance_kwh_m2 * sensor_capacity_kwp)::numeric, 2) as "Sum_POA_x_Capacity",
    ROUND(SUM(sensor_capacity_kwp)::numeric, 2) as "Sum_Capacity",
    -- Sensor info
    COUNT(DISTINCT asset_id) as "Sensor_Count",
    -- Individual device values (for cross-check)
    ROUND(MAX(CASE WHEN sensor_dev_name = 'IRR-NE-B' THEN daily_irradiance_kwh_m2 END)::numeric, 6) as "IRR_NE_B",
    ROUND(MAX(CASE WHEN sensor_dev_name = 'IRR-NW-A' THEN daily_irradiance_kwh_m2 END)::numeric, 6) as "IRR_NW_A",
    ROUND(MAX(CASE WHEN sensor_dev_name = 'IRR-SE-A' THEN daily_irradiance_kwh_m2 END)::numeric, 6) as "IRR_SE_A",
    ROUND(MAX(CASE WHEN sensor_dev_name = 'IRR-SW-B' THEN daily_irradiance_kwh_m2 END)::numeric, 6) as "IRR_SW_B",
    -- Min/Max for validation
    ROUND(MIN(daily_irradiance_kwh_m2)::numeric, 6) as "Min_POA",
    ROUND(MAX(daily_irradiance_kwh_m2)::numeric, 6) as "Max_POA"
FROM mart.mart_sensor_daily
WHERE sensor_type = 'POA'
    AND site_name = 'Garuda Metalindo 1'  -- ⚠️ CHANGE THIS
    AND date_key >= '2025-11-01'::date
    AND date_key <= '2025-11-30'::date
    AND daily_irradiance_kwh_m2 IS NOT NULL
    AND sensor_capacity_kwp IS NOT NULL
GROUP BY date_key, site_name
ORDER BY date_key;

-- ============================================
-- FORMAT B: Per Device Per Day (Pivot Table Format)
-- Best for: Creating Excel pivot table to analyze device-by-device
-- ============================================
/*
SELECT 
    date_key as "Date",
    site_name as "Site",
    sensor_dev_name as "Device",
    asset_id as "Asset_ID",
    ROUND(daily_irradiance_kwh_m2::numeric, 6) as "POA_kWh_m2",
    ROUND(sensor_capacity_kwp::numeric, 2) as "Capacity_kWp",
    ROUND((daily_irradiance_kwh_m2 * sensor_capacity_kwp)::numeric, 2) as "Weighted_Contribution"
FROM mart.mart_sensor_daily
WHERE sensor_type = 'POA'
    AND site_name = 'Garuda Metalindo 1'  -- ⚠️ CHANGE THIS
    AND date_key >= '2025-11-01'::date
    AND date_key <= '2025-11-30'::date
    AND daily_irradiance_kwh_m2 IS NOT NULL
    AND sensor_capacity_kwp IS NOT NULL
ORDER BY date_key, sensor_dev_name;
*/

