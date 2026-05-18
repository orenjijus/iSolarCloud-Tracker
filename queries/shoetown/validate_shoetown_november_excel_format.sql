-- Validation Query for Shoetown Ligung Indonesia - Excel Format Match
-- This query outputs data in the EXACT format as your Excel file
-- 
-- Note: There's a capacity issue - SLI-IRR-2-A and SLI-IRR-4-F should be 763.28 kWp
-- but database shows 76328.00 kWp (100x too large). This needs to be fixed in seed_sensor_config.

SELECT 
    -- Format: ISO_SITE_DD/MM/YYYY (matching your Excel)
    CONCAT('ISO_SITE_', TO_CHAR(date_key, 'DD/MM/YYYY')) as "site_id",
    date_key as "Date",
    -- Individual sensor values (matching Excel column names with underscore)
    ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-1-A' THEN daily_irradiance_kwh_m2 END)::numeric, 9) as "SLI_IRR_1_A",
    ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-2-A' THEN daily_irradiance_kwh_m2 END)::numeric, 9) as "SLI_IRR_2_A",
    ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-3-F' THEN daily_irradiance_kwh_m2 END)::numeric, 9) as "SLI_IRR_3_F",
    ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-4-F' THEN daily_irradiance_kwh_m2 END)::numeric, 9) as "SLI_IRR_4_F",
    -- Weighted Average (matching Excel)
    ROUND(
        (SUM(daily_irradiance_kwh_m2 * sensor_capacity_kwp) / NULLIF(SUM(sensor_capacity_kwp), 0))::numeric, 
        9
    ) as "Weighted_Avg_POA",
    -- Debug columns (remove these after validation)
    ROUND(SUM(daily_irradiance_kwh_m2 * sensor_capacity_kwp)::numeric, 2) as "Sum_POA_x_Capacity",
    ROUND(SUM(sensor_capacity_kwp)::numeric, 2) as "Sum_Capacity",
    -- Individual capacities for verification
    ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-1-A' THEN sensor_capacity_kwp END)::numeric, 2) as "Cap_1_A",
    ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-2-A' THEN sensor_capacity_kwp END)::numeric, 2) as "Cap_2_A",
    ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-3-F' THEN sensor_capacity_kwp END)::numeric, 2) as "Cap_3_F",
    ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-4-F' THEN sensor_capacity_kwp END)::numeric, 2) as "Cap_4_F"
FROM mart.mart_sensor_daily
WHERE sensor_type = 'POA'
    AND site_name = 'Shoetown Ligung Indonesia'
    AND date_key >= '2025-11-01'::date
    AND date_key <= '2025-11-30'::date
    AND daily_irradiance_kwh_m2 IS NOT NULL
    AND sensor_capacity_kwp IS NOT NULL
GROUP BY date_key
ORDER BY date_key;

