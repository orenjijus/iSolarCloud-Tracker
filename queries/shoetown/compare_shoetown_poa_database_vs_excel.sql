-- ============================================
-- Compare Shoetown POA: Database vs Excel
-- ============================================
-- This query helps validate daily POA by comparing:
-- 1. Individual sensor values per day
-- 2. Weighted average POA
-- 3. Sensor activation/deactivation dates
-- ============================================
-- 
-- Instructions:
-- 1. Run this query to get database values
-- 2. Compare with Excel file: shoetown_poa_daily.xlsx
-- 3. Check for differences in:
--    - Individual sensor values (SLI_IRR_1_A, SLI_IRR_2_A, SLI_IRR_3_F, SLI_IRR_4_F)
--    - Weighted average POA
--    - Active sensors per day
-- ============================================

WITH daily_poa_per_sensor AS (
    SELECT 
        date_key,
        sensor_dev_name,
        device_id,
        sensor_capacity_kwp,
        daily_irradiance_kwh_m2,
        daily_irradiance_kwh_m2 * sensor_capacity_kwp as weighted_contribution,
        -- Determine if sensor should be active based on replacement timeline
        CASE 
            -- Old sensors: exclude after Oct 1, 2025
            WHEN device_id = 'ISO_1479456_5_16_2' AND date_key > '2025-10-01'::date THEN FALSE  -- SLI-IRR-1-Aold
            WHEN device_id = 'ISO_1479456_5_15_2' AND date_key > '2025-10-01'::date THEN FALSE  -- SLI-IRR-2-Aold
            -- New sensors: only include after Oct 3, 2025
            WHEN device_id = 'ISO_1479456_5_24_1' AND date_key < '2025-10-03'::date THEN FALSE  -- SLI-IRR-1-A
            WHEN device_id = 'ISO_1479456_5_25_1' AND date_key < '2025-10-03'::date THEN FALSE  -- SLI-IRR-2-A
            -- SLI-IRR-3-F: exclude after Oct 3 (replaced by Meteo Station16 - needs verification)
            WHEN device_id = 'ISO_1479456_5_17_1' AND date_key >= '2025-10-03'::date THEN FALSE  -- SLI-IRR-3-F
            ELSE TRUE
        END as is_active,
        CASE 
            WHEN device_id = 'ISO_1479456_5_16_2' THEN 'OLD_SENSOR'
            WHEN device_id = 'ISO_1479456_5_15_2' THEN 'OLD_SENSOR'
            WHEN device_id = 'ISO_1479456_5_24_1' THEN 'NEW_SENSOR'
            WHEN device_id = 'ISO_1479456_5_25_1' THEN 'NEW_SENSOR'
            WHEN device_id = 'ISO_1479456_5_17_1' THEN 'REPLACED_SENSOR'
            ELSE 'ACTIVE_SENSOR'
        END as sensor_status
    FROM mart.mart_sensor_daily
    WHERE sensor_type = 'POA'
        AND site_name = 'Shoetown Ligung Indonesia'
        AND daily_irradiance_kwh_m2 IS NOT NULL
        AND sensor_capacity_kwp IS NOT NULL
),
daily_summary AS (
    SELECT 
        date_key,
        -- Individual sensor values (matching Excel columns)
        ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-1-A' AND is_active THEN daily_irradiance_kwh_m2 END)::numeric, 9) as SLI_IRR_1_A,
        ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-2-A' AND is_active THEN daily_irradiance_kwh_m2 END)::numeric, 9) as SLI_IRR_2_A,
        ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-3-F' AND is_active THEN daily_irradiance_kwh_m2 END)::numeric, 9) as SLI_IRR_3_F,
        ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-4-F' AND is_active THEN daily_irradiance_kwh_m2 END)::numeric, 9) as SLI_IRR_4_F,
        -- Old sensors (for reference)
        ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-1-Aold' AND is_active THEN daily_irradiance_kwh_m2 END)::numeric, 9) as SLI_IRR_1_Aold,
        ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-2-Aold' AND is_active THEN daily_irradiance_kwh_m2 END)::numeric, 9) as SLI_IRR_2_Aold,
        -- Weighted average (only active sensors)
        ROUND(
            (SUM(CASE WHEN is_active THEN weighted_contribution ELSE 0 END) / 
             NULLIF(SUM(CASE WHEN is_active THEN sensor_capacity_kwp ELSE 0 END), 0))::numeric, 
            9
        ) as Weighted_Avg_POA_DB,
        -- Calculation components (for debugging)
        ROUND(SUM(CASE WHEN is_active THEN weighted_contribution ELSE 0 END)::numeric, 2) as Sum_POA_x_Capacity,
        ROUND(SUM(CASE WHEN is_active THEN sensor_capacity_kwp ELSE 0 END)::numeric, 2) as Sum_Capacity,
        COUNT(DISTINCT CASE WHEN is_active THEN device_id END) as Active_Sensor_Count,
        STRING_AGG(DISTINCT CASE WHEN is_active THEN sensor_dev_name END, ', ' ORDER BY sensor_dev_name) as Active_Sensors,
        -- Show all sensors (including inactive) for debugging
        STRING_AGG(DISTINCT CONCAT(sensor_dev_name, ' (', sensor_status, ')'), ', ' ORDER BY sensor_dev_name) as All_Sensors_With_Status
    FROM daily_poa_per_sensor
    GROUP BY date_key
)
SELECT 
    date_key as "Date",
    -- Individual sensor values (compare with Excel)
    SLI_IRR_1_A,
    SLI_IRR_2_A,
    SLI_IRR_3_F,
    SLI_IRR_4_F,
    -- Weighted average (main comparison)
    Weighted_Avg_POA_DB as "Weighted_Avg_POA",
    -- Old sensors (for reference, should be NULL after Oct 1)
    SLI_IRR_1_Aold,
    SLI_IRR_2_Aold,
    -- Debug info
    Sum_POA_x_Capacity,
    Sum_Capacity,
    Active_Sensor_Count,
    Active_Sensors,
    All_Sensors_With_Status
FROM daily_summary
WHERE date_key >= '2025-10-01'::date  -- Start from sensor replacement period
ORDER BY date_key;

-- ============================================
-- Additional: Sensor Status Summary
-- ============================================
-- Run this separately to see sensor status per day
-- ============================================
/*
WITH sensor_status_per_day AS (
    SELECT 
        date_key,
        sensor_dev_name,
        device_id,
        sensor_capacity_kwp,
        daily_irradiance_kwh_m2,
        CASE 
            WHEN device_id = 'ISO_1479456_5_16_2' AND date_key > '2025-10-01'::date THEN 'EXCLUDED (old sensor)'
            WHEN device_id = 'ISO_1479456_5_15_2' AND date_key > '2025-10-01'::date THEN 'EXCLUDED (old sensor)'
            WHEN device_id = 'ISO_1479456_5_24_1' AND date_key < '2025-10-03'::date THEN 'EXCLUDED (not yet activated)'
            WHEN device_id = 'ISO_1479456_5_25_1' AND date_key < '2025-10-03'::date THEN 'EXCLUDED (not yet activated)'
            WHEN device_id = 'ISO_1479456_5_17_1' AND date_key >= '2025-10-03'::date THEN 'EXCLUDED (replaced)'
            ELSE 'ACTIVE'
        END as status
    FROM mart.mart_sensor_daily
    WHERE sensor_type = 'POA'
        AND site_name = 'Shoetown Ligung Indonesia'
        AND date_key >= '2025-09-25'::date
        AND date_key <= '2025-10-10'::date
)
SELECT 
    date_key,
    sensor_dev_name,
    device_id,
    ROUND(sensor_capacity_kwp::numeric, 2) as capacity_kwp,
    ROUND(daily_irradiance_kwh_m2::numeric, 6) as poa_kwh_m2,
    status
FROM sensor_status_per_day
ORDER BY date_key, sensor_dev_name;
*/

