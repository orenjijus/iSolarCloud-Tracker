-- ============================================
-- Final Validation: Shoetown Daily POA
-- ============================================
-- This query exports data in Excel format for comparison
-- Format: Date, SLI_IRR_1_A, SLI_IRR_2_A, SLI_IRR_3_F, SLI_IRR_4_F, Weighted_Avg_POA
-- ============================================
-- 
-- Sensor Configuration (Final):
-- - SLI-IRR-1-Aold (1479456_5_16_2): 452.4 kWp - Deactivated Oct 1, 2025
-- - SLI-IRR-2-Aold (1479456_5_15_2): 452.4 kWp - Deactivated Oct 1, 2025
-- - SLI-IRR-1-A (1479456_5_24_1): 452.4 kWp - Activated Oct 3, 2025 (reposisi)
-- - SLI-IRR-2-A (1479456_5_25_1): 452.4 kWp - Activated Oct 3, 2025 (reposisi)
-- - SLI-IRR-3-F (1479456_5_17_1): 928 kWp - Replaced Oct 3, 2025
-- - SLI-IRR-4-F (1479456_5_18_1): 763.28 kWp - Active (tidak direposisi)
-- - Meteo Station16 (1479456_5_27_1): 928 kWp - Activated Nov 12, 2025 (reposisi dari SLI-IRR-3-F)
-- ============================================

WITH daily_poa_per_sensor AS (
    SELECT 
        date_key,
        sensor_dev_name,
        device_id,
        sensor_capacity_kwp,
        daily_irradiance_kwh_m2,
        daily_irradiance_kwh_m2 * sensor_capacity_kwp as weighted_contribution
    FROM mart.mart_sensor_daily
    WHERE sensor_type = 'POA'
        AND site_name = 'Shoetown Ligung Indonesia'
        AND daily_irradiance_kwh_m2 IS NOT NULL
        AND sensor_capacity_kwp IS NOT NULL
),
-- Filter sensors based on replacement timeline
filtered_sensors AS (
    SELECT 
        date_key,
        sensor_dev_name,
        device_id,
        sensor_capacity_kwp,
        daily_irradiance_kwh_m2,
        weighted_contribution,
        -- Exclude old sensors after their deactivation date
        CASE 
            -- Old sensors should be excluded after Oct 1, 2025
            WHEN device_id = 'ISO_1479456_5_16_2' AND date_key > '2025-10-01'::date THEN FALSE  -- SLI-IRR-1-Aold
            WHEN device_id = 'ISO_1479456_5_15_2' AND date_key > '2025-10-01'::date THEN FALSE  -- SLI-IRR-2-Aold
            -- New sensors should only be included after their activation date
            WHEN device_id = 'ISO_1479456_5_24_1' AND date_key < '2025-10-03'::date THEN FALSE  -- SLI-IRR-1-A
            WHEN device_id = 'ISO_1479456_5_25_1' AND date_key < '2025-10-03'::date THEN FALSE  -- SLI-IRR-2-A
            -- SLI-IRR-3-F: Replaced by Meteo Station16 after Oct 3
            WHEN device_id = 'ISO_1479456_5_17_1' AND date_key >= '2025-10-03'::date THEN FALSE  -- SLI-IRR-3-F (replaced)
            -- Meteo Station16: Only include after activation (Nov 12, 2025)
            WHEN device_id = 'ISO_1479456_5_27_1' AND date_key < '2025-11-12'::date THEN FALSE  -- Meteo Station16 (not yet activated)
            ELSE TRUE
        END as is_active
    FROM daily_poa_per_sensor
),
-- Calculate daily weighted average using only active sensors
daily_summary AS (
    SELECT 
        date_key,
        -- Individual sensor values (matching Excel column names)
        ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-1-A' AND is_active THEN daily_irradiance_kwh_m2 END)::numeric, 9) as SLI_IRR_1_A,
        ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-2-A' AND is_active THEN daily_irradiance_kwh_m2 END)::numeric, 9) as SLI_IRR_2_A,
        -- SLI-IRR-3-F replaced by Meteo Station16 after Oct 3
        ROUND(MAX(CASE 
            WHEN sensor_dev_name = 'SLI-IRR-3-F' AND is_active THEN daily_irradiance_kwh_m2 
            WHEN sensor_dev_name = 'Meteo Station16' AND is_active THEN daily_irradiance_kwh_m2 
            ELSE NULL 
        END)::numeric, 9) as SLI_IRR_3_F,
        ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-4-F' AND is_active THEN daily_irradiance_kwh_m2 END)::numeric, 9) as SLI_IRR_4_F,
        -- Old sensors (for reference, should be NULL after Oct 1)
        ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-1-Aold' AND is_active THEN daily_irradiance_kwh_m2 END)::numeric, 9) as SLI_IRR_1_Aold,
        ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-2-Aold' AND is_active THEN daily_irradiance_kwh_m2 END)::numeric, 9) as SLI_IRR_2_Aold,
        -- Meteo Station16 (for reference)
        ROUND(MAX(CASE WHEN sensor_dev_name = 'Meteo Station16' AND is_active THEN daily_irradiance_kwh_m2 END)::numeric, 9) as Meteo_Station16,
        -- Weighted average (only active sensors)
        ROUND(
            (SUM(CASE WHEN is_active THEN weighted_contribution ELSE 0 END) / 
             NULLIF(SUM(CASE WHEN is_active THEN sensor_capacity_kwp ELSE 0 END), 0))::numeric, 
            9
        ) as Weighted_Avg_POA,
        -- Debug columns
        ROUND(SUM(CASE WHEN is_active THEN weighted_contribution ELSE 0 END)::numeric, 2) as Sum_POA_x_Capacity,
        ROUND(SUM(CASE WHEN is_active THEN sensor_capacity_kwp ELSE 0 END)::numeric, 2) as Sum_Capacity,
        COUNT(DISTINCT CASE WHEN is_active THEN device_id END) as Active_Sensor_Count,
        STRING_AGG(DISTINCT CASE WHEN is_active THEN sensor_dev_name END, ', ' ORDER BY sensor_dev_name) as Active_Sensors
    FROM filtered_sensors
    GROUP BY date_key
)
SELECT 
    date_key as "Date",
    SLI_IRR_1_A,
    SLI_IRR_2_A,
    SLI_IRR_3_F,
    SLI_IRR_4_F,
    Weighted_Avg_POA,
    -- Include old sensors and Meteo Station16 for reference
    SLI_IRR_1_Aold,
    SLI_IRR_2_Aold,
    Meteo_Station16,
    -- Debug columns (can be removed after validation)
    Sum_POA_x_Capacity,
    Sum_Capacity,
    Active_Sensor_Count,
    Active_Sensors
FROM daily_summary
WHERE date_key >= '2025-10-01'::date  -- Start from sensor replacement period
ORDER BY date_key;

