-- ============================================
-- Export Shoetown Daily POA for Excel Validation
-- ============================================
-- This query exports data in the format expected by Excel file: shoetown_poa_daily
-- Format matches Excel columns: Date, SLI_IRR_1_A, SLI_IRR_2_A, SLI_IRR_3_F, SLI_IRR_4_F, Weighted_Avg_POA
-- ============================================
-- 
-- Sensor Replacement Timeline (based on grid connection dates):
-- - SLI-IRR-1-Aold (1479456_5_16_2): Deactivated Oct 1, 2025 → Replaced by SLI-IRR-1-A (1479456_5_24_1) activated Oct 3, 2025
-- - SLI-IRR-2-Aold (1479456_5_15_2): Deactivated Oct 1, 2025 → Replaced by SLI-IRR-2-A (1479456_5_25_1) activated Oct 3, 2025
-- - SLI-IRR-3-F (1479456_5_17_1): Replaced by Meteo Station16 (1479456_5_27_1) activated Nov 12, 2025
--   NOTE: Grid connection date shows Nov 12, but user mentioned Oct 3 - may need adjustment
-- - Pyrano: SLI-PYR-01-F (1479456_5_21_1) deactivated Oct 3 → Replaced by SLI-PYR (1479456_5_26_1) activated Oct 12
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
            -- SLI-IRR-3-F: Replaced by Meteo Station16 (grid_connection_date: Nov 12, 2025)
            -- Using Oct 3 as replacement date based on user info (though grid_connection_date is Nov 12)
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
        -- Also include old sensors for reference (before deactivation)
        ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-1-Aold' AND is_active THEN daily_irradiance_kwh_m2 END)::numeric, 9) as SLI_IRR_1_Aold,
        ROUND(MAX(CASE WHEN sensor_dev_name = 'SLI-IRR-2-Aold' AND is_active THEN daily_irradiance_kwh_m2 END)::numeric, 9) as SLI_IRR_2_Aold,
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
    -- Include old sensors for reference
    SLI_IRR_1_Aold,
    SLI_IRR_2_Aold,
    -- Debug columns (can be removed after validation)
    Sum_POA_x_Capacity,
    Sum_Capacity,
    Active_Sensor_Count,
    Active_Sensors
FROM daily_summary
WHERE date_key >= '2025-10-01'::date  -- Start from sensor replacement period
ORDER BY date_key;

