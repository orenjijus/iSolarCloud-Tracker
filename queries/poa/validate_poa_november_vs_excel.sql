-- Validation Query: POA Calculation for November 2025
-- Compare with Excel to validate fundamental calculation and weighted average
-- 
-- Usage: 
-- 1. Run this query for your target site
-- 2. Export results to CSV
-- 3. Compare with Excel file

-- ============================================
-- OPTION 1: Detailed POA per device per day (for manual weighted average calculation)
-- ============================================
WITH poa_per_device AS (
    SELECT 
        date_key,
        site_name,
        sensor_dev_name,
        asset_id,
        daily_irradiance_kwh_m2 as poa_kwh_m2,
        sensor_capacity_kwp,
        -- Calculate weighted contribution
        daily_irradiance_kwh_m2 * sensor_capacity_kwp as weighted_contribution,
        measurement_count,
        timestamp_count
    FROM mart.mart_sensor_daily
    WHERE sensor_type = 'POA'
        AND site_name = 'Garuda Metalindo 1'  -- CHANGE THIS to your target site
        AND date_key >= '2025-11-01'::date
        AND date_key <= '2025-11-30'::date
        AND daily_irradiance_kwh_m2 IS NOT NULL
        AND sensor_capacity_kwp IS NOT NULL
),
weighted_avg_per_day AS (
    SELECT 
        date_key,
        site_name,
        -- Sum of all weighted contributions
        SUM(weighted_contribution) as total_weighted_sum,
        -- Sum of all capacities
        SUM(sensor_capacity_kwp) as total_capacity_kwp,
        -- Weighted average: SUM(poa * capacity) / SUM(capacity)
        SUM(weighted_contribution) / NULLIF(SUM(sensor_capacity_kwp), 0) as weighted_avg_poa_kwh_m2,
        -- Count sensors
        COUNT(DISTINCT asset_id) as sensor_count,
        -- Min and max POA values
        MIN(poa_kwh_m2) as min_poa_kwh_m2,
        MAX(poa_kwh_m2) as max_poa_kwh_m2,
        -- Average POA (simple average, not weighted)
        AVG(poa_kwh_m2) as simple_avg_poa_kwh_m2
    FROM poa_per_device
    GROUP BY date_key, site_name
)
-- Detailed view: per device per day + weighted average
SELECT 
    p.date_key,
    p.site_name,
    p.sensor_dev_name,
    p.asset_id,
    p.poa_kwh_m2,
    p.sensor_capacity_kwp,
    p.weighted_contribution,
    p.measurement_count,
    p.timestamp_count,
    -- Add weighted average for the day
    w.weighted_avg_poa_kwh_m2,
    w.total_capacity_kwp,
    w.sensor_count,
    w.min_poa_kwh_m2,
    w.max_poa_kwh_m2,
    w.simple_avg_poa_kwh_m2,
    -- Calculate difference from weighted average
    p.poa_kwh_m2 - w.weighted_avg_poa_kwh_m2 as diff_from_weighted_avg
FROM poa_per_device p
JOIN weighted_avg_per_day w 
    ON p.date_key = w.date_key 
    AND p.site_name = w.site_name
ORDER BY p.date_key, p.poa_kwh_m2 DESC;

-- ============================================
-- OPTION 2: Summary per day (easier to compare with Excel daily values)
-- ============================================
/*
SELECT 
    date_key,
    site_name,
    -- Weighted average POA (this is what should match Excel)
    SUM(daily_irradiance_kwh_m2 * sensor_capacity_kwp) / NULLIF(SUM(sensor_capacity_kwp), 0) as weighted_avg_poa_kwh_m2,
    -- Supporting metrics
    SUM(sensor_capacity_kwp) as total_capacity_kwp,
    COUNT(DISTINCT asset_id) as sensor_count,
    MIN(daily_irradiance_kwh_m2) as min_poa_kwh_m2,
    MAX(daily_irradiance_kwh_m2) as max_poa_kwh_m2,
    AVG(daily_irradiance_kwh_m2) as simple_avg_poa_kwh_m2,
    -- Breakdown for verification
    STRING_AGG(
        sensor_dev_name || ':' || ROUND(daily_irradiance_kwh_m2::numeric, 4)::text || ' (' || ROUND(sensor_capacity_kwp::numeric, 2)::text || 'kWp)',
        ' | ' ORDER BY daily_irradiance_kwh_m2 DESC
    ) as sensor_breakdown
FROM mart.mart_sensor_daily
WHERE sensor_type = 'POA'
    AND site_name = 'Garuda Metalindo 1'  -- CHANGE THIS to your target site
    AND date_key >= '2025-11-01'::date
    AND date_key <= '2025-11-30'::date
    AND daily_irradiance_kwh_m2 IS NOT NULL
    AND sensor_capacity_kwp IS NOT NULL
GROUP BY date_key, site_name
ORDER BY date_key;
*/

-- ============================================
-- OPTION 3: Per device breakdown (for Excel pivot table)
-- ============================================
/*
SELECT 
    date_key,
    site_name,
    sensor_dev_name,
    asset_id,
    daily_irradiance_kwh_m2 as poa_kwh_m2,
    sensor_capacity_kwp,
    daily_irradiance_kwh_m2 * sensor_capacity_kwp as weighted_contribution
FROM mart.mart_sensor_daily
WHERE sensor_type = 'POA'
    AND site_name = 'Garuda Metalindo 1'  -- CHANGE THIS to your target site
    AND date_key >= '2025-11-01'::date
    AND date_key <= '2025-11-30'::date
    AND daily_irradiance_kwh_m2 IS NOT NULL
    AND sensor_capacity_kwp IS NOT NULL
ORDER BY date_key, sensor_dev_name;
*/

-- ============================================
-- OPTION 4: Formula verification (showing calculation steps)
-- ============================================
/*
SELECT 
    date_key,
    site_name,
    -- Step 1: Individual POA values
    STRING_AGG(
        sensor_dev_name || ' = ' || ROUND(daily_irradiance_kwh_m2::numeric, 4)::text || ' kWh/m²',
        ', ' ORDER BY sensor_dev_name
    ) as individual_poa_values,
    -- Step 2: Weighted contributions
    STRING_AGG(
        sensor_dev_name || ': ' || ROUND(daily_irradiance_kwh_m2::numeric, 4)::text || ' × ' || 
        ROUND(sensor_capacity_kwp::numeric, 2)::text || ' = ' || 
        ROUND((daily_irradiance_kwh_m2 * sensor_capacity_kwp)::numeric, 2)::text,
        ' | ' ORDER BY sensor_dev_name
    ) as weighted_contributions,
    -- Step 3: Sum of weighted contributions
    SUM(daily_irradiance_kwh_m2 * sensor_capacity_kwp) as sum_weighted_contributions,
    -- Step 4: Sum of capacities
    SUM(sensor_capacity_kwp) as sum_capacities,
    -- Step 5: Final weighted average
    SUM(daily_irradiance_kwh_m2 * sensor_capacity_kwp) / NULLIF(SUM(sensor_capacity_kwp), 0) as weighted_avg_poa_kwh_m2
FROM mart.mart_sensor_daily
WHERE sensor_type = 'POA'
    AND site_name = 'Garuda Metalindo 1'  -- CHANGE THIS to your target site
    AND date_key >= '2025-11-01'::date
    AND date_key <= '2025-11-30'::date
    AND daily_irradiance_kwh_m2 IS NOT NULL
    AND sensor_capacity_kwp IS NOT NULL
GROUP BY date_key, site_name
ORDER BY date_key;
*/

