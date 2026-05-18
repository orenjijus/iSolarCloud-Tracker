-- ============================================
-- Validation Queries for 5-Minute Fact Tables
-- ============================================
-- Use these queries to validate the fact tables after building them
-- ============================================

-- 1. Check MIT calculation and fallback sources
-- This shows which irradiance source was used for MIT calculation per site
SELECT 
    date_key,
    site_name,
    mit_irradiance_source,
    COUNT(*) as interval_count,
    COUNT(DISTINCT timestamp) as unique_timestamps,
    SUM(CASE WHEN mit = 1 THEN 1 ELSE 0 END) as mit_1_count,
    SUM(CASE WHEN mit = 0 THEN 1 ELSE 0 END) as mit_0_count
FROM mart.fact_sensor_calculations_5min
WHERE date_key >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY date_key, site_name, mit_irradiance_source
ORDER BY date_key DESC, site_name, mit_irradiance_source;

-- 2. Check POA fallback usage (when POA was used instead of GHI)
-- This helps identify when GHI sensors were down and POA was used as fallback
SELECT 
    date_key,
    site_name,
    COUNT(*) as intervals_using_poa,
    MIN(timestamp) as first_poa_usage,
    MAX(timestamp) as last_poa_usage
FROM mart.fact_sensor_calculations_5min
WHERE mit_irradiance_source = 'POA'
    AND date_key >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY date_key, site_name
ORDER BY date_key DESC, site_name;

-- 3. Check inverter availability per site
-- This shows how many inverters are available vs total per site
SELECT 
    date_key,
    site_name,
    timestamp,
    total_inverters,
    available_inverters,
    power_available_ratio,
    mit,
    unavailability_ratio
FROM mart.fact_site_calculations_5min
WHERE date_key >= CURRENT_DATE - INTERVAL '1 day'
ORDER BY timestamp DESC, site_name
LIMIT 100;

-- 4. Check daily availability summary
-- This calculates daily availability from 5-minute fact table
SELECT 
    date_key,
    site_name,
    -- power_available_hours = SUM(power_available_ratio) / 12.0
    SUM(power_available_ratio) / 12.0 as power_available_hours,
    -- unavailability_hours = SUM(unavailability_ratio) / 12.0
    SUM(unavailability_ratio) / 12.0 as unavailability_hours,
    -- total_hours = power_available_hours + unavailability_hours
    (SUM(power_available_ratio) / 12.0) + (SUM(unavailability_ratio) / 12.0) as total_hours,
    -- availability_percent
    CASE 
        WHEN SUM(power_available_ratio) + SUM(unavailability_ratio) > 0
        THEN (SUM(power_available_ratio) / NULLIF(
            SUM(power_available_ratio) + SUM(unavailability_ratio),
            0
        )) * 100
        ELSE NULL
    END as availability_percent
FROM mart.fact_site_calculations_5min
WHERE date_key >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY date_key, site_name
ORDER BY date_key DESC, site_name;

-- 5. Check which inverters are down (availability = 0)
-- This helps identify problematic inverters
SELECT 
    date_key,
    site_name,
    inverter_id,
    inverter_name,
    COUNT(*) as intervals_down,
    MIN(timestamp) as first_down,
    MAX(timestamp) as last_down
FROM mart.fact_inverter_calculations_5min
WHERE inverter_availability = 0
    AND date_key >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY date_key, site_name, inverter_id, inverter_name
ORDER BY date_key DESC, intervals_down DESC, site_name, inverter_id;

-- 6. Compare MIT sources across sites
-- This shows the distribution of MIT calculation sources
SELECT 
    site_name,
    mit_irradiance_source,
    COUNT(*) as total_intervals,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY site_name), 2) as percentage
FROM mart.fact_sensor_calculations_5min
WHERE date_key >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY site_name, mit_irradiance_source
ORDER BY site_name, mit_irradiance_source;

-- 7. Check GHI fallback sites (MMKI II, III)
-- This validates that GHI fallback is working for sites without GHI sensors
SELECT 
    date_key,
    site_name,
    mit_irradiance_source,
    COUNT(*) as interval_count,
    AVG(irradiance_w_m2) as avg_irradiance,
    MIN(irradiance_w_m2) as min_irradiance,
    MAX(irradiance_w_m2) as max_irradiance
FROM mart.fact_sensor_calculations_5min
WHERE site_name IN ('PT. MMKI 5.7 MWp - Phase 2', 'PT. MMKI 4.292 MWP - Phase 3')
    AND date_key >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY date_key, site_name, mit_irradiance_source
ORDER BY date_key DESC, site_name;

