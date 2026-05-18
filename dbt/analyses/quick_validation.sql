-- ============================================
-- Quick Validation Queries
-- ============================================
-- Quick checks to validate fact tables are working correctly
-- ============================================

-- 1. Quick check: Count rows per fact table
SELECT 
    'fact_sensor_calculations_5min' as table_name,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp,
    COUNT(DISTINCT site_name) as distinct_sites
FROM mart.fact_sensor_calculations_5min

UNION ALL

SELECT 
    'fact_inverter_calculations_5min' as table_name,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp,
    COUNT(DISTINCT site_name) as distinct_sites
FROM mart.fact_inverter_calculations_5min

UNION ALL

SELECT 
    'fact_site_calculations_5min' as table_name,
    COUNT(*) as row_count,
    MIN(timestamp) as min_timestamp,
    MAX(timestamp) as max_timestamp,
    COUNT(DISTINCT site_name) as distinct_sites
FROM mart.fact_site_calculations_5min;

-- 2. Check MIT sources distribution (should see GHI, POA, and GHI_FALLBACK)
SELECT 
    mit_irradiance_source,
    COUNT(*) as row_count,
    COUNT(DISTINCT site_name) as distinct_sites,
    COUNT(DISTINCT date_key) as distinct_dates
FROM mart.fact_sensor_calculations_5min
GROUP BY mit_irradiance_source
ORDER BY row_count DESC;

-- 3. Check sites using POA fallback (recent 7 days)
SELECT 
    date_key,
    site_name,
    COUNT(*) as intervals_using_poa
FROM mart.fact_sensor_calculations_5min
WHERE mit_irradiance_source = 'POA'
    AND date_key >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY date_key, site_name
ORDER BY date_key DESC, site_name;

-- 4. Check GHI fallback sites (MMKI II, III)
SELECT 
    date_key,
    site_name,
    mit_irradiance_source,
    COUNT(*) as interval_count
FROM mart.fact_sensor_calculations_5min
WHERE site_name IN ('PT. MMKI 5.7 MWp - Phase 2', 'PT. MMKI 4.292 MWP - Phase 3')
    AND date_key >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY date_key, site_name, mit_irradiance_source
ORDER BY date_key DESC, site_name;

-- 5. Check inverter availability summary (recent day)
SELECT 
    date_key,
    site_name,
    COUNT(DISTINCT inverter_id) as total_inverters,
    COUNT(DISTINCT CASE WHEN inverter_availability = 1 THEN inverter_id END) as available_inverters,
    COUNT(DISTINCT CASE WHEN inverter_availability = 0 THEN inverter_id END) as unavailable_inverters
FROM mart.fact_inverter_calculations_5min
WHERE date_key >= CURRENT_DATE - INTERVAL '1 day'
GROUP BY date_key, site_name
ORDER BY date_key DESC, site_name;

-- 6. Check site-level availability (recent day)
SELECT 
    date_key,
    site_name,
    AVG(power_available_ratio) as avg_power_available_ratio,
    AVG(unavailability_ratio) as avg_unavailability_ratio,
    COUNT(*) as interval_count
FROM mart.fact_site_calculations_5min
WHERE date_key >= CURRENT_DATE - INTERVAL '1 day'
GROUP BY date_key, site_name
ORDER BY date_key DESC, site_name;

-- 7. Daily availability summary (recent 7 days)
SELECT 
    date_key,
    site_name,
    SUM(power_available_ratio) / 12.0 as power_available_hours,
    SUM(unavailability_ratio) / 12.0 as unavailability_hours,
    (SUM(power_available_ratio) / 12.0) + (SUM(unavailability_ratio) / 12.0) as total_hours,
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

