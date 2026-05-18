-- ============================================
-- Crosscheck with Excel - Daily Availability
-- ============================================
-- Export data untuk dibandingkan dengan Excel
-- ============================================

-- 1. Daily Availability Summary (untuk crosscheck dengan Excel)
-- Format: date_key, site_name, power_available_hours, unavailability_hours, total_hours, availability_percent
SELECT 
    date_key,
    site_name,
    -- power_available_hours = SUM(power_available_ratio) / 12.0
    ROUND(SUM(power_available_ratio) / 12.0, 4) as power_available_hours,
    -- unavailability_hours = SUM(unavailability_ratio) / 12.0
    ROUND(SUM(unavailability_ratio) / 12.0, 4) as unavailability_hours,
    -- total_hours = power_available_hours + unavailability_hours
    ROUND((SUM(power_available_ratio) / 12.0) + (SUM(unavailability_ratio) / 12.0), 4) as total_hours,
    -- availability_percent
    ROUND(
        CASE 
            WHEN SUM(power_available_ratio) + SUM(unavailability_ratio) > 0
            THEN (SUM(power_available_ratio) / NULLIF(
                SUM(power_available_ratio) + SUM(unavailability_ratio),
                0
            )) * 100
            ELSE NULL
        END, 2
    ) as availability_percent,
    -- Additional info untuk debugging
    COUNT(*) as interval_count,
    SUM(CASE WHEN mit = 1 THEN 1 ELSE 0 END) as intervals_with_mit
FROM mart.fact_site_calculations_5min
WHERE date_key >= '2025-11-11'  -- Recent 7 days, adjust as needed
GROUP BY date_key, site_name
ORDER BY date_key DESC, site_name;

-- 2. Detailed 5-minute data untuk sites dengan availability < 100%
-- Untuk debugging jika ada perbedaan dengan Excel
SELECT 
    timestamp,
    date_key,
    site_name,
    total_inverters,
    available_inverters,
    power_available_ratio,
    unavailability_ratio,
    mit
FROM mart.fact_site_calculations_5min
WHERE date_key >= '2025-11-11'
    AND site_name IN (
        -- Sites dengan availability < 100% pada recent days
        'PT. MMKI 1.75 MWp - Painting Building',
        'PT. MMKI 4.292 MWP - Phase 3',
        'PT. MMKI 5.7 MWp - Phase 2'
    )
ORDER BY timestamp DESC, site_name;

-- 3. Inverter-level detail untuk sites dengan availability < 100%
-- Untuk melihat inverter mana yang mati
SELECT 
    date_key,
    site_name,
    inverter_id,
    inverter_name,
    COUNT(*) as total_intervals,
    SUM(CASE WHEN inverter_availability = 1 THEN 1 ELSE 0 END) as intervals_available,
    SUM(CASE WHEN inverter_availability = 0 THEN 1 ELSE 0 END) as intervals_unavailable,
    ROUND(
        SUM(CASE WHEN inverter_availability = 1 THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(COUNT(*), 0), 2
    ) as availability_percent
FROM mart.fact_inverter_calculations_5min
WHERE date_key >= '2025-11-11'
    AND site_name IN (
        'PT. MMKI 1.75 MWp - Painting Building',
        'PT. MMKI 4.292 MWP - Phase 3',
        'PT. MMKI 5.7 MWp - Phase 2'
    )
GROUP BY date_key, site_name, inverter_id, inverter_name
ORDER BY date_key DESC, site_name, inverter_id;

-- 4. MIT calculation detail untuk sites dengan availability < 100%
-- Untuk melihat MIT calculation dan fallback usage
SELECT 
    date_key,
    site_name,
    mit_irradiance_source,
    COUNT(*) as interval_count,
    SUM(CASE WHEN mit = 1 THEN 1 ELSE 0 END) as intervals_mit_1,
    SUM(CASE WHEN mit = 0 THEN 1 ELSE 0 END) as intervals_mit_0,
    AVG(irradiance_w_m2) as avg_irradiance,
    MIN(irradiance_w_m2) as min_irradiance,
    MAX(irradiance_w_m2) as max_irradiance
FROM mart.fact_sensor_calculations_5min
WHERE date_key >= '2025-11-11'
    AND site_name IN (
        'PT. MMKI 1.75 MWp - Painting Building',
        'PT. MMKI 4.292 MWP - Phase 3',
        'PT. MMKI 5.7 MWp - Phase 2'
    )
GROUP BY date_key, site_name, mit_irradiance_source
ORDER BY date_key DESC, site_name, mit_irradiance_source;

-- 5. Export untuk Excel comparison (CSV format ready)
-- Copy hasil query ini ke Excel untuk comparison
SELECT 
    date_key,
    site_name,
    ROUND(SUM(power_available_ratio) / 12.0, 4) as power_available_hours,
    ROUND(SUM(unavailability_ratio) / 12.0, 4) as unavailability_hours,
    ROUND((SUM(power_available_ratio) / 12.0) + (SUM(unavailability_ratio) / 12.0), 4) as total_hours,
    ROUND(
        CASE 
            WHEN SUM(power_available_ratio) + SUM(unavailability_ratio) > 0
            THEN (SUM(power_available_ratio) / NULLIF(
                SUM(power_available_ratio) + SUM(unavailability_ratio),
                0
            )) * 100
            ELSE NULL
        END, 2
    ) as availability_percent
FROM mart.fact_site_calculations_5min
WHERE date_key >= '2025-11-11'
GROUP BY date_key, site_name
ORDER BY date_key DESC, site_name;

