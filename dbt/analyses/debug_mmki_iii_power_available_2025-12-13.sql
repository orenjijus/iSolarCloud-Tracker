-- ============================================
-- Debug: MMKI III Power Available Hours - 2025-12-13
-- ============================================
-- Query untuk menganalisis kenapa power_available_hours hanya 8.83 jam
-- bukannya ~12 jam untuk MMKI III pada tanggal 13 Desember 2025
-- 
-- Logika yang benar:
-- - Power Available Hours = semua interval dimana inverter menyala (tidak peduli MIT)
-- - Unavailability Hours = hanya ketika MIT=1 dan inverter tidak menyala
-- ============================================

-- 1. Summary per hari untuk MMKI III sekitar tanggal tersebut
SELECT 
    date_key,
    site_name,
    -- Power available hours
    ROUND(SUM(power_available_ratio) * 5.0 / 60.0, 2) as power_available_hours,
    -- Unavailability hours
    ROUND(SUM(unavailability_ratio) * 5.0 / 60.0, 2) as unavailability_hours,
    -- MIT hours (sun hours)
    ROUND(SUM(CASE WHEN mit = 1 THEN 1 ELSE 0 END) * 5.0 / 60.0, 2) as mit_hours,
    -- Total intervals
    COUNT(*) as total_intervals,
    -- Intervals with MIT = 1
    SUM(CASE WHEN mit = 1 THEN 1 ELSE 0 END) as intervals_with_mit,
    -- Intervals with power_available_ratio > 0 (inverter menyala)
    SUM(CASE WHEN power_available_ratio > 0 THEN 1 ELSE 0 END) as intervals_with_power,
    -- Intervals with power_available_ratio > 0 AND MIT = 1 (inverter menyala saat ada sinar matahari)
    SUM(CASE WHEN power_available_ratio > 0 AND mit = 1 THEN 1 ELSE 0 END) as intervals_with_power_and_mit,
    -- Average power_available_ratio
    ROUND(AVG(power_available_ratio), 4) as avg_power_available_ratio,
    -- Average power_available_ratio when MIT = 1
    ROUND(AVG(CASE WHEN mit = 1 THEN power_available_ratio ELSE NULL END), 4) as avg_power_available_ratio_when_mit_1,
    -- Total inverters (should be consistent)
    MAX(total_inverters) as total_inverters,
    -- Available inverters stats
    ROUND(AVG(available_inverters), 2) as avg_available_inverters,
    MIN(available_inverters) as min_available_inverters,
    MAX(available_inverters) as max_available_inverters
FROM mart.fact_site_calculations_5min
WHERE site_name LIKE '%MMKI%Phase 3%'
    AND date_key BETWEEN '2025-12-10' AND '2025-12-16'  -- 3 hari sebelum dan sesudah
GROUP BY date_key, site_name
ORDER BY date_key DESC;

-- 2. Detailed breakdown per jam untuk tanggal 13 Desember 2025
SELECT 
    DATE_TRUNC('hour', timestamp) as hour,
    COUNT(*) as intervals_in_hour,
    SUM(CASE WHEN mit = 1 THEN 1 ELSE 0 END) as intervals_with_mit,
    SUM(CASE WHEN power_available_ratio > 0 THEN 1 ELSE 0 END) as intervals_with_power,
    ROUND(AVG(power_available_ratio), 4) as avg_power_available_ratio,
    ROUND(SUM(power_available_ratio) * 5.0 / 60.0, 2) as power_available_hours_in_hour,
    ROUND(AVG(total_inverters), 1) as avg_total_inverters,
    ROUND(AVG(available_inverters), 2) as avg_available_inverters,
    MIN(available_inverters) as min_available_inverters,
    MAX(available_inverters) as max_available_inverters
FROM mart.fact_site_calculations_5min
WHERE site_name LIKE '%MMKI%Phase 3%'
    AND date_key = '2025-12-13'
GROUP BY DATE_TRUNC('hour', timestamp)
ORDER BY hour;

-- 3. Power Available Ratio breakdown by MIT status
SELECT 
    CASE 
        WHEN mit = 1 AND power_available_ratio > 0 THEN 'MIT=1, Power ON'
        WHEN mit = 1 AND power_available_ratio = 0 THEN 'MIT=1, Power OFF (Unavailable)'
        WHEN mit = 0 AND power_available_ratio > 0 THEN 'MIT=0, Power ON (Night/Cloudy but producing)'
        WHEN mit = 0 AND power_available_ratio = 0 THEN 'MIT=0, Power OFF (Normal)'
        ELSE 'Unknown'
    END as status,
    COUNT(*) as interval_count,
    ROUND(COUNT(*) * 5.0 / 60.0, 2) as hours,
    ROUND(AVG(power_available_ratio), 4) as avg_power_available_ratio,
    ROUND(SUM(power_available_ratio) * 5.0 / 60.0, 2) as power_available_hours_contribution
FROM mart.fact_site_calculations_5min
WHERE site_name LIKE '%MMKI%Phase 3%'
    AND date_key = '2025-12-13'
GROUP BY 
    CASE 
        WHEN mit = 1 AND power_available_ratio > 0 THEN 'MIT=1, Power ON'
        WHEN mit = 1 AND power_available_ratio = 0 THEN 'MIT=1, Power OFF (Unavailable)'
        WHEN mit = 0 AND power_available_ratio > 0 THEN 'MIT=0, Power ON (Night/Cloudy but producing)'
        WHEN mit = 0 AND power_available_ratio = 0 THEN 'MIT=0, Power OFF (Normal)'
        ELSE 'Unknown'
    END
ORDER BY interval_count DESC;

-- 4. Missing intervals analysis - cek apakah ada gap dalam data
WITH expected_intervals AS (
    SELECT 
        generate_series(
            '2025-12-13 00:00:00'::timestamp,
            '2025-12-13 23:55:00'::timestamp,
            '5 minutes'::interval
        ) as expected_timestamp
),
actual_intervals AS (
    SELECT DISTINCT timestamp
    FROM mart.fact_site_calculations_5min
    WHERE site_name LIKE '%MMKI%Phase 3%'
        AND date_key = '2025-12-13'
)
SELECT 
    ei.expected_timestamp,
    CASE WHEN ai.timestamp IS NULL THEN 'MISSING' ELSE 'EXISTS' END as status,
    fsc.total_inverters,
    fsc.available_inverters,
    fsc.power_available_ratio,
    fsc.mit
FROM expected_intervals ei
LEFT JOIN actual_intervals ai ON ei.expected_timestamp = ai.timestamp
LEFT JOIN mart.fact_site_calculations_5min fsc 
    ON ei.expected_timestamp = fsc.timestamp 
    AND fsc.site_name LIKE '%MMKI%Phase 3%'
    AND fsc.date_key = '2025-12-13'
WHERE ai.timestamp IS NULL  -- Only show missing intervals
ORDER BY ei.expected_timestamp
LIMIT 50;  -- Show first 50 missing intervals

-- 5. Intervals dengan power_available_ratio = 0 atau sangat rendah
SELECT 
    timestamp,
    total_inverters,
    available_inverters,
    power_available_ratio,
    unavailability_ratio,
    mit,
    CASE 
        WHEN mit = 1 AND power_available_ratio = 0 THEN 'MIT=1 but no power (unavailable)'
        WHEN mit = 0 AND power_available_ratio = 0 THEN 'No sun, no power (normal)'
        WHEN mit = 1 AND power_available_ratio > 0 AND power_available_ratio < 1 THEN 'Partial power'
        ELSE 'OK'
    END as status
FROM mart.fact_site_calculations_5min
WHERE site_name LIKE '%MMKI%Phase 3%'
    AND date_key = '2025-12-13'
    AND (
        power_available_ratio = 0 
        OR power_available_ratio < 0.5
    )
ORDER BY timestamp
LIMIT 100;

-- 6. Comparison dengan hari lain untuk melihat pola
SELECT 
    date_key,
    COUNT(*) as total_intervals,
    SUM(CASE WHEN mit = 1 THEN 1 ELSE 0 END) as intervals_with_mit,
    ROUND(SUM(power_available_ratio) * 5.0 / 60.0, 2) as power_available_hours,
    ROUND(SUM(CASE WHEN mit = 1 THEN 1 ELSE 0 END) * 5.0 / 60.0, 2) as mit_hours,
    ROUND(AVG(CASE WHEN mit = 1 THEN power_available_ratio ELSE NULL END), 4) as avg_power_ratio_when_mit_1
FROM mart.fact_site_calculations_5min
WHERE site_name LIKE '%MMKI%Phase 3%'
    AND date_key BETWEEN '2025-12-10' AND '2025-12-16'
GROUP BY date_key
ORDER BY date_key;

