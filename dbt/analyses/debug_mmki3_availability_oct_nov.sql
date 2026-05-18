-- ============================================
-- Debug: MMKI 3 Availability Missing di October-November
-- ============================================
-- Query untuk menganalisis mengapa availability tidak ada untuk MMKI 3 di October-November
-- ============================================

-- Step 1: Check apakah ada data inverter untuk MMKI 3 di October-November
WITH inverter_data_check AS (
    SELECT 
        date_key,
        site_name,
        COUNT(DISTINCT timestamp) as inverter_timestamps,
        COUNT(DISTINCT asset_id) as total_inverters,
        MIN(timestamp) as first_timestamp,
        MAX(timestamp) as last_timestamp
    FROM "MMSR"."mart"."mart_inverter_performance_5min"
    WHERE site_name = 'PT. MMKI 4.292 MWP - Phase 3'
        AND metric_name = 'inv_active_power'
        AND metric_value IS NOT NULL
        AND date_key >= '2025-10-01'::date
        AND date_key <= '2025-11-30'::date
    GROUP BY date_key, site_name
),

-- Step 2: Check apakah ada data MIT (irradiance) untuk MMKI 3 di October-November
-- MMKI 3 menggunakan GHI fallback dari MMKI I, jadi perlu check kedua site
mit_data_check AS (
    SELECT 
        s.date_key,
        s.site_name,
        COUNT(DISTINCT s.timestamp) as mit_timestamps,
        MIN(s.timestamp) as first_timestamp,
        MAX(s.timestamp) as last_timestamp,
        MIN(s.metric_value) as min_irradiance,
        MAX(s.metric_value) as max_irradiance
    FROM "MMSR"."mart"."mart_sensor_measurements_5min" s
    JOIN "MMSR"."mart"."seed_sensor_config" sc 
        ON s.asset_id = CONCAT(
            CASE 
                WHEN s.system = 'fusionsolar' THEN 'FS'
                WHEN s.system = 'isolarcloud' THEN 'ISO'
                ELSE UPPER(LEFT(s.system, 3))
            END, '_', sc.device_id
        )
    WHERE s.metric_name = 'irradiance'
        AND s.metric_value IS NOT NULL
        AND s.date_key >= '2025-10-01'::date
        AND date_key <= '2025-11-30'::date
        AND (
            -- Check MMKI 3 langsung (jika ada sensor sendiri)
            s.site_name = 'PT. MMKI 4.292 MWP - Phase 3'
            OR
            -- Check MMKI I (karena MMKI 3 pakai GHI fallback dari MMKI I)
            (s.site_name = 'PT. MMKI 1.75 MWp - Painting Building' AND sc.sensor_type = 'GHI')
        )
    GROUP BY s.date_key, s.site_name
),

-- Step 3: Check apakah ada match antara inverter data dan MIT data
inverter_availability_5min AS (
    SELECT 
        i.date_key,
        i.site_name,
        i.timestamp,
        COUNT(DISTINCT i.asset_id) as total_inverters,
        COUNT(DISTINCT CASE WHEN i.metric_value > 0 THEN i.asset_id END) as available_inverters,
        CASE 
            WHEN COUNT(DISTINCT i.asset_id) > 0
            THEN COUNT(DISTINCT CASE WHEN i.metric_value > 0 THEN i.asset_id END)::DECIMAL 
                 / COUNT(DISTINCT i.asset_id)::DECIMAL
            ELSE 0
        END as power_availability_ratio
    FROM "MMSR"."mart"."mart_inverter_performance_5min" i
    WHERE i.site_name = 'PT. MMKI 4.292 MWP - Phase 3'
        AND i.metric_name = 'inv_active_power'
        AND i.metric_value IS NOT NULL
        AND i.date_key >= '2025-10-01'::date
        AND i.date_key <= '2025-11-30'::date
    GROUP BY i.date_key, i.site_name, i.timestamp
),

irradiance_for_mit AS (
    SELECT 
        s.date_key,
        -- MMKI 3 pakai GHI fallback dari MMKI I, jadi perlu handle fallback logic
        CASE 
            WHEN s.site_name = 'PT. MMKI 4.292 MWP - Phase 3' THEN s.site_name
            WHEN s.site_name = 'PT. MMKI 1.75 MWp - Painting Building' AND sc.sensor_type = 'GHI' 
                THEN 'PT. MMKI 4.292 MWP - Phase 3'  -- Apply fallback
            ELSE NULL
        END as site_name,
        s.timestamp,
        COALESCE(
            MAX(CASE WHEN sc.sensor_type = 'GHI' AND s.site_name = 'PT. MMKI 4.292 MWP - Phase 3' THEN s.metric_value END),
            MAX(CASE WHEN sc.sensor_type = 'GHI' AND s.site_name = 'PT. MMKI 1.75 MWp - Painting Building' THEN s.metric_value END),
            MAX(CASE WHEN sc.sensor_type = 'POA' AND s.site_name = 'PT. MMKI 4.292 MWP - Phase 3' THEN s.metric_value END)
        ) as irradiance_w_m2
    FROM "MMSR"."mart"."mart_sensor_measurements_5min" s
    JOIN "MMSR"."mart"."seed_sensor_config" sc 
        ON s.asset_id = CONCAT(
            CASE 
                WHEN s.system = 'fusionsolar' THEN 'FS'
                WHEN s.system = 'isolarcloud' THEN 'ISO'
                ELSE UPPER(LEFT(s.system, 3))
            END, '_', sc.device_id
        )
    WHERE s.metric_name = 'irradiance'
        AND s.metric_value IS NOT NULL
        AND s.date_key >= '2025-10-01'::date
        AND s.date_key <= '2025-11-30'::date
        AND (
            s.site_name = 'PT. MMKI 4.292 MWP - Phase 3'
            OR (s.site_name = 'PT. MMKI 1.75 MWp - Painting Building' AND sc.sensor_type = 'GHI')
        )
    GROUP BY s.date_key, s.site_name, s.timestamp
    HAVING CASE 
        WHEN s.site_name = 'PT. MMKI 4.292 MWP - Phase 3' THEN s.site_name
        WHEN s.site_name = 'PT. MMKI 1.75 MWp - Painting Building' AND sc.sensor_type = 'GHI' 
            THEN 'PT. MMKI 4.292 MWP - Phase 3'
        ELSE NULL
    END IS NOT NULL
),

mit_calculation AS (
    SELECT 
        date_key,
        site_name,
        timestamp,
        CASE 
            WHEN irradiance_w_m2 > 40 THEN 1
            ELSE 0
        END as mit
    FROM irradiance_for_mit
    WHERE site_name = 'PT. MMKI 4.292 MWP - Phase 3'
),

-- Check match between inverter and MIT
match_check AS (
    SELECT 
        COALESCE(ia.date_key, m.date_key) as date_key,
        COUNT(DISTINCT ia.timestamp) as inverter_only_timestamps,
        COUNT(DISTINCT m.timestamp) as mit_only_timestamps,
        COUNT(DISTINCT CASE WHEN ia.timestamp IS NOT NULL AND m.timestamp IS NOT NULL THEN ia.timestamp END) as matched_timestamps
    FROM inverter_availability_5min ia
    FULL OUTER JOIN mit_calculation m 
        ON ia.date_key = m.date_key 
        AND ia.site_name = m.site_name 
        AND ia.timestamp = m.timestamp
    WHERE COALESCE(ia.date_key, m.date_key) >= '2025-10-01'::date
        AND COALESCE(ia.date_key, m.date_key) <= '2025-11-30'::date
    GROUP BY COALESCE(ia.date_key, m.date_key)
)

-- Summary Report
SELECT 
    'Inverter Data' as data_type,
    date_key,
    site_name,
    inverter_timestamps as timestamps_count,
    total_inverters,
    first_timestamp,
    last_timestamp
FROM inverter_data_check

UNION ALL

SELECT 
    'MIT Data' as data_type,
    date_key,
    site_name,
    mit_timestamps as timestamps_count,
    NULL as total_inverters,
    first_timestamp,
    last_timestamp
FROM mit_data_check

UNION ALL

SELECT 
    'Match Check' as data_type,
    date_key,
    'PT. MMKI 4.292 MWP - Phase 3' as site_name,
    matched_timestamps as timestamps_count,
    inverter_only_timestamps + mit_only_timestamps as total_inverters,
    NULL as first_timestamp,
    NULL as last_timestamp
FROM match_check

ORDER BY data_type, date_key;

-- Additional: Check daily availability yang sudah dihitung
SELECT 
    date_key,
    site_name,
    power_available_hours,
    unavailability_hours,
    availability_percent
FROM "MMSR"."mart"."mart_site_performance_daily"
WHERE site_name = 'PT. MMKI 4.292 MWP - Phase 3'
    AND date_key >= '2025-10-01'::date
    AND date_key <= '2025-11-30'::date
ORDER BY date_key;

