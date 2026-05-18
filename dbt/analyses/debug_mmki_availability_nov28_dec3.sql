-- ============================================
-- Debug: MMKI 2 & 3 Availability = 0 dari 28 Nov - 3 Des 2025
-- ============================================
-- Query untuk menganalisis mengapa availability = 0 untuk MMKI 2 & 3 di periode 28 Nov - 3 Des 2025
--
-- ROOT CAUSE ANALYSIS:
-- 1. MMKI 2 & 3 tidak punya GHI sensor sendiri
-- 2. Mereka dikonfigurasi untuk pakai GHI fallback dari MMKI I
-- 3. TAPI di fact_sensor_calculations_5min.sql (line 173-186), GHI fallback dari MMKI I 
--    untuk MMKI 2 & 3 di-SKIP karena GHI MMKI I bermasalah
-- 4. Jadi MMKI 2 & 3 HARUS pakai POA sensor mereka sendiri
-- 5. Jika POA data tidak ada untuk periode tersebut, maka:
--    - Tidak ada MIT data di fact_sensor_calculations_5min
--    - Tidak ada data di fact_inverter_calculations_5min (INNER JOIN dengan MIT)
--    - Tidak ada data di fact_site_calculations_5min
--    - Availability = NULL atau 0 di mart_site_performance_daily
--
-- SOLUSI:
-- - Pastikan POA sensor data untuk MMKI 2 & 3 sudah di-fetch dan di-ingest untuk periode tersebut
-- - Atau jika POA sensor bermasalah, pertimbangkan untuk enable GHI fallback (dengan catatan)
-- ============================================

-- Step 1: Check apakah ada data inverter untuk MMKI 2 & 3 di periode tersebut
WITH inverter_data_check AS (
    SELECT 
        date_key,
        site_name,
        COUNT(DISTINCT timestamp) as inverter_timestamps,
        COUNT(DISTINCT asset_id) as total_inverters,
        MIN(timestamp) as first_timestamp,
        MAX(timestamp) as last_timestamp,
        COUNT(*) as total_records
    FROM "MMSR"."mart"."mart_inverter_performance_5min"
    WHERE site_name IN ('PT. MMKI 5.7 MWp - Phase 2', 'PT. MMKI 4.292 MWP - Phase 3')
        AND metric_name = 'inv_active_power'
        AND metric_value IS NOT NULL
        AND date_key >= '2025-11-28'::date
        AND date_key <= '2025-12-03'::date
    GROUP BY date_key, site_name
),

-- Step 2: Check apakah ada data POA (irradiance) untuk MMKI 2 & 3
-- MMKI 2 & 3 menggunakan POA (bukan GHI fallback, karena GHI MMKI 1 bermasalah)
poa_data_check AS (
    SELECT 
        s.date_key,
        s.site_name,
        sc.sensor_type,
        COUNT(DISTINCT s.timestamp) as poa_timestamps,
        MIN(s.timestamp) as first_timestamp,
        MAX(s.timestamp) as last_timestamp,
        MIN(s.metric_value) as min_irradiance,
        MAX(s.metric_value) as max_irradiance,
        COUNT(*) as total_records
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
        AND sc.sensor_type = 'POA'
        AND s.date_key >= '2025-11-28'::date
        AND s.date_key <= '2025-12-03'::date
        AND s.site_name IN ('PT. MMKI 5.7 MWp - Phase 2', 'PT. MMKI 4.292 MWP - Phase 3')
    GROUP BY s.date_key, s.site_name, sc.sensor_type
),

-- Step 2B: Check GHI data (untuk referensi, meskipun tidak digunakan karena fallback di-skip)
ghi_data_check AS (
    SELECT 
        s.date_key,
        s.site_name,
        sc.sensor_type,
        COUNT(DISTINCT s.timestamp) as ghi_timestamps,
        MIN(s.timestamp) as first_timestamp,
        MAX(s.timestamp) as last_timestamp,
        MIN(s.metric_value) as min_irradiance,
        MAX(s.metric_value) as max_irradiance,
        COUNT(*) as total_records
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
        AND sc.sensor_type = 'GHI'
        AND s.date_key >= '2025-11-28'::date
        AND s.date_key <= '2025-12-03'::date
        AND (
            s.site_name IN ('PT. MMKI 5.7 MWp - Phase 2', 'PT. MMKI 4.292 MWP - Phase 3')
            OR s.site_name = 'PT. MMKI 1.75 MWp - Painting Building'  -- MMKI I GHI (fallback source, tapi di-skip)
        )
    GROUP BY s.date_key, s.site_name, sc.sensor_type
),

-- Step 3: Check fact_sensor_calculations_5min (yang digunakan untuk MIT)
fact_sensor_mit_check AS (
    SELECT 
        date_key,
        site_name,
        COUNT(DISTINCT timestamp) as mit_timestamps,
        COUNT(DISTINCT CASE WHEN mit = 1 THEN timestamp END) as mit_1_timestamps,
        COUNT(DISTINCT CASE WHEN mit = 0 THEN timestamp END) as mit_0_timestamps,
        MIN(timestamp) as first_timestamp,
        MAX(timestamp) as last_timestamp
    FROM "MMSR"."mart"."fact_sensor_calculations_5min"
    WHERE site_name IN ('PT. MMKI 5.7 MWp - Phase 2', 'PT. MMKI 4.292 MWP - Phase 3')
        AND date_key >= '2025-11-28'::date
        AND date_key <= '2025-12-03'::date
    GROUP BY date_key, site_name
),

-- Step 4: Check fact_inverter_calculations_5min (hasil join inverter + MIT)
fact_inverter_check AS (
    SELECT 
        date_key,
        site_name,
        COUNT(DISTINCT timestamp) as inverter_calc_timestamps,
        COUNT(DISTINCT inverter_id) as total_inverters,
        COUNT(DISTINCT CASE WHEN inverter_availability = 1 THEN inverter_id END) as available_inverters,
        MIN(timestamp) as first_timestamp,
        MAX(timestamp) as last_timestamp
    FROM "MMSR"."mart"."fact_inverter_calculations_5min"
    WHERE site_name IN ('PT. MMKI 5.7 MWp - Phase 2', 'PT. MMKI 4.292 MWP - Phase 3')
        AND date_key >= '2025-11-28'::date
        AND date_key <= '2025-12-03'::date
    GROUP BY date_key, site_name
),

-- Step 5: Check fact_site_calculations_5min (aggregate dari inverter)
fact_site_check AS (
    SELECT 
        date_key,
        site_name,
        COUNT(DISTINCT timestamp) as site_calc_timestamps,
        AVG(total_inverters) as avg_total_inverters,
        AVG(available_inverters) as avg_available_inverters,
        AVG(power_available_ratio) as avg_power_available_ratio,
        AVG(unavailability_ratio) as avg_unavailability_ratio,
        SUM(power_available_ratio) * 5.0 / 60.0 as power_available_hours,
        SUM(unavailability_ratio) * 5.0 / 60.0 as unavailability_hours,
        MIN(timestamp) as first_timestamp,
        MAX(timestamp) as last_timestamp
    FROM "MMSR"."mart"."fact_site_calculations_5min"
    WHERE site_name IN ('PT. MMKI 5.7 MWp - Phase 2', 'PT. MMKI 4.292 MWP - Phase 3')
        AND date_key >= '2025-11-28'::date
        AND date_key <= '2025-12-03'::date
    GROUP BY date_key, site_name
),

-- Step 6: Check daily availability yang sudah dihitung
daily_availability_check AS (
    SELECT 
        date_key,
        site_name,
        power_available_hours,
        unavailability_hours,
        mit_hours,
        total_hours,
        availability_percent
    FROM "MMSR"."mart"."mart_site_performance_daily"
    WHERE site_name IN ('PT. MMKI 5.7 MWp - Phase 2', 'PT. MMKI 4.292 MWP - Phase 3')
        AND date_key >= '2025-11-28'::date
        AND date_key <= '2025-12-03'::date
)

-- Summary Report: Inverter Data
SELECT 
    '1. Inverter Raw Data' as check_type,
    date_key,
    site_name,
    inverter_timestamps as timestamps_count,
    total_inverters,
    total_records,
    first_timestamp,
    last_timestamp
FROM inverter_data_check

UNION ALL

-- Summary Report: POA Data (Raw Sensor) - Yang seharusnya digunakan
SELECT 
    '2. POA Raw Data (Sensor)' as check_type,
    date_key,
    site_name,
    poa_timestamps as timestamps_count,
    NULL as total_inverters,
    total_records,
    first_timestamp,
    last_timestamp
FROM poa_data_check

UNION ALL

-- Summary Report: GHI Data (Raw Sensor) - Untuk referensi (fallback di-skip)
SELECT 
    '2B. GHI Raw Data (Sensor - Not Used)' as check_type,
    date_key,
    site_name,
    ghi_timestamps as timestamps_count,
    NULL as total_inverters,
    total_records,
    first_timestamp,
    last_timestamp
FROM ghi_data_check

UNION ALL

-- Summary Report: MIT Data (fact_sensor_calculations_5min)
SELECT 
    '3. MIT Calculated (fact_sensor)' as check_type,
    date_key,
    site_name,
    mit_timestamps as timestamps_count,
    NULL as total_inverters,
    NULL as total_records,
    first_timestamp,
    last_timestamp
FROM fact_sensor_mit_check

UNION ALL

-- Summary Report: Inverter Calculations (fact_inverter_calculations_5min)
SELECT 
    '4. Inverter Calculations (fact_inverter)' as check_type,
    date_key,
    site_name,
    inverter_calc_timestamps as timestamps_count,
    total_inverters,
    NULL as total_records,
    first_timestamp,
    last_timestamp
FROM fact_inverter_check

UNION ALL

-- Summary Report: Site Calculations (fact_site_calculations_5min)
SELECT 
    '5. Site Calculations (fact_site)' as check_type,
    date_key,
    site_name,
    site_calc_timestamps as timestamps_count,
    NULL as total_inverters,
    NULL as total_records,
    first_timestamp,
    last_timestamp
FROM fact_site_check

ORDER BY check_type, site_name, date_key;

-- Additional: Detailed daily availability
SELECT 
    'Daily Availability Summary' as report_type,
    date_key,
    site_name,
    power_available_hours,
    unavailability_hours,
    mit_hours,
    total_hours,
    availability_percent,
    CASE 
        WHEN availability_percent IS NULL THEN 'NULL (no data)'
        WHEN availability_percent = 0 THEN '0 (no availability)'
        ELSE 'Has availability'
    END as status
FROM daily_availability_check
ORDER BY site_name, date_key;

-- Additional: Check POA override mapping untuk MMKI 2 & 3
SELECT 
    'POA Override Mapping' as report_type,
    device_id as sensor_device_id,
    logical_site_id as target_site,
    mapping_type,
    effective_date_start,
    effective_date_end,
    CASE 
        WHEN effective_date_start IS NULL OR effective_date_start::text = '' THEN 'Always applies'
        WHEN '2025-11-28'::date >= effective_date_start::date THEN 'Applies for Nov 28 - Dec 3 2025'
        ELSE 'Does NOT apply for Nov 28 - Dec 3 2025 (effective_date_start too late)'
    END as override_status
FROM "MMSR"."mart"."seed_sensor_site_mapping"
WHERE mapping_type = 'POA_OVERRIDE'
    AND logical_site_id IN ('PT. MMKI 5.7 MWp - Phase 2', 'PT. MMKI 4.292 MWP - Phase 3')

UNION ALL

-- Additional: Check GHI fallback mapping untuk MMKI 2 & 3
SELECT 
    'GHI Fallback Mapping' as report_type,
    device_id as target_site,
    logical_site_id as source_site,
    mapping_type,
    effective_date_start,
    effective_date_end,
    CASE 
        WHEN effective_date_start IS NULL OR effective_date_start::text = '' THEN 'Always applies'
        WHEN '2025-11-28'::date >= effective_date_start::date THEN 'Applies for Nov 28 - Dec 3 2025'
        ELSE 'Does NOT apply for Nov 28 - Dec 3 2025 (effective_date_start too late)'
    END as override_status
FROM "MMSR"."mart"."seed_sensor_site_mapping"
WHERE device_id IN ('PT. MMKI 5.7 MWp - Phase 2', 'PT. MMKI 4.292 MWP - Phase 3')
    AND mapping_type = 'GHI_FALLBACK';

