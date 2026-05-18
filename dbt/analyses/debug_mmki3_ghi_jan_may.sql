-- Debug query untuk trace data GHI MMKI 3 di Januari-Mei 2025
-- Cek di setiap level untuk melihat di mana data muncul

-- Step 1: Cek di mart_sensor_measurements_5min
SELECT 
    'mart_sensor_measurements_5min' as source_table,
    COUNT(*) as row_count,
    COUNT(DISTINCT date_key) as distinct_dates,
    MIN(date_key) as min_date,
    MAX(date_key) as max_date
FROM "MMSR"."mart"."mart_sensor_measurements_5min"
WHERE site_name = 'PT. MMKI 4.292 MWP - Phase 3'
    AND sensor_type = 'GHI'
    AND date_key >= '2025-01-01'::date
    AND date_key <= '2025-05-31'::date;

-- Step 2: Cek di mart_sensor_daily
SELECT 
    'mart_sensor_daily' as source_table,
    COUNT(*) as row_count,
    COUNT(DISTINCT date_key) as distinct_dates,
    MIN(date_key) as min_date,
    MAX(date_key) as max_date,
    STRING_AGG(DISTINCT site_name, ', ') as site_names
FROM "MMSR"."mart"."mart_sensor_daily"
WHERE site_name = 'PT. MMKI 4.292 MWP - Phase 3'
    AND sensor_type = 'GHI'
    AND date_key >= '2025-01-01'::date
    AND date_key <= '2025-05-31'::date;

-- Step 3: Simulasi daily_ghi_raw (dengan filter NOT EXISTS)
SELECT 
    'daily_ghi_raw (simulated)' as source_table,
    COUNT(*) as row_count,
    COUNT(DISTINCT date_key) as distinct_dates,
    MIN(date_key) as min_date,
    MAX(date_key) as max_date
FROM "MMSR"."mart"."mart_sensor_daily" msd
WHERE sensor_type = 'GHI'
    AND daily_irradiance_kwh_m2 IS NOT NULL
    AND date_key >= '2025-01-01'::date
    AND date_key <= '2025-05-31'::date
    -- Exclude sites that use GHI fallback
    AND NOT EXISTS (
        SELECT 1 
        FROM "MMSR"."staging"."seed_sensor_site_mapping" ssm
        WHERE TRIM(UPPER(COALESCE(ssm.device_id, ''))) = TRIM(UPPER(COALESCE(msd.site_name, '')))
            AND ssm.mapping_type = 'GHI_FALLBACK'
    )
    AND msd.site_name = 'PT. MMKI 4.292 MWP - Phase 3';

-- Step 4: Cek apakah ada data GHI untuk MMKI 1 yang mungkin digunakan untuk fallback
SELECT 
    'mart_sensor_daily_MMKI1' as source_table,
    COUNT(*) as row_count,
    COUNT(DISTINCT date_key) as distinct_dates
FROM "MMSR"."mart"."mart_sensor_daily"
WHERE site_name = 'PT. MMKI 1.75 MWp - Painting Building'
    AND sensor_type = 'GHI'
    AND date_key >= '2025-01-01'::date
    AND date_key <= '2025-05-31'::date;

-- Step 5: Simulasi fallback logic (bagian kedua UNION)
SELECT 
    'fallback_logic (simulated)' as source_table,
    COUNT(*) as row_count,
    COUNT(DISTINCT ghi_source.date_key) as distinct_dates,
    MIN(ghi_source.date_key) as min_date,
    MAX(ghi_source.date_key) as max_date
FROM "MMSR"."mart"."mart_sensor_daily" ghi_source
INNER JOIN "MMSR"."staging"."seed_sensor_site_mapping" ssm
    ON TRIM(UPPER(COALESCE(ghi_source.site_name, ''))) = TRIM(UPPER(COALESCE(ssm.logical_site_id, '')))
    AND ssm.mapping_type = 'GHI_FALLBACK'
    AND ssm.device_id = 'PT. MMKI 4.292 MWP - Phase 3'
WHERE ghi_source.sensor_type = 'GHI'
    AND ghi_source.daily_irradiance_kwh_m2 IS NOT NULL
    AND ghi_source.date_key >= '2025-01-01'::date
    AND ghi_source.date_key <= '2025-05-31'::date
    -- Check effective_date_start
    AND (
        ssm.effective_date_start IS NULL 
        OR ssm.effective_date_start::text = ''
        OR (
            ssm.effective_date_start IS NOT NULL 
            AND ssm.effective_date_start::text != ''
            AND ghi_source.date_key >= ssm.effective_date_start::date
        )
    );

-- Step 6: Cek di mart_site_performance_daily (final output)
SELECT 
    'mart_site_performance_daily' as source_table,
    COUNT(*) as row_count,
    COUNT(DISTINCT date_key) as distinct_dates,
    MIN(date_key) as min_date,
    MAX(date_key) as max_date,
    COUNT(*) FILTER (WHERE daily_ghi_kwh_m2 IS NOT NULL) as rows_with_ghi,
    MIN(daily_ghi_kwh_m2) as min_ghi,
    MAX(daily_ghi_kwh_m2) as max_ghi
FROM "MMSR"."mart"."mart_site_performance_daily"
WHERE site_name = 'PT. MMKI 4.292 MWP - Phase 3'
    AND date_key >= '2025-01-01'::date
    AND date_key <= '2025-05-31'::date;

-- Step 7: Cek seed_sensor_site_mapping untuk MMKI 3
SELECT 
    'seed_sensor_site_mapping' as source_table,
    mapping_type,
    device_id,
    logical_site_id,
    effective_date_start,
    effective_date_end,
    notes
FROM "MMSR"."staging"."seed_sensor_site_mapping"
WHERE device_id = 'PT. MMKI 4.292 MWP - Phase 3'
    OR logical_site_id = 'PT. MMKI 4.292 MWP - Phase 3';

