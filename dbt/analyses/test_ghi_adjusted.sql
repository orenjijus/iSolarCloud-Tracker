-- Test query untuk memverifikasi ghi_adjusted logic
-- Test apakah ghi_adjusted bekerja dengan benar untuk MMKI group sites

-- 1. Test data availability untuk FLN sensor (November)
SELECT 
    'FLN Sensor (November)' as test_name,
    date_key,
    asset_id,
    sensor_dev_name,
    daily_irradiance_kwh_m2 as ghi_fln_kwh_m2,
    COUNT(*) OVER (PARTITION BY EXTRACT(YEAR FROM date_key), EXTRACT(MONTH FROM date_key)) as records_in_month
FROM "MMSR"."mart"."mart_sensor_daily"
WHERE asset_id = 'ISO_1628909_5_9_2'  -- FLN-Injection-PYR-01
    AND sensor_type = 'GHI'
    AND EXTRACT(MONTH FROM date_key) = 11  -- November
    AND date_key >= '2024-11-01'::date
    AND date_key <= '2024-11-30'::date
ORDER BY date_key DESC
LIMIT 10;

-- 2. Test data availability untuk WTST Assembly sensor (December)
SELECT 
    'WTST Assembly Sensor (December)' as test_name,
    date_key,
    asset_id,
    sensor_dev_name,
    COALESCE(
        daily_irradiance_kwh_m2,
        daily_horizontal_irradiation_kwh_m2
    ) as ghi_wtst_kwh_m2,
    daily_irradiance_kwh_m2,
    daily_horizontal_irradiation_kwh_m2,
    COUNT(*) OVER (PARTITION BY EXTRACT(YEAR FROM date_key), EXTRACT(MONTH FROM date_key)) as records_in_month
FROM "MMSR"."mart"."mart_sensor_daily"
WHERE asset_id = 'FS_EM041023B7436516'  -- WTST - Assembly
    AND (
        daily_irradiance_kwh_m2 IS NOT NULL 
        OR daily_horizontal_irradiation_kwh_m2 IS NOT NULL
    )
    AND EXTRACT(MONTH FROM date_key) = 12  -- December
    AND date_key >= '2024-12-01'::date
    AND date_key <= '2024-12-31'::date
ORDER BY date_key DESC
LIMIT 10;

-- 3. Test ghi_adjusted untuk MMKI group sites di November
SELECT 
    'MMKI Group - November (should use FLN)' as test_name,
    spd.date_key,
    spd.site_name,
    spd.daily_ghi_kwh_m2 as ghi_actual,
    spd.ghi_adjusted,
    ghi_fln.ghi_fln_kwh_m2 as ghi_fln_source,
    CASE 
        WHEN spd.ghi_adjusted = ghi_fln.ghi_fln_kwh_m2 THEN '✓ Match FLN'
        WHEN spd.ghi_adjusted = spd.daily_ghi_kwh_m2 THEN '⚠ Using actual (FLN not available)'
        ELSE '✗ Mismatch'
    END as validation_status
FROM "MMSR"."mart"."mart_site_performance_daily" spd
LEFT JOIN (
    SELECT 
        date_key,
        daily_irradiance_kwh_m2 as ghi_fln_kwh_m2
    FROM "MMSR"."mart"."mart_sensor_daily"
    WHERE asset_id = 'ISO_1628909_5_9_2'
        AND sensor_type = 'GHI'
        AND EXTRACT(MONTH FROM date_key) = 11
) ghi_fln ON spd.date_key = ghi_fln.date_key
WHERE (spd.site_name LIKE '%MMKI%Phase 2%' OR spd.site_name LIKE '%MMKI%5.7%' 
       OR spd.site_name LIKE '%MMKI%Phase 3%' OR spd.site_name LIKE '%MMKI%4.292%')
    AND EXTRACT(MONTH FROM spd.date_key) = 11  -- November
    AND spd.date_key >= '2024-11-01'::date
    AND spd.date_key <= '2024-11-30'::date
ORDER BY spd.site_name, spd.date_key DESC
LIMIT 20;

-- 4. Test ghi_adjusted untuk MMKI group sites di December
SELECT 
    'MMKI Group - December (should use WTST)' as test_name,
    spd.date_key,
    spd.site_name,
    spd.daily_ghi_kwh_m2 as ghi_actual,
    spd.ghi_adjusted,
    ghi_wtst.ghi_wtst_kwh_m2 as ghi_wtst_source,
    CASE 
        WHEN spd.ghi_adjusted = ghi_wtst.ghi_wtst_kwh_m2 THEN '✓ Match WTST'
        WHEN spd.ghi_adjusted = spd.daily_ghi_kwh_m2 THEN '⚠ Using actual (WTST not available)'
        ELSE '✗ Mismatch'
    END as validation_status
FROM "MMSR"."mart"."mart_site_performance_daily" spd
LEFT JOIN (
    SELECT 
        date_key,
        COALESCE(
            daily_irradiance_kwh_m2,
            daily_horizontal_irradiation_kwh_m2
        ) as ghi_wtst_kwh_m2
    FROM "MMSR"."mart"."mart_sensor_daily"
    WHERE asset_id = 'FS_EM041023B7436516'
        AND (
            daily_irradiance_kwh_m2 IS NOT NULL 
            OR daily_horizontal_irradiation_kwh_m2 IS NOT NULL
        )
        AND EXTRACT(MONTH FROM date_key) = 12
) ghi_wtst ON spd.date_key = ghi_wtst.date_key
WHERE (spd.site_name LIKE '%MMKI%Phase 2%' OR spd.site_name LIKE '%MMKI%5.7%' 
       OR spd.site_name LIKE '%MMKI%Phase 3%' OR spd.site_name LIKE '%MMKI%4.292%')
    AND EXTRACT(MONTH FROM spd.date_key) = 12  -- December
    AND spd.date_key >= '2024-12-01'::date
    AND spd.date_key <= '2024-12-31'::date
ORDER BY spd.site_name, spd.date_key DESC
LIMIT 20;

-- 5. Test ghi_adjusted untuk non-MMKI sites (should use ghi_actual)
SELECT 
    'Non-MMKI Sites (should use ghi_actual)' as test_name,
    spd.date_key,
    spd.site_name,
    spd.daily_ghi_kwh_m2 as ghi_actual,
    spd.ghi_adjusted,
    CASE 
        WHEN spd.ghi_adjusted = spd.daily_ghi_kwh_m2 THEN '✓ Match actual'
        WHEN spd.ghi_adjusted IS NULL AND spd.daily_ghi_kwh_m2 IS NULL THEN '✓ Both NULL'
        ELSE '✗ Mismatch'
    END as validation_status
FROM "MMSR"."mart"."mart_site_performance_daily" spd
WHERE NOT (spd.site_name LIKE '%MMKI%Phase 2%' OR spd.site_name LIKE '%MMKI%5.7%' 
           OR spd.site_name LIKE '%MMKI%Phase 3%' OR spd.site_name LIKE '%MMKI%4.292%')
    AND spd.date_key >= '2024-11-01'::date
    AND spd.date_key <= '2024-12-31'::date
    AND spd.daily_ghi_kwh_m2 IS NOT NULL
ORDER BY spd.site_name, spd.date_key DESC
LIMIT 20;
