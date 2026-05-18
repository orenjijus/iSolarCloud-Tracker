-- ============================================
-- DATABASE vs EXCEL CROSS-CHECK QUERIES
-- ============================================
-- File ini berisi semua query yang diperlukan untuk cross-check
-- antara database (mart_site_performance_daily) dengan Excel
-- 
-- Referensi lengkap: DATABASE_EXCEL_CROSS_CHECK_GUIDE.md
-- ============================================

-- ============================================
-- STEP 1: Export Database Data ke CSV
-- ============================================
-- Simpan hasil query ini sebagai CSV: database_daily_performance.csv
-- Format: CSV dengan header, delimiter comma

SELECT 
    date_key,
    site_name,
    site_id,
    daily_energy_mwh,
    daily_ghi_kwh_m2,
    daily_poa_weighted_kwh_m2,
    availability_percent,
    pr_ghi_actual,
    pr_poa_actual,
    system
FROM "MMSR"."mart"."mart_site_performance_daily"
WHERE date_key >= '2024-01-01'  -- Sesuaikan dengan range tanggal yang ingin dicek
    AND date_key <= CURRENT_DATE
ORDER BY site_name, date_key;


-- ============================================
-- STEP 2: Buat Temporary Table untuk Excel Data
-- ============================================

-- Drop table jika sudah ada (untuk refresh data)
DROP TABLE IF EXISTS temp_excel_daily_performance;

-- Buat temporary table dengan struktur yang sesuai
-- Hanya kolom yang ada di Excel daily performance (tidak termasuk target/vs target yang hanya ada di monthly/yearly)
CREATE TEMP TABLE temp_excel_daily_performance (
    date_key DATE,
    site_name VARCHAR(255),
    site_id VARCHAR(100),
    daily_energy_mwh NUMERIC(12, 4),
    daily_ghi_kwh_m2 NUMERIC(10, 4),
    daily_poa_weighted_kwh_m2 NUMERIC(10, 4),
    availability_percent NUMERIC(5, 2),
    pr_ghi_actual NUMERIC(5, 4),
    pr_poa_actual NUMERIC(5, 4),
    system VARCHAR(50),
    notes TEXT  -- Untuk catatan tambahan
);

-- Import data Excel ke temporary table
-- Opsi 1: Jika Excel sudah di-export ke CSV
-- COPY temp_excel_daily_performance (
--     date_key,
--     site_name,
--     site_id,
--     daily_energy_mwh,
--     daily_ghi_kwh_m2,
--     daily_poa_weighted_kwh_m2,
--     availability_percent,
--     pr_ghi_actual,
--     pr_poa_actual,
--     system
-- )
-- FROM 'C:\path\to\excel_export.csv'  -- Ganti dengan path file CSV Excel Anda
-- WITH (FORMAT csv, HEADER true, DELIMITER ',', QUOTE '"');

-- Opsi 2: Manual INSERT (jika data sedikit)
-- INSERT INTO temp_excel_daily_performance VALUES (...);

-- Verifikasi data yang di-import
SELECT 
    COUNT(*) as total_rows,
    MIN(date_key) as min_date,
    MAX(date_key) as max_date,
    COUNT(DISTINCT site_name) as total_sites
FROM temp_excel_daily_performance;


-- ============================================
-- STEP 3: Comparison Query - Identifikasi Discrepancy
-- ============================================
-- Query ini membandingkan database dengan Excel (Excel sebagai benchmark)
-- Menampilkan hanya data yang berbeda atau missing

WITH db_data AS (
    SELECT 
        date_key,
        site_name,
        site_id,
        daily_energy_mwh,
        daily_ghi_kwh_m2,
        daily_poa_weighted_kwh_m2,
        availability_percent,
        pr_ghi_actual,
        pr_poa_actual,
        system
    FROM "MMSR"."mart"."mart_site_performance_daily"
    WHERE date_key >= (SELECT MIN(date_key) FROM temp_excel_daily_performance)
        AND date_key <= (SELECT MAX(date_key) FROM temp_excel_daily_performance)
),
excel_data AS (
    SELECT 
        date_key,
        site_name,
        site_id,
        daily_energy_mwh,
        daily_ghi_kwh_m2,
        daily_poa_weighted_kwh_m2,
        availability_percent,
        pr_ghi_actual,
        pr_poa_actual,
        system
    FROM temp_excel_daily_performance
),
comparison AS (
    SELECT 
        COALESCE(db.date_key, excel.date_key) as date_key,
        COALESCE(db.site_name, excel.site_name) as site_name,
        COALESCE(db.site_id, excel.site_id) as site_id,
        
        -- Database values
        db.daily_energy_mwh as db_energy_mwh,
        db.daily_ghi_kwh_m2 as db_ghi,
        db.daily_poa_weighted_kwh_m2 as db_poa,
        db.availability_percent as db_availability,
        db.pr_ghi_actual as db_pr_ghi,
        db.pr_poa_actual as db_pr_poa,
        
        -- Excel values (benchmark)
        excel.daily_energy_mwh as excel_energy_mwh,
        excel.daily_ghi_kwh_m2 as excel_ghi,
        excel.daily_poa_weighted_kwh_m2 as excel_poa,
        excel.availability_percent as excel_availability,
        excel.pr_ghi_actual as excel_pr_ghi,
        excel.pr_poa_actual as excel_pr_poa,
        
        -- Absolute differences
        COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0) as energy_diff_mwh,
        COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0) as ghi_diff,
        COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0) as poa_diff,
        COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0) as availability_diff,
        
        -- Percentage differences (relative to Excel - benchmark)
        CASE 
            WHEN excel.daily_energy_mwh > 0 
            THEN ((COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) / excel.daily_energy_mwh) * 100
            ELSE NULL
        END as energy_diff_pct,
        
        CASE 
            WHEN excel.daily_ghi_kwh_m2 > 0 
            THEN ((COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) / excel.daily_ghi_kwh_m2) * 100
            ELSE NULL
        END as ghi_diff_pct,
        
        CASE 
            WHEN excel.availability_percent > 0 
            THEN ((COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0)) / excel.availability_percent) * 100
            ELSE NULL
        END as availability_diff_pct,
        
        -- Status flags untuk setiap metric
        CASE 
            WHEN ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) > 0.01 THEN 'MISMATCH'
            WHEN db.daily_energy_mwh IS NULL AND excel.daily_energy_mwh IS NOT NULL THEN 'MISSING_IN_DB'
            WHEN db.daily_energy_mwh IS NOT NULL AND excel.daily_energy_mwh IS NULL THEN 'MISSING_IN_EXCEL'
            ELSE 'MATCH'
        END as energy_status,
        
        CASE 
            WHEN ABS(COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) > 0.01 THEN 'MISMATCH'
            WHEN db.daily_ghi_kwh_m2 IS NULL AND excel.daily_ghi_kwh_m2 IS NOT NULL THEN 'MISSING_IN_DB'
            WHEN db.daily_ghi_kwh_m2 IS NOT NULL AND excel.daily_ghi_kwh_m2 IS NULL THEN 'MISSING_IN_EXCEL'
            ELSE 'MATCH'
        END as ghi_status,
        
        CASE 
            WHEN ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) > 0.01 THEN 'MISMATCH'
            WHEN db.daily_poa_weighted_kwh_m2 IS NULL AND excel.daily_poa_weighted_kwh_m2 IS NOT NULL THEN 'MISSING_IN_DB'
            WHEN db.daily_poa_weighted_kwh_m2 IS NOT NULL AND excel.daily_poa_weighted_kwh_m2 IS NULL THEN 'MISSING_IN_EXCEL'
            ELSE 'MATCH'
        END as poa_status,
        
        CASE 
            WHEN ABS(COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0)) > 1.0 THEN 'MISMATCH'  -- 1% tolerance
            WHEN db.availability_percent IS NULL AND excel.availability_percent IS NOT NULL THEN 'MISSING_IN_DB'
            WHEN db.availability_percent IS NOT NULL AND excel.availability_percent IS NULL THEN 'MISSING_IN_EXCEL'
            ELSE 'MATCH'
        END as availability_status,
        
        -- Data source indicator
        CASE 
            WHEN db.date_key IS NULL THEN 'ONLY_IN_EXCEL'
            WHEN excel.date_key IS NULL THEN 'ONLY_IN_DB'
            ELSE 'BOTH'
        END as data_source
        
    FROM db_data db
    FULL OUTER JOIN excel_data excel
        ON db.date_key = excel.date_key 
        AND UPPER(TRIM(db.site_name)) = UPPER(TRIM(excel.site_name))
)
SELECT 
    date_key,
    site_name,
    site_id,
    data_source,
    
    -- Energy comparison
    energy_status,
    db_energy_mwh,
    excel_energy_mwh,
    ROUND(energy_diff_mwh::numeric, 4) as energy_diff_mwh,
    ROUND(energy_diff_pct::numeric, 2) as energy_diff_pct,
    
    -- GHI comparison
    ghi_status,
    db_ghi,
    excel_ghi,
    ROUND(ghi_diff::numeric, 4) as ghi_diff,
    ROUND(ghi_diff_pct::numeric, 2) as ghi_diff_pct,
    
    -- POA comparison
    poa_status,
    db_poa,
    excel_poa,
    ROUND(poa_diff::numeric, 4) as poa_diff,
    
    -- Availability comparison
    availability_status,
    db_availability,
    excel_availability,
    ROUND(availability_diff::numeric, 2) as availability_diff,
    ROUND(availability_diff_pct::numeric, 2) as availability_diff_pct,
    
    -- PR comparison
    db_pr_ghi,
    excel_pr_ghi,
    db_pr_poa,
    excel_pr_poa,
    
    -- Overall status
    CASE 
        WHEN energy_status != 'MATCH' OR ghi_status != 'MATCH' 
            OR poa_status != 'MATCH' OR availability_status != 'MATCH' 
        THEN '⚠️ DISCREPANCY'
        WHEN data_source != 'BOTH'
        THEN '⚠️ MISSING_DATA'
        ELSE '✓ MATCH'
    END as overall_status
    
FROM comparison
WHERE 
    -- Tampilkan hanya discrepancy dan missing data
    energy_status != 'MATCH' 
    OR ghi_status != 'MATCH' 
    OR poa_status != 'MATCH'
    OR availability_status != 'MATCH'
    OR data_source != 'BOTH'
ORDER BY 
    overall_status DESC,
    site_name,
    date_key DESC;


-- ============================================
-- STEP 4: Summary Report - Overview Discrepancy
-- ============================================
-- Menampilkan ringkasan discrepancy per site

WITH db_data AS (
    SELECT 
        date_key,
        site_name,
        daily_energy_mwh,
        daily_ghi_kwh_m2,
        daily_poa_weighted_kwh_m2,
        availability_percent
    FROM "MMSR"."mart"."mart_site_performance_daily"
    WHERE date_key >= (SELECT MIN(date_key) FROM temp_excel_daily_performance)
        AND date_key <= (SELECT MAX(date_key) FROM temp_excel_daily_performance)
),
excel_data AS (
    SELECT 
        date_key,
        site_name,
        daily_energy_mwh,
        daily_ghi_kwh_m2,
        daily_poa_weighted_kwh_m2,
        availability_percent
    FROM temp_excel_daily_performance
),
comparison AS (
    SELECT 
        COALESCE(db.date_key, excel.date_key) as date_key,
        COALESCE(db.site_name, excel.site_name) as site_name,
        CASE 
            WHEN ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) > 0.01 THEN 1 ELSE 0
        END as energy_mismatch,
        CASE 
            WHEN ABS(COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) > 0.01 THEN 1 ELSE 0
        END as ghi_mismatch,
        CASE 
            WHEN ABS(COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0)) > 1.0 THEN 1 ELSE 0
        END as availability_mismatch,
        CASE 
            WHEN db.date_key IS NULL OR excel.date_key IS NULL THEN 1 ELSE 0
        END as missing_data
    FROM db_data db
    FULL OUTER JOIN excel_data excel
        ON db.date_key = excel.date_key 
        AND UPPER(TRIM(db.site_name)) = UPPER(TRIM(excel.site_name))
)
SELECT 
    site_name,
    COUNT(*) as total_days,
    SUM(energy_mismatch) as energy_mismatch_count,
    SUM(ghi_mismatch) as ghi_mismatch_count,
    SUM(availability_mismatch) as availability_mismatch_count,
    SUM(missing_data) as missing_data_count,
    ROUND(100.0 * SUM(energy_mismatch) / COUNT(*)::numeric, 2) as energy_mismatch_pct,
    ROUND(100.0 * SUM(ghi_mismatch) / COUNT(*)::numeric, 2) as ghi_mismatch_pct,
    ROUND(100.0 * SUM(availability_mismatch) / COUNT(*)::numeric, 2) as availability_mismatch_pct,
    CASE 
        WHEN SUM(energy_mismatch) > 0 OR SUM(ghi_mismatch) > 0 
            OR SUM(availability_mismatch) > 0 OR SUM(missing_data) > 0
        THEN '⚠️ HAS_ISSUES'
        ELSE '✓ CLEAN'
    END as status
FROM comparison
GROUP BY site_name
ORDER BY 
    (SUM(energy_mismatch) + SUM(ghi_mismatch) + SUM(availability_mismatch) + SUM(missing_data)) DESC,
    site_name;


-- ============================================
-- STEP 5: Root Cause Analysis - Drill Down ke Data Detail
-- ============================================
-- Gunakan query ini untuk investigasi detail ketika menemukan discrepancy
-- Ganti parameter di CTE problem_day dengan site dan tanggal yang bermasalah

WITH problem_day AS (
    SELECT 
        '2024-01-15'::DATE as problem_date,  -- Ganti dengan tanggal yang bermasalah
        'Site Name' as problem_site          -- Ganti dengan nama site yang bermasalah
),
-- Check meter data (untuk energy calculation)
meter_check AS (
    SELECT 
        m.date_key,
        m.site_name,
        m.asset_id,
        m.metric_name,
        COUNT(*) as reading_count,
        MIN(m.metric_value) as min_value,
        MAX(m.metric_value) as max_value,
        MAX(m.metric_value) - MIN(m.metric_value) as daily_delta,
        -- Check untuk cumulative meter
        LAG(MAX(m.metric_value)) OVER (
            PARTITION BY m.site_name, m.asset_id, m.metric_name 
            ORDER BY m.date_key
        ) as prev_day_max
    FROM "MMSR"."mart"."mart_meter_performance_5min" m
    CROSS JOIN problem_day pd
    WHERE m.date_key BETWEEN pd.problem_date - INTERVAL '1 day' AND pd.problem_date
        AND m.site_name = pd.problem_site
        AND m.metric_name IN ('positive_active_energy', 'negative_active_energy')
    GROUP BY m.date_key, m.site_name, m.asset_id, m.metric_name
),
-- Check sensor data (untuk GHI/POA calculation)
sensor_check AS (
    SELECT 
        s.date_key,
        s.site_name,
        s.asset_id,
        s.metric_name,
        COUNT(*) as reading_count,
        MAX(s.metric_value) as max_irradiance,
        AVG(s.metric_value) as avg_irradiance
    FROM "MMSR"."mart"."mart_sensor_measurements_5min" s
    CROSS JOIN problem_day pd
    WHERE s.date_key = pd.problem_date
        AND s.site_name = pd.problem_site
        AND s.metric_name IN ('daily_irradiance', 'irradiance')
    GROUP BY s.date_key, s.site_name, s.asset_id, s.metric_name
),
-- Check inverter data (untuk availability calculation)
inverter_check AS (
    SELECT 
        i.date_key,
        i.site_name,
        i.asset_id,
        COUNT(DISTINCT i.timestamp) as timestamp_count,
        COUNT(DISTINCT i.asset_id) as inverter_count,
        COUNT(DISTINCT CASE WHEN i.metric_value > 0 THEN i.asset_id END) as available_inverter_count,
        AVG(i.metric_value) as avg_power
    FROM "MMSR"."mart"."mart_inverter_performance_5min" i
    CROSS JOIN problem_day pd
    WHERE i.date_key = pd.problem_date
        AND i.site_name = pd.problem_site
        AND i.metric_name = 'inv_active_power'
    GROUP BY i.date_key, i.site_name, i.asset_id
)
-- Combine results
SELECT 
    'METER_DATA' as check_type,
    date_key,
    site_name,
    asset_id as device_id,
    metric_name,
    reading_count,
    ROUND(min_value::numeric, 2) as min_value,
    ROUND(max_value::numeric, 2) as max_value,
    ROUND(daily_delta::numeric, 2) as daily_delta,
    ROUND(prev_day_max::numeric, 2) as prev_day_max,
    CASE 
        WHEN prev_day_max IS NOT NULL 
            AND ABS(min_value - prev_day_max) < 0.01 
        THEN 'CUMULATIVE'
        ELSE 'RESETTING'
    END as meter_type
FROM meter_check
WHERE date_key = (SELECT problem_date FROM problem_day)

UNION ALL

SELECT 
    'SENSOR_DATA' as check_type,
    date_key,
    site_name,
    asset_id as device_id,
    metric_name,
    reading_count,
    NULL as min_value,
    ROUND(max_irradiance::numeric, 2) as max_value,
    NULL as daily_delta,
    NULL as prev_day_max,
    NULL as meter_type
FROM sensor_check

UNION ALL

SELECT 
    'INVERTER_DATA' as check_type,
    date_key,
    site_name,
    asset_id as device_id,
    'inv_active_power' as metric_name,
    timestamp_count as reading_count,
    NULL as min_value,
    NULL as max_value,
    NULL as daily_delta,
    NULL as prev_day_max,
    CONCAT(available_inverter_count, '/', inverter_count, ' available') as meter_type
FROM inverter_check

ORDER BY check_type, device_id;


-- ============================================
-- BONUS: Quick Check - Count Records
-- ============================================
-- Quick check untuk memastikan jumlah record sama

SELECT 
    'Database' as source,
    COUNT(*) as total_records,
    COUNT(DISTINCT site_name) as total_sites,
    MIN(date_key) as min_date,
    MAX(date_key) as max_date
FROM "MMSR"."mart"."mart_site_performance_daily"
WHERE date_key >= (SELECT MIN(date_key) FROM temp_excel_daily_performance)
    AND date_key <= (SELECT MAX(date_key) FROM temp_excel_daily_performance)

UNION ALL

SELECT 
    'Excel' as source,
    COUNT(*) as total_records,
    COUNT(DISTINCT site_name) as total_sites,
    MIN(date_key) as min_date,
    MAX(date_key) as max_date
FROM temp_excel_daily_performance;

