-- ============================================
-- MMKI BASELINE CROSS-CHECK QUERIES
-- ============================================
-- Query khusus untuk baseline cross-check MMKI I, II, III
-- Focus pada: POA values, GHI values, Availability, PR calculations
-- 
-- Hardcoded Sites:
-- - PT. MMKI 1.75 MWp - Painting Building (MMKI I)
-- - PT. MMKI 5.7 MWp - Phase 2 (MMKI II)
-- - PT. MMKI 4.292 MWP - Phase 3 (MMKI III)
-- 
-- Excel Data Source: public.site_daily_performance_excel_mmki
-- 
-- Usage:
-- 1. Excel data sudah di-import ke table: public.site_daily_performance_excel_mmki
-- 2. Jalankan query ini untuk baseline comparison
-- 3. Export hasil ke CSV: baseline_mmki_crosscheck_YYYYMMDD.csv
-- 
-- Note: Availability data mungkin tidak ada di Excel (NULL/empty), 
--       jadi availability comparison mungkin tidak akurat
-- ============================================

-- ============================================
-- STEP 1: Export Database Data untuk MMKI Sites
-- ============================================
-- Simpan hasil query ini sebagai CSV: database_mmki_daily_performance.csv

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
WHERE site_name IN (
    'PT. MMKI 1.75 MWp - Painting Building',  -- MMKI I
    'PT. MMKI 5.7 MWp - Phase 2',              -- MMKI II
    'PT. MMKI 4.292 MWP - Phase 3'             -- MMKI III
)
    AND date_key >= '2024-01-01'  -- Sesuaikan dengan range tanggal yang ingin dicek
    AND date_key <= CURRENT_DATE
ORDER BY site_name, date_key;


-- ============================================
-- STEP 2: MMKI Baseline Comparison Query
-- ============================================
-- Query ini membandingkan database dengan Excel untuk MMKI sites
-- Menampilkan semua data (match dan mismatch) untuk baseline documentation
-- Hardcoded site names: MMKI I, II, III

WITH db_data AS (
    SELECT 
        d.date_key,
        d.site_name,
        d.site_id,
        d.daily_energy_mwh,
        d.daily_ghi_kwh_m2,
        d.daily_poa_weighted_kwh_m2,
        d.availability_percent,
        d.pr_ghi_actual,
        d.pr_poa_actual,
        d.system
    FROM "MMSR"."mart"."mart_site_performance_daily" d
    WHERE d.site_name IN (
        'PT. MMKI 1.75 MWp - Painting Building',  -- MMKI I
        'PT. MMKI 5.7 MWp - Phase 2',              -- MMKI II
        'PT. MMKI 4.292 MWP - Phase 3'             -- MMKI III
    )
        AND d.date_key >= COALESCE((
            SELECT MIN(TO_DATE(date_key, 'MM/DD/YYYY')) 
            FROM public.site_daily_performance_excel_mmki
            WHERE site_name IN (
                'PT. MMKI 1.75 MWp - Painting Building',
                'PT. MMKI 5.7 MWp - Phase 2',
                'PT. MMKI 4.292 MWP - Phase 3'
            )
        ), '2024-01-01'::date)
        AND d.date_key <= COALESCE((
            SELECT MAX(TO_DATE(date_key, 'MM/DD/YYYY')) 
            FROM public.site_daily_performance_excel_mmki
            WHERE site_name IN (
                'PT. MMKI 1.75 MWp - Painting Building',
                'PT. MMKI 5.7 MWp - Phase 2',
                'PT. MMKI 4.292 MWP - Phase 3'
            )
        ), CURRENT_DATE)
),
excel_data AS (
    SELECT 
        TO_DATE(date_key, 'MM/DD/YYYY') as date_key,
        site_name,
        site_id,
        -- Convert energy from MWh (Excel) to MWh (DB) - same unit
        energy_actual_mwh::NUMERIC as daily_energy_mwh,
        -- Convert GHI from kWh/m² (Excel) to kWh/m² (DB) - same unit
        ghi_actual_kwh_m2::NUMERIC as daily_ghi_kwh_m2,
        -- Convert POA from kWh/m² (Excel) to kWh/m² (DB) - same unit
        poa_actual_kwh_m2::NUMERIC as daily_poa_weighted_kwh_m2,
        -- Convert availability from percentage string to numeric
        CASE 
            WHEN availability_percent IS NULL OR TRIM(availability_percent) = '' THEN NULL
            ELSE CAST(REPLACE(REPLACE(availability_percent, '%', ''), ',', '.') AS NUMERIC)
        END as availability_percent,
        -- Convert PR from percentage string to decimal (e.g., '76.55%' -> 0.7655)
        CASE 
            WHEN pr_ghi_actual IS NULL OR TRIM(pr_ghi_actual) = '' THEN NULL
            ELSE CAST(REPLACE(REPLACE(pr_ghi_actual, '%', ''), ',', '.') AS NUMERIC) / 100.0
        END as pr_ghi_actual,
        CASE 
            WHEN pr_poa_actual IS NULL OR TRIM(pr_poa_actual) = '' THEN NULL
            ELSE CAST(REPLACE(REPLACE(pr_poa_actual, '%', ''), ',', '.') AS NUMERIC) / 100.0
        END as pr_poa_actual,
        'fusionsolar' as system  -- Default system for MMKI sites
    FROM public.site_daily_performance_excel_mmki
    WHERE site_name IN (
        'PT. MMKI 1.75 MWp - Painting Building',  -- MMKI I
        'PT. MMKI 5.7 MWp - Phase 2',              -- MMKI II
        'PT. MMKI 4.292 MWP - Phase 3'             -- MMKI III
    )
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
            WHEN excel.daily_poa_weighted_kwh_m2 > 0 
            THEN ((COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) / excel.daily_poa_weighted_kwh_m2) * 100
            ELSE NULL
        END as poa_diff_pct,
        
        CASE 
            WHEN excel.availability_percent > 0 
            THEN ((COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0)) / excel.availability_percent) * 100
            ELSE NULL
        END as availability_diff_pct,
        
        -- Status flags dengan tolerance levels
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
        END as data_source,
        
        -- Categorize discrepancy type for analysis
        CASE 
            WHEN ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) > 0.01 THEN 'POA_RELATED'
            WHEN ABS(COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) > 0.01 THEN 'GHI_RELATED'
            WHEN ABS(COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0)) > 1.0 THEN 'AVAILABILITY_RELATED'
            WHEN ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) > 0.01 THEN 'ENERGY_RELATED'
            WHEN ABS(COALESCE(db.pr_poa_actual, 0) - COALESCE(excel.pr_poa_actual, 0)) > 0.001 THEN 'PR_RELATED'
            ELSE 'NO_ISSUE'
        END as discrepancy_category
        
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
    discrepancy_category,
    
    -- Energy comparison
    energy_status,
    ROUND(db_energy_mwh::numeric, 4) as db_energy_mwh,
    ROUND(excel_energy_mwh::numeric, 4) as excel_energy_mwh,
    ROUND(energy_diff_mwh::numeric, 4) as energy_diff_mwh,
    ROUND(energy_diff_pct::numeric, 2) as energy_diff_pct,
    
    -- GHI comparison (critical for MMKI II/III fallback)
    ghi_status,
    ROUND(db_ghi::numeric, 4) as db_ghi,
    ROUND(excel_ghi::numeric, 4) as excel_ghi,
    ROUND(ghi_diff::numeric, 4) as ghi_diff,
    ROUND(ghi_diff_pct::numeric, 2) as ghi_diff_pct,
    
    -- POA comparison (critical for POA override)
    poa_status,
    ROUND(db_poa::numeric, 4) as db_poa,
    ROUND(excel_poa::numeric, 4) as excel_poa,
    ROUND(poa_diff::numeric, 4) as poa_diff,
    ROUND(poa_diff_pct::numeric, 2) as poa_diff_pct,
    
    -- Availability comparison
    availability_status,
    ROUND(db_availability::numeric, 2) as db_availability,
    ROUND(excel_availability::numeric, 2) as excel_availability,
    ROUND(availability_diff::numeric, 2) as availability_diff,
    ROUND(availability_diff_pct::numeric, 2) as availability_diff_pct,
    
    -- PR comparison
    ROUND(db_pr_ghi::numeric, 4) as db_pr_ghi,
    ROUND(excel_pr_ghi::numeric, 4) as excel_pr_ghi,
    ROUND(db_pr_poa::numeric, 4) as db_pr_poa,
    ROUND(excel_pr_poa::numeric, 4) as excel_pr_poa,
    
    -- Overall status
    CASE 
        WHEN energy_status != 'MATCH' OR ghi_status != 'MATCH' 
            OR poa_status != 'MATCH' OR availability_status != 'MATCH' 
        THEN 'DISCREPANCY'
        WHEN data_source != 'BOTH'
        THEN 'MISSING_DATA'
        ELSE 'MATCH'
    END as overall_status
    
FROM comparison
ORDER BY 
    site_name,
    date_key DESC;


-- ============================================
-- STEP 3: MMKI Summary Report per Site
-- ============================================
-- Summary discrepancy per MMKI site untuk baseline documentation
-- Hardcoded site names: MMKI I, II, III

WITH db_data AS (
    SELECT 
        d.date_key,
        d.site_name,
        d.daily_energy_mwh,
        d.daily_ghi_kwh_m2,
        d.daily_poa_weighted_kwh_m2,
        d.availability_percent
    FROM "MMSR"."mart"."mart_site_performance_daily" d
    WHERE d.site_name IN (
        'PT. MMKI 1.75 MWp - Painting Building',  -- MMKI I
        'PT. MMKI 5.7 MWp - Phase 2',              -- MMKI II
        'PT. MMKI 4.292 MWP - Phase 3'             -- MMKI III
    )
        AND d.date_key >= COALESCE((
            SELECT MIN(TO_DATE(date_key, 'MM/DD/YYYY')) 
            FROM public.site_daily_performance_excel_mmki
            WHERE site_name IN (
                'PT. MMKI 1.75 MWp - Painting Building',
                'PT. MMKI 5.7 MWp - Phase 2',
                'PT. MMKI 4.292 MWP - Phase 3'
            )
        ), '2024-01-01'::date)
        AND d.date_key <= COALESCE((
            SELECT MAX(TO_DATE(date_key, 'MM/DD/YYYY')) 
            FROM public.site_daily_performance_excel_mmki
            WHERE site_name IN (
                'PT. MMKI 1.75 MWp - Painting Building',
                'PT. MMKI 5.7 MWp - Phase 2',
                'PT. MMKI 4.292 MWP - Phase 3'
            )
        ), CURRENT_DATE)
),
excel_data AS (
    SELECT 
        TO_DATE(date_key, 'MM/DD/YYYY') as date_key,
        site_name,
        energy_actual_mwh::NUMERIC as daily_energy_mwh,
        ghi_actual_kwh_m2::NUMERIC as daily_ghi_kwh_m2,
        poa_actual_kwh_m2::NUMERIC as daily_poa_weighted_kwh_m2,
        CASE 
            WHEN availability_percent IS NULL OR TRIM(availability_percent) = '' THEN NULL
            ELSE CAST(REPLACE(REPLACE(availability_percent, '%', ''), ',', '.') AS NUMERIC)
        END as availability_percent
    FROM public.site_daily_performance_excel_mmki
    WHERE site_name IN (
        'PT. MMKI 1.75 MWp - Painting Building',  -- MMKI I
        'PT. MMKI 5.7 MWp - Phase 2',              -- MMKI II
        'PT. MMKI 4.292 MWP - Phase 3'             -- MMKI III
    )
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
            WHEN ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) > 0.01 THEN 1 ELSE 0
        END as poa_mismatch,
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
    SUM(poa_mismatch) as poa_mismatch_count,
    SUM(availability_mismatch) as availability_mismatch_count,
    SUM(missing_data) as missing_data_count,
    ROUND(100.0 * SUM(energy_mismatch) / NULLIF(COUNT(*), 0), 2) as energy_mismatch_pct,
    ROUND(100.0 * SUM(ghi_mismatch) / NULLIF(COUNT(*), 0), 2) as ghi_mismatch_pct,
    ROUND(100.0 * SUM(poa_mismatch) / NULLIF(COUNT(*), 0), 2) as poa_mismatch_pct,
    ROUND(100.0 * SUM(availability_mismatch) / NULLIF(COUNT(*), 0), 2) as availability_mismatch_pct,
    ROUND(100.0 * SUM(missing_data) / NULLIF(COUNT(*), 0), 2) as missing_data_pct,
    CASE 
        WHEN SUM(energy_mismatch) > 0 OR SUM(ghi_mismatch) > 0 
            OR SUM(poa_mismatch) > 0 OR SUM(availability_mismatch) > 0 OR SUM(missing_data) > 0
        THEN 'HAS_ISSUES'
        ELSE 'CLEAN'
    END as status
FROM comparison
GROUP BY site_name
ORDER BY 
    (SUM(energy_mismatch) + SUM(ghi_mismatch) + SUM(poa_mismatch) + SUM(availability_mismatch) + SUM(missing_data)) DESC,
    site_name;


-- ============================================
-- STEP 4: MMKI Discrepancy by Category
-- ============================================
-- Breakdown discrepancy by category untuk identify patterns

WITH db_data AS (
    SELECT 
        d.date_key,
        d.site_name,
        d.daily_energy_mwh,
        d.daily_ghi_kwh_m2,
        d.daily_poa_weighted_kwh_m2,
        d.availability_percent
    FROM "MMSR"."mart"."mart_site_performance_daily" d
    WHERE d.site_name IN (
        'PT. MMKI 1.75 MWp - Painting Building',  -- MMKI I
        'PT. MMKI 5.7 MWp - Phase 2',              -- MMKI II
        'PT. MMKI 4.292 MWP - Phase 3'             -- MMKI III
    )
        AND d.date_key >= COALESCE((
            SELECT MIN(TO_DATE(date_key, 'MM/DD/YYYY')) 
            FROM public.site_daily_performance_excel_mmki
            WHERE site_name IN (
                'PT. MMKI 1.75 MWp - Painting Building',
                'PT. MMKI 5.7 MWp - Phase 2',
                'PT. MMKI 4.292 MWP - Phase 3'
            )
        ), '2024-01-01'::date)
        AND d.date_key <= COALESCE((
            SELECT MAX(TO_DATE(date_key, 'MM/DD/YYYY')) 
            FROM public.site_daily_performance_excel_mmki
            WHERE site_name IN (
                'PT. MMKI 1.75 MWp - Painting Building',
                'PT. MMKI 5.7 MWp - Phase 2',
                'PT. MMKI 4.292 MWP - Phase 3'
            )
        ), CURRENT_DATE)
),
excel_data AS (
    SELECT 
        TO_DATE(date_key, 'MM/DD/YYYY') as date_key,
        site_name,
        energy_actual_mwh::NUMERIC as daily_energy_mwh,
        ghi_actual_kwh_m2::NUMERIC as daily_ghi_kwh_m2,
        poa_actual_kwh_m2::NUMERIC as daily_poa_weighted_kwh_m2,
        CASE 
            WHEN availability_percent IS NULL OR TRIM(availability_percent) = '' THEN NULL
            ELSE CAST(REPLACE(REPLACE(availability_percent, '%', ''), ',', '.') AS NUMERIC)
        END as availability_percent
    FROM public.site_daily_performance_excel_mmki
    WHERE site_name IN (
        'PT. MMKI 1.75 MWp - Painting Building',  -- MMKI I
        'PT. MMKI 5.7 MWp - Phase 2',              -- MMKI II
        'PT. MMKI 4.292 MWP - Phase 3'             -- MMKI III
    )
),
comparison AS (
    SELECT 
        COALESCE(db.site_name, excel.site_name) as site_name,
        CASE 
            WHEN ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) > 0.01 THEN 'POA_RELATED'
            WHEN ABS(COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) > 0.01 THEN 'GHI_RELATED'
            WHEN ABS(COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0)) > 1.0 THEN 'AVAILABILITY_RELATED'
            WHEN ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) > 0.01 THEN 'ENERGY_RELATED'
            ELSE 'NO_ISSUE'
        END as discrepancy_category
    FROM db_data db
    FULL OUTER JOIN excel_data excel
        ON db.date_key = excel.date_key 
        AND UPPER(TRIM(db.site_name)) = UPPER(TRIM(excel.site_name))
    WHERE 
        ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) > 0.01
        OR ABS(COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) > 0.01
        OR ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) > 0.01
        OR ABS(COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0)) > 1.0
)
SELECT 
    site_name,
    discrepancy_category,
    COUNT(*) as discrepancy_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY site_name), 2) as pct_of_site_discrepancies
FROM comparison
GROUP BY site_name, discrepancy_category
ORDER BY site_name, discrepancy_count DESC;

