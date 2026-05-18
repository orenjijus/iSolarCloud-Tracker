-- ============================================
-- Side-by-Side Daily Comparison Query
-- All Sites EXCEPT MMKI
-- ============================================
-- Query ini menampilkan perbandingan per hari antara database dan Excel
-- untuk semua site kecuali MMKI (yang sudah dicek sebelumnya)
-- 
-- Usage: Jalankan query ini dan export ke CSV untuk analisis detail
-- ============================================

WITH db_data AS (
    SELECT 
        d.date_key,
        d.site_name,
        d.daily_energy_mwh,
        d.daily_ghi_kwh_m2,
        d.daily_poa_weighted_kwh_m2,
        d.availability_percent,
        d.pr_ghi_actual,
        d.pr_poa_actual
    FROM "MMSR"."mart"."mart_site_performance_daily" d
    WHERE d.site_name NOT IN (
        'PT. MMKI 1.75 MWp - Painting Building',
        'PT. MMKI 5.7 MWp - Phase 2',
        'PT. MMKI 4.292 MWP - Phase 3'
    )
),
excel_data AS (
    SELECT 
        TO_DATE(date_key, 'MM/DD/YYYY') as date_key,
        site_name,
        -- Energy sudah dalam MWh (tidak perlu konversi lagi)
        energy_actual_mwh::NUMERIC as daily_energy_mwh,
        ghi_actual_kwh_m2::NUMERIC as daily_ghi_kwh_m2,
        poa_actual_kwh_m2::NUMERIC as daily_poa_weighted_kwh_m2,
        CASE 
            WHEN availability_percent IS NULL OR TRIM(availability_percent) = '' THEN NULL
            ELSE CAST(REPLACE(REPLACE(availability_percent, '%', ''), ',', '.') AS NUMERIC)
        END as availability_percent,
        CASE 
            WHEN pr_ghi_actual IS NULL OR TRIM(pr_ghi_actual) = '' THEN NULL
            ELSE CAST(REPLACE(REPLACE(pr_ghi_actual, '%', ''), ',', '.') AS NUMERIC) / 100.0
        END as pr_ghi_actual,
        CASE 
            WHEN pr_poa_actual IS NULL OR TRIM(pr_poa_actual) = '' THEN NULL
            ELSE CAST(REPLACE(REPLACE(pr_poa_actual, '%', ''), ',', '.') AS NUMERIC) / 100.0
        END as pr_poa_actual
    FROM public.site_daily_performance_excel_except_mmki
),
comparison AS (
    SELECT 
        COALESCE(db.date_key, excel.date_key) as date_key,
        COALESCE(db.site_name, excel.site_name) as site_name,
        
        -- Energy
        db.daily_energy_mwh as db_energy,
        excel.daily_energy_mwh as excel_energy,
        COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0) as energy_diff,
        CASE 
            WHEN ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) > 0.01 THEN 'MISMATCH'
            WHEN db.daily_energy_mwh IS NULL AND excel.daily_energy_mwh IS NOT NULL THEN 'MISSING_DB'
            WHEN db.daily_energy_mwh IS NOT NULL AND excel.daily_energy_mwh IS NULL THEN 'MISSING_EXCEL'
            ELSE 'MATCH'
        END as energy_status,
        
        -- GHI
        db.daily_ghi_kwh_m2 as db_ghi,
        excel.daily_ghi_kwh_m2 as excel_ghi,
        COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0) as ghi_diff,
        CASE 
            WHEN ABS(COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) > 0.01 THEN 'MISMATCH'
            WHEN db.daily_ghi_kwh_m2 IS NULL AND excel.daily_ghi_kwh_m2 IS NOT NULL THEN 'MISSING_DB'
            WHEN db.daily_ghi_kwh_m2 IS NOT NULL AND excel.daily_ghi_kwh_m2 IS NULL THEN 'MISSING_EXCEL'
            ELSE 'MATCH'
        END as ghi_status,
        
        -- POA
        db.daily_poa_weighted_kwh_m2 as db_poa,
        excel.daily_poa_weighted_kwh_m2 as excel_poa,
        COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0) as poa_diff,
        CASE 
            WHEN ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) > 0.01 THEN 'MISMATCH'
            WHEN db.daily_poa_weighted_kwh_m2 IS NULL AND excel.daily_poa_weighted_kwh_m2 IS NOT NULL THEN 'MISSING_DB'
            WHEN db.daily_poa_weighted_kwh_m2 IS NOT NULL AND excel.daily_poa_weighted_kwh_m2 IS NULL THEN 'MISSING_EXCEL'
            ELSE 'MATCH'
        END as poa_status,
        
        -- Availability
        db.availability_percent as db_availability,
        excel.availability_percent as excel_availability,
        COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0) as availability_diff,
        CASE 
            WHEN ABS(COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0)) > 1.0 THEN 'MISMATCH'
            WHEN db.availability_percent IS NULL AND excel.availability_percent IS NOT NULL THEN 'MISSING_DB'
            WHEN db.availability_percent IS NOT NULL AND excel.availability_percent IS NULL THEN 'MISSING_EXCEL'
            ELSE 'MATCH'
        END as availability_status
        
    FROM db_data db
    FULL OUTER JOIN excel_data excel
        ON db.date_key = excel.date_key 
        AND UPPER(TRIM(db.site_name)) = UPPER(TRIM(excel.site_name))
),
comparison_with_status AS (
    SELECT 
        *,
        CASE 
            WHEN db_energy IS NULL THEN 'ONLY_EXCEL'
            WHEN excel_energy IS NULL THEN 'ONLY_DB'
            WHEN energy_status != 'MATCH' OR ghi_status != 'MATCH' OR poa_status != 'MATCH' OR availability_status != 'MATCH' THEN 'HAS_DISCREPANCY'
            ELSE 'ALL_MATCH'
        END as overall_status
    FROM comparison
)
SELECT 
    date_key,
    site_name,
    overall_status,
    
    -- Energy comparison
    ROUND(db_energy::numeric, 4) as db_energy_mwh,
    ROUND(excel_energy::numeric, 4) as excel_energy_mwh,
    ROUND(energy_diff::numeric, 4) as energy_diff_mwh,
    energy_status,
    
    -- GHI comparison
    ROUND(db_ghi::numeric, 4) as db_ghi,
    ROUND(excel_ghi::numeric, 4) as excel_ghi,
    ROUND(ghi_diff::numeric, 4) as ghi_diff,
    ghi_status,
    
    -- POA comparison
    ROUND(db_poa::numeric, 4) as db_poa,
    ROUND(excel_poa::numeric, 4) as excel_poa,
    ROUND(poa_diff::numeric, 4) as poa_diff,
    poa_status,
    
    -- Availability comparison
    ROUND(db_availability::numeric, 2) as db_availability,
    ROUND(excel_availability::numeric, 2) as excel_availability,
    ROUND(availability_diff::numeric, 2) as availability_diff,
    availability_status
    
FROM comparison_with_status
ORDER BY 
    site_name,
    date_key DESC;

