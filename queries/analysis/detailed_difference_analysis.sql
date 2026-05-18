-- ============================================
-- Detailed Difference Analysis
-- ============================================
-- Analisis detail perbedaan untuk mencari pola dan root cause
-- ============================================

WITH db_data AS (
    SELECT 
        d.date_key,
        d.site_name,
        d.daily_energy_mwh,
        d.daily_ghi_kwh_m2,
        d.daily_poa_weighted_kwh_m2
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
        energy_actual_mwh::NUMERIC as daily_energy_mwh,
        ghi_actual_kwh_m2::NUMERIC as daily_ghi_kwh_m2,
        poa_actual_kwh_m2::NUMERIC as daily_poa_weighted_kwh_m2
    FROM public.site_daily_performance_excel_except_mmki
),
comparison AS (
    SELECT 
        COALESCE(db.date_key, excel.date_key) as date_key,
        COALESCE(db.site_name, excel.site_name) as site_name,
        
        -- DB values
        db.daily_energy_mwh as db_energy,
        db.daily_ghi_kwh_m2 as db_ghi,
        db.daily_poa_weighted_kwh_m2 as db_poa,
        
        -- Excel values
        excel.daily_energy_mwh as excel_energy,
        excel.daily_ghi_kwh_m2 as excel_ghi,
        excel.daily_poa_weighted_kwh_m2 as excel_poa,
        
        -- Differences
        (COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) as energy_diff,
        ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) as energy_diff_abs,
        (COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) as ghi_diff,
        ABS(COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) as ghi_diff_abs,
        (COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) as poa_diff,
        ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) as poa_diff_abs,
        
        -- Percentage differences
        CASE 
            WHEN COALESCE(excel.daily_energy_mwh, 0) != 0 
            THEN ROUND(100.0 * (COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) / excel.daily_energy_mwh, 2)
            ELSE NULL
        END as energy_diff_pct,
        CASE 
            WHEN COALESCE(excel.daily_ghi_kwh_m2, 0) != 0 
            THEN ROUND(100.0 * (COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) / excel.daily_ghi_kwh_m2, 2)
            ELSE NULL
        END as ghi_diff_pct,
        CASE 
            WHEN COALESCE(excel.daily_poa_weighted_kwh_m2, 0) != 0 
            THEN ROUND(100.0 * (COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) / excel.daily_poa_weighted_kwh_m2, 2)
            ELSE NULL
        END as poa_diff_pct,
        
        -- Match status
        CASE 
            WHEN ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) <= 0.01 THEN 'MATCH'
            WHEN db.daily_energy_mwh IS NULL THEN 'MISSING_DB'
            WHEN excel.daily_energy_mwh IS NULL THEN 'MISSING_EXCEL'
            ELSE 'MISMATCH'
        END as energy_status,
        CASE 
            WHEN ABS(COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) <= 0.01 THEN 'MATCH'
            WHEN db.daily_ghi_kwh_m2 IS NULL THEN 'MISSING_DB'
            WHEN excel.daily_ghi_kwh_m2 IS NULL THEN 'MISSING_EXCEL'
            ELSE 'MISMATCH'
        END as ghi_status,
        CASE 
            WHEN ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) <= 0.01 THEN 'MATCH'
            WHEN db.daily_poa_weighted_kwh_m2 IS NULL THEN 'MISSING_DB'
            WHEN excel.daily_poa_weighted_kwh_m2 IS NULL THEN 'MISSING_EXCEL'
            ELSE 'MISMATCH'
        END as poa_status
        
    FROM db_data db
    FULL OUTER JOIN excel_data excel
        ON db.date_key = excel.date_key 
        AND UPPER(TRIM(db.site_name)) = UPPER(TRIM(excel.site_name))
)
-- Show mismatches with details
SELECT 
    site_name,
    date_key,
    
    -- Energy comparison
    ROUND(db_energy::numeric, 4) as db_energy_mwh,
    ROUND(excel_energy::numeric, 4) as excel_energy_mwh,
    ROUND(energy_diff::numeric, 4) as energy_diff_mwh,
    energy_diff_pct,
    energy_status,
    
    -- GHI comparison
    ROUND(db_ghi::numeric, 4) as db_ghi,
    ROUND(excel_ghi::numeric, 4) as excel_ghi,
    ROUND(ghi_diff::numeric, 4) as ghi_diff,
    ghi_diff_pct,
    ghi_status,
    
    -- POA comparison
    ROUND(db_poa::numeric, 4) as db_poa,
    ROUND(excel_poa::numeric, 4) as excel_poa,
    ROUND(poa_diff::numeric, 4) as poa_diff,
    poa_diff_pct,
    poa_status,
    
    -- Pattern indicators
    CASE 
        WHEN energy_diff > 0 THEN 'DB_HIGHER'
        WHEN energy_diff < 0 THEN 'EXCEL_HIGHER'
        ELSE 'MATCH'
    END as energy_pattern,
    CASE 
        WHEN ghi_diff > 0 THEN 'DB_HIGHER'
        WHEN ghi_diff < 0 THEN 'EXCEL_HIGHER'
        ELSE 'MATCH'
    END as ghi_pattern,
    CASE 
        WHEN poa_diff > 0 THEN 'DB_HIGHER'
        WHEN poa_diff < 0 THEN 'EXCEL_HIGHER'
        ELSE 'MATCH'
    END as poa_pattern
    
FROM comparison
WHERE energy_status != 'MATCH' 
    OR ghi_status != 'MATCH' 
    OR poa_status != 'MATCH'
ORDER BY 
    site_name,
    ABS(COALESCE(energy_diff_abs, 0)) DESC,
    date_key DESC;

