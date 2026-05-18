-- ============================================
-- Pattern Analysis - Root Cause Investigation
-- ============================================
-- Mencari pola sistematis dalam perbedaan
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
        
        -- Differences
        (COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) as energy_diff,
        ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) as energy_diff_abs,
        (COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) as ghi_diff,
        ABS(COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) as ghi_diff_abs,
        (COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) as poa_diff,
        ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) as poa_diff_abs,
        
        -- Values for ratio analysis
        COALESCE(db.daily_energy_mwh, 0) as db_energy,
        COALESCE(excel.daily_energy_mwh, 0) as excel_energy,
        COALESCE(db.daily_ghi_kwh_m2, 0) as db_ghi,
        COALESCE(excel.daily_ghi_kwh_m2, 0) as excel_ghi,
        COALESCE(db.daily_poa_weighted_kwh_m2, 0) as db_poa,
        COALESCE(excel.daily_poa_weighted_kwh_m2, 0) as excel_poa
        
    FROM db_data db
    FULL OUTER JOIN excel_data excel
        ON db.date_key = excel.date_key 
        AND UPPER(TRIM(db.site_name)) = UPPER(TRIM(excel.site_name))
    WHERE db.daily_energy_mwh IS NOT NULL 
        AND excel.daily_energy_mwh IS NOT NULL
        AND ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) > 0.01
)
SELECT 
    site_name,
    COUNT(*) as mismatch_count,
    
    -- Energy pattern analysis
    ROUND(AVG(energy_diff), 4) as avg_energy_diff,
    ROUND(MIN(energy_diff), 4) as min_energy_diff,
    ROUND(MAX(energy_diff), 4) as max_energy_diff,
    ROUND(STDDEV(energy_diff), 4) as stddev_energy_diff,
    CASE 
        WHEN AVG(energy_diff) > 0.1 THEN 'DB_CONSISTENTLY_HIGHER'
        WHEN AVG(energy_diff) < -0.1 THEN 'EXCEL_CONSISTENTLY_HIGHER'
        WHEN STDDEV(energy_diff) < 0.5 THEN 'CONSISTENT_DIFF'
        ELSE 'VARIABLE_DIFF'
    END as energy_pattern,
    
    -- Ratio analysis (to detect multiplier issues)
    ROUND(AVG(CASE WHEN excel_energy > 0 THEN db_energy / excel_energy ELSE NULL END), 4) as avg_energy_ratio,
    ROUND(MIN(CASE WHEN excel_energy > 0 THEN db_energy / excel_energy ELSE NULL END), 4) as min_energy_ratio,
    ROUND(MAX(CASE WHEN excel_energy > 0 THEN db_energy / excel_energy ELSE NULL END), 4) as max_energy_ratio,
    CASE 
        WHEN AVG(CASE WHEN excel_energy > 0 THEN db_energy / excel_energy ELSE NULL END) BETWEEN 0.99 AND 1.01 THEN 'RATIO_NEAR_1'
        WHEN AVG(CASE WHEN excel_energy > 0 THEN db_energy / excel_energy ELSE NULL END) BETWEEN 0.9 AND 1.1 THEN 'RATIO_CLOSE'
        ELSE 'RATIO_VARIABLE'
    END as energy_ratio_pattern,
    
    -- GHI pattern analysis
    ROUND(AVG(ghi_diff), 4) as avg_ghi_diff,
    ROUND(STDDEV(ghi_diff), 4) as stddev_ghi_diff,
    CASE 
        WHEN AVG(ghi_diff) > 0.1 THEN 'DB_CONSISTENTLY_HIGHER'
        WHEN AVG(ghi_diff) < -0.1 THEN 'EXCEL_CONSISTENTLY_HIGHER'
        WHEN STDDEV(ghi_diff) < 0.5 THEN 'CONSISTENT_DIFF'
        ELSE 'VARIABLE_DIFF'
    END as ghi_pattern,
    
    -- POA pattern analysis
    ROUND(AVG(poa_diff), 4) as avg_poa_diff,
    ROUND(STDDEV(poa_diff), 4) as stddev_poa_diff,
    CASE 
        WHEN AVG(poa_diff) > 0.1 THEN 'DB_CONSISTENTLY_HIGHER'
        WHEN AVG(poa_diff) < -0.1 THEN 'EXCEL_CONSISTENTLY_HIGHER'
        WHEN STDDEV(poa_diff) < 0.5 THEN 'CONSISTENT_DIFF'
        ELSE 'VARIABLE_DIFF'
    END as poa_pattern,
    
    -- Correlation analysis
    ROUND(CORR(energy_diff, ghi_diff), 4) as energy_ghi_correlation,
    ROUND(CORR(energy_diff, poa_diff), 4) as energy_poa_correlation,
    ROUND(CORR(ghi_diff, poa_diff), 4) as ghi_poa_correlation
    
FROM comparison
GROUP BY site_name
HAVING COUNT(*) > 0
ORDER BY 
    mismatch_count DESC,
    site_name;

