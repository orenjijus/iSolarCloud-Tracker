-- ============================================
-- Summary Report - All Sites EXCEPT MMKI
-- ============================================
-- Query ini menampilkan ringkasan perbandingan per site
-- untuk semua site kecuali MMKI
-- ============================================

WITH db_data AS (
    SELECT 
        d.date_key,
        d.site_name,
        d.daily_energy_mwh,
        d.daily_ghi_kwh_m2,
        d.daily_poa_weighted_kwh_m2,
        d.availability_percent
    FROM "MMSR"."mart"."mart_site_performance_daily" d
    WHERE d.site_name NOT IN (
        'PT. MMKI 1.75 MWp - Painting Building',
        'PT. MMKI 5.7 MWp - Phase 2',
        'PT. MMKI 4.292 MWP - Phase 3'
    )
        -- Exclude January for Shoetown (anomaly period)
        AND (d.site_name != 'Shoetown Ligung Indonesia' OR d.date_key >= '2025-02-01')
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
        END as availability_percent
    FROM public.site_daily_performance_excel_except_mmki
    WHERE (site_name != 'Shoetown Ligung Indonesia' OR TO_DATE(date_key, 'MM/DD/YYYY') >= '2025-02-01')
),
comparison AS (
    SELECT 
        COALESCE(db.date_key, excel.date_key) as date_key,
        COALESCE(db.site_name, excel.site_name) as site_name,
        
        -- Energy match status
        CASE 
            WHEN ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) <= 0.01 THEN 1 
            ELSE 0 
        END as energy_match,
        CASE 
            WHEN ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) > 0.01 THEN 1 
            ELSE 0 
        END as energy_mismatch,
        CASE 
            WHEN db.daily_energy_mwh IS NULL AND excel.daily_energy_mwh IS NOT NULL THEN 1 
            ELSE 0 
        END as energy_missing_db,
        CASE 
            WHEN db.daily_energy_mwh IS NOT NULL AND excel.daily_energy_mwh IS NULL THEN 1 
            ELSE 0 
        END as energy_missing_excel,
        ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) as energy_diff_abs,
        
        -- GHI match status
        CASE 
            WHEN ABS(COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) <= 0.01 THEN 1 
            ELSE 0 
        END as ghi_match,
        CASE 
            WHEN ABS(COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) > 0.01 THEN 1 
            ELSE 0 
        END as ghi_mismatch,
        CASE 
            WHEN db.daily_ghi_kwh_m2 IS NULL AND excel.daily_ghi_kwh_m2 IS NOT NULL THEN 1 
            ELSE 0 
        END as ghi_missing_db,
        CASE 
            WHEN db.daily_ghi_kwh_m2 IS NOT NULL AND excel.daily_ghi_kwh_m2 IS NULL THEN 1 
            ELSE 0 
        END as ghi_missing_excel,
        ABS(COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) as ghi_diff_abs,
        
        -- POA match status
        CASE 
            WHEN ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) <= 0.01 THEN 1 
            ELSE 0 
        END as poa_match,
        CASE 
            WHEN ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) > 0.01 THEN 1 
            ELSE 0 
        END as poa_mismatch,
        CASE 
            WHEN db.daily_poa_weighted_kwh_m2 IS NULL AND excel.daily_poa_weighted_kwh_m2 IS NOT NULL THEN 1 
            ELSE 0 
        END as poa_missing_db,
        CASE 
            WHEN db.daily_poa_weighted_kwh_m2 IS NOT NULL AND excel.daily_poa_weighted_kwh_m2 IS NULL THEN 1 
            ELSE 0 
        END as poa_missing_excel,
        ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) as poa_diff_abs,
        
        -- Availability match status
        CASE 
            WHEN ABS(COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0)) <= 1.0 THEN 1 
            ELSE 0 
        END as availability_match,
        CASE 
            WHEN ABS(COALESCE(db.availability_percent, 0) - COALESCE(excel.availability_percent, 0)) > 1.0 THEN 1 
            ELSE 0 
        END as availability_mismatch,
        CASE 
            WHEN db.availability_percent IS NULL AND excel.availability_percent IS NOT NULL THEN 1 
            ELSE 0 
        END as availability_missing_db,
        CASE 
            WHEN db.availability_percent IS NOT NULL AND excel.availability_percent IS NULL THEN 1 
            ELSE 0 
        END as availability_missing_excel
        
    FROM db_data db
    FULL OUTER JOIN excel_data excel
        ON db.date_key = excel.date_key 
        AND UPPER(TRIM(db.site_name)) = UPPER(TRIM(excel.site_name))
)
SELECT 
    site_name,
    COUNT(*) as total_days,
    
    -- Energy summary
    SUM(energy_match) as energy_match_count,
    ROUND(100.0 * SUM(energy_match) / NULLIF(COUNT(*), 0), 2) as energy_match_pct,
    SUM(energy_mismatch) as energy_mismatch_count,
    ROUND(100.0 * SUM(energy_mismatch) / NULLIF(COUNT(*), 0), 2) as energy_mismatch_pct,
    SUM(energy_missing_db) as energy_missing_db_count,
    SUM(energy_missing_excel) as energy_missing_excel_count,
    ROUND(AVG(CASE WHEN energy_mismatch = 1 THEN energy_diff_abs ELSE NULL END), 4) as energy_avg_diff_mwh,
    ROUND(MAX(CASE WHEN energy_mismatch = 1 THEN energy_diff_abs ELSE NULL END), 4) as energy_max_diff_mwh,
    
    -- GHI summary
    SUM(ghi_match) as ghi_match_count,
    ROUND(100.0 * SUM(ghi_match) / NULLIF(COUNT(*), 0), 2) as ghi_match_pct,
    SUM(ghi_mismatch) as ghi_mismatch_count,
    ROUND(100.0 * SUM(ghi_mismatch) / NULLIF(COUNT(*), 0), 2) as ghi_mismatch_pct,
    SUM(ghi_missing_db) as ghi_missing_db_count,
    SUM(ghi_missing_excel) as ghi_missing_excel_count,
    ROUND(AVG(CASE WHEN ghi_mismatch = 1 THEN ghi_diff_abs ELSE NULL END), 4) as ghi_avg_diff,
    ROUND(MAX(CASE WHEN ghi_mismatch = 1 THEN ghi_diff_abs ELSE NULL END), 4) as ghi_max_diff,
    
    -- POA summary
    SUM(poa_match) as poa_match_count,
    ROUND(100.0 * SUM(poa_match) / NULLIF(COUNT(*), 0), 2) as poa_match_pct,
    SUM(poa_mismatch) as poa_mismatch_count,
    ROUND(100.0 * SUM(poa_mismatch) / NULLIF(COUNT(*), 0), 2) as poa_mismatch_pct,
    SUM(poa_missing_db) as poa_missing_db_count,
    SUM(poa_missing_excel) as poa_missing_excel_count,
    ROUND(AVG(CASE WHEN poa_mismatch = 1 THEN poa_diff_abs ELSE NULL END), 4) as poa_avg_diff,
    ROUND(MAX(CASE WHEN poa_mismatch = 1 THEN poa_diff_abs ELSE NULL END), 4) as poa_max_diff,
    
    -- Availability summary
    SUM(availability_match) as availability_match_count,
    ROUND(100.0 * SUM(availability_match) / NULLIF(COUNT(*), 0), 2) as availability_match_pct,
    SUM(availability_mismatch) as availability_mismatch_count,
    ROUND(100.0 * SUM(availability_mismatch) / NULLIF(COUNT(*), 0), 2) as availability_mismatch_pct,
    SUM(availability_missing_db) as availability_missing_db_count,
    SUM(availability_missing_excel) as availability_missing_excel_count,
    
    -- Overall match (all metrics must match)
    SUM(CASE 
        WHEN energy_match = 1 AND ghi_match = 1 AND poa_match = 1 AND availability_match = 1 THEN 1 
        ELSE 0 
    END) as overall_match_count,
    ROUND(100.0 * SUM(CASE 
        WHEN energy_match = 1 AND ghi_match = 1 AND poa_match = 1 AND availability_match = 1 THEN 1 
        ELSE 0 
    END) / NULLIF(COUNT(*), 0), 2) as overall_match_pct
    
FROM comparison
GROUP BY site_name
ORDER BY 
    overall_match_pct ASC,
    site_name;

