-- ============================================
-- Quick Summary - All Sites EXCEPT MMKI
-- ============================================
-- Quick overview of match rates per site
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
        -- Energy sudah dalam MWh (tidak perlu konversi lagi)
        energy_actual_mwh::NUMERIC as daily_energy_mwh,
        ghi_actual_kwh_m2::NUMERIC as daily_ghi_kwh_m2,
        poa_actual_kwh_m2::NUMERIC as daily_poa_weighted_kwh_m2
    FROM public.site_daily_performance_excel_except_mmki
),
comparison AS (
    SELECT 
        COALESCE(db.date_key, excel.date_key) as date_key,
        COALESCE(db.site_name, excel.site_name) as site_name,
        
        -- Match flags
        CASE WHEN ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) <= 0.01 THEN 1 ELSE 0 END as energy_match,
        CASE WHEN ABS(COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0)) <= 0.01 THEN 1 ELSE 0 END as ghi_match,
        CASE WHEN ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) <= 0.01 THEN 1 ELSE 0 END as poa_match
        
    FROM db_data db
    FULL OUTER JOIN excel_data excel
        ON db.date_key = excel.date_key 
        AND UPPER(TRIM(db.site_name)) = UPPER(TRIM(excel.site_name))
)
SELECT 
    site_name,
    COUNT(*) as total_days,
    ROUND(100.0 * SUM(energy_match) / NULLIF(COUNT(*), 0), 2) as energy_match_pct,
    ROUND(100.0 * SUM(ghi_match) / NULLIF(COUNT(*), 0), 2) as ghi_match_pct,
    ROUND(100.0 * SUM(poa_match) / NULLIF(COUNT(*), 0), 2) as poa_match_pct,
    ROUND(100.0 * SUM(CASE WHEN energy_match = 1 AND ghi_match = 1 AND poa_match = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as overall_match_pct,
    CASE 
        WHEN ROUND(100.0 * SUM(energy_match) / NULLIF(COUNT(*), 0), 2) >= 90 THEN '✅ Excellent'
        WHEN ROUND(100.0 * SUM(energy_match) / NULLIF(COUNT(*), 0), 2) >= 80 THEN '✅ Good'
        WHEN ROUND(100.0 * SUM(energy_match) / NULLIF(COUNT(*), 0), 2) >= 50 THEN '⚠️ Fair'
        ELSE '❌ Poor'
    END as energy_status,
    CASE 
        WHEN ROUND(100.0 * SUM(poa_match) / NULLIF(COUNT(*), 0), 2) >= 80 THEN '✅ Good'
        WHEN ROUND(100.0 * SUM(poa_match) / NULLIF(COUNT(*), 0), 2) >= 50 THEN '⚠️ Fair'
        ELSE '❌ Poor'
    END as poa_status
FROM comparison
GROUP BY site_name
ORDER BY 
    energy_match_pct DESC,
    site_name;

