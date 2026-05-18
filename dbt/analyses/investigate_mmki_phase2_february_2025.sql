-- Detailed investigation: PT. MMKI 5.7 MWp - Phase 2 in February 2025
-- This site has the largest difference: -193.73 MWh (-41%)
-- 
-- Purpose: Understand why there's such a large discrepancy

-- 1. Daily data comparison
WITH db_daily AS (
    SELECT 
        date_key,
        daily_energy_mwh,
        daily_ghi_kwh_m2,
        pr_ghi_actual,
        is_issue_date,
        energy_target_mwh,
        ghi_target
    FROM "MMSR"."mart"."mart_site_performance_daily"
    WHERE year = 2025 
        AND month = 2
        AND site_id = 'FS_SITE_NE=51758766'
),

excel_daily AS (
    SELECT 
        date_key,
        daily_energy_mwh,
        daily_ghi_kwh_m2,
        pr_ghi_actual
    FROM "MMSR"."public"."site_daily_performance_excel_all"
    WHERE year = 2025 
        AND month = 2
        AND site_id = 'FS_SITE_NE=51758766'
)

SELECT 
    COALESCE(db.date_key, excel.date_key) as date_key,
    
    -- Energy comparison
    db.daily_energy_mwh as db_energy_mwh,
    excel.daily_energy_mwh as excel_energy_mwh,
    COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0) as energy_diff_mwh,
    
    -- GHI comparison
    db.daily_ghi_kwh_m2 as db_ghi_kwh_m2,
    excel.daily_ghi_kwh_m2 as excel_ghi_kwh_m2,
    COALESCE(db.daily_ghi_kwh_m2, 0) - COALESCE(excel.daily_ghi_kwh_m2, 0) as ghi_diff_kwh_m2,
    
    -- PR comparison
    db.pr_ghi_actual as db_pr_ghi,
    excel.pr_ghi_actual as excel_pr_ghi,
    
    -- Issue date flag
    db.is_issue_date,
    
    -- Target
    db.energy_target_mwh,
    db.ghi_target,
    
    -- Status
    CASE 
        WHEN db.date_key IS NULL THEN 'MISSING_IN_DB'
        WHEN excel.date_key IS NULL THEN 'MISSING_IN_EXCEL'
        WHEN ABS(COALESCE(db.daily_energy_mwh, 0) - COALESCE(excel.daily_energy_mwh, 0)) > 0.01
        THEN 'HAS_DIFFERENCES'
        ELSE 'MATCH'
    END as status

FROM db_daily db
FULL OUTER JOIN excel_daily excel
    ON db.date_key = excel.date_key

ORDER BY date_key

