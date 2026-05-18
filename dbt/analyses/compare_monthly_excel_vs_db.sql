-- Comparison query: Monthly aggregation from database vs Excel
-- This query helps identify discrepancies between DB monthly aggregation and Excel monthly data
-- 
-- Usage: Run this query to see side-by-side comparison and differences

WITH db_monthly AS (
    SELECT 
        year,
        month,
        site_id,
        site_name,
        daily_energy_mwh as db_energy_mwh,
        daily_ghi_kwh_m2 as db_ghi_kwh_m2,
        pr_ghi_actual as db_pr_ghi,
        energy_target_mwh as db_energy_target_mwh,
        ghi_target as db_ghi_target,
        energy_actual_vs_target_pct as db_energy_vs_target_pct,
        ghi_actual_vs_target_pct as db_ghi_vs_target_pct,
        energy_vs_ghi_variance_pct as db_energy_vs_ghi_variance_pct
    FROM {{ ref('mart_site_performance_monthly') }}
),

excel_monthly AS (
    SELECT 
        year,
        month,
        site_id,
        site_name,
        daily_energy_mwh as excel_energy_mwh,
        daily_ghi_kwh_m2 as excel_ghi_kwh_m2,
        pr_ghi_actual as excel_pr_ghi,
        energy_target_mwh as excel_energy_target_mwh,
        ghi_target as excel_ghi_target,
        energy_actual_vs_target_pct as excel_energy_vs_target_pct,
        ghi_actual_vs_target_pct as excel_ghi_vs_target_pct,
        energy_vs_ghi_variance_pct as excel_energy_vs_ghi_variance_pct
    FROM "MMSR"."public"."site_monthly_performance_excel"
)

SELECT 
    COALESCE(db.year, excel.year) as year,
    COALESCE(db.month, excel.month) as month,
    COALESCE(db.site_id, excel.site_id) as site_id,
    COALESCE(db.site_name, excel.site_name) as site_name,
    
    -- Energy comparison
    db.db_energy_mwh,
    excel.excel_energy_mwh,
    (db.db_energy_mwh - excel.excel_energy_mwh) as energy_diff_mwh,
    CASE 
        WHEN excel.excel_energy_mwh IS NOT NULL AND excel.excel_energy_mwh != 0
        THEN ((db.db_energy_mwh - excel.excel_energy_mwh) / excel.excel_energy_mwh * 100)
        ELSE NULL
    END as energy_diff_pct,
    
    -- GHI comparison
    db.db_ghi_kwh_m2,
    excel.excel_ghi_kwh_m2,
    (db.db_ghi_kwh_m2 - excel.excel_ghi_kwh_m2) as ghi_diff_kwh_m2,
    CASE 
        WHEN excel.excel_ghi_kwh_m2 IS NOT NULL AND excel.excel_ghi_kwh_m2 != 0
        THEN ((db.db_ghi_kwh_m2 - excel.excel_ghi_kwh_m2) / excel.excel_ghi_kwh_m2 * 100)
        ELSE NULL
    END as ghi_diff_pct,
    
    -- PR comparison
    db.db_pr_ghi,
    excel.excel_pr_ghi,
    (db.db_pr_ghi - excel.excel_pr_ghi) as pr_diff,
    CASE 
        WHEN excel.excel_pr_ghi IS NOT NULL AND excel.excel_pr_ghi != 0
        THEN ((db.db_pr_ghi - excel.excel_pr_ghi) / excel.excel_pr_ghi * 100)
        ELSE NULL
    END as pr_diff_pct,
    
    -- Target comparison
    db.db_energy_target_mwh,
    excel.excel_energy_target_mwh,
    (db.db_energy_target_mwh - excel.excel_energy_target_mwh) as energy_target_diff_mwh,
    
    db.db_ghi_target,
    excel.excel_ghi_target,
    (db.db_ghi_target - excel.excel_ghi_target) as ghi_target_diff_kwh_m2,
    
    -- Actual vs Target comparison
    db.db_energy_vs_target_pct,
    excel.excel_energy_vs_target_pct,
    (db.db_energy_vs_target_pct - excel.excel_energy_vs_target_pct) as energy_vs_target_diff,
    
    db.db_ghi_vs_target_pct,
    excel.excel_ghi_vs_target_pct,
    (db.db_ghi_vs_target_pct - excel.excel_ghi_vs_target_pct) as ghi_vs_target_diff,
    
    -- Variance comparison
    db.db_energy_vs_ghi_variance_pct,
    excel.excel_energy_vs_ghi_variance_pct,
    (db.db_energy_vs_ghi_variance_pct - excel.excel_energy_vs_ghi_variance_pct) as variance_diff,
    
    -- Flags
    CASE 
        WHEN db.site_id IS NULL THEN 'MISSING_IN_DB'
        WHEN excel.site_id IS NULL THEN 'MISSING_IN_EXCEL'
        ELSE 'BOTH_EXIST'
    END as data_status,
    
    -- Check if differences are significant (> 0.1% or > 0.01 MWh)
    CASE 
        WHEN ABS(db.db_energy_mwh - excel.excel_energy_mwh) > 0.01 
            OR (excel.excel_energy_mwh IS NOT NULL AND excel.excel_energy_mwh != 0 
                AND ABS((db.db_energy_mwh - excel.excel_energy_mwh) / excel.excel_energy_mwh) > 0.001)
        THEN TRUE
        ELSE FALSE
    END as has_significant_energy_diff,
    
    CASE 
        WHEN ABS(db.db_pr_ghi - excel.excel_pr_ghi) > 0.001
        THEN TRUE
        ELSE FALSE
    END as has_significant_pr_diff

FROM db_monthly db
FULL OUTER JOIN excel_monthly excel
    ON db.year = excel.year
    AND db.month = excel.month
    AND db.site_id = excel.site_id

ORDER BY 
    COALESCE(db.year, excel.year) DESC,
    COALESCE(db.month, excel.month) DESC,
    COALESCE(db.site_id, excel.site_id)

