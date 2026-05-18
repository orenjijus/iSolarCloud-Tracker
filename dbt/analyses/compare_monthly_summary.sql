-- Summary comparison: Monthly totals from database vs Excel
-- This shows aggregated totals to quickly identify if there are discrepancies
-- 
-- Usage: Run this to see high-level summary of differences

WITH db_monthly AS (
    SELECT 
        year,
        month,
        SUM(daily_energy_mwh) as total_energy_mwh,
        SUM(daily_ghi_kwh_m2) as total_ghi_kwh_m2,
        COUNT(DISTINCT site_id) as site_count,
        AVG(pr_ghi_actual) as avg_pr_ghi
    FROM {{ ref('mart_site_performance_monthly') }}
    GROUP BY year, month
),

excel_monthly AS (
    SELECT 
        year,
        month,
        SUM(daily_energy_mwh) as total_energy_mwh,
        SUM(daily_ghi_kwh_m2) as total_ghi_kwh_m2,
        COUNT(DISTINCT site_id) as site_count,
        AVG(pr_ghi_actual) as avg_pr_ghi
    FROM "MMSR"."public"."site_monthly_performance_excel"
    GROUP BY year, month
)

SELECT 
    COALESCE(db.year, excel.year) as year,
    COALESCE(db.month, excel.month) as month,
    
    -- Totals comparison
    db.total_energy_mwh as db_total_energy_mwh,
    excel.total_energy_mwh as excel_total_energy_mwh,
    (db.total_energy_mwh - excel.total_energy_mwh) as energy_diff_mwh,
    CASE 
        WHEN excel.total_energy_mwh IS NOT NULL AND excel.total_energy_mwh != 0
        THEN ((db.total_energy_mwh - excel.total_energy_mwh) / excel.total_energy_mwh * 100)
        ELSE NULL
    END as energy_diff_pct,
    
    db.total_ghi_kwh_m2 as db_total_ghi_kwh_m2,
    excel.total_ghi_kwh_m2 as excel_total_ghi_kwh_m2,
    (db.total_ghi_kwh_m2 - excel.total_ghi_kwh_m2) as ghi_diff_kwh_m2,
    CASE 
        WHEN excel.total_ghi_kwh_m2 IS NOT NULL AND excel.total_ghi_kwh_m2 != 0
        THEN ((db.total_ghi_kwh_m2 - excel.total_ghi_kwh_m2) / excel.total_ghi_kwh_m2 * 100)
        ELSE NULL
    END as ghi_diff_pct,
    
    -- Site count comparison
    db.site_count as db_site_count,
    excel.site_count as excel_site_count,
    (db.site_count - excel.site_count) as site_count_diff,
    
    -- Average PR comparison
    db.avg_pr_ghi as db_avg_pr_ghi,
    excel.avg_pr_ghi as excel_avg_pr_ghi,
    (db.avg_pr_ghi - excel.avg_pr_ghi) as avg_pr_diff,
    
    -- Status
    CASE 
        WHEN db.year IS NULL THEN 'MISSING_IN_DB'
        WHEN excel.year IS NULL THEN 'MISSING_IN_EXCEL'
        WHEN ABS(db.total_energy_mwh - excel.total_energy_mwh) > 0.1 
            OR ABS(db.total_ghi_kwh_m2 - excel.total_ghi_kwh_m2) > 0.1
        THEN 'HAS_DIFFERENCES'
        ELSE 'MATCH'
    END as comparison_status

FROM db_monthly db
FULL OUTER JOIN excel_monthly excel
    ON db.year = excel.year
    AND db.month = excel.month

ORDER BY 
    COALESCE(db.year, excel.year) DESC,
    COALESCE(db.month, excel.month) DESC

