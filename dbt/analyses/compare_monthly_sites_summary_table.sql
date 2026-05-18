-- Summary table: Sites causing differences by month
-- This creates a pivot-like view showing which sites have issues in which months
-- 
-- Usage: Run this to see a comprehensive overview

WITH db_monthly AS (
    SELECT 
        year,
        month,
        site_id,
        site_name,
        daily_energy_mwh as db_energy_mwh
    FROM "MMSR"."mart"."mart_site_performance_monthly"
    WHERE year = 2025
),

excel_monthly AS (
    SELECT 
        year,
        month,
        site_id,
        site_name,
        daily_energy_mwh as excel_energy_mwh
    FROM "MMSR"."public"."site_monthly_performance_excel"
    WHERE year = 2025
),

site_comparison AS (
    SELECT 
        COALESCE(db.year, excel.year) as year,
        COALESCE(db.month, excel.month) as month,
        COALESCE(db.site_id, excel.site_id) as site_id,
        COALESCE(db.site_name, excel.site_name) as site_name,
        
        COALESCE(db.db_energy_mwh, 0) - COALESCE(excel.excel_energy_mwh, 0) as energy_diff_mwh,
        
        CASE 
            WHEN db.site_id IS NULL THEN 'MISSING_IN_DB'
            WHEN excel.site_id IS NULL THEN 'MISSING_IN_EXCEL'
            WHEN ABS(COALESCE(db.db_energy_mwh, 0) - COALESCE(excel.excel_energy_mwh, 0)) > 0.01 
            THEN 'HAS_DIFFERENCES'
            ELSE 'MATCH'
        END as status
        
    FROM db_monthly db
    FULL OUTER JOIN excel_monthly excel
        ON db.year = excel.year
        AND db.month = excel.month
        AND db.site_id = excel.site_id
),

-- Get all problematic sites
problematic_sites AS (
    SELECT DISTINCT site_id, site_name
    FROM site_comparison
    WHERE status != 'MATCH'
),

-- Create summary by site and month
site_month_summary AS (
    SELECT 
        ps.site_id,
        ps.site_name,
        sc.month,
        sc.status,
        sc.energy_diff_mwh,
        CASE 
            WHEN sc.status = 'MISSING_IN_EXCEL' THEN 'MISS'
            WHEN sc.status = 'HAS_DIFFERENCES' AND sc.energy_diff_mwh > 0 THEN 'DB+'
            WHEN sc.status = 'HAS_DIFFERENCES' AND sc.energy_diff_mwh < 0 THEN 'DB-'
            ELSE 'OK'
        END as status_code
    FROM problematic_sites ps
    CROSS JOIN (SELECT DISTINCT month FROM site_comparison WHERE year = 2025) months
    LEFT JOIN site_comparison sc
        ON ps.site_id = sc.site_id
        AND months.month = sc.month
        AND sc.status != 'MATCH'
)

-- Pivot-like summary
SELECT 
    site_id,
    site_name,
    MAX(CASE WHEN month = 1 THEN status_code ELSE NULL END) as "Jan",
    MAX(CASE WHEN month = 2 THEN status_code ELSE NULL END) as "Feb",
    MAX(CASE WHEN month = 3 THEN status_code ELSE NULL END) as "Mar",
    MAX(CASE WHEN month = 4 THEN status_code ELSE NULL END) as "Apr",
    MAX(CASE WHEN month = 5 THEN status_code ELSE NULL END) as "May",
    MAX(CASE WHEN month = 6 THEN status_code ELSE NULL END) as "Jun",
    MAX(CASE WHEN month = 7 THEN status_code ELSE NULL END) as "Jul",
    MAX(CASE WHEN month = 8 THEN status_code ELSE NULL END) as "Aug",
    MAX(CASE WHEN month = 9 THEN status_code ELSE NULL END) as "Sep",
    MAX(CASE WHEN month = 10 THEN status_code ELSE NULL END) as "Oct",
    MAX(CASE WHEN month = 11 THEN status_code ELSE NULL END) as "Nov",
    COUNT(DISTINCT month) as months_with_issues,
    SUM(energy_diff_mwh) as total_energy_diff_mwh
FROM site_month_summary
GROUP BY site_id, site_name
ORDER BY 
    months_with_issues DESC,
    ABS(SUM(energy_diff_mwh)) DESC

