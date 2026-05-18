-- Perbandingan bulanan per site samping-sampingan dengan semua nilai
-- Menampilkan Energy, GHI, dan PR per site per bulan secara horizontal
-- 
-- Versi langsung (tanpa dbt template) - bisa langsung dijalankan di database
-- Usage: Jalankan query ini langsung di database

WITH db_monthly AS (
    SELECT 
        year,
        month,
        site_id,
        site_name,
        daily_energy_mwh as db_energy_mwh,
        daily_ghi_kwh_m2 as db_ghi_kwh_m2,
        pr_ghi_actual as db_pr_ghi
    FROM "MMSR"."mart"."mart_site_performance_monthly"
    WHERE year = 2025
),

excel_monthly AS (
    SELECT 
        year,
        month,
        site_id,
        site_name,
        daily_energy_mwh as excel_energy_mwh,
        daily_ghi_kwh_m2 as excel_ghi_kwh_m2,
        pr_ghi_actual as excel_pr_ghi
    FROM "MMSR"."public"."site_monthly_performance_excel"
    WHERE year = 2025
),

site_comparison AS (
    SELECT 
        COALESCE(db.year, excel.year) as year,
        COALESCE(db.month, excel.month) as month,
        COALESCE(db.site_id, excel.site_id) as site_id,
        COALESCE(db.site_name, excel.site_name) as site_name,
        db.db_energy_mwh,
        excel.excel_energy_mwh,
        COALESCE(db.db_energy_mwh, 0) - COALESCE(excel.excel_energy_mwh, 0) as energy_diff_mwh,
        db.db_ghi_kwh_m2,
        excel.excel_ghi_kwh_m2,
        COALESCE(db.db_ghi_kwh_m2, 0) - COALESCE(excel.excel_ghi_kwh_m2, 0) as ghi_diff_kwh_m2,
        db.db_pr_ghi,
        excel.excel_pr_ghi,
        COALESCE(db.db_pr_ghi, 0) - COALESCE(excel.excel_pr_ghi, 0) as pr_diff
    FROM db_monthly db
    FULL OUTER JOIN excel_monthly excel
        ON db.year = excel.year
        AND db.month = excel.month
        AND db.site_id = excel.site_id
)

SELECT 
    site_id,
    LEFT(site_name, 50) as site_name,
    
    -- Energy per month (DB, Excel, Diff)
    ROUND(MAX(CASE WHEN month = 1 THEN db_energy_mwh END)::numeric, 2)::text as "E_DB_Jan",
    ROUND(MAX(CASE WHEN month = 1 THEN excel_energy_mwh END)::numeric, 2)::text as "E_Excel_Jan",
    ROUND(MAX(CASE WHEN month = 1 THEN energy_diff_mwh END)::numeric, 2)::text as "E_Diff_Jan",
    ROUND(MAX(CASE WHEN month = 2 THEN db_energy_mwh END)::numeric, 2)::text as "E_DB_Feb",
    ROUND(MAX(CASE WHEN month = 2 THEN excel_energy_mwh END)::numeric, 2)::text as "E_Excel_Feb",
    ROUND(MAX(CASE WHEN month = 2 THEN energy_diff_mwh END)::numeric, 2)::text as "E_Diff_Feb",
    ROUND(MAX(CASE WHEN month = 3 THEN db_energy_mwh END)::numeric, 2)::text as "E_DB_Mar",
    ROUND(MAX(CASE WHEN month = 3 THEN excel_energy_mwh END)::numeric, 2)::text as "E_Excel_Mar",
    ROUND(MAX(CASE WHEN month = 3 THEN energy_diff_mwh END)::numeric, 2)::text as "E_Diff_Mar",
    ROUND(MAX(CASE WHEN month = 4 THEN db_energy_mwh END)::numeric, 2)::text as "E_DB_Apr",
    ROUND(MAX(CASE WHEN month = 4 THEN excel_energy_mwh END)::numeric, 2)::text as "E_Excel_Apr",
    ROUND(MAX(CASE WHEN month = 4 THEN energy_diff_mwh END)::numeric, 2)::text as "E_Diff_Apr",
    ROUND(MAX(CASE WHEN month = 5 THEN db_energy_mwh END)::numeric, 2)::text as "E_DB_May",
    ROUND(MAX(CASE WHEN month = 5 THEN excel_energy_mwh END)::numeric, 2)::text as "E_Excel_May",
    ROUND(MAX(CASE WHEN month = 5 THEN energy_diff_mwh END)::numeric, 2)::text as "E_Diff_May",
    ROUND(MAX(CASE WHEN month = 6 THEN db_energy_mwh END)::numeric, 2)::text as "E_DB_Jun",
    ROUND(MAX(CASE WHEN month = 6 THEN excel_energy_mwh END)::numeric, 2)::text as "E_Excel_Jun",
    ROUND(MAX(CASE WHEN month = 6 THEN energy_diff_mwh END)::numeric, 2)::text as "E_Diff_Jun",
    ROUND(MAX(CASE WHEN month = 7 THEN db_energy_mwh END)::numeric, 2)::text as "E_DB_Jul",
    ROUND(MAX(CASE WHEN month = 7 THEN excel_energy_mwh END)::numeric, 2)::text as "E_Excel_Jul",
    ROUND(MAX(CASE WHEN month = 7 THEN energy_diff_mwh END)::numeric, 2)::text as "E_Diff_Jul",
    ROUND(MAX(CASE WHEN month = 8 THEN db_energy_mwh END)::numeric, 2)::text as "E_DB_Aug",
    ROUND(MAX(CASE WHEN month = 8 THEN excel_energy_mwh END)::numeric, 2)::text as "E_Excel_Aug",
    ROUND(MAX(CASE WHEN month = 8 THEN energy_diff_mwh END)::numeric, 2)::text as "E_Diff_Aug",
    ROUND(MAX(CASE WHEN month = 9 THEN db_energy_mwh END)::numeric, 2)::text as "E_DB_Sep",
    ROUND(MAX(CASE WHEN month = 9 THEN excel_energy_mwh END)::numeric, 2)::text as "E_Excel_Sep",
    ROUND(MAX(CASE WHEN month = 9 THEN energy_diff_mwh END)::numeric, 2)::text as "E_Diff_Sep",
    ROUND(MAX(CASE WHEN month = 10 THEN db_energy_mwh END)::numeric, 2)::text as "E_DB_Oct",
    ROUND(MAX(CASE WHEN month = 10 THEN excel_energy_mwh END)::numeric, 2)::text as "E_Excel_Oct",
    ROUND(MAX(CASE WHEN month = 10 THEN energy_diff_mwh END)::numeric, 2)::text as "E_Diff_Oct",
    ROUND(MAX(CASE WHEN month = 11 THEN db_energy_mwh END)::numeric, 2)::text as "E_DB_Nov",
    ROUND(MAX(CASE WHEN month = 11 THEN excel_energy_mwh END)::numeric, 2)::text as "E_Excel_Nov",
    ROUND(MAX(CASE WHEN month = 11 THEN energy_diff_mwh END)::numeric, 2)::text as "E_Diff_Nov",
    
    -- GHI per month (DB, Excel, Diff)
    ROUND(MAX(CASE WHEN month = 1 THEN db_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_DB_Jan",
    ROUND(MAX(CASE WHEN month = 1 THEN excel_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_Excel_Jan",
    ROUND(MAX(CASE WHEN month = 1 THEN ghi_diff_kwh_m2 END)::numeric, 2)::text as "GHI_Diff_Jan",
    ROUND(MAX(CASE WHEN month = 2 THEN db_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_DB_Feb",
    ROUND(MAX(CASE WHEN month = 2 THEN excel_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_Excel_Feb",
    ROUND(MAX(CASE WHEN month = 2 THEN ghi_diff_kwh_m2 END)::numeric, 2)::text as "GHI_Diff_Feb",
    ROUND(MAX(CASE WHEN month = 3 THEN db_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_DB_Mar",
    ROUND(MAX(CASE WHEN month = 3 THEN excel_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_Excel_Mar",
    ROUND(MAX(CASE WHEN month = 3 THEN ghi_diff_kwh_m2 END)::numeric, 2)::text as "GHI_Diff_Mar",
    ROUND(MAX(CASE WHEN month = 4 THEN db_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_DB_Apr",
    ROUND(MAX(CASE WHEN month = 4 THEN excel_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_Excel_Apr",
    ROUND(MAX(CASE WHEN month = 4 THEN ghi_diff_kwh_m2 END)::numeric, 2)::text as "GHI_Diff_Apr",
    ROUND(MAX(CASE WHEN month = 5 THEN db_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_DB_May",
    ROUND(MAX(CASE WHEN month = 5 THEN excel_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_Excel_May",
    ROUND(MAX(CASE WHEN month = 5 THEN ghi_diff_kwh_m2 END)::numeric, 2)::text as "GHI_Diff_May",
    ROUND(MAX(CASE WHEN month = 6 THEN db_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_DB_Jun",
    ROUND(MAX(CASE WHEN month = 6 THEN excel_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_Excel_Jun",
    ROUND(MAX(CASE WHEN month = 6 THEN ghi_diff_kwh_m2 END)::numeric, 2)::text as "GHI_Diff_Jun",
    ROUND(MAX(CASE WHEN month = 7 THEN db_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_DB_Jul",
    ROUND(MAX(CASE WHEN month = 7 THEN excel_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_Excel_Jul",
    ROUND(MAX(CASE WHEN month = 7 THEN ghi_diff_kwh_m2 END)::numeric, 2)::text as "GHI_Diff_Jul",
    ROUND(MAX(CASE WHEN month = 8 THEN db_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_DB_Aug",
    ROUND(MAX(CASE WHEN month = 8 THEN excel_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_Excel_Aug",
    ROUND(MAX(CASE WHEN month = 8 THEN ghi_diff_kwh_m2 END)::numeric, 2)::text as "GHI_Diff_Aug",
    ROUND(MAX(CASE WHEN month = 9 THEN db_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_DB_Sep",
    ROUND(MAX(CASE WHEN month = 9 THEN excel_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_Excel_Sep",
    ROUND(MAX(CASE WHEN month = 9 THEN ghi_diff_kwh_m2 END)::numeric, 2)::text as "GHI_Diff_Sep",
    ROUND(MAX(CASE WHEN month = 10 THEN db_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_DB_Oct",
    ROUND(MAX(CASE WHEN month = 10 THEN excel_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_Excel_Oct",
    ROUND(MAX(CASE WHEN month = 10 THEN ghi_diff_kwh_m2 END)::numeric, 2)::text as "GHI_Diff_Oct",
    ROUND(MAX(CASE WHEN month = 11 THEN db_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_DB_Nov",
    ROUND(MAX(CASE WHEN month = 11 THEN excel_ghi_kwh_m2 END)::numeric, 2)::text as "GHI_Excel_Nov",
    ROUND(MAX(CASE WHEN month = 11 THEN ghi_diff_kwh_m2 END)::numeric, 2)::text as "GHI_Diff_Nov",
    
    -- PR per month (DB, Excel, Diff)
    ROUND(MAX(CASE WHEN month = 1 THEN db_pr_ghi END)::numeric, 4)::text as "PR_DB_Jan",
    ROUND(MAX(CASE WHEN month = 1 THEN excel_pr_ghi END)::numeric, 4)::text as "PR_Excel_Jan",
    ROUND(MAX(CASE WHEN month = 1 THEN pr_diff END)::numeric, 4)::text as "PR_Diff_Jan",
    ROUND(MAX(CASE WHEN month = 2 THEN db_pr_ghi END)::numeric, 4)::text as "PR_DB_Feb",
    ROUND(MAX(CASE WHEN month = 2 THEN excel_pr_ghi END)::numeric, 4)::text as "PR_Excel_Feb",
    ROUND(MAX(CASE WHEN month = 2 THEN pr_diff END)::numeric, 4)::text as "PR_Diff_Feb",
    ROUND(MAX(CASE WHEN month = 3 THEN db_pr_ghi END)::numeric, 4)::text as "PR_DB_Mar",
    ROUND(MAX(CASE WHEN month = 3 THEN excel_pr_ghi END)::numeric, 4)::text as "PR_Excel_Mar",
    ROUND(MAX(CASE WHEN month = 3 THEN pr_diff END)::numeric, 4)::text as "PR_Diff_Mar",
    ROUND(MAX(CASE WHEN month = 4 THEN db_pr_ghi END)::numeric, 4)::text as "PR_DB_Apr",
    ROUND(MAX(CASE WHEN month = 4 THEN excel_pr_ghi END)::numeric, 4)::text as "PR_Excel_Apr",
    ROUND(MAX(CASE WHEN month = 4 THEN pr_diff END)::numeric, 4)::text as "PR_Diff_Apr",
    ROUND(MAX(CASE WHEN month = 5 THEN db_pr_ghi END)::numeric, 4)::text as "PR_DB_May",
    ROUND(MAX(CASE WHEN month = 5 THEN excel_pr_ghi END)::numeric, 4)::text as "PR_Excel_May",
    ROUND(MAX(CASE WHEN month = 5 THEN pr_diff END)::numeric, 4)::text as "PR_Diff_May",
    ROUND(MAX(CASE WHEN month = 6 THEN db_pr_ghi END)::numeric, 4)::text as "PR_DB_Jun",
    ROUND(MAX(CASE WHEN month = 6 THEN excel_pr_ghi END)::numeric, 4)::text as "PR_Excel_Jun",
    ROUND(MAX(CASE WHEN month = 6 THEN pr_diff END)::numeric, 4)::text as "PR_Diff_Jun",
    ROUND(MAX(CASE WHEN month = 7 THEN db_pr_ghi END)::numeric, 4)::text as "PR_DB_Jul",
    ROUND(MAX(CASE WHEN month = 7 THEN excel_pr_ghi END)::numeric, 4)::text as "PR_Excel_Jul",
    ROUND(MAX(CASE WHEN month = 7 THEN pr_diff END)::numeric, 4)::text as "PR_Diff_Jul",
    ROUND(MAX(CASE WHEN month = 8 THEN db_pr_ghi END)::numeric, 4)::text as "PR_DB_Aug",
    ROUND(MAX(CASE WHEN month = 8 THEN excel_pr_ghi END)::numeric, 4)::text as "PR_Excel_Aug",
    ROUND(MAX(CASE WHEN month = 8 THEN pr_diff END)::numeric, 4)::text as "PR_Diff_Aug",
    ROUND(MAX(CASE WHEN month = 9 THEN db_pr_ghi END)::numeric, 4)::text as "PR_DB_Sep",
    ROUND(MAX(CASE WHEN month = 9 THEN excel_pr_ghi END)::numeric, 4)::text as "PR_Excel_Sep",
    ROUND(MAX(CASE WHEN month = 9 THEN pr_diff END)::numeric, 4)::text as "PR_Diff_Sep",
    ROUND(MAX(CASE WHEN month = 10 THEN db_pr_ghi END)::numeric, 4)::text as "PR_DB_Oct",
    ROUND(MAX(CASE WHEN month = 10 THEN excel_pr_ghi END)::numeric, 4)::text as "PR_Excel_Oct",
    ROUND(MAX(CASE WHEN month = 10 THEN pr_diff END)::numeric, 4)::text as "PR_Diff_Oct",
    ROUND(MAX(CASE WHEN month = 11 THEN db_pr_ghi END)::numeric, 4)::text as "PR_DB_Nov",
    ROUND(MAX(CASE WHEN month = 11 THEN excel_pr_ghi END)::numeric, 4)::text as "PR_Excel_Nov",
    ROUND(MAX(CASE WHEN month = 11 THEN pr_diff END)::numeric, 4)::text as "PR_Diff_Nov",
    
    -- Summary
    COUNT(DISTINCT month)::text as months_with_data,
    ROUND(SUM(ABS(COALESCE(energy_diff_mwh, 0)))::numeric, 2)::text as total_abs_diff_mwh

FROM site_comparison
GROUP BY site_id, site_name
HAVING SUM(ABS(COALESCE(energy_diff_mwh, 0))) > 0.01  -- Only show sites with differences
ORDER BY SUM(ABS(COALESCE(energy_diff_mwh, 0))) DESC, site_id

