-- Perbandingan bulanan per site samping-sampingan
-- Menampilkan nilai per site per bulan secara horizontal
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
    LEFT(site_name, 60) as site_name,
    
    -- Energy values per month (DB, Excel, Diff)
    ROUND(MAX(CASE WHEN month = 1 THEN db_energy_mwh END)::numeric, 2)::text as "DB_Jan",
    ROUND(MAX(CASE WHEN month = 1 THEN excel_energy_mwh END)::numeric, 2)::text as "Excel_Jan",
    ROUND(MAX(CASE WHEN month = 1 THEN energy_diff_mwh END)::numeric, 2)::text as "Diff_Jan",
    
    ROUND(MAX(CASE WHEN month = 2 THEN db_energy_mwh END)::numeric, 2)::text as "DB_Feb",
    ROUND(MAX(CASE WHEN month = 2 THEN excel_energy_mwh END)::numeric, 2)::text as "Excel_Feb",
    ROUND(MAX(CASE WHEN month = 2 THEN energy_diff_mwh END)::numeric, 2)::text as "Diff_Feb",
    
    ROUND(MAX(CASE WHEN month = 3 THEN db_energy_mwh END)::numeric, 2)::text as "DB_Mar",
    ROUND(MAX(CASE WHEN month = 3 THEN excel_energy_mwh END)::numeric, 2)::text as "Excel_Mar",
    ROUND(MAX(CASE WHEN month = 3 THEN energy_diff_mwh END)::numeric, 2)::text as "Diff_Mar",
    
    ROUND(MAX(CASE WHEN month = 4 THEN db_energy_mwh END)::numeric, 2)::text as "DB_Apr",
    ROUND(MAX(CASE WHEN month = 4 THEN excel_energy_mwh END)::numeric, 2)::text as "Excel_Apr",
    ROUND(MAX(CASE WHEN month = 4 THEN energy_diff_mwh END)::numeric, 2)::text as "Diff_Apr",
    
    ROUND(MAX(CASE WHEN month = 5 THEN db_energy_mwh END)::numeric, 2)::text as "DB_May",
    ROUND(MAX(CASE WHEN month = 5 THEN excel_energy_mwh END)::numeric, 2)::text as "Excel_May",
    ROUND(MAX(CASE WHEN month = 5 THEN energy_diff_mwh END)::numeric, 2)::text as "Diff_May",
    
    ROUND(MAX(CASE WHEN month = 6 THEN db_energy_mwh END)::numeric, 2)::text as "DB_Jun",
    ROUND(MAX(CASE WHEN month = 6 THEN excel_energy_mwh END)::numeric, 2)::text as "Excel_Jun",
    ROUND(MAX(CASE WHEN month = 6 THEN energy_diff_mwh END)::numeric, 2)::text as "Diff_Jun",
    
    ROUND(MAX(CASE WHEN month = 7 THEN db_energy_mwh END)::numeric, 2)::text as "DB_Jul",
    ROUND(MAX(CASE WHEN month = 7 THEN excel_energy_mwh END)::numeric, 2)::text as "Excel_Jul",
    ROUND(MAX(CASE WHEN month = 7 THEN energy_diff_mwh END)::numeric, 2)::text as "Diff_Jul",
    
    ROUND(MAX(CASE WHEN month = 8 THEN db_energy_mwh END)::numeric, 2)::text as "DB_Aug",
    ROUND(MAX(CASE WHEN month = 8 THEN excel_energy_mwh END)::numeric, 2)::text as "Excel_Aug",
    ROUND(MAX(CASE WHEN month = 8 THEN energy_diff_mwh END)::numeric, 2)::text as "Diff_Aug",
    
    ROUND(MAX(CASE WHEN month = 9 THEN db_energy_mwh END)::numeric, 2)::text as "DB_Sep",
    ROUND(MAX(CASE WHEN month = 9 THEN excel_energy_mwh END)::numeric, 2)::text as "Excel_Sep",
    ROUND(MAX(CASE WHEN month = 9 THEN energy_diff_mwh END)::numeric, 2)::text as "Diff_Sep",
    
    ROUND(MAX(CASE WHEN month = 10 THEN db_energy_mwh END)::numeric, 2)::text as "DB_Oct",
    ROUND(MAX(CASE WHEN month = 10 THEN excel_energy_mwh END)::numeric, 2)::text as "Excel_Oct",
    ROUND(MAX(CASE WHEN month = 10 THEN energy_diff_mwh END)::numeric, 2)::text as "Diff_Oct",
    
    ROUND(MAX(CASE WHEN month = 11 THEN db_energy_mwh END)::numeric, 2)::text as "DB_Nov",
    ROUND(MAX(CASE WHEN month = 11 THEN excel_energy_mwh END)::numeric, 2)::text as "Excel_Nov",
    ROUND(MAX(CASE WHEN month = 11 THEN energy_diff_mwh END)::numeric, 2)::text as "Diff_Nov",
    
    -- Summary
    COUNT(DISTINCT month) as months_with_data,
    ROUND(SUM(ABS(energy_diff_mwh))::numeric, 2)::text as total_abs_diff_mwh

FROM site_comparison
GROUP BY site_id, site_name
HAVING SUM(ABS(COALESCE(energy_diff_mwh, 0))) > 0.01  -- Only show sites with differences
ORDER BY SUM(ABS(COALESCE(energy_diff_mwh, 0))) DESC, site_id

