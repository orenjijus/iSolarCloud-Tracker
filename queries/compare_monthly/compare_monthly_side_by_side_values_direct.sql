-- Perbandingan bulanan samping-sampingan dengan nilai
-- Menampilkan nilai DB dan Excel per bulan secara horizontal untuk mudah dibandingkan
-- 
-- Versi langsung (tanpa dbt template) - bisa langsung dijalankan di database
-- Usage: Jalankan query ini langsung di database

WITH db_monthly AS (
    SELECT 
        year,
        month,
        SUM(daily_energy_mwh) as total_energy_mwh,
        SUM(daily_ghi_kwh_m2) as total_ghi_kwh_m2,
        COUNT(DISTINCT site_id) as site_count,
        AVG(pr_ghi_actual) as avg_pr_ghi
    FROM "MMSR"."mart"."mart_site_performance_monthly"
    WHERE year = 2025
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
    WHERE year = 2025
    GROUP BY year, month
)

SELECT 
    'Total Energy (MWh)' as metric_name,
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 1 THEN db.total_energy_mwh END)::numeric, 2)::text as "DB_Jan",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 1 THEN excel.total_energy_mwh END)::numeric, 2)::text as "Excel_Jan",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 1 THEN (db.total_energy_mwh - excel.total_energy_mwh) END)::numeric, 2)::text as "Diff_Jan",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 2 THEN db.total_energy_mwh END)::numeric, 2)::text as "DB_Feb",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 2 THEN excel.total_energy_mwh END)::numeric, 2)::text as "Excel_Feb",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 2 THEN (db.total_energy_mwh - excel.total_energy_mwh) END)::numeric, 2)::text as "Diff_Feb",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 3 THEN db.total_energy_mwh END)::numeric, 2)::text as "DB_Mar",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 3 THEN excel.total_energy_mwh END)::numeric, 2)::text as "Excel_Mar",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 3 THEN (db.total_energy_mwh - excel.total_energy_mwh) END)::numeric, 2)::text as "Diff_Mar",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 4 THEN db.total_energy_mwh END)::numeric, 2)::text as "DB_Apr",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 4 THEN excel.total_energy_mwh END)::numeric, 2)::text as "Excel_Apr",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 4 THEN (db.total_energy_mwh - excel.total_energy_mwh) END)::numeric, 2)::text as "Diff_Apr",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 5 THEN db.total_energy_mwh END)::numeric, 2)::text as "DB_May",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 5 THEN excel.total_energy_mwh END)::numeric, 2)::text as "Excel_May",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 5 THEN (db.total_energy_mwh - excel.total_energy_mwh) END)::numeric, 2)::text as "Diff_May",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 6 THEN db.total_energy_mwh END)::numeric, 2)::text as "DB_Jun",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 6 THEN excel.total_energy_mwh END)::numeric, 2)::text as "Excel_Jun",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 6 THEN (db.total_energy_mwh - excel.total_energy_mwh) END)::numeric, 2)::text as "Diff_Jun",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 7 THEN db.total_energy_mwh END)::numeric, 2)::text as "DB_Jul",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 7 THEN excel.total_energy_mwh END)::numeric, 2)::text as "Excel_Jul",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 7 THEN (db.total_energy_mwh - excel.total_energy_mwh) END)::numeric, 2)::text as "Diff_Jul",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 8 THEN db.total_energy_mwh END)::numeric, 2)::text as "DB_Aug",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 8 THEN excel.total_energy_mwh END)::numeric, 2)::text as "Excel_Aug",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 8 THEN (db.total_energy_mwh - excel.total_energy_mwh) END)::numeric, 2)::text as "Diff_Aug",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 9 THEN db.total_energy_mwh END)::numeric, 2)::text as "DB_Sep",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 9 THEN excel.total_energy_mwh END)::numeric, 2)::text as "Excel_Sep",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 9 THEN (db.total_energy_mwh - excel.total_energy_mwh) END)::numeric, 2)::text as "Diff_Sep",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 10 THEN db.total_energy_mwh END)::numeric, 2)::text as "DB_Oct",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 10 THEN excel.total_energy_mwh END)::numeric, 2)::text as "Excel_Oct",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 10 THEN (db.total_energy_mwh - excel.total_energy_mwh) END)::numeric, 2)::text as "Diff_Oct",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 11 THEN db.total_energy_mwh END)::numeric, 2)::text as "DB_Nov",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 11 THEN excel.total_energy_mwh END)::numeric, 2)::text as "Excel_Nov",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 11 THEN (db.total_energy_mwh - excel.total_energy_mwh) END)::numeric, 2)::text as "Diff_Nov"
FROM db_monthly db
FULL OUTER JOIN excel_monthly excel
    ON db.year = excel.year AND db.month = excel.month

UNION ALL

SELECT 
    'Total GHI (kWh/m²)' as metric_name,
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 1 THEN db.total_ghi_kwh_m2 END)::numeric, 2)::text as "DB_Jan",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 1 THEN excel.total_ghi_kwh_m2 END)::numeric, 2)::text as "Excel_Jan",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 1 THEN (db.total_ghi_kwh_m2 - excel.total_ghi_kwh_m2) END)::numeric, 2)::text as "Diff_Jan",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 2 THEN db.total_ghi_kwh_m2 END)::numeric, 2)::text as "DB_Feb",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 2 THEN excel.total_ghi_kwh_m2 END)::numeric, 2)::text as "Excel_Feb",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 2 THEN (db.total_ghi_kwh_m2 - excel.total_ghi_kwh_m2) END)::numeric, 2)::text as "Diff_Feb",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 3 THEN db.total_ghi_kwh_m2 END)::numeric, 2)::text as "DB_Mar",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 3 THEN excel.total_ghi_kwh_m2 END)::numeric, 2)::text as "Excel_Mar",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 3 THEN (db.total_ghi_kwh_m2 - excel.total_ghi_kwh_m2) END)::numeric, 2)::text as "Diff_Mar",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 4 THEN db.total_ghi_kwh_m2 END)::numeric, 2)::text as "DB_Apr",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 4 THEN excel.total_ghi_kwh_m2 END)::numeric, 2)::text as "Excel_Apr",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 4 THEN (db.total_ghi_kwh_m2 - excel.total_ghi_kwh_m2) END)::numeric, 2)::text as "Diff_Apr",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 5 THEN db.total_ghi_kwh_m2 END)::numeric, 2)::text as "DB_May",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 5 THEN excel.total_ghi_kwh_m2 END)::numeric, 2)::text as "Excel_May",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 5 THEN (db.total_ghi_kwh_m2 - excel.total_ghi_kwh_m2) END)::numeric, 2)::text as "Diff_May",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 6 THEN db.total_ghi_kwh_m2 END)::numeric, 2)::text as "DB_Jun",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 6 THEN excel.total_ghi_kwh_m2 END)::numeric, 2)::text as "Excel_Jun",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 6 THEN (db.total_ghi_kwh_m2 - excel.total_ghi_kwh_m2) END)::numeric, 2)::text as "Diff_Jun",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 7 THEN db.total_ghi_kwh_m2 END)::numeric, 2)::text as "DB_Jul",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 7 THEN excel.total_ghi_kwh_m2 END)::numeric, 2)::text as "Excel_Jul",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 7 THEN (db.total_ghi_kwh_m2 - excel.total_ghi_kwh_m2) END)::numeric, 2)::text as "Diff_Jul",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 8 THEN db.total_ghi_kwh_m2 END)::numeric, 2)::text as "DB_Aug",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 8 THEN excel.total_ghi_kwh_m2 END)::numeric, 2)::text as "Excel_Aug",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 8 THEN (db.total_ghi_kwh_m2 - excel.total_ghi_kwh_m2) END)::numeric, 2)::text as "Diff_Aug",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 9 THEN db.total_ghi_kwh_m2 END)::numeric, 2)::text as "DB_Sep",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 9 THEN excel.total_ghi_kwh_m2 END)::numeric, 2)::text as "Excel_Sep",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 9 THEN (db.total_ghi_kwh_m2 - excel.total_ghi_kwh_m2) END)::numeric, 2)::text as "Diff_Sep",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 10 THEN db.total_ghi_kwh_m2 END)::numeric, 2)::text as "DB_Oct",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 10 THEN excel.total_ghi_kwh_m2 END)::numeric, 2)::text as "Excel_Oct",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 10 THEN (db.total_ghi_kwh_m2 - excel.total_ghi_kwh_m2) END)::numeric, 2)::text as "Diff_Oct",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 11 THEN db.total_ghi_kwh_m2 END)::numeric, 2)::text as "DB_Nov",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 11 THEN excel.total_ghi_kwh_m2 END)::numeric, 2)::text as "Excel_Nov",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 11 THEN (db.total_ghi_kwh_m2 - excel.total_ghi_kwh_m2) END)::numeric, 2)::text as "Diff_Nov"
FROM db_monthly db
FULL OUTER JOIN excel_monthly excel
    ON db.year = excel.year AND db.month = excel.month

UNION ALL

SELECT 
    'Site Count' as metric_name,
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 1 THEN db.site_count END)::text as "DB_Jan",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 1 THEN excel.site_count END)::text as "Excel_Jan",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 1 THEN (db.site_count - excel.site_count) END)::text as "Diff_Jan",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 2 THEN db.site_count END)::text as "DB_Feb",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 2 THEN excel.site_count END)::text as "Excel_Feb",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 2 THEN (db.site_count - excel.site_count) END)::text as "Diff_Feb",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 3 THEN db.site_count END)::text as "DB_Mar",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 3 THEN excel.site_count END)::text as "Excel_Mar",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 3 THEN (db.site_count - excel.site_count) END)::text as "Diff_Mar",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 4 THEN db.site_count END)::text as "DB_Apr",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 4 THEN excel.site_count END)::text as "Excel_Apr",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 4 THEN (db.site_count - excel.site_count) END)::text as "Diff_Apr",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 5 THEN db.site_count END)::text as "DB_May",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 5 THEN excel.site_count END)::text as "Excel_May",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 5 THEN (db.site_count - excel.site_count) END)::text as "Diff_May",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 6 THEN db.site_count END)::text as "DB_Jun",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 6 THEN excel.site_count END)::text as "Excel_Jun",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 6 THEN (db.site_count - excel.site_count) END)::text as "Diff_Jun",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 7 THEN db.site_count END)::text as "DB_Jul",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 7 THEN excel.site_count END)::text as "Excel_Jul",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 7 THEN (db.site_count - excel.site_count) END)::text as "Diff_Jul",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 8 THEN db.site_count END)::text as "DB_Aug",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 8 THEN excel.site_count END)::text as "Excel_Aug",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 8 THEN (db.site_count - excel.site_count) END)::text as "Diff_Aug",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 9 THEN db.site_count END)::text as "DB_Sep",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 9 THEN excel.site_count END)::text as "Excel_Sep",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 9 THEN (db.site_count - excel.site_count) END)::text as "Diff_Sep",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 10 THEN db.site_count END)::text as "DB_Oct",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 10 THEN excel.site_count END)::text as "Excel_Oct",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 10 THEN (db.site_count - excel.site_count) END)::text as "Diff_Oct",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 11 THEN db.site_count END)::text as "DB_Nov",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 11 THEN excel.site_count END)::text as "Excel_Nov",
    MAX(CASE WHEN COALESCE(db.month, excel.month) = 11 THEN (db.site_count - excel.site_count) END)::text as "Diff_Nov"
FROM db_monthly db
FULL OUTER JOIN excel_monthly excel
    ON db.year = excel.year AND db.month = excel.month

UNION ALL

SELECT 
    'Avg PR GHI' as metric_name,
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 1 THEN db.avg_pr_ghi END)::numeric, 4)::text as "DB_Jan",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 1 THEN excel.avg_pr_ghi END)::numeric, 4)::text as "Excel_Jan",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 1 THEN (db.avg_pr_ghi - excel.avg_pr_ghi) END)::numeric, 4)::text as "Diff_Jan",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 2 THEN db.avg_pr_ghi END)::numeric, 4)::text as "DB_Feb",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 2 THEN excel.avg_pr_ghi END)::numeric, 4)::text as "Excel_Feb",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 2 THEN (db.avg_pr_ghi - excel.avg_pr_ghi) END)::numeric, 4)::text as "Diff_Feb",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 3 THEN db.avg_pr_ghi END)::numeric, 4)::text as "DB_Mar",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 3 THEN excel.avg_pr_ghi END)::numeric, 4)::text as "Excel_Mar",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 3 THEN (db.avg_pr_ghi - excel.avg_pr_ghi) END)::numeric, 4)::text as "Diff_Mar",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 4 THEN db.avg_pr_ghi END)::numeric, 4)::text as "DB_Apr",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 4 THEN excel.avg_pr_ghi END)::numeric, 4)::text as "Excel_Apr",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 4 THEN (db.avg_pr_ghi - excel.avg_pr_ghi) END)::numeric, 4)::text as "Diff_Apr",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 5 THEN db.avg_pr_ghi END)::numeric, 4)::text as "DB_May",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 5 THEN excel.avg_pr_ghi END)::numeric, 4)::text as "Excel_May",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 5 THEN (db.avg_pr_ghi - excel.avg_pr_ghi) END)::numeric, 4)::text as "Diff_May",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 6 THEN db.avg_pr_ghi END)::numeric, 4)::text as "DB_Jun",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 6 THEN excel.avg_pr_ghi END)::numeric, 4)::text as "Excel_Jun",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 6 THEN (db.avg_pr_ghi - excel.avg_pr_ghi) END)::numeric, 4)::text as "Diff_Jun",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 7 THEN db.avg_pr_ghi END)::numeric, 4)::text as "DB_Jul",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 7 THEN excel.avg_pr_ghi END)::numeric, 4)::text as "Excel_Jul",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 7 THEN (db.avg_pr_ghi - excel.avg_pr_ghi) END)::numeric, 4)::text as "Diff_Jul",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 8 THEN db.avg_pr_ghi END)::numeric, 4)::text as "DB_Aug",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 8 THEN excel.avg_pr_ghi END)::numeric, 4)::text as "Excel_Aug",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 8 THEN (db.avg_pr_ghi - excel.avg_pr_ghi) END)::numeric, 4)::text as "Diff_Aug",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 9 THEN db.avg_pr_ghi END)::numeric, 4)::text as "DB_Sep",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 9 THEN excel.avg_pr_ghi END)::numeric, 4)::text as "Excel_Sep",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 9 THEN (db.avg_pr_ghi - excel.avg_pr_ghi) END)::numeric, 4)::text as "Diff_Sep",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 10 THEN db.avg_pr_ghi END)::numeric, 4)::text as "DB_Oct",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 10 THEN excel.avg_pr_ghi END)::numeric, 4)::text as "Excel_Oct",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 10 THEN (db.avg_pr_ghi - excel.avg_pr_ghi) END)::numeric, 4)::text as "Diff_Oct",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 11 THEN db.avg_pr_ghi END)::numeric, 4)::text as "DB_Nov",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 11 THEN excel.avg_pr_ghi END)::numeric, 4)::text as "Excel_Nov",
    ROUND(MAX(CASE WHEN COALESCE(db.month, excel.month) = 11 THEN (db.avg_pr_ghi - excel.avg_pr_ghi) END)::numeric, 4)::text as "Diff_Nov"
FROM db_monthly db
FULL OUTER JOIN excel_monthly excel
    ON db.year = excel.year AND db.month = excel.month

ORDER BY metric_name

