-- Summary of sites causing differences, grouped by month
-- This shows which sites contribute most to aggregate differences each month
-- 
-- Usage: Run this to see a summary view of problematic sites per month

WITH db_monthly AS (
    SELECT 
        year,
        month,
        site_id,
        site_name,
        daily_energy_mwh as db_energy_mwh,
        daily_ghi_kwh_m2 as db_ghi_kwh_m2
    FROM {{ ref('mart_site_performance_monthly') }}
    WHERE year >= 2025
),

excel_monthly AS (
    SELECT 
        year,
        month,
        site_id,
        site_name,
        daily_energy_mwh as excel_energy_mwh,
        daily_ghi_kwh_m2 as excel_ghi_kwh_m2
    FROM "MMSR"."public"."site_monthly_performance_excel"
    WHERE year >= 2025
),

site_diffs AS (
    SELECT 
        COALESCE(db.year, excel.year) as year,
        COALESCE(db.month, excel.month) as month,
        COALESCE(db.site_id, excel.site_id) as site_id,
        COALESCE(db.site_name, excel.site_name) as site_name,
        
        COALESCE(db.db_energy_mwh, 0) - COALESCE(excel.excel_energy_mwh, 0) as energy_diff_mwh,
        COALESCE(db.db_ghi_kwh_m2, 0) - COALESCE(excel.excel_ghi_kwh_m2, 0) as ghi_diff_kwh_m2,
        
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

monthly_totals AS (
    SELECT 
        year,
        month,
        COUNT(DISTINCT CASE WHEN status = 'MISSING_IN_EXCEL' THEN site_id END) as sites_missing_in_excel,
        COUNT(DISTINCT CASE WHEN status = 'MISSING_IN_DB' THEN site_id END) as sites_missing_in_db,
        COUNT(DISTINCT CASE WHEN status = 'HAS_DIFFERENCES' THEN site_id END) as sites_with_differences,
        SUM(CASE WHEN status IN ('MISSING_IN_EXCEL', 'HAS_DIFFERENCES') THEN energy_diff_mwh ELSE 0 END) as total_energy_diff_from_missing_and_diffs,
        SUM(CASE WHEN status IN ('MISSING_IN_DB', 'HAS_DIFFERENCES') THEN -energy_diff_mwh ELSE 0 END) as total_energy_diff_from_db_missing,
        SUM(CASE WHEN status IN ('MISSING_IN_EXCEL', 'HAS_DIFFERENCES') THEN ghi_diff_kwh_m2 ELSE 0 END) as total_ghi_diff_from_missing_and_diffs
    FROM site_diffs
    WHERE status != 'MATCH'
    GROUP BY year, month
)

SELECT 
    mt.year,
    mt.month,
    
    -- Site count differences
    mt.sites_missing_in_excel,
    mt.sites_missing_in_db,
    mt.sites_with_differences,
    (mt.sites_missing_in_excel + mt.sites_with_differences) as total_problematic_sites,
    
    -- Total impact
    mt.total_energy_diff_from_missing_and_diffs as total_energy_diff_mwh,
    mt.total_ghi_diff_from_missing_and_diffs as total_ghi_diff_kwh_m2,
    
    -- Top contributing sites (up to 5 per month)
    (
        SELECT STRING_AGG(
            sd.site_name || ' (' || ROUND(sd.energy_diff_mwh::numeric, 2)::text || ' MWh)',
            ', ' 
            ORDER BY ABS(sd.energy_diff_mwh) DESC
        )
        FROM (
            SELECT DISTINCT ON (site_id)
                site_name,
                energy_diff_mwh
            FROM site_diffs
            WHERE site_diffs.year = mt.year
                AND site_diffs.month = mt.month
                AND site_diffs.status != 'MATCH'
            ORDER BY site_id, ABS(energy_diff_mwh) DESC
        ) sd
        ORDER BY ABS(sd.energy_diff_mwh) DESC
        LIMIT 5
    ) as top_contributing_sites

FROM monthly_totals mt
ORDER BY mt.year DESC, mt.month DESC

