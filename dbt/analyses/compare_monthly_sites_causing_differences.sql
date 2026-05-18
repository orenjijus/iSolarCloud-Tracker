-- Identify sites causing differences in monthly aggregation
-- This query shows which sites contribute to the differences between DB and Excel totals
-- 
-- Usage: Run this to see per-site differences that cause aggregate discrepancies

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
        ghi_target as db_ghi_target
    FROM {{ ref('mart_site_performance_monthly') }}
    WHERE year >= 2025  -- Focus on 2025 data
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
        ghi_target as excel_ghi_target
    FROM "MMSR"."public"."site_monthly_performance_excel"
    WHERE year >= 2025
),

site_comparison AS (
    SELECT 
        COALESCE(db.year, excel.year) as year,
        COALESCE(db.month, excel.month) as month,
        COALESCE(db.site_id, excel.site_id) as site_id,
        COALESCE(db.site_name, excel.site_name) as site_name,
        
        -- Energy comparison
        db.db_energy_mwh,
        excel.excel_energy_mwh,
        COALESCE(db.db_energy_mwh, 0) - COALESCE(excel.excel_energy_mwh, 0) as energy_diff_mwh,
        CASE 
            WHEN excel.excel_energy_mwh IS NOT NULL AND excel.excel_energy_mwh != 0
            THEN ((COALESCE(db.db_energy_mwh, 0) - excel.excel_energy_mwh) / excel.excel_energy_mwh * 100)
            WHEN db.db_energy_mwh IS NOT NULL AND db.db_energy_mwh != 0
            THEN 100.0  -- 100% difference if missing in Excel
            ELSE NULL
        END as energy_diff_pct,
        
        -- GHI comparison
        db.db_ghi_kwh_m2,
        excel.excel_ghi_kwh_m2,
        COALESCE(db.db_ghi_kwh_m2, 0) - COALESCE(excel.excel_ghi_kwh_m2, 0) as ghi_diff_kwh_m2,
        CASE 
            WHEN excel.excel_ghi_kwh_m2 IS NOT NULL AND excel.excel_ghi_kwh_m2 != 0
            THEN ((COALESCE(db.db_ghi_kwh_m2, 0) - excel.excel_ghi_kwh_m2) / excel.excel_ghi_kwh_m2 * 100)
            WHEN db.db_ghi_kwh_m2 IS NOT NULL AND db.db_ghi_kwh_m2 != 0
            THEN 100.0  -- 100% difference if missing in Excel
            ELSE NULL
        END as ghi_diff_pct,
        
        -- PR comparison
        db.db_pr_ghi,
        excel.excel_pr_ghi,
        COALESCE(db.db_pr_ghi, 0) - COALESCE(excel.excel_pr_ghi, 0) as pr_diff,
        
        -- Status flags
        CASE 
            WHEN db.site_id IS NULL THEN 'MISSING_IN_DB'
            WHEN excel.site_id IS NULL THEN 'MISSING_IN_EXCEL'
            WHEN ABS(COALESCE(db.db_energy_mwh, 0) - COALESCE(excel.excel_energy_mwh, 0)) > 0.01 
                OR (excel.excel_energy_mwh IS NOT NULL AND excel.excel_energy_mwh != 0 
                    AND ABS((COALESCE(db.db_energy_mwh, 0) - excel.excel_energy_mwh) / excel.excel_energy_mwh) > 0.001)
            THEN 'HAS_DIFFERENCES'
            ELSE 'MATCH'
        END as status,
        
        -- Impact score: absolute difference in energy (for sorting by impact)
        ABS(COALESCE(db.db_energy_mwh, 0) - COALESCE(excel.excel_energy_mwh, 0)) as energy_impact_mwh
        
    FROM db_monthly db
    FULL OUTER JOIN excel_monthly excel
        ON db.year = excel.year
        AND db.month = excel.month
        AND db.site_id = excel.site_id
)

SELECT 
    year,
    month,
    site_id,
    site_name,
    status,
    
    -- Energy details
    db_energy_mwh,
    excel_energy_mwh,
    energy_diff_mwh,
    energy_diff_pct,
    
    -- GHI details
    db_ghi_kwh_m2,
    excel_ghi_kwh_m2,
    ghi_diff_kwh_m2,
    ghi_diff_pct,
    
    -- PR details
    db_pr_ghi,
    excel_pr_ghi,
    pr_diff,
    
    -- Impact ranking
    energy_impact_mwh,
    
    -- Contribution to total difference (for sites with differences)
    CASE 
        WHEN status IN ('HAS_DIFFERENCES', 'MISSING_IN_EXCEL', 'MISSING_IN_DB')
        THEN energy_diff_mwh
        ELSE 0
    END as contribution_to_total_diff_mwh

FROM site_comparison
WHERE status != 'MATCH'  -- Only show sites with differences
ORDER BY 
    year DESC,
    month DESC,
    energy_impact_mwh DESC,  -- Sort by impact (largest differences first)
    site_id

