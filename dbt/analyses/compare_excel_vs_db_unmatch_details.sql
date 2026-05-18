-- Detailed Unmatch Records: Excel vs Database
-- Shows all records with mismatches for manual crosscheck
-- Includes all values (Excel vs DB) and differences

WITH excel_data AS (
    SELECT 
        CASE 
            WHEN date_key LIKE '%/%' THEN TO_DATE(date_key, 'MM/DD/YYYY')
            WHEN date_key LIKE '%-%' THEN TO_DATE(date_key, 'YYYY-MM-DD')
            ELSE date_key::date
        END as date_key,
        site_name,
        site_id,
        NULLIF(TRIM(energy_actual_mwh::text), '')::numeric as excel_energy_mwh,
        NULLIF(TRIM(ghi_actual_kwh_m2::text), '')::numeric as excel_ghi_kwh_m2,
        NULLIF(TRIM(poa_actual_kwh_m2::text), '')::numeric as excel_poa_kwh_m2,
        -- FIXED: Convert PR from decimal (0-1) to percentage (0-100)
        CASE 
            WHEN pr_ghi_actual IS NULL THEN NULL
            ELSE pr_ghi_actual * 100.0
        END as excel_pr_ghi,
        CASE 
            WHEN pr_poa_actual IS NULL THEN NULL
            ELSE pr_poa_actual * 100.0
        END as excel_pr_poa,
        CASE 
            WHEN availability_percent IS NULL OR TRIM(availability_percent::text) = '' THEN NULL
            WHEN availability_percent::text LIKE '%' THEN 
                NULLIF(REPLACE(availability_percent::text, '%', ''), '')::numeric / 100.0
            ELSE 
                NULLIF(TRIM(availability_percent::text), '')::numeric / 100.0
        END as excel_availability
    FROM public.site_daily_performance_excel_all
    WHERE date_key IS NOT NULL
        AND site_id IS NOT NULL
),

db_data AS (
    SELECT 
        date_key,
        site_name,
        site_id,
        daily_energy_mwh as db_energy_mwh,
        daily_ghi_kwh_m2 as db_ghi_kwh_m2,
        daily_poa_weighted_kwh_m2 as db_poa_kwh_m2,
        pr_ghi_actual as db_pr_ghi,
        pr_poa_actual as db_pr_poa,
        availability_percent / 100.0 as db_availability,
        actual_capacity_kw
    FROM mart.mart_site_performance_daily
),

comparison AS (
    SELECT 
        COALESCE(e.date_key, d.date_key) as date_key,
        COALESCE(e.site_name, d.site_name) as site_name,
        COALESCE(e.site_id, d.site_id) as site_id,
        
        -- Excel values
        e.excel_energy_mwh,
        e.excel_ghi_kwh_m2,
        e.excel_poa_kwh_m2,
        e.excel_pr_ghi,
        e.excel_pr_poa,
        e.excel_availability,
        
        -- Database values
        d.db_energy_mwh,
        d.db_ghi_kwh_m2,
        d.db_poa_kwh_m2,
        d.db_pr_ghi,
        d.db_pr_poa,
        d.db_availability,
        d.actual_capacity_kw,
        
        -- Differences
        e.excel_energy_mwh - d.db_energy_mwh as energy_diff_mwh,
        e.excel_ghi_kwh_m2 - d.db_ghi_kwh_m2 as ghi_diff_kwh_m2,
        e.excel_poa_kwh_m2 - d.db_poa_kwh_m2 as poa_diff_kwh_m2,
        e.excel_pr_ghi - d.db_pr_ghi as pr_ghi_diff,
        e.excel_pr_poa - d.db_pr_poa as pr_poa_diff,
        e.excel_availability - d.db_availability as availability_diff,
        
        -- Match flags (with updated tolerance)
        CASE 
            WHEN e.excel_energy_mwh IS NULL OR d.db_energy_mwh IS NULL THEN NULL
            WHEN ABS(e.excel_energy_mwh - d.db_energy_mwh) <= 0.01 THEN TRUE
            ELSE FALSE
        END as energy_match,
        CASE 
            WHEN e.excel_ghi_kwh_m2 IS NULL OR d.db_ghi_kwh_m2 IS NULL THEN NULL
            WHEN ABS(e.excel_ghi_kwh_m2 - d.db_ghi_kwh_m2) / NULLIF(GREATEST(ABS(e.excel_ghi_kwh_m2), ABS(d.db_ghi_kwh_m2)), 0) <= 0.01 THEN TRUE
            ELSE FALSE
        END as ghi_match,
        CASE 
            WHEN e.excel_poa_kwh_m2 IS NULL OR d.db_poa_kwh_m2 IS NULL THEN NULL
            WHEN ABS(e.excel_poa_kwh_m2 - d.db_poa_kwh_m2) / NULLIF(GREATEST(ABS(e.excel_poa_kwh_m2), ABS(d.db_poa_kwh_m2)), 0) <= 0.01 THEN TRUE
            ELSE FALSE
        END as poa_match,
        CASE 
            WHEN e.excel_pr_ghi IS NULL OR d.db_pr_ghi IS NULL THEN NULL
            WHEN ABS(e.excel_pr_ghi - d.db_pr_ghi) <= 0.5 THEN TRUE  -- Updated: 0.5% tolerance
            ELSE FALSE
        END as pr_ghi_match,
        CASE 
            WHEN e.excel_pr_poa IS NULL OR d.db_pr_poa IS NULL THEN NULL
            WHEN ABS(e.excel_pr_poa - d.db_pr_poa) <= 0.5 THEN TRUE  -- Updated: 0.5% tolerance
            ELSE FALSE
        END as pr_poa_match,
        CASE 
            WHEN e.excel_availability IS NULL OR d.db_availability IS NULL THEN NULL
            WHEN ABS(e.excel_availability - d.db_availability) <= 0.01 THEN TRUE
            ELSE FALSE
        END as availability_match,
        
        -- Missing flags
        CASE WHEN e.date_key IS NULL THEN TRUE ELSE FALSE END as missing_in_excel,
        CASE WHEN d.date_key IS NULL THEN TRUE ELSE FALSE END as missing_in_db
        
    FROM excel_data e
    FULL OUTER JOIN db_data d
        ON e.date_key = d.date_key
        AND e.site_id = d.site_id
)

-- Show all unmatch records with detailed values
SELECT 
    date_key,
    site_name,
    site_id,
    actual_capacity_kw,
    
    -- Energy
    ROUND(excel_energy_mwh::numeric, 4) as excel_energy_mwh,
    ROUND(db_energy_mwh::numeric, 4) as db_energy_mwh,
    ROUND(energy_diff_mwh::numeric, 4) as energy_diff_mwh,
    CASE WHEN energy_match THEN '✓' ELSE '✗' END as energy_match,
    
    -- GHI
    ROUND(excel_ghi_kwh_m2::numeric, 4) as excel_ghi_kwh_m2,
    ROUND(db_ghi_kwh_m2::numeric, 4) as db_ghi_kwh_m2,
    ROUND(ghi_diff_kwh_m2::numeric, 4) as ghi_diff_kwh_m2,
    CASE WHEN ghi_match THEN '✓' ELSE '✗' END as ghi_match,
    
    -- POA
    ROUND(excel_poa_kwh_m2::numeric, 4) as excel_poa_kwh_m2,
    ROUND(db_poa_kwh_m2::numeric, 4) as db_poa_kwh_m2,
    ROUND(poa_diff_kwh_m2::numeric, 4) as poa_diff_kwh_m2,
    CASE WHEN poa_match THEN '✓' ELSE '✗' END as poa_match,
    
    -- PR GHI
    ROUND(excel_pr_ghi::numeric, 2) as excel_pr_ghi,
    ROUND(db_pr_ghi::numeric, 2) as db_pr_ghi,
    ROUND(pr_ghi_diff::numeric, 2) as pr_ghi_diff,
    CASE WHEN pr_ghi_match THEN '✓' ELSE '✗' END as pr_ghi_match,
    
    -- PR POA
    ROUND(excel_pr_poa::numeric, 2) as excel_pr_poa,
    ROUND(db_pr_poa::numeric, 2) as db_pr_poa,
    ROUND(pr_poa_diff::numeric, 2) as pr_poa_diff,
    CASE WHEN pr_poa_match THEN '✓' ELSE '✗' END as pr_poa_match,
    
    -- Availability
    ROUND(excel_availability::numeric, 4) as excel_availability,
    ROUND(db_availability::numeric, 4) as db_availability,
    ROUND(availability_diff::numeric, 4) as availability_diff,
    CASE WHEN availability_match THEN '✓' ELSE '✗' END as availability_match,
    
    -- Missing flags
    CASE WHEN missing_in_excel THEN 'Missing in Excel' ELSE '' END as missing_in_excel,
    CASE WHEN missing_in_db THEN 'Missing in DB' ELSE '' END as missing_in_db,
    
    -- Unmatch summary
    CASE 
        WHEN NOT energy_match THEN 'Energy'
        WHEN NOT ghi_match THEN 'GHI'
        WHEN NOT poa_match THEN 'POA'
        WHEN NOT pr_ghi_match THEN 'PR_GHI'
        WHEN NOT pr_poa_match THEN 'PR_POA'
        WHEN NOT availability_match THEN 'Availability'
        WHEN missing_in_excel THEN 'Missing_Excel'
        WHEN missing_in_db THEN 'Missing_DB'
        ELSE 'OK'
    END as unmatch_type

FROM comparison
WHERE 
    -- Show records with any mismatch or missing data
    (energy_match = FALSE OR energy_match IS NULL)
    OR (ghi_match = FALSE OR ghi_match IS NULL)
    OR (poa_match = FALSE OR poa_match IS NULL)
    OR (pr_ghi_match = FALSE OR pr_ghi_match IS NULL)
    OR (pr_poa_match = FALSE OR pr_poa_match IS NULL)
    OR (availability_match = FALSE OR availability_match IS NULL)
    OR missing_in_excel 
    OR missing_in_db
ORDER BY 
    site_name, 
    date_key DESC,
    ABS(COALESCE(pr_ghi_diff, 0)) DESC,
    ABS(COALESCE(pr_poa_diff, 0)) DESC

