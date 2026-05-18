-- Comparison Analysis: Excel Data vs Current Database Values
-- Compares public.site_daily_performance_excel_all with mart.mart_site_performance_daily
-- For all sites

WITH excel_data AS (
    SELECT 
        -- Convert date string to date format
        CASE 
            WHEN date_key LIKE '%/%' THEN 
                TO_DATE(date_key, 'MM/DD/YYYY')
            WHEN date_key LIKE '%-%' THEN 
                TO_DATE(date_key, 'YYYY-MM-DD')
            ELSE 
                date_key::date
        END as date_key,
        site_name,
        site_id,
        NULLIF(TRIM(energy_actual_mwh::text), '')::numeric as excel_energy_mwh,
        NULLIF(TRIM(ghi_actual_kwh_m2::text), '')::numeric as excel_ghi_kwh_m2,
        NULLIF(TRIM(poa_actual_kwh_m2::text), '')::numeric as excel_poa_kwh_m2,
        -- PR is already in decimal (0-1) format in Excel, no conversion needed
        -- DB now also uses decimal (0-1) format to match Excel
        pr_ghi_actual as excel_pr_ghi,
        pr_poa_actual as excel_pr_poa,
        -- Convert availability from percentage string to decimal
        CASE 
            WHEN availability_percent IS NULL OR TRIM(availability_percent) = '' THEN NULL
            WHEN availability_percent LIKE '%' THEN 
                NULLIF(REPLACE(availability_percent, '%', ''), '')::numeric / 100.0
            ELSE 
                NULLIF(TRIM(availability_percent), '')::numeric / 100.0
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
        -- Availability is now in decimal (0-1) format, no conversion needed
        availability_percent as db_availability
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
        
        -- Differences
        e.excel_energy_mwh - d.db_energy_mwh as energy_diff_mwh,
        e.excel_ghi_kwh_m2 - d.db_ghi_kwh_m2 as ghi_diff_kwh_m2,
        e.excel_poa_kwh_m2 - d.db_poa_kwh_m2 as poa_diff_kwh_m2,
        e.excel_pr_ghi - d.db_pr_ghi as pr_ghi_diff,
        e.excel_pr_poa - d.db_pr_poa as pr_poa_diff,
        e.excel_availability - d.db_availability as availability_diff,
        
        -- Match flags (with tolerance)
        CASE 
            WHEN e.excel_energy_mwh IS NULL OR d.db_energy_mwh IS NULL THEN NULL
            WHEN ABS(e.excel_energy_mwh - d.db_energy_mwh) <= 0.01 THEN TRUE
            ELSE FALSE
        END as energy_match,
        CASE 
            WHEN e.excel_ghi_kwh_m2 IS NULL OR d.db_ghi_kwh_m2 IS NULL THEN NULL
            -- Relative tolerance: difference < 1% of the larger value
            WHEN ABS(e.excel_ghi_kwh_m2 - d.db_ghi_kwh_m2) / NULLIF(GREATEST(ABS(e.excel_ghi_kwh_m2), ABS(d.db_ghi_kwh_m2)), 0) <= 0.01 THEN TRUE
            ELSE FALSE
        END as ghi_match,
        CASE 
            WHEN e.excel_poa_kwh_m2 IS NULL OR d.db_poa_kwh_m2 IS NULL THEN NULL
            -- Relative tolerance: difference < 1% of the larger value
            WHEN ABS(e.excel_poa_kwh_m2 - d.db_poa_kwh_m2) / NULLIF(GREATEST(ABS(e.excel_poa_kwh_m2), ABS(d.db_poa_kwh_m2)), 0) <= 0.01 THEN TRUE
            ELSE FALSE
        END as poa_match,
        CASE 
            WHEN e.excel_pr_ghi IS NULL OR d.db_pr_ghi IS NULL THEN NULL
            -- Tolerance: 0.005 (0.5% in decimal format, e.g., 0.005 = 0.5% difference)
            WHEN ABS(e.excel_pr_ghi - d.db_pr_ghi) <= 0.005 THEN TRUE
            ELSE FALSE
        END as pr_ghi_match,
        CASE 
            WHEN e.excel_pr_poa IS NULL OR d.db_pr_poa IS NULL THEN NULL
            -- Tolerance: 0.005 (0.5% in decimal format, e.g., 0.005 = 0.5% difference)
            WHEN ABS(e.excel_pr_poa - d.db_pr_poa) <= 0.005 THEN TRUE
            ELSE FALSE
        END as pr_poa_match,
        CASE 
            WHEN e.excel_availability IS NULL OR d.db_availability IS NULL THEN NULL
            -- Tolerance: 0.01 (1% in decimal format, e.g., 0.01 = 1% difference)
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

-- Summary Statistics by Site
SELECT 
    site_name,
    site_id,
    COUNT(*) as total_records,
    
    -- Energy comparison
    COUNT(CASE WHEN energy_match = TRUE THEN 1 END) as energy_match_count,
    COUNT(CASE WHEN energy_match = FALSE AND NOT missing_in_excel AND NOT missing_in_db THEN 1 END) as energy_mismatch_count,
    COUNT(CASE WHEN missing_in_excel THEN 1 END) as energy_missing_in_excel,
    COUNT(CASE WHEN missing_in_db THEN 1 END) as energy_missing_in_db,
    ROUND(AVG(ABS(energy_diff_mwh))::numeric, 4) as avg_energy_diff_mwh,
    ROUND(MAX(ABS(energy_diff_mwh))::numeric, 4) as max_energy_diff_mwh,
    ROUND(
        COUNT(CASE WHEN energy_match = TRUE THEN 1 END)::numeric / 
        NULLIF(COUNT(CASE WHEN NOT missing_in_excel AND NOT missing_in_db THEN 1 END), 0) * 100, 
        2
    ) as energy_match_pct,
    
    -- GHI comparison
    COUNT(CASE WHEN ghi_match = TRUE THEN 1 END) as ghi_match_count,
    COUNT(CASE WHEN ghi_match = FALSE AND NOT missing_in_excel AND NOT missing_in_db THEN 1 END) as ghi_mismatch_count,
    COUNT(CASE WHEN missing_in_excel OR excel_ghi_kwh_m2 IS NULL THEN 1 END) as ghi_missing_in_excel,
    COUNT(CASE WHEN missing_in_db OR db_ghi_kwh_m2 IS NULL THEN 1 END) as ghi_missing_in_db,
    ROUND(AVG(ABS(ghi_diff_kwh_m2))::numeric, 4) as avg_ghi_diff_kwh_m2,
    ROUND(MAX(ABS(ghi_diff_kwh_m2))::numeric, 4) as max_ghi_diff_kwh_m2,
    ROUND(
        COUNT(CASE WHEN ghi_match = TRUE THEN 1 END)::numeric / 
        NULLIF(COUNT(CASE WHEN NOT missing_in_excel AND NOT missing_in_db AND excel_ghi_kwh_m2 IS NOT NULL AND db_ghi_kwh_m2 IS NOT NULL THEN 1 END), 0) * 100, 
        2
    ) as ghi_match_pct,
    
    -- POA comparison
    COUNT(CASE WHEN poa_match = TRUE THEN 1 END) as poa_match_count,
    COUNT(CASE WHEN poa_match = FALSE AND NOT missing_in_excel AND NOT missing_in_db THEN 1 END) as poa_mismatch_count,
    COUNT(CASE WHEN missing_in_excel OR excel_poa_kwh_m2 IS NULL THEN 1 END) as poa_missing_in_excel,
    COUNT(CASE WHEN missing_in_db OR db_poa_kwh_m2 IS NULL THEN 1 END) as poa_missing_in_db,
    ROUND(AVG(ABS(poa_diff_kwh_m2))::numeric, 4) as avg_poa_diff_kwh_m2,
    ROUND(MAX(ABS(poa_diff_kwh_m2))::numeric, 4) as max_poa_diff_kwh_m2,
    ROUND(
        COUNT(CASE WHEN poa_match = TRUE THEN 1 END)::numeric / 
        NULLIF(COUNT(CASE WHEN NOT missing_in_excel AND NOT missing_in_db AND excel_poa_kwh_m2 IS NOT NULL AND db_poa_kwh_m2 IS NOT NULL THEN 1 END), 0) * 100, 
        2
    ) as poa_match_pct,
    
    -- PR GHI comparison
    COUNT(CASE WHEN pr_ghi_match = TRUE THEN 1 END) as pr_ghi_match_count,
    COUNT(CASE WHEN pr_ghi_match = FALSE AND NOT missing_in_excel AND NOT missing_in_db THEN 1 END) as pr_ghi_mismatch_count,
    COUNT(CASE WHEN missing_in_excel OR excel_pr_ghi IS NULL THEN 1 END) as pr_ghi_missing_in_excel,
    COUNT(CASE WHEN missing_in_db OR db_pr_ghi IS NULL THEN 1 END) as pr_ghi_missing_in_db,
    ROUND(AVG(ABS(pr_ghi_diff))::numeric, 4) as avg_pr_ghi_diff,
    ROUND(MAX(ABS(pr_ghi_diff))::numeric, 4) as max_pr_ghi_diff,
    ROUND(
        COUNT(CASE WHEN pr_ghi_match = TRUE THEN 1 END)::numeric / 
        NULLIF(COUNT(CASE WHEN NOT missing_in_excel AND NOT missing_in_db AND excel_pr_ghi IS NOT NULL AND db_pr_ghi IS NOT NULL THEN 1 END), 0) * 100, 
        2
    ) as pr_ghi_match_pct,
    
    -- PR POA comparison
    COUNT(CASE WHEN pr_poa_match = TRUE THEN 1 END) as pr_poa_match_count,
    COUNT(CASE WHEN pr_poa_match = FALSE AND NOT missing_in_excel AND NOT missing_in_db THEN 1 END) as pr_poa_mismatch_count,
    COUNT(CASE WHEN missing_in_excel OR excel_pr_poa IS NULL THEN 1 END) as pr_poa_missing_in_excel,
    COUNT(CASE WHEN missing_in_db OR db_pr_poa IS NULL THEN 1 END) as pr_poa_missing_in_db,
    ROUND(AVG(ABS(pr_poa_diff))::numeric, 4) as avg_pr_poa_diff,
    ROUND(MAX(ABS(pr_poa_diff))::numeric, 4) as max_pr_poa_diff,
    ROUND(
        COUNT(CASE WHEN pr_poa_match = TRUE THEN 1 END)::numeric / 
        NULLIF(COUNT(CASE WHEN NOT missing_in_excel AND NOT missing_in_db AND excel_pr_poa IS NOT NULL AND db_pr_poa IS NOT NULL THEN 1 END), 0) * 100, 
        2
    ) as pr_poa_match_pct,
    
    -- Availability comparison
    COUNT(CASE WHEN availability_match = TRUE THEN 1 END) as availability_match_count,
    COUNT(CASE WHEN availability_match = FALSE AND NOT missing_in_excel AND NOT missing_in_db THEN 1 END) as availability_mismatch_count,
    COUNT(CASE WHEN missing_in_excel OR excel_availability IS NULL THEN 1 END) as availability_missing_in_excel,
    COUNT(CASE WHEN missing_in_db OR db_availability IS NULL THEN 1 END) as availability_missing_in_db,
    ROUND(AVG(ABS(availability_diff))::numeric, 4) as avg_availability_diff,
    ROUND(MAX(ABS(availability_diff))::numeric, 4) as max_availability_diff,
    ROUND(
        COUNT(CASE WHEN availability_match = TRUE THEN 1 END)::numeric / 
        NULLIF(COUNT(CASE WHEN NOT missing_in_excel AND NOT missing_in_db AND excel_availability IS NOT NULL AND db_availability IS NOT NULL THEN 1 END), 0) * 100, 
        2
    ) as availability_match_pct,
    
    -- Overall match (all metrics match)
    COUNT(CASE 
        WHEN energy_match = TRUE
            AND ghi_match = TRUE
            AND poa_match = TRUE
            AND pr_ghi_match = TRUE
            AND pr_poa_match = TRUE
            AND availability_match = TRUE
            AND NOT missing_in_excel 
            AND NOT missing_in_db
        THEN 1 
    END) as overall_match_count,
    ROUND(
        COUNT(CASE 
            WHEN energy_match = TRUE
                AND ghi_match = TRUE
                AND poa_match = TRUE
                AND pr_ghi_match = TRUE
                AND pr_poa_match = TRUE
                AND availability_match = TRUE
                AND NOT missing_in_excel 
                AND NOT missing_in_db
            THEN 1 
        END)::numeric / 
        NULLIF(COUNT(CASE WHEN NOT missing_in_excel AND NOT missing_in_db THEN 1 END), 0) * 100, 
        2
    ) as overall_match_pct

FROM comparison
GROUP BY site_name, site_id
ORDER BY site_name

