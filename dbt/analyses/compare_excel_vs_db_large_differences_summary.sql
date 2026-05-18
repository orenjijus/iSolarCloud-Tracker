-- Summary: Count of large differences per site and metric type
-- Quick overview to identify which sites have the most anomalies

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
        -- Convert PR from decimal (0-1) to percentage (0-100) to match DB format
        -- Excel stores PR as decimal (0-1), DB stores as percentage (0-100)
        CASE 
            WHEN pr_ghi_actual IS NULL THEN NULL
            ELSE pr_ghi_actual * 100.0
        END as excel_pr_ghi,
        CASE 
            WHEN pr_poa_actual IS NULL THEN NULL
            ELSE pr_poa_actual * 100.0
        END as excel_pr_poa,
        CASE 
            WHEN availability_percent IS NULL OR TRIM(availability_percent) = '' THEN NULL
            WHEN availability_percent LIKE '%' THEN NULLIF(REPLACE(availability_percent, '%', ''), '')::numeric / 100.0
            ELSE NULLIF(TRIM(availability_percent), '')::numeric / 100.0
        END as excel_availability
    FROM public.site_daily_performance_excel_all
    WHERE date_key IS NOT NULL AND site_id IS NOT NULL
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
        availability_percent / 100.0 as db_availability
    FROM mart.mart_site_performance_daily
),

comparison AS (
    SELECT 
        COALESCE(e.date_key, d.date_key) as date_key,
        COALESCE(e.site_name, d.site_name) as site_name,
        COALESCE(e.site_id, d.site_id) as site_id,
        e.excel_energy_mwh - d.db_energy_mwh as energy_diff_mwh,
        e.excel_ghi_kwh_m2 - d.db_ghi_kwh_m2 as ghi_diff_kwh_m2,
        e.excel_poa_kwh_m2 - d.db_poa_kwh_m2 as poa_diff_kwh_m2,
        e.excel_pr_ghi - d.db_pr_ghi as pr_ghi_diff,
        e.excel_pr_poa - d.db_pr_poa as pr_poa_diff,
        e.excel_availability - d.db_availability as availability_diff,
        CASE 
            WHEN e.excel_ghi_kwh_m2 IS NOT NULL AND d.db_ghi_kwh_m2 IS NOT NULL 
                AND GREATEST(ABS(e.excel_ghi_kwh_m2), ABS(d.db_ghi_kwh_m2)) > 0
            THEN ABS(e.excel_ghi_kwh_m2 - d.db_ghi_kwh_m2) / GREATEST(ABS(e.excel_ghi_kwh_m2), ABS(d.db_ghi_kwh_m2)) * 100
            ELSE NULL
        END as ghi_diff_pct,
        CASE 
            WHEN e.excel_poa_kwh_m2 IS NOT NULL AND d.db_poa_kwh_m2 IS NOT NULL 
                AND GREATEST(ABS(e.excel_poa_kwh_m2), ABS(d.db_poa_kwh_m2)) > 0
            THEN ABS(e.excel_poa_kwh_m2 - d.db_poa_kwh_m2) / GREATEST(ABS(e.excel_poa_kwh_m2), ABS(d.db_poa_kwh_m2)) * 100
            ELSE NULL
        END as poa_diff_pct,
        e.excel_energy_mwh,
        d.db_energy_mwh,
        e.excel_ghi_kwh_m2,
        d.db_ghi_kwh_m2,
        e.excel_poa_kwh_m2,
        d.db_poa_kwh_m2,
        e.excel_pr_ghi,
        d.db_pr_ghi,
        e.excel_pr_poa,
        d.db_pr_poa,
        e.excel_availability,
        d.db_availability
    FROM excel_data e
    INNER JOIN db_data d ON e.date_key = d.date_key AND e.site_id = d.site_id
)

SELECT 
    site_name,
    site_id,
    COUNT(*) as total_comparable_records,
    
    -- Energy large differences
    COUNT(CASE 
        WHEN excel_energy_mwh IS NOT NULL AND db_energy_mwh IS NOT NULL
            AND (ABS(energy_diff_mwh) > 0.1 
                OR ABS(energy_diff_mwh) / NULLIF(GREATEST(ABS(excel_energy_mwh), ABS(db_energy_mwh)), 0) > 0.05)
        THEN 1 
    END) as energy_large_diff_count,
    ROUND(MAX(CASE WHEN ABS(energy_diff_mwh) > 0.1 THEN ABS(energy_diff_mwh) END)::numeric, 4) as energy_max_diff_mwh,
    
    -- GHI large differences
    COUNT(CASE 
        WHEN excel_ghi_kwh_m2 IS NOT NULL AND db_ghi_kwh_m2 IS NOT NULL
            AND (ghi_diff_pct > 1.0 OR ABS(ghi_diff_kwh_m2) > 0.5)
        THEN 1 
    END) as ghi_large_diff_count,
    ROUND(MAX(CASE WHEN ghi_diff_pct > 1.0 THEN ghi_diff_pct END)::numeric, 2) as ghi_max_diff_pct,
    
    -- POA large differences
    COUNT(CASE 
        WHEN excel_poa_kwh_m2 IS NOT NULL AND db_poa_kwh_m2 IS NOT NULL
            AND (poa_diff_pct > 1.0 OR ABS(poa_diff_kwh_m2) > 0.5)
        THEN 1 
    END) as poa_large_diff_count,
    ROUND(MAX(CASE WHEN poa_diff_pct > 1.0 THEN poa_diff_pct END)::numeric, 2) as poa_max_diff_pct,
    
    -- PR GHI large differences (> 1.0 absolute or > 5% relative)
    -- Updated: Threshold changed from 0.01 to 1.0 to match updated match tolerance
    COUNT(CASE 
        WHEN excel_pr_ghi IS NOT NULL AND db_pr_ghi IS NOT NULL
            AND (ABS(pr_ghi_diff) > 1.0 
                OR ABS(pr_ghi_diff) / NULLIF(GREATEST(ABS(excel_pr_ghi), ABS(db_pr_ghi)), 0) > 0.05)
        THEN 1 
    END) as pr_ghi_large_diff_count,
    ROUND(MAX(CASE WHEN ABS(pr_ghi_diff) > 1.0 THEN ABS(pr_ghi_diff) END)::numeric, 4) as pr_ghi_max_diff,
    
    -- PR POA large differences (> 1.0 absolute or > 5% relative)
    -- Updated: Threshold changed from 0.01 to 1.0 to match updated match tolerance
    COUNT(CASE 
        WHEN excel_pr_poa IS NOT NULL AND db_pr_poa IS NOT NULL
            AND (ABS(pr_poa_diff) > 1.0 
                OR ABS(pr_poa_diff) / NULLIF(GREATEST(ABS(excel_pr_poa), ABS(db_pr_poa)), 0) > 0.05)
        THEN 1 
    END) as pr_poa_large_diff_count,
    ROUND(MAX(CASE WHEN ABS(pr_poa_diff) > 1.0 THEN ABS(pr_poa_diff) END)::numeric, 4) as pr_poa_max_diff,
    
    -- Availability large differences
    COUNT(CASE 
        WHEN excel_availability IS NOT NULL AND db_availability IS NOT NULL
            AND (ABS(availability_diff) > 0.05 
                OR ABS(availability_diff) / NULLIF(GREATEST(ABS(excel_availability), ABS(db_availability)), 0) > 0.05)
        THEN 1 
    END) as availability_large_diff_count,
    ROUND(MAX(CASE WHEN ABS(availability_diff) > 0.05 THEN ABS(availability_diff) END)::numeric, 4) as availability_max_diff

FROM comparison
GROUP BY site_name, site_id
ORDER BY 
    (energy_large_diff_count + ghi_large_diff_count + poa_large_diff_count + 
     pr_ghi_large_diff_count + pr_poa_large_diff_count + availability_large_diff_count) DESC,
    site_name

