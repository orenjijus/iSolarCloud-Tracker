-- List of dates with large differences per site and metric
-- Simplified format for easy reference and investigation
-- Shows date ranges and key dates for each site

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
        e.excel_energy_mwh - d.db_energy_mwh as energy_diff,
        e.excel_ghi_kwh_m2 - d.db_ghi_kwh_m2 as ghi_diff,
        e.excel_poa_kwh_m2 - d.db_poa_kwh_m2 as poa_diff,
        e.excel_pr_ghi - d.db_pr_ghi as pr_ghi_diff,
        e.excel_pr_poa - d.db_pr_poa as pr_poa_diff,
        e.excel_availability - d.db_availability as availability_diff,
        CASE 
            WHEN GREATEST(ABS(e.excel_ghi_kwh_m2), ABS(d.db_ghi_kwh_m2)) > 0
            THEN ABS(e.excel_ghi_kwh_m2 - d.db_ghi_kwh_m2) / GREATEST(ABS(e.excel_ghi_kwh_m2), ABS(d.db_ghi_kwh_m2)) * 100
            ELSE NULL
        END as ghi_diff_pct,
        CASE 
            WHEN GREATEST(ABS(e.excel_poa_kwh_m2), ABS(d.db_poa_kwh_m2)) > 0
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
),
large_diffs AS (
    SELECT 
        'ENERGY' as metric_type,
        site_name,
        date_key,
        ABS(energy_diff) as diff_value
    FROM comparison
    WHERE excel_energy_mwh IS NOT NULL AND db_energy_mwh IS NOT NULL
        AND (ABS(energy_diff) > 0.1 OR ABS(energy_diff) / NULLIF(GREATEST(ABS(excel_energy_mwh), ABS(db_energy_mwh)), 0) > 0.05)
    
    UNION ALL
    
    SELECT 
        'GHI' as metric_type,
        site_name,
        date_key,
        ABS(ghi_diff) as diff_value
    FROM comparison
    WHERE excel_ghi_kwh_m2 IS NOT NULL AND db_ghi_kwh_m2 IS NOT NULL
        AND (ghi_diff_pct > 1.0 OR ABS(ghi_diff) > 0.5)
    
    UNION ALL
    
    SELECT 
        'POA' as metric_type,
        site_name,
        date_key,
        ABS(poa_diff) as diff_value
    FROM comparison
    WHERE excel_poa_kwh_m2 IS NOT NULL AND db_poa_kwh_m2 IS NOT NULL
        AND (poa_diff_pct > 1.0 OR ABS(poa_diff) > 0.5)
    
    UNION ALL
    
    SELECT 
        'PR_GHI' as metric_type,
        site_name,
        date_key,
        ABS(pr_ghi_diff) as diff_value
    FROM comparison
    WHERE excel_pr_ghi IS NOT NULL AND db_pr_ghi IS NOT NULL
        AND (ABS(pr_ghi_diff) > 1.0 OR ABS(pr_ghi_diff) / NULLIF(GREATEST(ABS(excel_pr_ghi), ABS(db_pr_ghi)), 0) > 0.05)  -- Updated: threshold 0.01 -> 1.0
    
    UNION ALL
    
    SELECT 
        'PR_POA' as metric_type,
        site_name,
        date_key,
        ABS(pr_poa_diff) as diff_value
    FROM comparison
    WHERE excel_pr_poa IS NOT NULL AND db_pr_poa IS NOT NULL
        AND (ABS(pr_poa_diff) > 1.0 OR ABS(pr_poa_diff) / NULLIF(GREATEST(ABS(excel_pr_poa), ABS(db_pr_poa)), 0) > 0.05)  -- Updated: threshold 0.01 -> 1.0
    
    UNION ALL
    
    SELECT 
        'AVAILABILITY' as metric_type,
        site_name,
        date_key,
        ABS(availability_diff) as diff_value
    FROM comparison
    WHERE excel_availability IS NOT NULL AND db_availability IS NOT NULL
        AND (ABS(availability_diff) > 0.05 OR ABS(availability_diff) / NULLIF(GREATEST(ABS(excel_availability), ABS(db_availability)), 0) > 0.05)
)

-- Summary: Dates per site and metric
SELECT 
    site_name,
    metric_type,
    COUNT(*) as anomaly_count,
    MIN(date_key) as first_anomaly_date,
    MAX(date_key) as last_anomaly_date,
    ROUND(MAX(diff_value)::numeric, 4) as max_difference,
    -- List first 20 dates (to avoid too long output)
    STRING_AGG(
        date_key::text || ' (' || ROUND(diff_value::numeric, 2)::text || ')', 
        ', ' 
        ORDER BY diff_value DESC, date_key
    ) FILTER (WHERE ROW_NUMBER() OVER (PARTITION BY site_name, metric_type ORDER BY diff_value DESC, date_key) <= 20) as top_20_dates
FROM (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY site_name, metric_type ORDER BY diff_value DESC, date_key) as rn
    FROM large_diffs
) ranked
WHERE rn <= 20 OR diff_value = (SELECT MAX(diff_value) FROM large_diffs ld2 WHERE ld2.site_name = ranked.site_name AND ld2.metric_type = ranked.metric_type)
GROUP BY site_name, metric_type
ORDER BY site_name, metric_type, anomaly_count DESC

