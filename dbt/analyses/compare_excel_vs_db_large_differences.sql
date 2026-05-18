-- List of dates with large differences per site
-- Helps identify anomalies that need immediate checking
-- Shows records where difference exceeds threshold for each metric

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
        availability_percent / 100.0 as db_availability
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
        
        -- Relative differences (for GHI and POA)
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
        END as poa_diff_pct
        
    FROM excel_data e
    FULL OUTER JOIN db_data d
        ON e.date_key = d.date_key
        AND e.site_id = d.site_id
    WHERE e.date_key IS NOT NULL 
        AND d.date_key IS NOT NULL
        AND NOT (e.date_key IS NULL AND d.date_key IS NULL)
)

-- Energy Large Differences (> 0.1 MWh or > 5% relative)
SELECT 
    'ENERGY' as metric_type,
    site_name,
    site_id,
    date_key,
    ROUND(excel_energy_mwh::numeric, 4) as excel_value,
    ROUND(db_energy_mwh::numeric, 4) as db_value,
    ROUND(ABS(energy_diff_mwh)::numeric, 4) as absolute_diff,
    ROUND(ABS(energy_diff_mwh) / NULLIF(GREATEST(ABS(excel_energy_mwh), ABS(db_energy_mwh)), 0) * 100, 2) as relative_diff_pct,
    CASE 
        WHEN energy_diff_mwh > 0 THEN 'Excel > DB'
        WHEN energy_diff_mwh < 0 THEN 'Excel < DB'
        ELSE 'Equal'
    END as direction
FROM comparison
WHERE excel_energy_mwh IS NOT NULL 
    AND db_energy_mwh IS NOT NULL
    AND (
        ABS(energy_diff_mwh) > 0.1  -- Absolute threshold: > 0.1 MWh
        OR ABS(energy_diff_mwh) / NULLIF(GREATEST(ABS(excel_energy_mwh), ABS(db_energy_mwh)), 0) > 0.05  -- Relative threshold: > 5%
    )

UNION ALL

-- GHI Large Differences (> 1% relative or > 0.5 kWh/m² absolute)
SELECT 
    'GHI' as metric_type,
    site_name,
    site_id,
    date_key,
    ROUND(excel_ghi_kwh_m2::numeric, 4) as excel_value,
    ROUND(db_ghi_kwh_m2::numeric, 4) as db_value,
    ROUND(ABS(ghi_diff_kwh_m2)::numeric, 4) as absolute_diff,
    ROUND(ghi_diff_pct::numeric, 2) as relative_diff_pct,
    CASE 
        WHEN ghi_diff_kwh_m2 > 0 THEN 'Excel > DB'
        WHEN ghi_diff_kwh_m2 < 0 THEN 'Excel < DB'
        ELSE 'Equal'
    END as direction
FROM comparison
WHERE excel_ghi_kwh_m2 IS NOT NULL 
    AND db_ghi_kwh_m2 IS NOT NULL
    AND (
        ghi_diff_pct > 1.0  -- Relative threshold: > 1%
        OR ABS(ghi_diff_kwh_m2) > 0.5  -- Absolute threshold: > 0.5 kWh/m²
    )

UNION ALL

-- POA Large Differences (> 1% relative or > 0.5 kWh/m² absolute)
SELECT 
    'POA' as metric_type,
    site_name,
    site_id,
    date_key,
    ROUND(excel_poa_kwh_m2::numeric, 4) as excel_value,
    ROUND(db_poa_kwh_m2::numeric, 4) as db_value,
    ROUND(ABS(poa_diff_kwh_m2)::numeric, 4) as absolute_diff,
    ROUND(poa_diff_pct::numeric, 2) as relative_diff_pct,
    CASE 
        WHEN poa_diff_kwh_m2 > 0 THEN 'Excel > DB'
        WHEN poa_diff_kwh_m2 < 0 THEN 'Excel < DB'
        ELSE 'Equal'
    END as direction
FROM comparison
WHERE excel_poa_kwh_m2 IS NOT NULL 
    AND db_poa_kwh_m2 IS NOT NULL
    AND (
        poa_diff_pct > 1.0  -- Relative threshold: > 1%
        OR ABS(poa_diff_kwh_m2) > 0.5  -- Absolute threshold: > 0.5 kWh/m²
    )

UNION ALL

-- PR GHI Large Differences (> 1.0 absolute or > 5% relative)
-- Updated: Threshold changed from 0.01 (1%) to 1.0 (100%) to match updated match tolerance of 0.5%
SELECT 
    'PR_GHI' as metric_type,
    site_name,
    site_id,
    date_key,
    ROUND(excel_pr_ghi::numeric, 4) as excel_value,
    ROUND(db_pr_ghi::numeric, 4) as db_value,
    ROUND(ABS(pr_ghi_diff)::numeric, 4) as absolute_diff,
    ROUND(ABS(pr_ghi_diff) / NULLIF(GREATEST(ABS(excel_pr_ghi), ABS(db_pr_ghi)), 0) * 100, 2) as relative_diff_pct,
    CASE 
        WHEN pr_ghi_diff > 0 THEN 'Excel > DB'
        WHEN pr_ghi_diff < 0 THEN 'Excel < DB'
        ELSE 'Equal'
    END as direction
FROM comparison
WHERE excel_pr_ghi IS NOT NULL 
    AND db_pr_ghi IS NOT NULL
    AND (
        ABS(pr_ghi_diff) > 1.0  -- Absolute threshold: > 1.0 (100%) - Updated from 0.01
        OR ABS(pr_ghi_diff) / NULLIF(GREATEST(ABS(excel_pr_ghi), ABS(db_pr_ghi)), 0) > 0.05  -- Relative threshold: > 5%
    )

UNION ALL

-- PR POA Large Differences (> 1.0 absolute or > 5% relative)
-- Updated: Threshold changed from 0.01 (1%) to 1.0 (100%) to match updated match tolerance of 0.5%
SELECT 
    'PR_POA' as metric_type,
    site_name,
    site_id,
    date_key,
    ROUND(excel_pr_poa::numeric, 4) as excel_value,
    ROUND(db_pr_poa::numeric, 4) as db_value,
    ROUND(ABS(pr_poa_diff)::numeric, 4) as absolute_diff,
    ROUND(ABS(pr_poa_diff) / NULLIF(GREATEST(ABS(excel_pr_poa), ABS(db_pr_poa)), 0) * 100, 2) as relative_diff_pct,
    CASE 
        WHEN pr_poa_diff > 0 THEN 'Excel > DB'
        WHEN pr_poa_diff < 0 THEN 'Excel < DB'
        ELSE 'Equal'
    END as direction
FROM comparison
WHERE excel_pr_poa IS NOT NULL 
    AND db_pr_poa IS NOT NULL
    AND (
        ABS(pr_poa_diff) > 1.0  -- Absolute threshold: > 1.0 (100%) - Updated from 0.01
        OR ABS(pr_poa_diff) / NULLIF(GREATEST(ABS(excel_pr_poa), ABS(db_pr_poa)), 0) > 0.05  -- Relative threshold: > 5%
    )

UNION ALL

-- Availability Large Differences (> 0.05 absolute or > 5% relative)
SELECT 
    'AVAILABILITY' as metric_type,
    site_name,
    site_id,
    date_key,
    ROUND(excel_availability::numeric, 4) as excel_value,
    ROUND(db_availability::numeric, 4) as db_value,
    ROUND(ABS(availability_diff)::numeric, 4) as absolute_diff,
    ROUND(ABS(availability_diff) / NULLIF(GREATEST(ABS(excel_availability), ABS(db_availability)), 0) * 100, 2) as relative_diff_pct,
    CASE 
        WHEN availability_diff > 0 THEN 'Excel > DB'
        WHEN availability_diff < 0 THEN 'Excel < DB'
        ELSE 'Equal'
    END as direction
FROM comparison
WHERE excel_availability IS NOT NULL 
    AND db_availability IS NOT NULL
    AND (
        ABS(availability_diff) > 0.05  -- Absolute threshold: > 0.05 (5%)
        OR ABS(availability_diff) / NULLIF(GREATEST(ABS(excel_availability), ABS(db_availability)), 0) > 0.05  -- Relative threshold: > 5%
    )

ORDER BY 
    site_name,
    metric_type,
    absolute_diff DESC,
    date_key

