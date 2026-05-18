-- Sample PR values comparison to understand the format issue
-- Shows actual Excel values and how they're being converted

WITH excel_samples AS (
    SELECT 
        CASE 
            WHEN date_key LIKE '%/%' THEN TO_DATE(date_key, 'MM/DD/YYYY')
            WHEN date_key LIKE '%-%' THEN TO_DATE(date_key, 'YYYY-MM-DD')
            ELSE date_key::date
        END as date_key,
        site_name,
        site_id,
        pr_ghi_actual as excel_pr_ghi_raw,
        -- Check if it has % sign
        CASE WHEN pr_ghi_actual LIKE '%' THEN 'HAS_%' ELSE 'NO_%' END as excel_format,
        -- Current conversion (divides by 100 if has %)
        CASE 
            WHEN pr_ghi_actual IS NULL OR TRIM(pr_ghi_actual) = '' THEN NULL
            WHEN pr_ghi_actual LIKE '%' THEN 
                NULLIF(REPLACE(pr_ghi_actual, '%', ''), '')::numeric / 100.0
            ELSE 
                NULLIF(TRIM(pr_ghi_actual), '')::numeric
        END as excel_pr_ghi_current_logic,
        -- Alternative: Always treat as percentage (0-100) - remove % but don't divide
        CASE 
            WHEN pr_ghi_actual IS NULL OR TRIM(pr_ghi_actual) = '' THEN NULL
            WHEN pr_ghi_actual LIKE '%' THEN 
                NULLIF(REPLACE(pr_ghi_actual, '%', ''), '')::numeric
            ELSE 
                NULLIF(TRIM(pr_ghi_actual), '')::numeric
        END as excel_pr_ghi_as_percent
    FROM public.site_daily_performance_excel_all
    WHERE date_key IS NOT NULL
        AND site_id IS NOT NULL
        AND pr_ghi_actual IS NOT NULL
        AND TRIM(pr_ghi_actual) != ''
        -- Focus on MMKI 1 for now
        AND site_name LIKE '%MMKI%1.75%'
    LIMIT 50
),

db_data AS (
    SELECT 
        date_key,
        site_name,
        site_id,
        pr_ghi_actual as db_pr_ghi,
        daily_energy_mwh,
        daily_ghi_kwh_m2,
        actual_capacity_kw
    FROM mart.mart_site_performance_daily
    WHERE site_name LIKE '%MMKI%1.75%'
        AND pr_ghi_actual IS NOT NULL
)

SELECT 
    e.date_key,
    e.site_name,
    e.excel_pr_ghi_raw,
    e.excel_format,
    e.excel_pr_ghi_current_logic,
    e.excel_pr_ghi_as_percent,
    d.db_pr_ghi,
    d.daily_energy_mwh,
    d.daily_ghi_kwh_m2,
    d.actual_capacity_kw,
    -- Calculate PR manually to verify
    CASE 
        WHEN d.daily_ghi_kwh_m2 IS NOT NULL 
            AND d.daily_ghi_kwh_m2 >= 0.1
            AND d.daily_energy_mwh IS NOT NULL
            AND d.daily_energy_mwh >= 0.01
            AND d.actual_capacity_kw IS NOT NULL 
            AND d.actual_capacity_kw > 0
        THEN ((d.daily_energy_mwh * 1000.0) / d.daily_ghi_kwh_m2 / d.actual_capacity_kw) * 100.0
        ELSE NULL
    END as calculated_pr_ghi,
    -- Differences
    ROUND(ABS(e.excel_pr_ghi_current_logic - d.db_pr_ghi), 4) as diff_current,
    ROUND(ABS(e.excel_pr_ghi_as_percent - d.db_pr_ghi), 4) as diff_as_percent,
    -- Match flags
    CASE WHEN ABS(e.excel_pr_ghi_current_logic - d.db_pr_ghi) <= 0.001 THEN '✓' ELSE '✗' END as match_current,
    CASE WHEN ABS(e.excel_pr_ghi_as_percent - d.db_pr_ghi) <= 0.001 THEN '✓' ELSE '✗' END as match_as_percent
FROM excel_samples e
INNER JOIN db_data d
    ON e.date_key = d.date_key
    AND e.site_id = d.site_id
ORDER BY e.date_key DESC
LIMIT 30

