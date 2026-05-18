-- Analyze PR format issue systematically
-- Check if Excel PR values with % vs without % are causing the mismatch

WITH excel_data AS (
    SELECT 
        CASE 
            WHEN date_key LIKE '%/%' THEN TO_DATE(date_key, 'MM/DD/YYYY')
            WHEN date_key LIKE '%-%' THEN TO_DATE(date_key, 'YYYY-MM-DD')
            ELSE date_key::date
        END as date_key,
        site_name,
        site_id,
        pr_ghi_actual as excel_pr_ghi_raw,
        -- Check format
        CASE WHEN pr_ghi_actual LIKE '%' THEN 'HAS_%' ELSE 'NO_%' END as excel_format,
        -- Current conversion (divides by 100 if has %)
        CASE 
            WHEN pr_ghi_actual IS NULL OR TRIM(pr_ghi_actual) = '' THEN NULL
            WHEN pr_ghi_actual LIKE '%' THEN 
                NULLIF(REPLACE(pr_ghi_actual, '%', ''), '')::numeric / 100.0
            ELSE 
                NULLIF(TRIM(pr_ghi_actual), '')::numeric
        END as excel_pr_ghi_current,
        -- Alternative: Always treat as percentage (0-100)
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
),

db_data AS (
    SELECT 
        date_key,
        site_name,
        site_id,
        pr_ghi_actual as db_pr_ghi
    FROM mart.mart_site_performance_daily
    WHERE pr_ghi_actual IS NOT NULL
),

comparison AS (
    SELECT 
        e.date_key,
        e.site_name,
        e.site_id,
        e.excel_pr_ghi_raw,
        e.excel_format,
        e.excel_pr_ghi_current,
        e.excel_pr_ghi_as_percent,
        d.db_pr_ghi,
        -- Differences with current logic
        ABS(e.excel_pr_ghi_current - d.db_pr_ghi) as diff_current,
        -- Differences if treated as percentage
        ABS(e.excel_pr_ghi_as_percent - d.db_pr_ghi) as diff_as_percent,
        -- Match flags
        CASE WHEN ABS(e.excel_pr_ghi_current - d.db_pr_ghi) <= 0.001 THEN TRUE ELSE FALSE END as match_current,
        CASE WHEN ABS(e.excel_pr_ghi_as_percent - d.db_pr_ghi) <= 0.001 THEN TRUE ELSE FALSE END as match_as_percent
    FROM excel_data e
    INNER JOIN db_data d
        ON e.date_key = d.date_key
        AND e.site_id = d.site_id
)

-- Summary by format
SELECT 
    excel_format,
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE match_current) as match_with_current_logic,
    COUNT(*) FILTER (WHERE match_as_percent) as match_if_treated_as_percent,
    ROUND(100.0 * COUNT(*) FILTER (WHERE match_current) / COUNT(*), 2) as match_rate_current,
    ROUND(100.0 * COUNT(*) FILTER (WHERE match_as_percent) / COUNT(*), 2) as match_rate_as_percent,
    ROUND(AVG(diff_current), 4) as avg_diff_current,
    ROUND(AVG(diff_as_percent), 4) as avg_diff_as_percent,
    ROUND(MAX(diff_current), 4) as max_diff_current,
    ROUND(MAX(diff_as_percent), 4) as max_diff_as_percent
FROM comparison
GROUP BY excel_format

UNION ALL

-- Overall summary
SELECT 
    'TOTAL' as excel_format,
    COUNT(*) as total_records,
    COUNT(*) FILTER (WHERE match_current) as match_with_current_logic,
    COUNT(*) FILTER (WHERE match_as_percent) as match_if_treated_as_percent,
    ROUND(100.0 * COUNT(*) FILTER (WHERE match_current) / COUNT(*), 2) as match_rate_current,
    ROUND(100.0 * COUNT(*) FILTER (WHERE match_as_percent) / COUNT(*), 2) as match_rate_as_percent,
    ROUND(AVG(diff_current), 4) as avg_diff_current,
    ROUND(AVG(diff_as_percent), 4) as avg_diff_as_percent,
    ROUND(MAX(diff_current), 4) as max_diff_current,
    ROUND(MAX(diff_as_percent), 4) as max_diff_as_percent
FROM comparison

ORDER BY excel_format

