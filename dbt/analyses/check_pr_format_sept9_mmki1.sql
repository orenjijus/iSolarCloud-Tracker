-- Check PR format for September 9, MMKI 1
-- To understand why PR matches in this case

WITH excel_raw AS (
    SELECT 
        date_key as excel_date_raw,
        CASE 
            WHEN date_key LIKE '%/%' THEN TO_DATE(date_key, 'MM/DD/YYYY')
            WHEN date_key LIKE '%-%' THEN TO_DATE(date_key, 'YYYY-MM-DD')
            ELSE date_key::date
        END as date_key,
        site_name,
        site_id,
        pr_ghi_actual as excel_pr_ghi_raw,
        pr_poa_actual as excel_poa_raw,
        -- Check if it has % sign
        CASE WHEN pr_ghi_actual LIKE '%' THEN 'HAS_%' ELSE 'NO_%' END as excel_pr_ghi_format,
        -- Convert PR from percentage string to decimal (current logic)
        CASE 
            WHEN pr_ghi_actual IS NULL OR TRIM(pr_ghi_actual) = '' THEN NULL
            WHEN pr_ghi_actual LIKE '%' THEN 
                NULLIF(REPLACE(pr_ghi_actual, '%', ''), '')::numeric / 100.0
            ELSE 
                NULLIF(TRIM(pr_ghi_actual), '')::numeric
        END as excel_pr_ghi_converted,
        -- Also try keeping as percentage if no %
        CASE 
            WHEN pr_ghi_actual IS NULL OR TRIM(pr_ghi_actual) = '' THEN NULL
            WHEN pr_ghi_actual LIKE '%' THEN 
                NULLIF(REPLACE(pr_ghi_actual, '%', ''), '')::numeric
            ELSE 
                NULLIF(TRIM(pr_ghi_actual), '')::numeric
        END as excel_pr_ghi_as_percentage
    FROM public.site_daily_performance_excel_all
    WHERE site_id = (SELECT site_id FROM mart.mart_site_performance_daily WHERE site_name LIKE '%MMKI%1.75%' LIMIT 1)
        AND (
            date_key LIKE '%9/9/%' 
            OR date_key LIKE '%09/09/%'
            OR date_key LIKE '2024-09-09'
            OR date_key LIKE '2025-09-09'
        )
),

db_data AS (
    SELECT 
        date_key,
        site_name,
        site_id,
        pr_ghi_actual as db_pr_ghi,
        pr_poa_actual as db_pr_poa,
        daily_energy_mwh,
        daily_ghi_kwh_m2,
        actual_capacity_kw
    FROM mart.mart_site_performance_daily
    WHERE site_name LIKE '%MMKI%1.75%'
        AND date_key = '2024-09-09'
)

SELECT 
    e.excel_date_raw,
    e.date_key,
    e.site_name,
    e.excel_pr_ghi_raw,
    e.excel_pr_ghi_format,
    e.excel_pr_ghi_converted as excel_pr_ghi_as_decimal,
    e.excel_pr_ghi_as_percentage as excel_pr_ghi_as_percent,
    d.db_pr_ghi,
    d.daily_energy_mwh,
    d.daily_ghi_kwh_m2,
    d.actual_capacity_kw,
    -- Calculate PR manually to verify formula
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
    ABS(e.excel_pr_ghi_converted - d.db_pr_ghi) as diff_decimal_vs_db,
    ABS(e.excel_pr_ghi_as_percentage - d.db_pr_ghi) as diff_percent_vs_db,
    -- Match flags
    CASE WHEN ABS(e.excel_pr_ghi_converted - d.db_pr_ghi) <= 0.001 THEN 'MATCH (decimal)' ELSE 'NO MATCH (decimal)' END as match_decimal,
    CASE WHEN ABS(e.excel_pr_ghi_as_percentage - d.db_pr_ghi) <= 0.001 THEN 'MATCH (percent)' ELSE 'NO MATCH (percent)' END as match_percent
FROM excel_raw e
FULL OUTER JOIN db_data d ON e.date_key = d.date_key
ORDER BY e.date_key

