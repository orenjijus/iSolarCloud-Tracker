-- ============================================
-- Compare Fact Tables with Excel Data
-- ============================================
-- Compare availability_percent dari fact tables dengan Excel
-- ============================================

-- 1. Compare MMKI Sites
-- Convert Excel date format (M/D/YYYY) to DATE and compare
WITH excel_mmki AS (
    SELECT 
        CASE 
            WHEN date_key LIKE '%/%/%' THEN 
                TO_DATE(date_key, 'FMMM/FMDD/FMYYYY')
            ELSE NULL
        END as date_key_converted,
        site_name,
        -- Convert availability_percent from string (e.g., '100%') to numeric
        CASE 
            WHEN availability_percent LIKE '%' THEN 
                CAST(REPLACE(availability_percent, '%', '') AS DECIMAL)
            ELSE NULL
        END as excel_availability_percent
    FROM public.site_daily_performance_excel_mmki
    WHERE date_key LIKE '%/%/%'
),
fact_mmki AS (
    SELECT 
        date_key,
        site_name,
        ROUND(
            CASE 
                WHEN SUM(power_available_ratio) + SUM(unavailability_ratio) > 0
                THEN (SUM(power_available_ratio) / NULLIF(
                    SUM(power_available_ratio) + SUM(unavailability_ratio),
                    0
                )) * 100
                ELSE NULL
            END, 2
        ) as db_availability_percent
    FROM mart.fact_site_calculations_5min
    WHERE site_name LIKE '%MMKI%'
    GROUP BY date_key, site_name
)
SELECT 
    COALESCE(e.date_key_converted, f.date_key) as date_key,
    COALESCE(e.site_name, f.site_name) as site_name,
    e.excel_availability_percent,
    f.db_availability_percent,
    ABS(COALESCE(e.excel_availability_percent, 0) - COALESCE(f.db_availability_percent, 0)) as difference,
    CASE 
        WHEN ABS(COALESCE(e.excel_availability_percent, 0) - COALESCE(f.db_availability_percent, 0)) < 0.1 THEN 'MATCH'
        WHEN ABS(COALESCE(e.excel_availability_percent, 0) - COALESCE(f.db_availability_percent, 0)) < 1 THEN 'CLOSE'
        WHEN ABS(COALESCE(e.excel_availability_percent, 0) - COALESCE(f.db_availability_percent, 0)) < 5 THEN 'DIFFERENT'
        ELSE 'VERY DIFFERENT'
    END as match_status
FROM excel_mmki e
FULL OUTER JOIN fact_mmki f
    ON e.date_key_converted = f.date_key
    AND e.site_name = f.site_name
WHERE e.date_key_converted IS NOT NULL OR f.date_key IS NOT NULL
ORDER BY date_key DESC, site_name;

-- 2. Compare Other Sites
WITH excel_other AS (
    SELECT 
        CASE 
            WHEN date_key LIKE '%/%/%' THEN 
                TO_DATE(date_key, 'FMMM/FMDD/FMYYYY')
            ELSE NULL
        END as date_key_converted,
        site_name,
        CASE 
            WHEN availability_percent LIKE '%' THEN 
                CAST(REPLACE(availability_percent, '%', '') AS DECIMAL)
            ELSE NULL
        END as excel_availability_percent
    FROM public.site_daily_performance_excel_except_mmki
    WHERE date_key LIKE '%/%/%'
),
fact_other AS (
    SELECT 
        date_key,
        site_name,
        ROUND(
            CASE 
                WHEN SUM(power_available_ratio) + SUM(unavailability_ratio) > 0
                THEN (SUM(power_available_ratio) / NULLIF(
                    SUM(power_available_ratio) + SUM(unavailability_ratio),
                    0
                )) * 100
                ELSE NULL
            END, 2
        ) as db_availability_percent
    FROM mart.fact_site_calculations_5min
    WHERE site_name NOT LIKE '%MMKI%'
    GROUP BY date_key, site_name
)
SELECT 
    COALESCE(e.date_key_converted, f.date_key) as date_key,
    COALESCE(e.site_name, f.site_name) as site_name,
    e.excel_availability_percent,
    f.db_availability_percent,
    ABS(COALESCE(e.excel_availability_percent, 0) - COALESCE(f.db_availability_percent, 0)) as difference,
    CASE 
        WHEN ABS(COALESCE(e.excel_availability_percent, 0) - COALESCE(f.db_availability_percent, 0)) < 0.1 THEN 'MATCH'
        WHEN ABS(COALESCE(e.excel_availability_percent, 0) - COALESCE(f.db_availability_percent, 0)) < 1 THEN 'CLOSE'
        WHEN ABS(COALESCE(e.excel_availability_percent, 0) - COALESCE(f.db_availability_percent, 0)) < 5 THEN 'DIFFERENT'
        ELSE 'VERY DIFFERENT'
    END as match_status
FROM excel_other e
FULL OUTER JOIN fact_other f
    ON e.date_key_converted = f.date_key
    AND e.site_name = f.site_name
WHERE e.date_key_converted IS NOT NULL OR f.date_key IS NOT NULL
ORDER BY date_key DESC, site_name;

-- 3. Summary Comparison (MMKI Sites)
WITH excel_mmki AS (
    SELECT 
        CASE 
            WHEN date_key LIKE '%/%/%' THEN 
                TO_DATE(date_key, 'FMMM/FMDD/FMYYYY')
            ELSE NULL
        END as date_key_converted,
        site_name,
        CASE 
            WHEN availability_percent LIKE '%' THEN 
                CAST(REPLACE(availability_percent, '%', '') AS DECIMAL)
            ELSE NULL
        END as excel_availability_percent
    FROM public.site_daily_performance_excel_mmki
    WHERE date_key LIKE '%/%/%'
),
fact_mmki AS (
    SELECT 
        date_key,
        site_name,
        ROUND(
            CASE 
                WHEN SUM(power_available_ratio) + SUM(unavailability_ratio) > 0
                THEN (SUM(power_available_ratio) / NULLIF(
                    SUM(power_available_ratio) + SUM(unavailability_ratio),
                    0
                )) * 100
                ELSE NULL
            END, 2
        ) as db_availability_percent
    FROM mart.fact_site_calculations_5min
    WHERE site_name LIKE '%MMKI%'
    GROUP BY date_key, site_name
),
comparison AS (
    SELECT 
        COALESCE(e.date_key_converted, f.date_key) as date_key,
        COALESCE(e.site_name, f.site_name) as site_name,
        e.excel_availability_percent,
        f.db_availability_percent,
        ABS(COALESCE(e.excel_availability_percent, 0) - COALESCE(f.db_availability_percent, 0)) as difference
    FROM excel_mmki e
    FULL OUTER JOIN fact_mmki f
        ON e.date_key_converted = f.date_key
        AND e.site_name = f.site_name
    WHERE e.date_key_converted IS NOT NULL AND f.date_key IS NOT NULL
)
SELECT 
    COUNT(*) as total_comparisons,
    COUNT(CASE WHEN difference < 0.1 THEN 1 END) as exact_matches,
    COUNT(CASE WHEN difference < 1 THEN 1 END) as close_matches,
    COUNT(CASE WHEN difference >= 1 AND difference < 5 THEN 1 END) as different,
    COUNT(CASE WHEN difference >= 5 THEN 1 END) as very_different,
    AVG(difference) as avg_difference,
    MAX(difference) as max_difference,
    MIN(difference) as min_difference
FROM comparison;

-- 4. Detailed comparison for specific dates (recent dates in both)
WITH excel_mmki AS (
    SELECT 
        CASE 
            WHEN date_key LIKE '%/%/%' THEN 
                TO_DATE(date_key, 'FMMM/FMDD/FMYYYY')
            ELSE NULL
        END as date_key_converted,
        site_name,
        CASE 
            WHEN availability_percent LIKE '%' THEN 
                CAST(REPLACE(availability_percent, '%', '') AS DECIMAL)
            ELSE NULL
        END as excel_availability_percent
    FROM public.site_daily_performance_excel_mmki
    WHERE date_key LIKE '%/%/%'
),
fact_mmki AS (
    SELECT 
        date_key,
        site_name,
        ROUND(SUM(power_available_ratio) / 12.0, 4) as power_available_hours,
        ROUND(SUM(unavailability_ratio) / 12.0, 4) as unavailability_hours,
        ROUND((SUM(power_available_ratio) / 12.0) + (SUM(unavailability_ratio) / 12.0), 4) as total_hours,
        ROUND(
            CASE 
                WHEN SUM(power_available_ratio) + SUM(unavailability_ratio) > 0
                THEN (SUM(power_available_ratio) / NULLIF(
                    SUM(power_available_ratio) + SUM(unavailability_ratio),
                    0
                )) * 100
                ELSE NULL
            END, 2
        ) as db_availability_percent
    FROM mart.fact_site_calculations_5min
    WHERE site_name LIKE '%MMKI%'
    GROUP BY date_key, site_name
)
SELECT 
    e.date_key_converted as date_key,
    e.site_name,
    e.excel_availability_percent,
    f.db_availability_percent,
    f.power_available_hours,
    f.unavailability_hours,
    f.total_hours,
    ABS(COALESCE(e.excel_availability_percent, 0) - COALESCE(f.db_availability_percent, 0)) as difference,
    CASE 
        WHEN ABS(COALESCE(e.excel_availability_percent, 0) - COALESCE(f.db_availability_percent, 0)) < 0.1 THEN '✅ MATCH'
        WHEN ABS(COALESCE(e.excel_availability_percent, 0) - COALESCE(f.db_availability_percent, 0)) < 1 THEN '⚠️ CLOSE'
        WHEN ABS(COALESCE(e.excel_availability_percent, 0) - COALESCE(f.db_availability_percent, 0)) < 5 THEN '❌ DIFFERENT'
        ELSE '🚨 VERY DIFFERENT'
    END as match_status
FROM excel_mmki e
INNER JOIN fact_mmki f
    ON e.date_key_converted = f.date_key
    AND e.site_name = f.site_name
WHERE e.date_key_converted >= '2025-09-01'  -- Recent dates
ORDER BY e.date_key_converted DESC, e.site_name;

