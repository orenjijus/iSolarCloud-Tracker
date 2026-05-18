-- ============================================
-- Investigate POA Calculation Differences
-- ============================================
-- Focus on 5 sites with <20% POA match:
-- 1. Shoetown Ligung Indonesia: 3.05%
-- 2. PLTS Frina Lestari Nusantara: 12.10%
-- 3. PLTS Rooftop Sumatera Prima Fibreboard: 10.64%
-- 4. Charoen Pokphand Bandung: 13.79%
-- 5. Garuda Metalindo (IKP): 15.88%
-- ============================================

-- ============================================
-- 1. POA Differences Summary per Site
-- ============================================
WITH db_data AS (
    SELECT 
        d.date_key,
        d.site_name,
        d.daily_poa_weighted_kwh_m2
    FROM "MMSR"."mart"."mart_site_performance_daily" d
    WHERE d.site_name IN (
        'Shoetown Ligung Indonesia',
        'PLTS Frina Lestari Nusantara',
        'PLTS Rooftop Sumatera Prima Fibreboard',
        'Charoen Pokphand Bandung',
        'Garuda Metalindo (IKP)'
    )
),
excel_data AS (
    SELECT 
        TO_DATE(date_key, 'MM/DD/YYYY') as date_key,
        site_name,
        poa_actual_kwh_m2::NUMERIC as daily_poa_weighted_kwh_m2
    FROM public.site_daily_performance_excel_except_mmki
    WHERE site_name IN (
        'Shoetown Ligung Indonesia',
        'PLTS Frina Lestari Nusantara',
        'PLTS Rooftop Sumatera Prima Fibreboard',
        'Charoen Pokphand Bandung',
        'Garuda Metalindo (IKP)'
    )
),
comparison AS (
    SELECT 
        COALESCE(db.site_name, excel.site_name) as site_name,
        COALESCE(db.date_key, excel.date_key) as date_key,
        db.daily_poa_weighted_kwh_m2 as db_poa,
        excel.daily_poa_weighted_kwh_m2 as excel_poa,
        (COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) as poa_diff,
        ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) as poa_diff_abs,
        CASE 
            WHEN excel.daily_poa_weighted_kwh_m2 > 0 
            THEN ROUND(100.0 * (COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) / excel.daily_poa_weighted_kwh_m2, 2)
            ELSE NULL
        END as poa_diff_pct
    FROM db_data db
    FULL OUTER JOIN excel_data excel
        ON db.date_key = excel.date_key 
        AND UPPER(TRIM(db.site_name)) = UPPER(TRIM(excel.site_name))
)
SELECT 
    site_name,
    COUNT(*) as total_days,
    SUM(CASE WHEN ABS(poa_diff) <= 0.01 THEN 1 ELSE 0 END) as match_count,
    SUM(CASE WHEN ABS(poa_diff) > 0.01 THEN 1 ELSE 0 END) as mismatch_count,
    ROUND(100.0 * SUM(CASE WHEN ABS(poa_diff) <= 0.01 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) as match_pct,
    ROUND(AVG(CASE WHEN ABS(poa_diff) > 0.01 THEN poa_diff_abs ELSE NULL END), 4) as avg_diff,
    ROUND(MAX(CASE WHEN ABS(poa_diff) > 0.01 THEN poa_diff_abs ELSE NULL END), 4) as max_diff,
    ROUND(AVG(CASE WHEN ABS(poa_diff) > 0.01 THEN poa_diff ELSE NULL END), 4) as avg_diff_signed,
    ROUND(AVG(CASE WHEN ABS(poa_diff) > 0.01 AND excel_poa > 0 THEN poa_diff_pct ELSE NULL END), 2) as avg_diff_pct,
    CASE 
        WHEN AVG(CASE WHEN ABS(poa_diff) > 0.01 THEN poa_diff ELSE NULL END) > 0.1 THEN 'DB_CONSISTENTLY_HIGHER'
        WHEN AVG(CASE WHEN ABS(poa_diff) > 0.01 THEN poa_diff ELSE NULL END) < -0.1 THEN 'EXCEL_CONSISTENTLY_HIGHER'
        ELSE 'VARIABLE'
    END as pattern
FROM comparison
GROUP BY site_name
ORDER BY match_pct ASC;

-- ============================================
-- 2. POA Differences Detail - Top Mismatches
-- ============================================
WITH db_data AS (
    SELECT 
        d.date_key,
        d.site_name,
        d.daily_poa_weighted_kwh_m2
    FROM "MMSR"."mart"."mart_site_performance_daily" d
    WHERE d.site_name IN (
        'Shoetown Ligung Indonesia',
        'PLTS Frina Lestari Nusantara',
        'PLTS Rooftop Sumatera Prima Fibreboard',
        'Charoen Pokphand Bandung',
        'Garuda Metalindo (IKP)'
    )
),
excel_data AS (
    SELECT 
        TO_DATE(date_key, 'MM/DD/YYYY') as date_key,
        site_name,
        poa_actual_kwh_m2::NUMERIC as daily_poa_weighted_kwh_m2
    FROM public.site_daily_performance_excel_except_mmki
    WHERE site_name IN (
        'Shoetown Ligung Indonesia',
        'PLTS Frina Lestari Nusantara',
        'PLTS Rooftop Sumatera Prima Fibreboard',
        'Charoen Pokphand Bandung',
        'Garuda Metalindo (IKP)'
    )
)
SELECT 
    COALESCE(db.site_name, excel.site_name) as site_name,
    COALESCE(db.date_key, excel.date_key) as date_key,
    ROUND(db.daily_poa_weighted_kwh_m2::numeric, 4) as db_poa,
    ROUND(excel.daily_poa_weighted_kwh_m2::numeric, 4) as excel_poa,
    ROUND((COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0))::numeric, 4) as poa_diff,
    CASE 
        WHEN excel.daily_poa_weighted_kwh_m2 > 0 
        THEN ROUND(100.0 * (COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) / excel.daily_poa_weighted_kwh_m2, 2)
        ELSE NULL
    END as poa_diff_pct,
    CASE 
        WHEN db.daily_poa_weighted_kwh_m2 IS NULL THEN 'MISSING_DB'
        WHEN excel.daily_poa_weighted_kwh_m2 IS NULL THEN 'MISSING_EXCEL'
        WHEN ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) <= 0.01 THEN 'MATCH'
        ELSE 'MISMATCH'
    END as status
FROM db_data db
FULL OUTER JOIN excel_data excel
    ON db.date_key = excel.date_key 
    AND UPPER(TRIM(db.site_name)) = UPPER(TRIM(excel.site_name))
WHERE ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) > 0.01
ORDER BY 
    site_name,
    ABS(COALESCE(db.daily_poa_weighted_kwh_m2, 0) - COALESCE(excel.daily_poa_weighted_kwh_m2, 0)) DESC,
    date_key DESC
LIMIT 100;

-- ============================================
-- 3. POA Ratio Analysis (to detect multiplier issues)
-- ============================================
WITH db_data AS (
    SELECT 
        d.date_key,
        d.site_name,
        d.daily_poa_weighted_kwh_m2
    FROM "MMSR"."mart"."mart_site_performance_daily" d
    WHERE d.site_name IN (
        'Shoetown Ligung Indonesia',
        'PLTS Frina Lestari Nusantara',
        'PLTS Rooftop Sumatera Prima Fibreboard',
        'Charoen Pokphand Bandung',
        'Garuda Metalindo (IKP)'
    )
        AND d.daily_poa_weighted_kwh_m2 IS NOT NULL
        AND d.daily_poa_weighted_kwh_m2 > 0
),
excel_data AS (
    SELECT 
        TO_DATE(date_key, 'MM/DD/YYYY') as date_key,
        site_name,
        poa_actual_kwh_m2::NUMERIC as daily_poa_weighted_kwh_m2
    FROM public.site_daily_performance_excel_except_mmki
    WHERE site_name IN (
        'Shoetown Ligung Indonesia',
        'PLTS Frina Lestari Nusantara',
        'PLTS Rooftop Sumatera Prima Fibreboard',
        'Charoen Pokphand Bandung',
        'Garuda Metalindo (IKP)'
    )
        AND poa_actual_kwh_m2 IS NOT NULL
        AND poa_actual_kwh_m2 > 0
)
SELECT 
    COALESCE(db.site_name, excel.site_name) as site_name,
    COUNT(*) as total_days,
    ROUND(AVG(db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)), 4) as avg_poa_ratio,
    ROUND(MIN(db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)), 4) as min_poa_ratio,
    ROUND(MAX(db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)), 4) as max_poa_ratio,
    ROUND(STDDEV(db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)), 4) as stddev_poa_ratio,
    CASE 
        WHEN AVG(db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)) BETWEEN 0.99 AND 1.01 THEN 'RATIO_NEAR_1'
        WHEN AVG(db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)) BETWEEN 0.9 AND 1.1 THEN 'RATIO_CLOSE'
        WHEN STDDEV(db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)) < 0.1 THEN 'CONSISTENT_RATIO'
        ELSE 'VARIABLE_RATIO'
    END as ratio_pattern
FROM db_data db
INNER JOIN excel_data excel
    ON db.date_key = excel.date_key 
    AND UPPER(TRIM(db.site_name)) = UPPER(TRIM(excel.site_name))
GROUP BY COALESCE(db.site_name, excel.site_name)
ORDER BY avg_poa_ratio;

