-- ============================================
-- POA Systematic Check - Detailed Analysis
-- ============================================
-- Check if POA differences are systematic
-- Exclude January for Shoetown (anomaly period)
-- ============================================

-- ============================================
-- 1. POA Ratio Analysis (to detect multiplier/calculation differences)
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
        AND (d.site_name != 'Shoetown Ligung Indonesia' OR d.date_key >= '2025-02-01')
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
        AND (site_name != 'Shoetown Ligung Indonesia' OR TO_DATE(date_key, 'MM/DD/YYYY') >= '2025-02-01')
        AND poa_actual_kwh_m2 IS NOT NULL
        AND poa_actual_kwh_m2 > 0
)
SELECT 
    COALESCE(db.site_name, excel.site_name) as site_name,
    COUNT(*) as total_days,
    ROUND(AVG(db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)), 4) as avg_ratio,
    ROUND(MIN(db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)), 4) as min_ratio,
    ROUND(MAX(db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)), 4) as max_ratio,
    ROUND(STDDEV(db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)), 4) as stddev_ratio,
    ROUND(AVG(db.daily_poa_weighted_kwh_m2 - excel.daily_poa_weighted_kwh_m2), 4) as avg_diff_absolute,
    ROUND(AVG((db.daily_poa_weighted_kwh_m2 - excel.daily_poa_weighted_kwh_m2) / NULLIF(excel.daily_poa_weighted_kwh_m2, 0) * 100), 2) as avg_diff_percentage,
    CASE 
        WHEN AVG(db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)) BETWEEN 0.99 AND 1.01 THEN 'RATIO_NEAR_1'
        WHEN AVG(db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)) BETWEEN 0.95 AND 1.05 THEN 'RATIO_CLOSE'
        WHEN STDDEV(db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)) < 0.02 THEN 'HIGHLY_SYSTEMATIC'
        WHEN STDDEV(db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)) < 0.05 THEN 'SYSTEMATIC'
        WHEN STDDEV(db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)) < 0.1 THEN 'MOSTLY_SYSTEMATIC'
        ELSE 'NOT_SYSTEMATIC'
    END as systematic_level
FROM db_data db
INNER JOIN excel_data excel
    ON db.date_key = excel.date_key 
    AND UPPER(TRIM(db.site_name)) = UPPER(TRIM(excel.site_name))
GROUP BY COALESCE(db.site_name, excel.site_name)
ORDER BY 
    CASE 
        WHEN STDDEV(db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)) < 0.02 THEN 1
        WHEN STDDEV(db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)) < 0.05 THEN 2
        WHEN STDDEV(db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0)) < 0.1 THEN 3
        ELSE 4
    END,
    site_name;

-- ============================================
-- 2. POA Differences by Date Range (to detect date-based patterns)
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
        AND (d.site_name != 'Shoetown Ligung Indonesia' OR d.date_key >= '2025-02-01')
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
        AND (site_name != 'Shoetown Ligung Indonesia' OR TO_DATE(date_key, 'MM/DD/YYYY') >= '2025-02-01')
)
SELECT 
    COALESCE(db.site_name, excel.site_name) as site_name,
    DATE_TRUNC('month', COALESCE(db.date_key, excel.date_key)) as month,
    COUNT(*) as days_in_month,
    ROUND(AVG((db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0))), 4) as avg_ratio,
    ROUND(STDDEV((db.daily_poa_weighted_kwh_m2 / NULLIF(excel.daily_poa_weighted_kwh_m2, 0))), 4) as stddev_ratio,
    ROUND(AVG((db.daily_poa_weighted_kwh_m2 - excel.daily_poa_weighted_kwh_m2) / NULLIF(excel.daily_poa_weighted_kwh_m2, 0) * 100), 2) as avg_diff_pct
FROM db_data db
INNER JOIN excel_data excel
    ON db.date_key = excel.date_key 
    AND UPPER(TRIM(db.site_name)) = UPPER(TRIM(excel.site_name))
WHERE db.daily_poa_weighted_kwh_m2 IS NOT NULL 
    AND excel.daily_poa_weighted_kwh_m2 IS NOT NULL
    AND ABS(db.daily_poa_weighted_kwh_m2 - excel.daily_poa_weighted_kwh_m2) > 0.01
GROUP BY COALESCE(db.site_name, excel.site_name), DATE_TRUNC('month', COALESCE(db.date_key, excel.date_key))
ORDER BY site_name, month;

